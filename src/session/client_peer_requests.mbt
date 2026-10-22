///|
/// Peer-initiated SUBSCRIBE request handling.

///|
fn ClientSession::receive_peer_request(
  self : ClientSession,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.inbound_subscriptions.get(stream) {
    Some(inbound) if inbound.lifetime.cancelled ||
      inbound.lifetime.peer_send_closed => return Ok([])
    _ => ()
  }
  match self.namespace_request_streams.get(stream) {
    Some(request_id) => {
      let request = self.namespace_requests.get(request_id).unwrap()
      if request.lifetime.cancelled ||
        request.lifetime.peer_send_closed ||
        request.state == NamespaceTerminal {
        return Ok([])
      }
    }
    None => ()
  }
  let buffer = match self.inbound_buffers.get(stream) {
    Some(value) => value
    None => b""
  }
  if bytes.length() > 65546 - buffer.length() {
    return Err(BufferLimitExceeded)
  }
  let mut remaining = buffer + bytes
  let actions : Array[ClientSessionAction] = []
  while !remaining.is_empty() {
    match
      @protocol.decode_control_with_context(remaining, @protocol.RequestStream) {
      @protocol.Incomplete => {
        self.inbound_buffers[stream] = remaining
        return Ok(actions)
      }
      @protocol.Invalid(_) => return Err(UnexpectedMessage)
      @protocol.Complete(message, consumed) => {
        remaining = remaining[consumed:].to_owned()
        match (self.inbound_subscriptions.get(stream), message) {
          (None, @protocol.Subscribe(request)) => {
            if (request.request_id & 1UL) != 1UL {
              return Err(InvalidRequestId)
            }
            if self.peer_request_ids.get(request.request_id) is Some(_) {
              return Err(DuplicateRequestId)
            }
            self.peer_request_ids[request.request_id] = stream
            self.note_peer_request_id(request.request_id)
            if self.control_goaway_sent {
              let error = match
                @protocol.encode_request_error({
                  code: request_error_going_away,
                  retry_interval: 0UL,
                  reason: b"",
                  redirect: b"",
                }) {
                Ok(value) => value
                Err(error) => return Err(Encoding(error))
              }
              actions.push(Send(stream, error, true))
            } else {
              let publisher_request_id = match
                self.find_publisher(request.track) {
                Some(value) => value
                None => return Err(TrackNotPublished)
              }
              self.inbound_subscriptions[stream] = {
                request_id: request.request_id,
                publisher_request_id,
                state: Pending,
                parameters: request.parameters,
                lifetime: RequestLifetime::new(),
              }
              actions.push(
                Notify(
                  SubscriptionRequested(
                    request.request_id,
                    stream,
                    request.track,
                  ),
                ),
              )
            }
          }
          (None, @protocol.PublishNamespace(request)) => {
            if (request.request_id & 1UL) != 1UL {
              return Err(InvalidRequestId)
            }
            if self.peer_request_ids.get(request.request_id) is Some(_) {
              return Err(DuplicateRequestId)
            }
            self.peer_request_ids[request.request_id] = stream
            self.note_peer_request_id(request.request_id)
            if self.control_goaway_sent {
              let error = match
                @protocol.encode_request_error({
                  code: request_error_going_away,
                  retry_interval: 0UL,
                  reason: b"",
                  redirect: b"",
                }) {
                Ok(value) => value
                Err(error) => return Err(Encoding(error))
              }
              actions.push(Send(stream, error, true))
            } else {
              let namespace_request : NamespaceRequest = {
                track_namespace: request.track_namespace,
                parameters: request.parameters,
                locally_initiated: false,
                announced: true,
                stream: Some(stream),
                response_buffer: b"",
                state: NamespacePending,
                ignore_late_bytes: false,
                lifetime: RequestLifetime::new(),
                pending_updates: [],
                pending_local_update_id: None,
                pending_local_update_parameters: [],
              }
              self.namespace_requests[request.request_id] = namespace_request
              self.namespace_request_streams[stream] = request.request_id
              actions.push(
                Notify(
                  NamespacePublicationRequested(
                    request.request_id,
                    stream,
                    request.track_namespace,
                    request.parameters,
                  ),
                ),
              )
            }
          }
          (Some(inbound), @protocol.GoAwayRequest(goaway)) => {
            for action in self.receive_request_goaway(stream, goaway) {
              actions.push(action)
            }
            if self.state == Closed {
              return Ok(actions)
            }
            ignore(inbound)
          }
          (None, @protocol.GoAwayRequest(goaway)) if self.namespace_request_streams.get(
              stream,
            )
            is Some(_) =>
            for action in self.receive_request_goaway(stream, goaway) {
              actions.push(action)
            }
          (Some(_), @protocol.RequestUpdate(update)) =>
            match self.receive_peer_subscription_update(stream, update) {
              Ok(values) =>
                for action in values {
                  actions.push(action)
                }
              Err(error) => return Err(error)
            }
          (None, @protocol.RequestUpdate(update)) if self.namespace_request_streams.get(
              stream,
            )
            is Some(_) =>
            match self.receive_peer_namespace_update(stream, update) {
              Ok(values) =>
                for action in values {
                  actions.push(action)
                }
              Err(error) => return Err(error)
            }
          _ => return Err(UnexpectedMessage)
        }
      }
    }
  }
  self.inbound_buffers.remove(stream)
  Ok(actions)
}

///|
fn ClientSession::find_publisher(
  self : ClientSession,
  track : @protocol.TrackName,
) -> UInt64? {
  for request_id in self.published_requests {
    let request = self.requests.get(request_id).unwrap()
    if request.track == track && request.is_accepted() && !request.is_terminal() {
      return Some(request_id)
    }
  }
  None
}

///|
/// Accept a peer SUBSCRIBE that targets one of this session's published tracks.
pub fn ClientSession::accept_subscription(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let inbound = match self.inbound_subscriptions.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  if inbound.state != Pending {
    return Err(UnexpectedMessage)
  }
  let request = self.requests.get(inbound.publisher_request_id).unwrap()
  let response = match
    @protocol.encode_subscribe_ok({
      track_alias: request.track_alias.unwrap(),
      parameters: match
        self.largest_published_locations.get(inbound.publisher_request_id) {
        Some((group_id, object_id)) =>
          [{ kind: 0x09UL, value: @protocol.Location(group_id, object_id), }]
        None => []
      },
      properties: b"",
    }) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  inbound.state = Active
  self.published_subscriptions[stream] = {
    opened_stream_count: 0UL,
    open_stream_count: 0UL,
    parameters: inbound.parameters,
    pending_updates: [],
    pending_failure: None,
    resolved_filter: resolve_subscription_filter(
      inbound.parameters,
      self.largest_published_locations.get(inbound.publisher_request_id),
    ),
    request_id: inbound.request_id,
  }
  Ok(
    self.finish_actions([
      Send(stream, response, false),
      Notify(
        SubscriptionAccepted(
          inbound.request_id,
          stream,
          request.track_alias.unwrap(),
        ),
      ),
    ]),
  )
}

///|
/// Reject a peer SUBSCRIBE with REQUEST_ERROR.
pub fn ClientSession::reject_subscription(
  self : ClientSession,
  stream : Int64,
  error : @protocol.RequestErrorMessage,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let inbound = match self.inbound_subscriptions.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  if inbound.state != Pending {
    return Err(UnexpectedMessage)
  }
  let response = match @protocol.encode_request_error(error) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  inbound.state = Ended
  self.note_request_local_fin(stream)
  Ok(
    self.finish_actions([
      Send(stream, response, true),
      Notify(SubscriptionRejected(inbound.request_id, stream)),
    ]),
  )
}

///|
fn ClientSession::end_inbound_subscription(
  self : ClientSession,
  stream : Int64,
  event : RequestLifetimeEvent,
) -> Array[ClientSessionAction] {
  self.request_goaway_deadlines.remove(stream)
  let inbound = match self.inbound_subscriptions.get(stream) {
    Some(value) => value
    None => return []
  }
  if inbound.state == Ended &&
    !self.has_pending_publish_done(stream) &&
    !inbound.lifetime.cancelled {
    return []
  }
  let effects = inbound.lifetime.transition(event, true)
  if !effects.abort_send && !effects.stop_receiving && !effects.notify_end {
    return []
  }
  let error_code = inbound.lifetime.cancel_error_code.unwrap()
  inbound.state = Ended
  self.inbound_buffers.remove(stream)
  let actions : Array[ClientSessionAction] = []
  if effects.abort_send {
    actions.push(AbortSend(stream, error_code))
  }
  if effects.stop_receiving {
    actions.push(StopReceiving(stream, error_code))
  }
  for action in self.cancel_outgoing_object_streams(stream, error_code) {
    actions.push(action)
  }
  if effects.notify_end {
    actions.push(Notify(SubscriptionEnded(inbound.request_id, stream)))
  }
  actions
}
