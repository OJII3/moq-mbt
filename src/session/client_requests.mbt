///|
priv enum ClientRequestKind {
  Publishing
  Subscribing
} derive(Eq)

///|
priv struct ClientRequest {
  kind : ClientRequestKind
  track : @protocol.TrackName
  mut track_alias : UInt64?
  mut stream : Int64?
  mut response_buffer : Bytes
  mut accepted : Bool
  mut terminal : Bool
  mut dynamic_groups : Bool
  lifetime : RequestLifetime
  mut observed_object_streams : UInt64
  mut active_object_streams : UInt64
  mut pending_publish_done : @protocol.PublishDoneMessage?
  mut parameters : Array[@protocol.MessageParameter]
  mut pending_local_update : PendingSubscriptionUpdate?
  mut update_failed : Bool
}

///|
priv struct PendingSubscriptionUpdate {
  request_id : UInt64
  effective_parameters : Array[@protocol.MessageParameter]
  filter_updated : Bool
}

///|
priv struct InboundSubscription {
  request_id : UInt64
  publisher_request_id : UInt64
  mut state : SubscriptionState
  parameters : Array[@protocol.MessageParameter]
  lifetime : RequestLifetime
}

///|
priv enum SubscriptionState {
  Pending
  Active
  Ended
} derive(Eq)

///|
fn ClientSession::allocate_request_id(self : ClientSession) -> UInt64 {
  let request_id = self.next_request_id
  self.next_request_id = self.next_request_id + 2UL
  request_id
}

///|
fn ClientSession::subscription_is_active(
  self : ClientSession,
  stream : Int64,
) -> Bool {
  let active = match self.inbound_subscriptions.get(stream) {
    Some(inbound) => inbound.state == Active
    None =>
      match self.request_streams.get(stream) {
        Some(request_id) => {
          let request = self.requests.get(request_id).unwrap()
          request.kind == Publishing && request.accepted && !request.terminal
        }
        None => false
      }
  }
  if !active {
    return false
  }
  match self.published_subscriptions.get(stream) {
    Some(subscription) => subscription_forward(subscription.parameters)
    None => true
  }
}

///|
fn ClientSession::receive_request(
  self : ClientSession,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.request_streams.get(stream) {
    Some(request_id) => self.receive_response(request_id, stream, bytes)
    None => self.receive_peer_request(stream, bytes)
  }
}

///|
fn ClientSession::receive_response(
  self : ClientSession,
  request_id : UInt64,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let request = self.requests.get(request_id).unwrap()
  if request.lifetime.cancelled {
    if request.kind == Subscribing && request.track_alias is None {
      self.remember_cancelled_subscribe_alias(request_id, request, bytes)
    }
    return Ok([])
  }
  if request.lifetime.peer_send_closed {
    return Ok([])
  }
  if request.terminal {
    return Err(UnexpectedMessage)
  }
  if bytes.length() > 65546 - request.response_buffer.length() {
    return Err(BufferLimitExceeded)
  }
  request.response_buffer = request.response_buffer + bytes
  let context = match (request.kind, request.accepted) {
    (Publishing, false) =>
      @protocol.RequestStreamFor(@protocol.RequestOkContext::Publish)
    (Subscribing, true) if request.pending_local_update is Some(_) =>
      @protocol.RequestStreamFor(@protocol.RequestOkContext::RequestUpdate)
    _ => @protocol.RequestStream
  }
  match
    @protocol.decode_control_with_context(request.response_buffer, context) {
    @protocol.Incomplete => Ok([])
    @protocol.Invalid(_) => Err(UnexpectedMessage)
    @protocol.Complete(message, consumed) => {
      let trailing = request.response_buffer[consumed:].to_owned()
      request.response_buffer = b""
      let actions = match (request.kind, message) {
        (Publishing, @protocol.RequestOk(response)) if !request.accepted => {
          request.accepted = true
          request.parameters = response.parameters
          self.published_subscriptions[stream] = {
            opened_stream_count: 0UL,
            open_stream_count: 0UL,
            parameters: response.parameters,
            pending_updates: [],
            pending_failure: None,
            resolved_filter: resolve_subscription_filter(
              response.parameters,
              self.largest_published_locations.get(request_id),
            ),
            request_id,
          }
          [Notify(PublishAccepted(request_id))]
        }
        (Subscribing, @protocol.SubscribeOk(response)) if !request.accepted => {
          let dynamic_groups = match
            track_supports_dynamic_groups(response.properties) {
            Some(value) => value
            None =>
              return Ok(
                self.end_session(
                  Local(close_protocol_violation, b""),
                  Some(close_protocol_violation),
                  b"",
                ),
              )
          }
          if self.aliases.get(response.track_alias) is Some(_) {
            return Err(AliasInUse)
          }
          self.aliases[response.track_alias] = request_id
          request.track_alias = Some(response.track_alias)
          request.dynamic_groups = dynamic_groups
          request.accepted = true
          [Notify(SubscribeEstablished(request_id, response.track_alias))]
        }
        (Publishing, @protocol.RequestError(error)) if !request.accepted => {
          request.terminal = true
          self.request_local_fin_actions(stream) +
          [Notify(PublishRejected(request_id, error))]
        }
        (Subscribing, @protocol.RequestError(error)) if !request.accepted => {
          request.terminal = true
          self.request_local_fin_actions(stream) +
          [Notify(SubscribeRejected(request_id, error))]
        }
        (Publishing, @protocol.RequestUpdate(update)) if request.accepted =>
          match self.receive_peer_subscription_update(stream, update) {
            Ok(values) => values
            Err(error) => return Err(error)
          }
        (Subscribing, @protocol.RequestOk(response)) if request.pending_local_update
          is Some(pending) => {
          request.parameters = pending.effective_parameters
          request.pending_local_update = None
          [
            Notify(
              SubscriptionUpdateAccepted(
                request_id,
                pending.request_id,
                response,
              ),
            ),
          ]
        }
        (Subscribing, @protocol.RequestError(error)) if request.pending_local_update
          is Some(pending) => {
          request.pending_local_update = None
          request.update_failed = true
          [
            Notify(
              SubscriptionUpdateRejected(request_id, pending.request_id, error),
            ),
          ]
        }
        (Subscribing, @protocol.PublishDone(message)) if request.accepted =>
          match self.receive_publish_done(request_id, message) {
            Ok(values) => values
            Err(error) => return Err(error)
          }
        (_, @protocol.GoAwayRequest(goaway)) =>
          self.receive_request_goaway(stream, goaway)
        _ => return Err(UnexpectedMessage)
      }
      if trailing.is_empty() {
        Ok(actions)
      } else {
        match self.receive_response(request_id, stream, trailing) {
          Ok(more) => Ok(actions + more)
          Err(error) => Err(error)
        }
      }
    }
  }
}

///|
fn ClientSession::remember_cancelled_subscribe_alias(
  self : ClientSession,
  request_id : UInt64,
  request : ClientRequest,
  bytes : Bytes,
) -> Unit {
  if bytes.length() > 65546 - request.response_buffer.length() {
    request.response_buffer = b""
    return
  }
  request.response_buffer = request.response_buffer + bytes
  match
    @protocol.decode_control_with_context(
      request.response_buffer,
      @protocol.RequestStream,
    ) {
    @protocol.Incomplete => ()
    @protocol.Invalid(_) => request.response_buffer = b""
    @protocol.Complete(@protocol.SubscribeOk(response), _) => {
      if self.aliases.get(response.track_alias) is None {
        self.aliases[response.track_alias] = request_id
        request.track_alias = Some(response.track_alias)
      }
      request.response_buffer = b""
    }
    @protocol.Complete(_, _) => request.response_buffer = b""
  }
}

///|
fn ClientSession::receive_publish_done(
  self : ClientSession,
  request_id : UInt64,
  message : @protocol.PublishDoneMessage,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let request = self.requests.get(request_id).unwrap()
  if !request.accepted ||
    request.terminal ||
    message.stream_count < request.observed_object_streams ||
    (request.update_failed && message.code != 8UL) ||
    (!request.update_failed && message.code == 8UL) {
    return Err(UnexpectedMessage)
  }
  request.terminal = true
  request.pending_local_update = None
  request.pending_publish_done = Some(message)
  let fin_actions = match request.stream {
    Some(stream) => self.request_local_fin_actions(stream)
    None => []
  }
  match self.take_ready_publish_done(request_id) {
    Some(done) => Ok(fin_actions + [Notify(PublishDone(request_id, done))])
    None => Ok(fin_actions)
  }
}

///|
/// Close the local direction of a completed request stream exactly once.
fn ClientSession::request_local_fin_actions(
  self : ClientSession,
  stream : Int64,
) -> Array[ClientSessionAction] {
  let request_id = match self.request_streams.get(stream) {
    Some(value) => value
    None => return []
  }
  let request = self.requests.get(request_id).unwrap()
  if request.lifetime.local_send_closed {
    return []
  }
  self.note_request_local_fin(stream)
  [Send(stream, b"", true)]
}

///|
/// Decode one variable-length integer from raw Track Properties.
fn read_track_property_vi64(input : Bytes, offset : Int32) -> (UInt64, Int32)? {
  let end = if offset + 9 < input.length() {
    offset + 9
  } else {
    input.length()
  }
  match @protocol.decode_vi64(input[offset:end].to_owned()) {
    @protocol.Complete(value, consumed) => Some((value, offset + consumed))
    _ => None
  }
}

///|
/// Scan Draft 18 Track Properties, ignoring unknown properties safely.
fn track_supports_dynamic_groups(properties : Bytes) -> Bool? {
  let mut offset = 0
  let mut previous = 0UL
  let mut supported = false
  while offset < properties.length() {
    let (delta, after_delta) = match
      read_track_property_vi64(properties, offset) {
      Some(value) => value
      None => return None
    }
    if delta > 0xffffffffffffffffUL - previous {
      return None
    }
    let kind = previous + delta
    offset = after_delta
    if (kind & 1UL) == 0UL {
      let (value, next) = match read_track_property_vi64(properties, offset) {
        Some(value) => value
        None => return None
      }
      if kind == 0x30UL {
        if value > 1UL {
          return None
        }
        supported = value == 1UL
      }
      offset = next
    } else {
      let (length, after_length) = match
        read_track_property_vi64(properties, offset) {
        Some(value) => value
        None => return None
      }
      if length > 65535UL ||
        length > (properties.length() - after_length).to_uint64() {
        return None
      }
      offset = after_length + length.to_int()
    }
    previous = kind
  }
  Some(supported)
}

///|
fn ClientSession::take_ready_publish_done(
  self : ClientSession,
  request_id : UInt64,
) -> @protocol.PublishDoneMessage? {
  let request = self.requests.get(request_id).unwrap()
  match request.pending_publish_done {
    Some(message) if request.observed_object_streams == message.stream_count &&
      request.active_object_streams == 0UL => {
      request.pending_publish_done = None
      match request.track_alias {
        Some(track_alias_value) if self.aliases.get(track_alias_value) ==
          Some(request_id) => self.aliases.remove(track_alias_value)
        _ => ()
      }
      request.track_alias = None
      Some(message)
    }
    _ => None
  }
}

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
          (Some(inbound), @protocol.GoAwayRequest(goaway)) => {
            for action in self.receive_request_goaway(stream, goaway) {
              actions.push(action)
            }
            if self.state == Closed {
              return Ok(actions)
            }
            ignore(inbound)
          }
          (Some(_), @protocol.RequestUpdate(update)) =>
            match self.receive_peer_subscription_update(stream, update) {
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
    if request.track == track && request.accepted && !request.terminal {
      return Some(request_id)
    }
  }
  None
}

///|
fn ClientSession::note_request_local_fin(
  self : ClientSession,
  stream : Int64,
) -> Unit {
  match self.inbound_subscriptions.get(stream) {
    Some(inbound) => ignore(inbound.lifetime.transition(LocalFin, true))
    None =>
      match self.request_streams.get(stream) {
        Some(request_id) =>
          ignore(
            self.requests.get(request_id).unwrap().lifetime.transition(
              LocalFin,
              true,
            ),
          )
        None => ()
      }
  }
}

///|
/// Start a local PUBLISH request. IDs are shared with SUBSCRIBE and increase by
/// two, starting at the client-reserved even ID 0.
pub fn ClientSession::publish_track(
  self : ClientSession,
  track : @protocol.TrackName,
  track_alias : UInt64,
) -> Result[(UInt64, Array[ClientSessionAction]), ClientSessionError] {
  match self.ensure_new_request_allowed() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if !self.ready {
    return Err(HandshakeNotComplete)
  }
  for request_id in self.published_requests {
    let request = self.requests.get(request_id).unwrap()
    if !request.terminal &&
      (request.track == track || request.track_alias == Some(track_alias)) {
      return Err(AliasInUse)
    }
  }
  let request_id = self.allocate_request_id()
  self.requests[request_id] = {
    kind: Publishing,
    track,
    track_alias: Some(track_alias),
    stream: None,
    response_buffer: b"",
    accepted: false,
    terminal: false,
    dynamic_groups: false,
    lifetime: RequestLifetime::new(),
    observed_object_streams: 0UL,
    active_object_streams: 0UL,
    pending_publish_done: None,
    parameters: [],
    pending_local_update: None,
    update_failed: false,
  }
  self.published_requests.push(request_id)
  Ok(
    (
      request_id,
      self.finish_actions([OpenStream(Bidirectional, Request(request_id))]),
    ),
  )
}

///|
/// Start a local unfiltered SUBSCRIBE request.
pub fn ClientSession::subscribe_track(
  self : ClientSession,
  track : @protocol.TrackName,
) -> Result[(UInt64, Array[ClientSessionAction]), ClientSessionError] {
  match self.ensure_new_request_allowed() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if !self.ready {
    return Err(HandshakeNotComplete)
  }
  let request_id = self.allocate_request_id()
  self.requests[request_id] = {
    kind: Subscribing,
    track,
    track_alias: None,
    stream: None,
    response_buffer: b"",
    accepted: false,
    terminal: false,
    dynamic_groups: false,
    lifetime: RequestLifetime::new(),
    observed_object_streams: 0UL,
    active_object_streams: 0UL,
    pending_publish_done: None,
    parameters: [],
    pending_local_update: None,
    update_failed: false,
  }
  Ok(
    (
      request_id,
      self.finish_actions([OpenStream(Bidirectional, Request(request_id))]),
    ),
  )
}

///|
/// Finish sending updates on a local SUBSCRIBE request stream.
/// The response direction remains open for SUBSCRIBE_OK, Objects, and
/// PUBLISH_DONE. Calling this more than once has no effect.
pub fn ClientSession::finish_subscription_updates(
  self : ClientSession,
  request_id : UInt64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let request = match self.requests.get(request_id) {
    Some(value) => value
    None => return Err(UnknownRequest)
  }
  if request.kind != Subscribing {
    return Err(SubscriptionNotAccepted)
  }
  if request.lifetime.local_send_closed {
    return Ok([])
  }
  if request.terminal {
    return Err(SubscriptionNotAccepted)
  }
  let stream = match request.stream {
    Some(value) => value
    None => return Err(SubscriptionNotAccepted)
  }
  Ok(self.finish_actions(self.request_local_fin_actions(stream)))
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
/// Cancel a local PUBLISH/SUBSCRIBE or a peer SUBSCRIBE by Request ID.
/// Cancellation uses the request stream's transport directions, not a control
/// message. Repeating cancellation after termination is a no-op.
pub fn ClientSession::cancel_request(
  self : ClientSession,
  request_id : UInt64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  match self.requests.get(request_id) {
    Some(request) => {
      let publisher_pending_publish_done = match request.stream {
        Some(stream) => self.has_pending_publish_done(stream)
        None => false
      }
      let pending_publish_done = request.pending_publish_done is Some(_) ||
        publisher_pending_publish_done
      if request.lifetime.cancelled ||
        (request.terminal && !pending_publish_done) {
        return Ok([])
      }
      match request.stream {
        Some(stream) =>
          Ok(self.finish_actions(self.end_request_stream(stream, Cancel(1UL))))
        None => {
          request.terminal = true
          ignore(request.lifetime.transition(Cancel(1UL), false))
          request.response_buffer = b""
          request.pending_local_update = None
          Ok(self.finish_actions([]))
        }
      }
    }
    None =>
      match self.peer_request_ids.get(request_id) {
        Some(stream) =>
          Ok(self.finish_actions(self.end_request_stream(stream, Cancel(1UL))))
        None => Err(UnknownRequest)
      }
  }
}

///|
fn ClientSession::end_request_stream(
  self : ClientSession,
  stream : Int64,
  event : RequestLifetimeEvent,
) -> Array[ClientSessionAction] {
  self.request_goaway_deadlines.remove(stream)
  match self.inbound_subscriptions.get(stream) {
    Some(_) => return self.end_inbound_subscription(stream, event)
    None => ()
  }
  let request_id = match self.request_streams.get(stream) {
    Some(value) => value
    None => return []
  }
  let request = self.requests.get(request_id).unwrap()
  let publisher_pending_publish_done = self.has_pending_publish_done(stream)
  let pending_publish_done = request.pending_publish_done is Some(_) ||
    publisher_pending_publish_done
  if request.terminal && !pending_publish_done && !request.lifetime.cancelled {
    return []
  }
  let effects = request.lifetime.transition(event, true)
  if !effects.abort_send && !effects.stop_receiving && !effects.notify_end {
    return []
  }
  let error_code = request.lifetime.cancel_error_code.unwrap()
  request.terminal = true
  if request.kind != Subscribing ||
    request.accepted ||
    request.lifetime.peer_send_closed {
    request.response_buffer = b""
  }
  request.pending_local_update = None
  request.pending_publish_done = None

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
  if request.kind == Subscribing {
    for action in self.cancel_incoming_object_streams(request_id, error_code) {
      actions.push(action)
    }
  }
  if request.kind == Publishing {
    let child_streams : Array[Int64] = []
    for (child_stream, inbound) in self.inbound_subscriptions {
      if inbound.publisher_request_id == request_id &&
        (inbound.state != Ended || self.has_pending_publish_done(child_stream)) {
        child_streams.push(child_stream)
      }
    }
    for child_stream in child_streams {
      for
        action in self.end_inbound_subscription(
          child_stream,
          Cancel(error_code),
        ) {
        actions.push(action)
      }
    }
  }
  if effects.notify_end {
    actions.push(Notify(SubscriptionEnded(request_id, stream)))
  }
  actions
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

///|
fn ClientSession::cancel_outgoing_object_streams(
  self : ClientSession,
  subscription_stream : Int64,
  error_code : UInt64,
) -> Array[ClientSessionAction] {
  match self.published_subscriptions.get(subscription_stream) {
    Some(subscription) => {
      subscription.pending_updates = []
      subscription.pending_failure = None
    }
    None => ()
  }
  for (_, pending) in self.pending_object_streams {
    if pending.subscription_stream == subscription_stream {
      pending.cancelled_error_code = Some(error_code)
    }
  }
  let object_streams : Array[Int64] = []
  for (object_stream, outgoing) in self.outgoing_object_streams {
    if outgoing.subscription_stream == subscription_stream {
      object_streams.push(object_stream)
    }
  }
  let actions : Array[ClientSessionAction] = []
  for object_stream in object_streams {
    match self.outgoing_object_streams.get(object_stream) {
      Some(outgoing) if !outgoing.abort_sent => {
        outgoing.abort_sent = true
        actions.push(AbortSend(object_stream, error_code))
      }
      _ => ()
    }
    self.close_outgoing_object_stream(object_stream)
  }
  self.published_subscriptions.remove(subscription_stream)
  actions
}

///|
fn ClientSession::has_pending_publish_done(
  self : ClientSession,
  subscription_stream : Int64,
) -> Bool {
  match self.published_subscriptions.get(subscription_stream) {
    Some(subscription) => subscription.pending_failure is Some(_)
    None => false
  }
}

///|
fn ClientSession::cancel_incoming_object_streams(
  self : ClientSession,
  request_id : UInt64,
  error_code : UInt64,
) -> Array[ClientSessionAction] {
  let object_streams : Array[Int64] = []
  for (object_stream, state) in self.incoming_object_streams {
    if state.request_id == Some(request_id) {
      object_streams.push(object_stream)
    }
  }
  let actions : Array[ClientSessionAction] = []
  for object_stream in object_streams {
    self.incoming_object_streams.remove(object_stream)
    self.ignored_object_streams[object_stream] = {
      request_id,
      counted_active: true,
    }
    actions.push(StopReceiving(object_stream, error_code))
  }
  actions
}
