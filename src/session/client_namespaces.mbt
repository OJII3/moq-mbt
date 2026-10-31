///|
priv enum NamespaceRequestState {
  NamespacePending
  NamespaceAccepted
  NamespaceTerminal
} derive(Eq)

///|
priv struct NamespaceRequest {
  track_namespace : @protocol.TrackNamespace
  mut parameters : Array[@protocol.MessageParameter]
  locally_initiated : Bool
  mut announced : Bool
  mut stream : Int64?
  mut response_buffer : Bytes
  mut state : NamespaceRequestState
  mut ignore_late_bytes : Bool
  lifetime : RequestLifetime
  mut pending_updates : Array[(UInt64, Array[@protocol.MessageParameter])]
  mut pending_local_update_id : UInt64?
  mut pending_local_update_parameters : Array[@protocol.MessageParameter]
}

///|
fn ClientSession::receive_peer_namespace_publication(
  self : ClientSession,
  stream : Int64,
  message : @protocol.NamespaceRequestMessage,
) -> Result[Array[SessionAction], ClientSessionError] {
  if self.namespace_request_streams.get(stream) is Some(_) {
    return Err(UnexpectedMessage)
  }
  if (message.request_id & 1UL) != 1UL {
    return Err(InvalidRequestId)
  }
  if self.peer_request_ids.get(message.request_id) is Some(_) {
    return Err(DuplicateRequestId)
  }
  self.peer_request_ids[message.request_id] = stream
  self.note_peer_request_id(message.request_id)
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
    return Ok([SendFin(stream, error)])
  }
  self.namespace_requests[message.request_id] = {
    track_namespace: message.track_namespace,
    parameters: message.parameters,
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
  self.namespace_request_streams[stream] = message.request_id
  Ok([
    EmitNotice(
      NamespacePublicationRequested(
        RequestId(message.request_id),
        StreamId(stream),
        message.track_namespace,
        message.parameters,
      ),
    ),
  ])
}

///|
fn ClientSession::ignore_late_namespace_peer_request(
  self : ClientSession,
  stream : Int64,
) -> Bool {
  match self.namespace_request_streams.get(stream) {
    Some(request_id) => {
      let request = self.namespace_requests.get(request_id).unwrap()
      if request.lifetime.cancelled ||
        request.lifetime.peer_send_closed ||
        request.state == NamespaceTerminal {
        return true
      }
    }
    None => ()
  }
  match self.inbound_namespace_subscriptions.get(stream) {
    Some(inbound) if inbound.lifetime.cancelled ||
      inbound.lifetime.peer_send_closed ||
      inbound.state == Ended => true
    _ => false
  }
}

///|
fn ClientSession::namespace_stream_exists(
  self : ClientSession,
  stream : Int64,
) -> Bool {
  self.namespace_request_streams.get(stream) is Some(_) ||
  self.namespace_subscription_streams.get(stream) is Some(_)
}

///|
fn ClientSession::open_namespace_stream(
  self : ClientSession,
  request_id : UInt64,
  stream : Int64,
) -> Result[Array[SessionAction], ClientSessionError]? {
  match self.namespace_subscriptions.get(request_id) {
    Some(request) if request.stream is None => {
      request.stream = Some(stream)
      self.namespace_subscription_streams[stream] = request_id
      self.request_streams[stream] = request_id
      if request.lifetime.cancelled {
        return Some(Ok([AbortSend(stream, 1UL), StopReceiving(stream, 1UL)]))
      }
      let bytes = match
        @protocol.encode_subscribe_namespace({
          request_id,
          track_namespace: request.prefix,
          parameters: [],
        }) {
        Ok(value) => value
        Err(error) => return Some(Err(Encoding(error)))
      }
      Some(Ok([SendChunk(stream, bytes)]))
    }
    Some(_) => Some(Err(DuplicateStream))
    None =>
      match self.namespace_requests.get(request_id) {
        Some(request) if request.stream is None => {
          request.stream = Some(stream)
          self.namespace_request_streams[stream] = request_id
          if request.locally_initiated {
            self.request_streams[stream] = request_id
          }
          if request.state == NamespaceTerminal {
            if request.lifetime.cancelled {
              let error_code = request.lifetime.cancel_error_code.unwrap()
              return Some(
                Ok(self.end_namespace_request(stream, Cancel(error_code))),
              )
            }
            return Some(Ok([]))
          }
          let bytes = match
            @protocol.encode_publish_namespace({
              request_id,
              track_namespace: request.track_namespace,
              parameters: request.parameters,
            }) {
            Ok(value) => value
            Err(error) => return Some(Err(Encoding(error)))
          }
          request.announced = true
          Some(Ok([SendChunk(stream, bytes)]))
        }
        Some(_) => Some(Err(DuplicateStream))
        None => None
      }
  }
}

///|
fn ClientSession::namespace_send_complete(
  self : ClientSession,
  stream : Int64,
  completion : SendCompletion,
) -> Array[SessionAction]? {
  match self.namespace_request_streams.get(stream) {
    Some(request_id) =>
      match self.namespace_requests.get(request_id) {
        Some(request) if request.lifetime.cancelled => Some([])
        _ => Some([EmitNotice(StreamSendFinished(stream, completion))])
      }
    None =>
      match self.namespace_subscription_streams.get(stream) {
        Some(request_id) =>
          match self.namespace_subscriptions.get(request_id) {
            Some(request) if request.lifetime.cancelled => Some([])
            _ => Some([EmitNotice(StreamSendFinished(stream, completion))])
          }
        None => None
      }
  }
}

///|
/// Advertise a local Track Namespace on a new bidirectional request stream.
pub fn ClientSession::publish_namespace(
  self : ClientSession,
  track_namespace : @protocol.TrackNamespace,
  parameters : Array[@protocol.MessageParameter],
) -> Result[(RequestId, Array[SessionAction]), ClientSessionError] {
  match self.ensure_new_request_allowed() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if !self.ready {
    return Err(HandshakeNotComplete)
  }
  let request_id = self.allocate_request_id()
  self.namespace_requests[request_id] = {
    track_namespace,
    parameters,
    locally_initiated: true,
    announced: false,
    stream: None,
    response_buffer: b"",
    state: NamespacePending,
    ignore_late_bytes: false,
    lifetime: RequestLifetime::new(),
    pending_updates: [],
    pending_local_update_id: None,
    pending_local_update_parameters: [],
  }
  Ok(
    (
      RequestId(request_id),
      self.finish_actions([
        OpenStream(Bidirectional, Request(RequestId(request_id))),
      ]),
    ),
  )
}

///|
/// Update parameters on an accepted local PUBLISH_NAMESPACE request.
pub fn ClientSession::update_published_namespace(
  self : ClientSession,
  request_id : RequestId,
  parameters : Array[@protocol.MessageParameter],
) -> Result[(RequestId, Array[SessionAction]), ClientSessionError] {
  let request_id = request_id.to_uint64()
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let request = match self.namespace_requests.get(request_id) {
    Some(value) if value.locally_initiated && value.state == NamespaceAccepted =>
      value
    Some(_) => return Err(UnexpectedMessage)
    None => return Err(UnknownRequest)
  }
  if request.pending_local_update_id is Some(_) {
    return Err(RequestUpdatePending)
  }
  for parameter in parameters {
    if parameter.kind != 0x03UL {
      return Err(UnexpectedMessage)
    }
  }
  let stream = match request.stream {
    Some(value) => value
    None => return Err(UnknownRequest)
  }
  let update_id = self.allocate_request_id()
  let bytes = match
    @protocol.encode_request_update_for(
      { request_id: update_id, parameters, },
      @protocol.RequestStreamFor(@protocol.RequestOkContext::Namespace),
    ) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  request.pending_local_update_id = Some(update_id)
  request.pending_local_update_parameters = parameters
  Ok((RequestId(update_id), self.finish_actions([SendChunk(stream, bytes)])))
}

///|
/// Accept a peer PUBLISH_NAMESPACE after application authorization.
pub fn ClientSession::accept_namespace_publication(
  self : ClientSession,
  stream : StreamId,
) -> Result[Array[SessionAction], ClientSessionError] {
  let stream = stream.to_int64()
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let request_id = match self.namespace_request_streams.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  let request = self.namespace_requests.get(request_id).unwrap()
  if request.locally_initiated || request.state != NamespacePending {
    return Err(UnexpectedMessage)
  }
  let bytes = match
    @protocol.encode_request_ok_for(
      { parameters: [], properties: b"", },
      @protocol.Namespace,
    ) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  request.state = NamespaceAccepted
  Ok(
    self.finish_actions([
      SendChunk(stream, bytes),
      EmitNotice(
        NamespacePublicationAccepted(RequestId(request_id), StreamId(stream)),
      ),
    ]),
  )
}

///|
/// Reject a peer PUBLISH_NAMESPACE with REQUEST_ERROR and FIN.
pub fn ClientSession::reject_namespace_publication(
  self : ClientSession,
  stream : StreamId,
  error : @protocol.RequestErrorMessage,
) -> Result[Array[SessionAction], ClientSessionError] {
  let stream = stream.to_int64()
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let request_id = match self.namespace_request_streams.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  let request = self.namespace_requests.get(request_id).unwrap()
  if request.locally_initiated || request.state != NamespacePending {
    return Err(UnexpectedMessage)
  }
  let bytes = match @protocol.encode_request_error(error) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  request.state = NamespaceTerminal
  request.ignore_late_bytes = true
  self.note_request_local_fin(stream)
  Ok(
    self.finish_actions([
      SendFin(stream, bytes),
      EmitNotice(
        NamespacePublicationRejected(
          RequestId(request_id),
          StreamId(stream),
          error,
        ),
      ),
    ]),
  )
}

///|
/// Accept the next peer REQUEST_UPDATE for a namespace publication.
pub fn ClientSession::accept_namespace_update(
  self : ClientSession,
  stream : StreamId,
  update_id : RequestId,
  response : @protocol.RequestOkMessage,
) -> Result[Array[SessionAction], ClientSessionError] {
  let stream = stream.to_int64()
  let update_id = update_id.to_uint64()
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let request_id = match self.namespace_request_streams.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  let request = self.namespace_requests.get(request_id).unwrap()
  if request.locally_initiated ||
    request.state != NamespaceAccepted ||
    request.pending_updates.is_empty() ||
    request.pending_updates[0].0 != update_id {
    return Err(UnexpectedMessage)
  }
  let update_parameters = request.pending_updates[0].1
  for parameter in update_parameters {
    if parameter.kind != 0x03UL {
      return Err(UnexpectedMessage)
    }
  }
  let bytes = match
    @protocol.encode_request_ok_for(response, @protocol.RequestUpdate) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  ignore(request.pending_updates.remove(0))
  request.parameters = merge_namespace_parameters(
    request.parameters,
    update_parameters,
  )
  Ok(
    self.finish_actions([
      SendChunk(stream, bytes),
      EmitNotice(
        NamespaceUpdateAccepted(
          RequestId(request_id),
          RequestId(update_id),
          response,
        ),
      ),
    ]),
  )
}

///|
/// Reject the next peer REQUEST_UPDATE and end the namespace request.
pub fn ClientSession::reject_namespace_update(
  self : ClientSession,
  stream : StreamId,
  update_id : RequestId,
  error : @protocol.RequestErrorMessage,
) -> Result[Array[SessionAction], ClientSessionError] {
  let stream = stream.to_int64()
  let update_id = update_id.to_uint64()
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let request_id = match self.namespace_request_streams.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  let request = self.namespace_requests.get(request_id).unwrap()
  if request.locally_initiated ||
    request.state != NamespaceAccepted ||
    request.pending_updates.is_empty() ||
    request.pending_updates[0].0 != update_id {
    return Err(UnexpectedMessage)
  }
  let bytes = match @protocol.encode_request_error(error) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  let rejected_updates : Array[UInt64] = []
  for pending in request.pending_updates {
    rejected_updates.push(pending.0)
  }
  request.pending_updates = []
  request.state = NamespaceTerminal
  request.ignore_late_bytes = true
  self.note_request_local_fin(stream)
  let actions = self.end_namespace_request(stream, Cancel(1UL))
  let responses : Array[SessionAction] = [SendFin(stream, bytes)]
  for rejected_id in rejected_updates {
    responses.push(
      EmitNotice(
        NamespaceUpdateRejected(
          RequestId(request_id),
          RequestId(rejected_id),
          error,
        ),
      ),
    )
  }
  Ok(self.finish_actions(responses + actions))
}

///|
fn ClientSession::receive_namespace_response(
  self : ClientSession,
  request_id : UInt64,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[SessionAction], ClientSessionError] {
  let request = self.namespace_requests.get(request_id).unwrap()
  if request.lifetime.cancelled || request.lifetime.peer_send_closed {
    return Ok([])
  }
  if request.state == NamespaceTerminal {
    if request.ignore_late_bytes {
      return Ok([])
    }
    return Err(UnexpectedMessage)
  }
  if bytes.length() > 65546 - request.response_buffer.length() {
    return Err(BufferLimitExceeded)
  }
  request.response_buffer = request.response_buffer + bytes
  let context = if request.state == NamespacePending {
    @protocol.RequestStreamFor(@protocol.RequestOkContext::Namespace)
  } else if request.pending_local_update_id is Some(_) {
    @protocol.RequestStreamFor(@protocol.RequestOkContext::RequestUpdate)
  } else {
    @protocol.RequestStream
  }
  match
    @protocol.decode_control_with_context(request.response_buffer, context) {
    @protocol.Incomplete => Ok([])
    @protocol.Invalid(_) => Err(UnexpectedMessage)
    @protocol.Complete(message, consumed) => {
      let trailing = request.response_buffer[consumed:].to_owned()
      request.response_buffer = b""
      let actions = match (request.state, message) {
        (NamespacePending, @protocol.RequestOk(_)) => {
          request.state = NamespaceAccepted
          let actions : Array[SessionAction] = [
            EmitNotice(
              NamespacePublicationAccepted(
                RequestId(request_id),
                StreamId(stream),
              ),
            ),
          ]
          match self.broadcast_namespace_available(request.track_namespace) {
            Ok(extra) =>
              for action in extra {
                actions.push(action)
              }
            Err(error) => return Err(error)
          }
          actions
        }
        (NamespacePending, @protocol.RequestError(error)) => {
          request.state = NamespaceTerminal
          request.pending_updates = []
          self.request_local_fin_actions(stream) +
          [
            EmitNotice(
              NamespacePublicationRejected(
                RequestId(request_id),
                StreamId(stream),
                error,
              ),
            ),
          ]
        }
        (NamespaceAccepted, @protocol.RequestOk(response)) if request.pending_local_update_id
          is Some(update_id) => {
          request.pending_local_update_id = None
          request.parameters = merge_namespace_parameters(
            request.parameters,
            request.pending_local_update_parameters,
          )
          request.pending_local_update_parameters = []
          [
            EmitNotice(
              NamespaceUpdateAccepted(
                RequestId(request_id),
                RequestId(update_id),
                response,
              ),
            ),
          ]
        }
        (NamespaceAccepted, @protocol.RequestError(error)) if request.pending_local_update_id
          is Some(update_id) => {
          request.pending_local_update_id = None
          request.state = NamespaceTerminal
          request.ignore_late_bytes = true
          self.end_request_stream(stream, Cancel(1UL)) +
          [
            EmitNotice(
              NamespaceUpdateRejected(
                RequestId(request_id),
                RequestId(update_id),
                error,
              ),
            ),
          ]
        }
        (NamespaceAccepted, @protocol.GoAwayRequest(goaway)) =>
          self.receive_request_goaway(stream, goaway)
        _ => return Err(UnexpectedMessage)
      }
      if trailing.is_empty() {
        Ok(actions)
      } else {
        match self.receive_namespace_response(request_id, stream, trailing) {
          Ok(more) => Ok(actions + more)
          Err(error) => Err(error)
        }
      }
    }
  }
}

///|
fn ClientSession::receive_peer_namespace_update(
  self : ClientSession,
  stream : Int64,
  update : @protocol.RequestUpdateMessage,
) -> Result[Array[SessionAction], ClientSessionError] {
  let request_id = self.namespace_request_streams.get(stream).unwrap()
  let request = self.namespace_requests.get(request_id).unwrap()
  if (update.request_id & 1UL) != 1UL {
    return Err(InvalidRequestId)
  }
  if self.peer_request_ids.get(update.request_id) is Some(_) {
    return Err(DuplicateRequestId)
  }
  if request.locally_initiated {
    return Err(UnexpectedMessage)
  }
  self.peer_request_ids[update.request_id] = stream
  self.note_peer_request_id(update.request_id)
  if self.control_goaway_sent {
    let error : @protocol.RequestErrorMessage = {
      code: request_error_going_away,
      retry_interval: 0UL,
      reason: b"",
      redirect: b"",
    }
    let bytes = match @protocol.encode_request_error(error) {
      Ok(value) => value
      Err(value) => return Err(Encoding(value))
    }
    request.pending_updates = []
    request.state = NamespaceTerminal
    ignore(request.lifetime.transition(LocalFin, true))
    let actions = self.end_namespace_request(
      stream,
      Cancel(request_reset_going_away),
    )
    return Ok(
      self.finish_actions(
        [
          SendFin(stream, bytes),
          EmitNotice(
            NamespaceUpdateRejected(
              RequestId(request_id),
              RequestId(update.request_id),
              error,
            ),
          ),
        ] +
        actions,
      ),
    )
  }
  request.pending_updates.push((update.request_id, update.parameters))
  Ok([
    EmitNotice(
      NamespaceUpdateRequested(
        RequestId(request_id),
        RequestId(update.request_id),
        StreamId(stream),
        update.parameters,
      ),
    ),
  ])
}

///|
fn merge_namespace_parameters(
  current : Array[@protocol.MessageParameter],
  updates : Array[@protocol.MessageParameter],
) -> Array[@protocol.MessageParameter] {
  let merged : Array[@protocol.MessageParameter] = []
  for parameter in current {
    if parameter.kind != 0x03UL {
      merged.push(parameter)
    }
  }
  for parameter in updates {
    if parameter.kind == 0x03UL {
      merged.push(parameter)
    }
  }
  merged
}

///|
fn ClientSession::end_namespace_request(
  self : ClientSession,
  stream : Int64,
  event : RequestLifetimeEvent,
) -> Array[SessionAction] {
  let request_id = match self.namespace_request_streams.get(stream) {
    Some(value) => value
    None => return []
  }
  let request = self.namespace_requests.get(request_id).unwrap()
  let was_active = request.locally_initiated &&
    request.state == NamespaceAccepted
  let effects = request.lifetime.transition(event, true)
  if !effects.abort_send && !effects.stop_receiving && !effects.notify_end {
    return []
  }
  request.state = NamespaceTerminal
  request.ignore_late_bytes = true
  request.response_buffer = b""
  request.pending_updates = []
  request.pending_local_update_id = None
  request.pending_local_update_parameters = []
  self.inbound_buffers.remove(stream)
  self.request_goaway_deadlines.remove(stream)
  let error_code = request.lifetime.cancel_error_code.unwrap()
  let actions : Array[SessionAction] = []
  if was_active {
    match self.broadcast_namespace_done(request.track_namespace) {
      Ok(values) =>
        for action in values {
          actions.push(action)
        }
      Err(_) => ()
    }
  }
  if effects.abort_send {
    actions.push(AbortSend(stream, error_code))
  }
  if effects.stop_receiving {
    actions.push(StopReceiving(stream, error_code))
  }
  if effects.notify_end {
    if request.announced {
      let local_cancel = match event {
        Cancel(_) => true
        _ => false
      }
      let publisher_withdrew = request.locally_initiated == local_cancel
      if publisher_withdrew {
        actions.push(
          EmitNotice(
            NamespaceWithdrawn(RequestId(request_id), StreamId(stream)),
          ),
        )
      } else {
        actions.push(
          EmitNotice(
            NamespaceAcceptanceRevoked(RequestId(request_id), StreamId(stream)),
          ),
        )
      }
    }
  }
  actions
}

///|
fn ClientSession::finish_namespace_stream(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[SessionAction], ClientSessionError]? {
  match self.finish_namespace_subscription_stream(stream) {
    Some(result) => return Some(result)
    None => ()
  }
  match self.namespace_request_streams.get(stream) {
    Some(request_id) => {
      let request = self.namespace_requests.get(request_id).unwrap()
      if (request.locally_initiated && request.state == NamespacePending) ||
        !request.response_buffer.is_empty() ||
        (
          request.locally_initiated &&
          request.pending_local_update_id is Some(_)
        ) ||
        !self.inbound_buffers.get(stream).unwrap_or(b"").is_empty() {
        return Some(Err(UnexpectedMessage))
      }
      ignore(request.lifetime.transition(PeerFin, true))
      request.response_buffer = b""
      self.inbound_buffers.remove(stream)
      Some(Ok([]))
    }
    None => None
  }
}

///|
fn ClientSession::reset_namespace_stream(
  self : ClientSession,
  stream : Int64,
  error_code : UInt64,
) -> Result[Array[SessionAction], ClientSessionError]? {
  match self.reset_namespace_subscription_stream(stream, error_code) {
    Some(result) => return Some(result)
    None => ()
  }
  if self.namespace_request_streams.get(stream) is Some(_) {
    return Some(Ok(self.end_namespace_request(stream, PeerReset(error_code))))
  }
  None
}

///|
fn ClientSession::has_active_namespace_work(self : ClientSession) -> Bool {
  for (_, request) in self.namespace_requests {
    if request.state != NamespaceTerminal {
      return true
    }
  }
  for (_, request) in self.namespace_subscriptions {
    if !request.lifetime.cancelled && !request.lifetime.peer_send_closed {
      return true
    }
  }
  for (_, subscription) in self.inbound_namespace_subscriptions {
    if subscription.state != Ended {
      return true
    }
  }
  false
}

///|
fn ClientSession::reject_namespace_requests_for_goaway(
  self : ClientSession,
  request_id_boundary : UInt64,
) -> Array[SessionAction] {
  let actions : Array[SessionAction] = []
  for (request_id, request) in self.namespace_requests {
    if request.locally_initiated &&
      request_id >= request_id_boundary &&
      request.state == NamespacePending {
      request.state = NamespaceTerminal
      request.pending_local_update_id = None
      request.pending_local_update_parameters = []
      request.pending_updates = []
      let effects = request.lifetime.transition(
        Cancel(request_reset_going_away),
        request.stream is Some(_),
      )
      request.lifetime.end_notified = true
      if request.stream is Some(stream) {
        if effects.abort_send {
          actions.push(AbortSend(stream, request_reset_going_away))
        }
        if effects.stop_receiving {
          actions.push(StopReceiving(stream, request_reset_going_away))
        }
        actions.push(
          EmitNotice(
            NamespacePublicationRejected(
              RequestId(request_id),
              StreamId(stream),
              {
                code: request_error_going_away,
                retry_interval: 0UL,
                reason: b"",
                redirect: b"",
              },
            ),
          ),
        )
      }
    }
  }
  actions
}
