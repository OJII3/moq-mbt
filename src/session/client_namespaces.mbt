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
/// Advertise a local Track Namespace on a new bidirectional request stream.
pub fn ClientSession::publish_namespace(
  self : ClientSession,
  track_namespace : @protocol.TrackNamespace,
  parameters : Array[@protocol.MessageParameter],
) -> Result[(UInt64, Array[ClientSessionAction]), ClientSessionError] {
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
      request_id,
      self.finish_actions([OpenStream(Bidirectional, Request(request_id))]),
    ),
  )
}

///|
/// Update parameters on an accepted local PUBLISH_NAMESPACE request.
pub fn ClientSession::update_published_namespace(
  self : ClientSession,
  request_id : UInt64,
  parameters : Array[@protocol.MessageParameter],
) -> Result[(UInt64, Array[ClientSessionAction]), ClientSessionError] {
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
  Ok((update_id, self.finish_actions([Send(stream, bytes, false)])))
}

///|
/// Accept a peer PUBLISH_NAMESPACE after application authorization.
pub fn ClientSession::accept_namespace_publication(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
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
      Send(stream, bytes, false),
      Notify(NamespacePublicationAccepted(request_id, stream)),
    ]),
  )
}

///|
/// Reject a peer PUBLISH_NAMESPACE with REQUEST_ERROR and FIN.
pub fn ClientSession::reject_namespace_publication(
  self : ClientSession,
  stream : Int64,
  error : @protocol.RequestErrorMessage,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
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
      Send(stream, bytes, true),
      Notify(NamespacePublicationRejected(request_id, stream, error)),
    ]),
  )
}

///|
/// Accept the next peer REQUEST_UPDATE for a namespace publication.
pub fn ClientSession::accept_namespace_update(
  self : ClientSession,
  stream : Int64,
  update_id : UInt64,
  response : @protocol.RequestOkMessage,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
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
      Send(stream, bytes, false),
      Notify(NamespaceUpdateAccepted(request_id, update_id, response)),
    ]),
  )
}

///|
/// Reject the next peer REQUEST_UPDATE and end the namespace request.
pub fn ClientSession::reject_namespace_update(
  self : ClientSession,
  stream : Int64,
  update_id : UInt64,
  error : @protocol.RequestErrorMessage,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
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
  let actions = self.end_namespace_request(stream, PeerStop(1UL))
  let responses : Array[ClientSessionAction] = [Send(stream, bytes, true)]
  for rejected_id in rejected_updates {
    responses.push(
      Notify(NamespaceUpdateRejected(request_id, rejected_id, error)),
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
) -> Result[Array[ClientSessionAction], ClientSessionError] {
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
          [Notify(NamespacePublicationAccepted(request_id, stream))]
        }
        (NamespacePending, @protocol.RequestError(error)) => {
          request.state = NamespaceTerminal
          request.ignore_late_bytes = true
          self.request_local_fin_actions(stream) +
          [Notify(NamespacePublicationRejected(request_id, stream, error))]
        }
        (NamespaceAccepted, @protocol.RequestOk(response)) if request.pending_local_update_id
          is Some(update_id) => {
          request.pending_local_update_id = None
          request.parameters = merge_namespace_parameters(
            request.parameters,
            request.pending_local_update_parameters,
          )
          request.pending_local_update_parameters = []
          [Notify(NamespaceUpdateAccepted(request_id, update_id, response))]
        }
        (NamespaceAccepted, @protocol.RequestError(error)) if request.pending_local_update_id
          is Some(update_id) => {
          request.pending_local_update_id = None
          request.state = NamespaceTerminal
          request.ignore_late_bytes = true
          self.end_request_stream(stream, Cancel(1UL)) +
          [Notify(NamespaceUpdateRejected(request_id, update_id, error))]
        }
        (_, @protocol.GoAwayRequest(goaway)) =>
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
) -> Result[Array[ClientSessionAction], ClientSessionError] {
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
  request.pending_updates.push((update.request_id, update.parameters))
  Ok([
    Notify(
      NamespaceUpdateRequested(
        request_id,
        update.request_id,
        stream,
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
) -> Array[ClientSessionAction] {
  let request_id = match self.namespace_request_streams.get(stream) {
    Some(value) => value
    None => return []
  }
  let request = self.namespace_requests.get(request_id).unwrap()
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
  let actions : Array[ClientSessionAction] = []
  if effects.abort_send {
    actions.push(AbortSend(stream, error_code))
  }
  if effects.stop_receiving {
    actions.push(StopReceiving(stream, error_code))
  }
  if effects.notify_end {
    if request.announced {
      if request.locally_initiated {
        actions.push(Notify(NamespaceWithdrawn(request_id, stream)))
      } else {
        actions.push(Notify(NamespaceAcceptanceRevoked(request_id, stream)))
      }
    }
  }
  actions
}
