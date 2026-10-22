///|
priv struct NamespaceSubscription {
  prefix : @protocol.TrackNamespace
  mut stream : Int64?
  mut response_buffer : Bytes
  mut established : Bool
  mut rejected : Bool
  mut active_namespaces : Array[@protocol.TrackNamespace]
  lifetime : RequestLifetime
}

///|
priv struct InboundNamespaceSubscription {
  request_id : UInt64
  prefix : @protocol.TrackNamespace
  mut active_namespaces : Array[@protocol.TrackNamespace]
  mut state : SubscriptionState
  lifetime : RequestLifetime
}

///|
fn namespace_has_prefix(
  ns : @protocol.TrackNamespace,
  prefix : @protocol.TrackNamespace,
) -> Bool {
  if prefix.fields.length() > ns.fields.length() {
    return false
  }
  for index in 0..<prefix.fields.length() {
    if ns.fields[index] != prefix.fields[index] {
      return false
    }
  }
  true
}

///|
fn namespace_prefixes_overlap(
  left : @protocol.TrackNamespace,
  right : @protocol.TrackNamespace,
) -> Bool {
  namespace_has_prefix(left, right) || namespace_has_prefix(right, left)
}

///|
fn namespace_join(
  prefix : @protocol.TrackNamespace,
  suffix : @protocol.TrackNamespace,
) -> Result[@protocol.TrackNamespace, @protocol.CodecError] {
  @protocol.join_track_namespace(prefix, suffix)
}

///|
/// Request ns discovery on a new bidirectional stream.
pub fn ClientSession::subscribe_namespace(
  self : ClientSession,
  prefix : @protocol.TrackNamespace,
) -> Result[(UInt64, Array[ClientSessionAction]), ClientSessionError] {
  match self.ensure_new_request_allowed() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if !self.ready {
    return Err(HandshakeNotComplete)
  }
  let request_id = self.allocate_request_id()
  self.namespace_subscriptions[request_id] = {
    prefix,
    stream: None,
    response_buffer: b"",
    established: false,
    rejected: false,
    active_namespaces: [],
    lifetime: RequestLifetime::new(),
  }
  Ok(
    (
      request_id,
      self.finish_actions([OpenStream(Bidirectional, Request(request_id))]),
    ),
  )
}

///|
/// Accept a peer ns subscription and provide application-owned names.
/// Names already advertised through PUBLISH_NAMESPACE are included automatically.
pub fn ClientSession::accept_namespace_subscription(
  self : ClientSession,
  stream : Int64,
  application_namespaces : Array[@protocol.TrackNamespace],
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let inbound = match self.inbound_namespace_subscriptions.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  if inbound.state != Pending {
    return Err(UnexpectedMessage)
  }
  for (other_stream, other) in self.inbound_namespace_subscriptions {
    if other_stream != stream &&
      other.state == Active &&
      namespace_prefixes_overlap(other.prefix, inbound.prefix) {
      let error : @protocol.RequestErrorMessage = {
        code: 0x30UL,
        retry_interval: 0UL,
        reason: b"",
        redirect: b"",
      }
      let response = match @protocol.encode_request_error(error) {
        Ok(value) => value
        Err(value) => return Err(Encoding(value))
      }
      inbound.state = Ended
      self.note_request_local_fin(stream)
      return Ok(
        self.finish_actions([
          Send(stream, response, true),
          Notify(
            NamespaceSubscriptionRejected(inbound.request_id, stream, error),
          ),
        ]),
      )
    }
  }
  let bytes = match
    @protocol.encode_request_ok_for(
      { parameters: [], properties: b"", },
      @protocol.Namespace,
    ) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  inbound.state = Active
  for ns in application_namespaces {
    append_unique_namespace(self.application_namespaces, ns)
  }
  let actions : Array[ClientSessionAction] = [
    Send(stream, bytes, false),
    Notify(NamespaceSubscriptionEstablished(inbound.request_id, stream)),
  ]
  let known : Array[@protocol.TrackNamespace] = []
  for (_, publication) in self.namespace_requests {
    if publication.locally_initiated &&
      publication.state == NamespaceAccepted &&
      namespace_has_prefix(publication.track_namespace, inbound.prefix) {
      append_unique_namespace(known, publication.track_namespace)
    }
  }
  for ns in application_namespaces {
    if namespace_has_prefix(ns, inbound.prefix) {
      append_unique_namespace(known, ns)
    }
  }
  for ns in known {
    match self.send_namespace_available(stream, ns) {
      Ok(message_actions) =>
        for action in message_actions {
          actions.push(action)
        }
      Err(error) => return Err(error)
    }
  }
  Ok(self.finish_actions(actions))
}

///|
/// Reject a peer ns subscription.
pub fn ClientSession::reject_namespace_subscription(
  self : ClientSession,
  stream : Int64,
  error : @protocol.RequestErrorMessage,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let inbound = match self.inbound_namespace_subscriptions.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  if inbound.state != Pending {
    return Err(UnexpectedMessage)
  }
  let bytes = match @protocol.encode_request_error(error) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  inbound.state = Ended
  self.note_request_local_fin(stream)
  Ok(
    self.finish_actions([
      Send(stream, bytes, true),
      Notify(NamespaceSubscriptionRejected(inbound.request_id, stream, error)),
    ]),
  )
}

///|
/// Tell active peer subscriptions that an application-owned ns is available.
pub fn ClientSession::publish_application_namespace(
  self : ClientSession,
  ns : @protocol.TrackNamespace,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  append_unique_namespace(self.application_namespaces, ns)
  match self.broadcast_namespace_available(ns) {
    Ok(actions) => Ok(self.finish_actions(actions))
    Err(error) => Err(error)
  }
}

///|
/// Tell active peer subscriptions that an application-owned ns is withdrawn.
pub fn ClientSession::withdraw_application_namespace(
  self : ClientSession,
  ns : @protocol.TrackNamespace,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  remove_namespace(self.application_namespaces, ns)
  match self.broadcast_namespace_done(ns) {
    Ok(actions) => Ok(self.finish_actions(actions))
    Err(error) => Err(error)
  }
}

///|
fn append_unique_namespace(
  namespaces : Array[@protocol.TrackNamespace],
  ns : @protocol.TrackNamespace,
) -> Unit {
  if !namespace_array_contains(namespaces, ns) {
    namespaces.push(ns)
  }
}

///|
fn namespace_array_contains(
  namespaces : Array[@protocol.TrackNamespace],
  ns : @protocol.TrackNamespace,
) -> Bool {
  for item in namespaces {
    if item == ns {
      return true
    }
  }
  false
}

///|
fn namespace_array_index(
  namespaces : Array[@protocol.TrackNamespace],
  ns : @protocol.TrackNamespace,
) -> Int32? {
  for index in 0..<namespaces.length() {
    if namespaces[index] == ns {
      return Some(index)
    }
  }
  None
}

///|
fn remove_namespace(
  namespaces : Array[@protocol.TrackNamespace],
  ns : @protocol.TrackNamespace,
) -> Unit {
  let index = namespace_array_index(namespaces, ns)
  match index {
    Some(value) => ignore(namespaces.remove(value))
    None => ()
  }
}

///|
fn ClientSession::send_namespace_available(
  self : ClientSession,
  stream : Int64,
  ns : @protocol.TrackNamespace,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let request_id = self.inbound_namespace_subscriptions.get(stream).unwrap().request_id
  let inbound = self.inbound_namespace_subscriptions.get(stream).unwrap()
  if inbound.state != Active || !namespace_has_prefix(ns, inbound.prefix) {
    return Ok([])
  }
  if namespace_array_contains(inbound.active_namespaces, ns) {
    return Ok([])
  }
  let suffix : @protocol.TrackNamespace = {
    fields: ns.fields[inbound.prefix.fields.length():].to_owned(),
  }
  let bytes = match @protocol.encode_namespace({ track_namespace: suffix, }) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  inbound.active_namespaces.push(ns)
  Ok([Send(stream, bytes, false), Notify(NamespaceDiscovered(request_id, ns))])
}

///|
fn ClientSession::send_namespace_done(
  self : ClientSession,
  stream : Int64,
  ns : @protocol.TrackNamespace,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let inbound = self.inbound_namespace_subscriptions.get(stream).unwrap()
  let index = match namespace_array_index(inbound.active_namespaces, ns) {
    Some(value) => value
    None => return Ok([])
  }
  let suffix : @protocol.TrackNamespace = {
    fields: ns.fields[inbound.prefix.fields.length():].to_owned(),
  }
  let bytes = match
    @protocol.encode_namespace_done({ track_namespace: suffix, }) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  ignore(inbound.active_namespaces.remove(index))
  Ok([
    Send(stream, bytes, false),
    Notify(NamespaceNoLongerAvailable(inbound.request_id, ns)),
  ])
}

///|
fn ClientSession::broadcast_namespace_available(
  self : ClientSession,
  ns : @protocol.TrackNamespace,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let actions : Array[ClientSessionAction] = []
  for (stream, _) in self.inbound_namespace_subscriptions {
    match self.send_namespace_available(stream, ns) {
      Ok(values) =>
        for action in values {
          actions.push(action)
        }
      Err(error) => return Err(error)
    }
  }
  Ok(actions)
}

///|
fn ClientSession::broadcast_namespace_done(
  self : ClientSession,
  ns : @protocol.TrackNamespace,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let actions : Array[ClientSessionAction] = []
  for (stream, _) in self.inbound_namespace_subscriptions {
    match self.send_namespace_done(stream, ns) {
      Ok(values) =>
        for action in values {
          actions.push(action)
        }
      Err(error) => return Err(error)
    }
  }
  Ok(actions)
}

///|
fn ClientSession::receive_namespace_subscription_response(
  self : ClientSession,
  request_id : UInt64,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let request = self.namespace_subscriptions.get(request_id).unwrap()
  if request.lifetime.cancelled || request.lifetime.peer_send_closed {
    return Ok([])
  }
  if bytes.length() > 65546 - request.response_buffer.length() {
    return Err(BufferLimitExceeded)
  }
  request.response_buffer = request.response_buffer + bytes
  let context = if request.established {
    @protocol.RequestStream
  } else {
    @protocol.RequestStreamFor(@protocol.RequestOkContext::Namespace)
  }
  match
    @protocol.decode_control_with_context(request.response_buffer, context) {
    @protocol.Incomplete => Ok([])
    @protocol.Invalid(_) => Err(UnexpectedMessage)
    @protocol.Complete(message, consumed) => {
      let trailing = request.response_buffer[consumed:].to_owned()
      request.response_buffer = b""
      let actions = match (request.established, message) {
        (false, @protocol.RequestOk(_)) => {
          request.established = true
          [Notify(NamespaceSubscriptionEstablished(request_id, stream))]
        }
        (false, @protocol.RequestError(error)) => {
          request.rejected = true
          request.lifetime.local_send_closed = true
          [
            Notify(NamespaceSubscriptionRejected(request_id, stream, error)),
            Send(stream, b"", true),
          ]
        }
        (true, @protocol.Namespace(suffix)) => {
          let ns = match
            namespace_join(request.prefix, suffix.track_namespace) {
            Ok(value) => value
            Err(_) => return Err(UnexpectedMessage)
          }
          if namespace_array_contains(request.active_namespaces, ns) {
            return Err(UnexpectedMessage)
          }
          request.active_namespaces.push(ns)
          [Notify(NamespaceDiscovered(request_id, ns))]
        }
        (true, @protocol.NamespaceDone(suffix)) => {
          let ns = match
            namespace_join(request.prefix, suffix.track_namespace) {
            Ok(value) => value
            Err(_) => return Err(UnexpectedMessage)
          }
          let index = match
            namespace_array_index(request.active_namespaces, ns) {
            Some(value) => value
            None => return Err(UnexpectedMessage)
          }
          ignore(request.active_namespaces.remove(index))
          [Notify(NamespaceNoLongerAvailable(request_id, ns))]
        }
        (true, @protocol.GoAwayRequest(goaway)) =>
          self.receive_request_goaway(stream, goaway)
        _ => return Err(UnexpectedMessage)
      }
      if trailing.is_empty() {
        Ok(actions)
      } else {
        match
          self.receive_namespace_subscription_response(
            request_id, stream, trailing,
          ) {
          Ok(more) => Ok(actions + more)
          Err(error) => Err(error)
        }
      }
    }
  }
}

///|
fn ClientSession::end_namespace_subscription(
  self : ClientSession,
  stream : Int64,
  event : RequestLifetimeEvent,
) -> Array[ClientSessionAction] {
  let request_id = match self.namespace_subscription_streams.get(stream) {
    Some(value) => value
    None => return []
  }
  let request = self.namespace_subscriptions.get(request_id).unwrap()
  let effects = request.lifetime.transition(event, true)
  let fin_end = match event {
    PeerFin if !request.lifetime.end_notified => {
      request.lifetime.end_notified = true
      true
    }
    _ => false
  }
  if !effects.abort_send &&
    !effects.stop_receiving &&
    !effects.notify_end &&
    !fin_end {
    return []
  }
  request.response_buffer = b""
  let actions : Array[ClientSessionAction] = []
  let code = request.lifetime.cancel_error_code.unwrap_or(0UL)
  if effects.abort_send {
    actions.push(AbortSend(stream, code))
  }
  if effects.stop_receiving {
    actions.push(StopReceiving(stream, code))
  }
  if fin_end && !request.lifetime.local_send_closed {
    request.lifetime.local_send_closed = true
    actions.push(Send(stream, b"", true))
  }
  if effects.notify_end || fin_end {
    for ns in request.active_namespaces {
      actions.push(Notify(NamespaceNoLongerAvailable(request_id, ns)))
    }
    request.active_namespaces = []
    actions.push(Notify(NamespaceSubscriptionEnded(request_id, stream)))
  }
  actions
}

///|
/// Cancel discovery by closing the request side with FIN. SUBSCRIBE_NAMESPACE
/// uses FIN as cancellation, unlike SUBSCRIBE_TRACKS where FIN only ends updates.
fn ClientSession::cancel_namespace_subscription(
  self : ClientSession,
  request_id : UInt64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let request = self.namespace_subscriptions.get(request_id).unwrap()
  if request.lifetime.cancelled {
    return Ok([])
  }
  request.lifetime.cancelled = true
  request.lifetime.local_send_closed = true
  request.lifetime.peer_send_closed = true
  request.lifetime.end_notified = true
  request.response_buffer = b""
  let actions : Array[ClientSessionAction] = []
  match request.stream {
    Some(stream) => {
      actions.push(Send(stream, b"", true))
      actions.push(StopReceiving(stream, 1UL))
      for ns in request.active_namespaces {
        actions.push(Notify(NamespaceNoLongerAvailable(request_id, ns)))
      }
      request.active_namespaces = []
      actions.push(Notify(NamespaceSubscriptionEnded(request_id, stream)))
    }
    None => ()
  }
  Ok(self.finish_actions(actions))
}

///|
fn ClientSession::end_inbound_namespace_subscription(
  self : ClientSession,
  stream : Int64,
  event : RequestLifetimeEvent,
) -> Array[ClientSessionAction] {
  let inbound = match self.inbound_namespace_subscriptions.get(stream) {
    Some(value) => value
    None => return []
  }
  let effects = inbound.lifetime.transition(event, true)
  let fin_end = match event {
    PeerFin if !inbound.lifetime.end_notified => {
      inbound.lifetime.end_notified = true
      true
    }
    _ => false
  }
  if !effects.abort_send &&
    !effects.stop_receiving &&
    !effects.notify_end &&
    !fin_end {
    return []
  }
  inbound.state = Ended
  let actions : Array[ClientSessionAction] = []
  let code = inbound.lifetime.cancel_error_code.unwrap_or(0UL)
  if effects.abort_send {
    actions.push(AbortSend(stream, code))
  }
  if effects.stop_receiving {
    actions.push(StopReceiving(stream, code))
  }
  if fin_end && !inbound.lifetime.local_send_closed {
    inbound.lifetime.local_send_closed = true
    actions.push(Send(stream, b"", true))
  }
  if effects.notify_end || fin_end {
    for ns in inbound.active_namespaces {
      actions.push(Notify(NamespaceNoLongerAvailable(inbound.request_id, ns)))
    }
    inbound.active_namespaces = []
    actions.push(Notify(NamespaceSubscriptionEnded(inbound.request_id, stream)))
  }
  actions
}
