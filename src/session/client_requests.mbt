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
  mut publish_done_received : Bool
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
}

///|
priv struct InboundSubscription {
  request_id : UInt64
  publisher_request_id : UInt64
  mut state : SubscriptionState
  parameters : Array[@protocol.MessageParameter]
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
            request_id,
          }
          [Notify(PublishAccepted(request_id))]
        }
        (Subscribing, @protocol.SubscribeOk(response)) if !request.accepted => {
          if self.aliases.get(response.track_alias) is Some(_) {
            return Err(AliasInUse)
          }
          self.aliases[response.track_alias] = request_id
          request.track_alias = Some(response.track_alias)
          request.accepted = true
          [Notify(SubscribeEstablished(request_id, response.track_alias))]
        }
        (Publishing, @protocol.RequestError(error)) if !request.accepted => {
          request.terminal = true
          [Notify(PublishRejected(request_id, error))]
        }
        (Subscribing, @protocol.RequestError(error)) if !request.accepted => {
          request.terminal = true
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
  request.publish_done_received = true
  request.pending_publish_done = Some(message)
  match self.take_ready_publish_done(request_id) {
    Some(done) => Ok([Notify(PublishDone(request_id, done))])
    None => Ok([])
  }
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
            let publisher_request_id = match
              self.find_publisher(request.track) {
              Some(value) => value
              None => return Err(TrackNotPublished)
            }
            self.peer_request_ids[request.request_id] = stream
            self.inbound_subscriptions[stream] = {
              request_id: request.request_id,
              publisher_request_id,
              state: Pending,
              parameters: request.parameters,
            }
            actions.push(
              Notify(
                SubscriptionRequested(request.request_id, stream, request.track),
              ),
            )
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
    if request.track == track && request.accepted {
      return Some(request_id)
    }
  }
  None
}

///|
/// Start a local PUBLISH request. IDs are shared with SUBSCRIBE and increase by
/// two, starting at the client-reserved even ID 0.
pub fn ClientSession::publish_track(
  self : ClientSession,
  track : @protocol.TrackName,
  track_alias : UInt64,
) -> Result[(UInt64, Array[ClientSessionAction]), ClientSessionError] {
  if !self.ready {
    return Err(HandshakeNotComplete)
  }
  for request_id in self.published_requests {
    let request = self.requests.get(request_id).unwrap()
    if request.track == track || request.track_alias == Some(track_alias) {
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
    publish_done_received: false,
    observed_object_streams: 0UL,
    active_object_streams: 0UL,
    pending_publish_done: None,
    parameters: [],
    pending_local_update: None,
    update_failed: false,
  }
  self.published_requests.push(request_id)
  Ok((request_id, [OpenStream(Bidirectional, Request(request_id))]))
}

///|
/// Start a local unfiltered SUBSCRIBE request.
pub fn ClientSession::subscribe_track(
  self : ClientSession,
  track : @protocol.TrackName,
) -> Result[(UInt64, Array[ClientSessionAction]), ClientSessionError] {
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
    publish_done_received: false,
    observed_object_streams: 0UL,
    active_object_streams: 0UL,
    pending_publish_done: None,
    parameters: [],
    pending_local_update: None,
    update_failed: false,
  }
  Ok((request_id, [OpenStream(Bidirectional, Request(request_id))]))
}

///|
/// Accept a peer SUBSCRIBE that targets one of this session's published tracks.
pub fn ClientSession::accept_subscription(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
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
      parameters: [],
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
    request_id: inbound.request_id,
  }
  Ok([
    Send(stream, response, false),
    Notify(
      SubscriptionAccepted(
        inbound.request_id,
        stream,
        request.track_alias.unwrap(),
      ),
    ),
  ])
}

///|
/// Reject a peer SUBSCRIBE with REQUEST_ERROR.
pub fn ClientSession::reject_subscription(
  self : ClientSession,
  stream : Int64,
  error : @protocol.RequestErrorMessage,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
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
  Ok([
    Send(stream, response, true),
    Notify(SubscriptionRejected(inbound.request_id, stream)),
  ])
}

///|
fn ClientSession::end_request_stream(
  self : ClientSession,
  stream : Int64,
  error_code : UInt64,
) -> Array[ClientSessionAction] {
  let actions : Array[ClientSessionAction] = []
  match self.published_subscriptions.get(stream) {
    Some(subscription) => {
      subscription.pending_updates = []
      subscription.pending_failure = None
    }
    None => ()
  }
  match self.inbound_subscriptions.get(stream) {
    Some(inbound) if inbound.state != Ended => {
      inbound.state = Ended
      self.inbound_buffers.remove(stream)
      actions.push(AbortSend(stream, error_code))
      for (_, pending) in self.pending_object_streams {
        if pending.subscription_stream == stream {
          pending.cancelled_error_code = Some(error_code)
        }
      }
      let object_streams : Array[Int64] = []
      for (object_stream, outgoing) in self.outgoing_object_streams {
        if outgoing.subscription_stream == stream {
          object_streams.push(object_stream)
        }
      }
      for object_stream in object_streams {
        self.close_outgoing_object_stream(object_stream)
        actions.push(AbortSend(object_stream, error_code))
      }
      actions.push(Notify(SubscriptionEnded(inbound.request_id, stream)))
    }
    _ =>
      match self.request_streams.get(stream) {
        Some(request_id) => {
          let request = self.requests.get(request_id).unwrap()
          if request.kind == Publishing && !request.terminal {
            request.terminal = true
            actions.push(AbortSend(stream, error_code))
            for (_, pending) in self.pending_object_streams {
              if pending.subscription_stream == stream {
                pending.cancelled_error_code = Some(error_code)
              }
            }
            let object_streams : Array[Int64] = []
            for (object_stream, outgoing) in self.outgoing_object_streams {
              if outgoing.subscription_stream == stream {
                object_streams.push(object_stream)
              }
            }
            for object_stream in object_streams {
              self.close_outgoing_object_stream(object_stream)
              actions.push(AbortSend(object_stream, error_code))
            }
            actions.push(Notify(SubscriptionEnded(request_id, stream)))
          } else if request.kind == Subscribing &&
            !request.terminal &&
            !request.publish_done_received {
            request.terminal = true
            request.pending_local_update = None
            actions.push(AbortSend(stream, error_code))
            let object_streams : Array[Int64] = []
            for (object_stream, state) in self.incoming_object_streams {
              if state.request_id == Some(request_id) {
                object_streams.push(object_stream)
              }
            }
            for object_stream in object_streams {
              self.incoming_object_streams.remove(object_stream)
              self.ignored_object_streams[object_stream] = {
                request_id,
                counted_active: true,
              }
              actions.push(StopReceiving(object_stream, error_code))
            }
            actions.push(Notify(SubscriptionEnded(request_id, stream)))
          }
        }
        None => ()
      }
  }
  actions
}
