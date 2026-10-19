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
}

///|
priv struct InboundSubscription {
  request_id : UInt64
  publisher_request_id : UInt64
  mut state : SubscriptionState
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
  match self.inbound_subscriptions.get(stream) {
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
  match @protocol.decode_control(request.response_buffer) {
    @protocol.Incomplete => Ok([])
    @protocol.Invalid(_) => Err(UnexpectedMessage)
    @protocol.Complete(message, consumed) => {
      let trailing = request.response_buffer[consumed:].to_owned()
      request.response_buffer = b""
      let actions = match (request.kind, message) {
        (Publishing, @protocol.RequestOk(_)) if !request.accepted => {
          request.accepted = true
          self.published_subscriptions[stream] = {
            opened_stream_count: 0UL,
            open_stream_count: 0UL,
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
    message.stream_count < request.observed_object_streams {
    return Err(UnexpectedMessage)
  }
  request.terminal = true
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
  if self.inbound_subscriptions.get(stream) is Some(_) {
    return Err(UnexpectedMessage)
  }
  let buffer = match self.inbound_buffers.get(stream) {
    Some(value) => value
    None => b""
  }
  if bytes.length() > 65546 - buffer.length() {
    return Err(BufferLimitExceeded)
  }
  let combined = buffer + bytes
  match @protocol.decode_control(combined) {
    @protocol.Incomplete => {
      self.inbound_buffers[stream] = combined
      Ok([])
    }
    @protocol.Invalid(_) => Err(UnexpectedMessage)
    @protocol.Complete(message, consumed) =>
      match message {
        @protocol.Subscribe(request) => {
          if (request.request_id & 1UL) != 1UL {
            return Err(InvalidRequestId)
          }
          if self.peer_request_ids.get(request.request_id) is Some(_) {
            return Err(DuplicateRequestId)
          }
          let publisher_request_id = match self.find_publisher(request.track) {
            Some(value) => value
            None => return Err(TrackNotPublished)
          }
          if consumed != combined.length() {
            return Err(UnexpectedMessage)
          }
          self.inbound_buffers.remove(stream)
          self.peer_request_ids[request.request_id] = stream
          self.inbound_subscriptions[stream] = {
            request_id: request.request_id,
            publisher_request_id,
            state: Pending,
          }
          Ok([
            Notify(
              SubscriptionRequested(request.request_id, stream, request.track),
            ),
          ])
        }
        _ => Err(UnexpectedMessage)
      }
  }
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
