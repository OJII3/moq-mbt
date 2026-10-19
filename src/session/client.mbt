///|
pub(all) enum ClientStreamDirection {
  Unidirectional
  Bidirectional
} derive(Debug, Eq)

///|
pub(all) enum ClientStreamPurpose {
  Control
  Request(UInt64)
  Object(UInt64)
} derive(Debug, Eq)

///|
pub(all) enum ClientSessionEvent {
  ControlData(Int64, Bytes)
  RequestStreamStarted(Int64)
  RequestData(Int64, Bytes)
  SubgroupStreamStarted(Int64)
  SubgroupData(Int64, Bytes)
  StreamOpened(ClientStreamPurpose, Int64)
  StreamFinished(Int64)
  StreamReset(Int64, UInt64)
  SendComplete(Int64, Bool)
} derive(Debug, Eq)

///|
pub(all) enum ClientSessionNotice {
  HandshakeComplete(@protocol.Setup)
  ControlMessageReceived(@protocol.ControlMessage)
  PublishAccepted(UInt64)
  PublishRejected(UInt64, @protocol.RequestErrorMessage)
  SubscribeEstablished(UInt64, UInt64)
  SubscribeRejected(UInt64, @protocol.RequestErrorMessage)
  SubscriptionRequested(UInt64, Int64, @protocol.TrackName)
  SubscriptionAccepted(UInt64, Int64, UInt64)
  SubscriptionRejected(UInt64, Int64)
  ObjectReceived(
    UInt64,
    @protocol.SubgroupHeader,
    Array[@protocol.SubgroupFrame]
  )
  ObjectStreamReset(UInt64, Int64)
  ObjectStreamFinished(Int64)
  GroupFinished(UInt64, UInt64)
  PublishDone(UInt64, @protocol.PublishDoneMessage)
  SendFinished(Int64, Bool)
} derive(Debug, Eq)

///|
pub(all) enum ClientSessionAction {
  OpenStream(ClientStreamDirection, ClientStreamPurpose)
  Send(Int64, Bytes, Bool)
  CloseConnection(UInt64, Bytes)
  Notify(ClientSessionNotice)
} derive(Debug, Eq)

///|
pub(all) enum ClientSessionError {
  Handshake(HandshakeError)
  Track(TrackSessionError)
  Encoding(@protocol.CodecError)
  HandshakeNotComplete
  InvalidRequestId
  DuplicateRequestId
  DuplicateStream
  UnknownStream
  UnknownRequest
  TrackNotPublished
  AliasInUse
  SubscriptionNotAccepted
  UnexpectedMessage
  BufferLimitExceeded
} derive(Debug, Eq)

///|
priv enum ClientRequestKind {
  Publishing
  Subscribing
} derive(Eq)

///|
priv struct ClientRequest {
  kind : ClientRequestKind
  track : @protocol.TrackName
  publisher : SingleTrackPublisher?
  subscriber : SingleTrackSubscriber?
  mut stream : Int64?
  mut response_buffer : Bytes
  mut accepted : Bool
  mut terminal : Bool
  mut observed_object_streams : UInt64
  mut active_object_streams : UInt64
  mut pending_publish_done : @protocol.PublishDoneMessage?
}

///|
priv struct InboundSubscription {
  request_id : UInt64
  publisher_request_id : UInt64
  mut accepted : Bool
  mut finished : Bool
}

///|
priv struct PendingObjectStream {
  subscription_stream : Int64
  publisher_request_id : UInt64
  header : @protocol.SubgroupHeader
}

///|
priv struct OutgoingObjectStream {
  publisher_request_id : UInt64
  mut finishing : Bool
  mut pending_send_count : UInt64
}

///|
/// Draft 18 client-side session coordinator. It owns request IDs, stream
/// routing, and track aliases; transport references remain opaque.
pub struct ClientSession {
  priv handshake : ClientHandshake
  priv mut started : Bool
  priv mut ready : Bool
  priv mut outbound_control_stream : Int64?
  priv mut next_request_id : UInt64
  priv mut next_object_token : UInt64
  priv mut peer_control_stream : Int64?
  priv mut control_buffer : Bytes
  priv requests : Map[UInt64, ClientRequest]
  priv request_streams : Map[Int64, UInt64]
  priv inbound_buffers : Map[Int64, Bytes]
  priv inbound_subscriptions : Map[Int64, InboundSubscription]
  priv peer_request_ids : Map[UInt64, Int64]
  priv published_requests : Array[UInt64]
  priv aliases : Map[UInt64, UInt64]
  priv subgroup_buffers : Map[Int64, Bytes]
  priv subgroup_requests : Map[Int64, UInt64]
  priv pending_object_streams : Map[UInt64, PendingObjectStream]
  priv outgoing_object_streams : Map[Int64, OutgoingObjectStream]
}

///|
pub fn ClientSession::new(
  version : Version,
  setup : @protocol.Setup,
) -> Result[ClientSession, @protocol.CodecError] {
  match ClientHandshake::new(version, setup) {
    Err(error) => Err(error)
    Ok(handshake) =>
      Ok({
        handshake,
        started: false,
        ready: false,
        outbound_control_stream: None,
        next_request_id: 0UL,
        next_object_token: 0UL,
        peer_control_stream: None,
        control_buffer: b"",
        requests: {},
        request_streams: {},
        inbound_buffers: {},
        inbound_subscriptions: {},
        peer_request_ids: {},
        published_requests: [],
        aliases: {},
        subgroup_buffers: {},
        subgroup_requests: {},
        pending_object_streams: {},
        outgoing_object_streams: {},
      })
  }
}

///|
/// Ask the adapter to open the local unidirectional control stream.
pub fn ClientSession::start(
  self : ClientSession,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  if self.started {
    return Err(UnexpectedMessage)
  }
  self.started = true
  Ok([OpenStream(Unidirectional, Control)])
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
    if request.track == track ||
      request.publisher.unwrap().track_alias() == track_alias {
      return Err(AliasInUse)
    }
  }
  let request_id = self.allocate_request_id()
  let publisher = SingleTrackPublisher::new_with_request_id(
    track, track_alias, request_id,
  )
  self.requests[request_id] = {
    kind: Publishing,
    track,
    publisher: Some(publisher),
    subscriber: None,
    stream: None,
    response_buffer: b"",
    accepted: false,
    terminal: false,
    observed_object_streams: 0UL,
    active_object_streams: 0UL,
    pending_publish_done: None,
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
  let subscriber = SingleTrackSubscriber::new_with_request_id(track, request_id)
  self.requests[request_id] = {
    kind: Subscribing,
    track,
    publisher: None,
    subscriber: Some(subscriber),
    stream: None,
    response_buffer: b"",
    accepted: false,
    terminal: false,
    observed_object_streams: 0UL,
    active_object_streams: 0UL,
    pending_publish_done: None,
  }
  Ok((request_id, [OpenStream(Bidirectional, Request(request_id))]))
}

///|
pub fn ClientSession::receive(
  self : ClientSession,
  event : ClientSessionEvent,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match event {
    ControlData(stream, bytes) => self.receive_control(stream, bytes)
    RequestStreamStarted(stream) => {
      if self.inbound_buffers.get(stream) is Some(_) ||
        self.inbound_subscriptions.get(stream) is Some(_) ||
        self.request_streams.get(stream) is Some(_) {
        return Err(DuplicateStream)
      }
      self.inbound_buffers[stream] = b""
      Ok([])
    }
    RequestData(stream, bytes) => self.receive_request(stream, bytes)
    SubgroupStreamStarted(stream) => {
      if self.subgroup_buffers.get(stream) is Some(_) ||
        self.subgroup_requests.get(stream) is Some(_) {
        return Err(DuplicateStream)
      }
      self.subgroup_buffers[stream] = b""
      Ok([])
    }
    SubgroupData(stream, bytes) => self.receive_subgroup(stream, bytes)
    StreamOpened(purpose, stream) => self.stream_opened(purpose, stream)
    StreamFinished(stream) => self.stream_finished(stream)
    StreamReset(stream, _code) => self.stream_reset(stream)
    SendComplete(stream, canceled) => self.send_complete(stream, canceled)
  }
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
  if inbound.accepted || inbound.finished {
    return Err(UnexpectedMessage)
  }
  let publisher = self.publisher(inbound.publisher_request_id)
  let response = match publisher.accept_subscribe(stream) {
    Ok(value) => value
    Err(error) => return Err(Track(error))
  }
  inbound.accepted = true
  Ok([
    Send(stream, response, false),
    Notify(
      SubscriptionAccepted(inbound.request_id, stream, publisher.track_alias()),
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
  if inbound.accepted || inbound.finished {
    return Err(UnexpectedMessage)
  }
  let response = match @protocol.encode_request_error(error) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  inbound.finished = true
  Ok([
    Send(stream, response, true),
    Notify(SubscriptionRejected(inbound.request_id, stream)),
  ])
}

///|
/// Ask the adapter to open an object stream for an accepted subscription,
/// whether this endpoint accepted an inbound SUBSCRIBE or sent a PUBLISH.
pub fn ClientSession::open_object_stream(
  self : ClientSession,
  subscription_stream : Int64,
  header : @protocol.SubgroupHeader,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let publisher_request_id = match
    self.inbound_subscriptions.get(subscription_stream) {
    Some(inbound) => {
      if !inbound.accepted || inbound.finished {
        return Err(SubscriptionNotAccepted)
      }
      inbound.publisher_request_id
    }
    None =>
      match self.request_streams.get(subscription_stream) {
        Some(request_id) => {
          let request = self.requests.get(request_id).unwrap()
          if request.kind != Publishing || !request.accepted || request.terminal {
            return Err(SubscriptionNotAccepted)
          }
          request_id
        }
        None => return Err(UnknownStream)
      }
  }
  let publisher = self.publisher(publisher_request_id)
  if header.track_alias != publisher.track_alias() {
    return Err(Track(AliasMismatch))
  }
  let token = self.next_object_token
  self.next_object_token = token + 1UL
  self.pending_object_streams[token] = {
    subscription_stream,
    publisher_request_id,
    header,
  }
  Ok([OpenStream(Unidirectional, Object(token))])
}

///|
/// Encode and send an Object on an open outgoing Subgroup stream.
pub fn ClientSession::send_object(
  self : ClientSession,
  stream : Int64,
  frame : @protocol.SubgroupFrame,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let outgoing = match self.outgoing_object_streams.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  let publisher = self.publisher(outgoing.publisher_request_id)
  if outgoing.finishing {
    return Err(UnexpectedMessage)
  }
  let bytes = match publisher.object_stream(stream, frame) {
    Ok(value) => value
    Err(error) => return Err(Track(error))
  }
  outgoing.pending_send_count = outgoing.pending_send_count + 1UL
  Ok([Send(stream, bytes, false)])
}

///|
/// Finish an outgoing Subgroup stream after all of its Objects have been sent.
pub fn ClientSession::finish_object_stream(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let outgoing = match self.outgoing_object_streams.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  if outgoing.finishing {
    return Err(UnexpectedMessage)
  }
  outgoing.finishing = true
  outgoing.pending_send_count = outgoing.pending_send_count + 1UL
  Ok([Send(stream, b"", true)])
}

///|
/// Send PUBLISH_DONE after all object streams for this subscription finish.
pub fn ClientSession::publish_done(
  self : ClientSession,
  subscription_stream : Int64,
  code : UInt64,
  reason : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let (request_id, publisher_request_id, local_publish) = match
    self.inbound_subscriptions.get(subscription_stream) {
    Some(inbound) => {
      if !inbound.accepted || inbound.finished {
        return Err(SubscriptionNotAccepted)
      }
      (inbound.request_id, inbound.publisher_request_id, false)
    }
    None =>
      match self.request_streams.get(subscription_stream) {
        Some(request_id) => {
          let request = self.requests.get(request_id).unwrap()
          if request.kind != Publishing || !request.accepted || request.terminal {
            return Err(SubscriptionNotAccepted)
          }
          (request_id, request_id, true)
        }
        None => return Err(UnknownStream)
      }
  }
  let publisher = self.publisher(publisher_request_id)
  let bytes = match publisher.publish_done(subscription_stream, code, reason) {
    Ok(value) => value
    Err(error) => return Err(Track(error))
  }
  if local_publish {
    self.requests.get(request_id).unwrap().terminal = true
  } else {
    self.inbound_subscriptions.get(subscription_stream).unwrap().finished = true
  }
  let message = match @protocol.decode_control(bytes) {
    @protocol.Complete(@protocol.PublishDone(value), _) => value
    _ => return Err(UnexpectedMessage)
  }
  Ok([
    Send(subscription_stream, bytes, true),
    Notify(PublishDone(request_id, message)),
  ])
}

///|
/// Request a transport-level connection close.
pub fn ClientSession::close(
  _self : ClientSession,
  code : UInt64,
  reason : Bytes,
) -> Array[ClientSessionAction] {
  [CloseConnection(code, reason)]
}

///|
fn ClientSession::allocate_request_id(self : ClientSession) -> UInt64 {
  let request_id = self.next_request_id
  self.next_request_id = self.next_request_id + 2UL
  request_id
}

///|
fn ClientSession::publisher(
  self : ClientSession,
  request_id : UInt64,
) -> SingleTrackPublisher {
  self.requests.get(request_id).unwrap().publisher.unwrap()
}

///|
fn ClientSession::receive_control(
  self : ClientSession,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  if !self.ready {
    match self.handshake.receive(stream, bytes) {
      Err(error) => Err(Handshake(error))
      Ok(Pending) => Ok([])
      Ok(Ready(setup, trailing)) => {
        self.peer_control_stream = Some(stream)
        self.ready = true
        let actions = [Notify(HandshakeComplete(setup))]
        if trailing.is_empty() {
          Ok(actions)
        } else {
          match self.decode_control_messages(trailing) {
            Ok(more) => Ok(actions + more)
            Err(error) => Err(error)
          }
        }
      }
    }
  } else {
    if self.peer_control_stream != Some(stream) {
      return Err(Handshake(DuplicateControlStream))
    }
    if bytes.length() > 1024 * 1024 - self.control_buffer.length() {
      return Err(BufferLimitExceeded)
    }
    self.control_buffer = self.control_buffer + bytes
    self.decode_control_messages(b"")
  }
}

///|
fn ClientSession::decode_control_messages(
  self : ClientSession,
  first : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  if first.length() > 1024 * 1024 - self.control_buffer.length() {
    return Err(BufferLimitExceeded)
  }
  self.control_buffer = self.control_buffer + first
  let actions : Array[ClientSessionAction] = []
  while !self.control_buffer.is_empty() {
    match @protocol.decode_control(self.control_buffer) {
      @protocol.Incomplete => return Ok(actions)
      @protocol.Invalid(_) => return Err(UnexpectedMessage)
      @protocol.Complete(message, consumed) => {
        actions.push(Notify(ControlMessageReceived(message)))
        self.control_buffer = self.control_buffer[consumed:].to_owned()
      }
    }
  }
  Ok(actions)
}

///|
fn ClientSession::stream_opened(
  self : ClientSession,
  purpose : ClientStreamPurpose,
  stream : Int64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match purpose {
    Control => {
      if !self.started || self.outbound_control_stream is Some(_) {
        return Err(UnexpectedMessage)
      }
      self.outbound_control_stream = Some(stream)
      Ok([Send(stream, self.handshake.outbound_setup(), false)])
    }
    Request(request_id) => {
      if self.request_streams.get(stream) is Some(_) {
        return Err(DuplicateStream)
      }
      let request = match self.requests.get(request_id) {
        Some(value) if value.stream is None => value
        Some(_) => return Err(DuplicateStream)
        None => return Err(UnknownRequest)
      }
      request.stream = Some(stream)
      self.request_streams[stream] = request_id
      let encoded = match request.kind {
        Publishing => request.publisher.unwrap().publish_request()
        Subscribing => request.subscriber.unwrap().subscribe_request()
      }
      let bytes = match encoded {
        Ok(value) => value
        Err(error) => return Err(Encoding(error))
      }
      Ok([Send(stream, bytes, false)])
    }
    Object(token) => {
      let pending = match self.pending_object_streams.get(token) {
        Some(value) => value
        None => return Err(UnknownStream)
      }
      let publisher = self.publisher(pending.publisher_request_id)
      let header = match
        publisher.open_object_stream(
          pending.subscription_stream,
          stream,
          pending.header,
        ) {
        Ok(value) => value
        Err(error) => return Err(Track(error))
      }
      self.pending_object_streams.remove(token)
      self.outgoing_object_streams[stream] = {
        publisher_request_id: pending.publisher_request_id,
        finishing: false,
        pending_send_count: 1UL,
      }
      Ok([Send(stream, header, false)])
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
  match request.kind {
    Publishing => {
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
          match message {
            @protocol.RequestOk(_) if !request.accepted => {
              request.accepted = true
              match request.publisher.unwrap().publish_accepted(stream) {
                Ok(_) => ()
                Err(error) => return Err(Track(error))
              }
              let actions = [Notify(PublishAccepted(request_id))]
              if trailing.is_empty() {
                Ok(actions)
              } else {
                match self.receive_response(request_id, stream, trailing) {
                  Ok(more) => Ok(actions + more)
                  Err(error) => Err(error)
                }
              }
            }
            @protocol.RequestError(error) => {
              request.terminal = true
              Ok([Notify(PublishRejected(request_id, error))])
            }
            _ => Err(UnexpectedMessage)
          }
        }
      }
    }
    Subscribing => self.receive_subscriber_response(request_id, stream, bytes)
  }
}

///|
fn ClientSession::receive_subscriber_response(
  self : ClientSession,
  request_id : UInt64,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let request = self.requests.get(request_id).unwrap()
  if request.terminal {
    return Err(UnexpectedMessage)
  }
  let subscriber = request.subscriber.unwrap()
  let progress = match subscriber.receive_response(stream, bytes) {
    Ok(value) => value
    Err(error) => return Err(Track(error))
  }
  match progress {
    Pending => Ok([])
    Established(track_alias, trailing) => {
      if self.aliases.get(track_alias) is Some(_) {
        return Err(AliasInUse)
      }
      self.aliases[track_alias] = request_id
      request.accepted = true
      let actions = [Notify(SubscribeEstablished(request_id, track_alias))]
      if trailing.is_empty() {
        Ok(actions)
      } else {
        match self.receive_subscriber_response(request_id, stream, trailing) {
          Ok(more) => Ok(actions + more)
          Err(error) => Err(error)
        }
      }
    }
    Rejected(error, trailing) => {
      request.terminal = true
      let actions = [Notify(SubscribeRejected(request_id, error))]
      if trailing.is_empty() {
        Ok(actions)
      } else {
        match self.receive_subscriber_response(request_id, stream, trailing) {
          Ok(more) => Ok(actions + more)
          Err(error) => Err(error)
        }
      }
    }
    Finished(message, trailing) => {
      let actions = match self.receive_publish_done(request_id, message) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      if trailing.is_empty() {
        Ok(actions)
      } else {
        match self.receive_subscriber_response(request_id, stream, trailing) {
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
          let publisher = self.publisher(publisher_request_id)
          let progress = match publisher.receive_subscribe(stream, combined) {
            Ok(value) => value
            Err(error) => return Err(Track(error))
          }
          match progress {
            PublisherProgress::Pending => Ok([])
            SubscribeRequested(_, _, trailing) => {
              if !trailing.is_empty() || consumed != combined.length() {
                return Err(UnexpectedMessage)
              }
              self.inbound_buffers.remove(stream)
              self.peer_request_ids[request.request_id] = stream
              self.inbound_subscriptions[stream] = {
                request_id: request.request_id,
                publisher_request_id,
                accepted: false,
                finished: false,
              }
              Ok([
                Notify(
                  SubscriptionRequested(
                    request.request_id,
                    stream,
                    request.track,
                  ),
                ),
              ])
            }
          }
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

///|
fn ClientSession::receive_subgroup(
  self : ClientSession,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.subgroup_requests.get(stream) {
    Some(request_id) => {
      let subscriber = self.requests.get(request_id).unwrap().subscriber.unwrap()
      let progress = match subscriber.receive_object(stream, bytes) {
        Ok(value) => value
        Err(error) => return Err(Track(error))
      }
      match progress {
        Pending => Ok([])
        Received(header, frames) =>
          Ok([Notify(ObjectReceived(request_id, header, frames))])
      }
    }
    None => {
      let previous = match self.subgroup_buffers.get(stream) {
        Some(value) => value
        None => b""
      }
      if bytes.length() > 1024 * 1024 - previous.length() {
        return Err(BufferLimitExceeded)
      }
      let combined = previous + bytes
      match @protocol.decode_subgroup_header(combined) {
        @protocol.Incomplete => {
          self.subgroup_buffers[stream] = combined
          Ok([])
        }
        @protocol.Invalid(_) => Err(UnexpectedMessage)
        @protocol.Complete(header, _) => {
          let request_id = match self.aliases.get(header.track_alias) {
            Some(value) => value
            None => return Err(UnknownRequest)
          }
          let request = self.requests.get(request_id).unwrap()
          match request.pending_publish_done {
            Some(done) if request.observed_object_streams >= done.stream_count =>
              return Err(UnexpectedMessage)
            _ => ()
          }
          self.subgroup_buffers.remove(stream)
          self.subgroup_requests[stream] = request_id
          let subscriber = self.requests.get(request_id).unwrap().subscriber.unwrap()
          let progress = match subscriber.receive_object(stream, combined) {
            Ok(value) => value
            Err(error) => return Err(Track(error))
          }
          request.observed_object_streams = request.observed_object_streams +
            1UL
          request.active_object_streams = request.active_object_streams + 1UL
          match progress {
            Pending => Ok([])
            Received(actual_header, frames) =>
              Ok([Notify(ObjectReceived(request_id, actual_header, frames))])
          }
        }
      }
    }
  }
}

///|
fn ClientSession::stream_finished(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  if self.peer_control_stream == Some(stream) {
    return Err(Handshake(self.handshake.control_closed()))
  }
  match self.subgroup_requests.get(stream) {
    Some(request_id) => {
      let subscriber = self.requests.get(request_id).unwrap().subscriber.unwrap()
      let group_id = match subscriber.object_stream_group_id(stream) {
        Ok(value) => value
        Err(error) => return Err(Track(error))
      }
      let group_finished = match subscriber.finish_object_stream(stream) {
        Ok(value) => value
        Err(error) => return Err(Track(error))
      }
      let request = self.requests.get(request_id).unwrap()
      request.active_object_streams = request.active_object_streams - 1UL
      self.subgroup_requests.remove(stream)
      self.subgroup_buffers.remove(stream)
      let actions : Array[ClientSessionAction] = []
      if group_finished {
        actions.push(Notify(GroupFinished(request_id, group_id)))
      }
      match self.take_ready_publish_done(request_id) {
        Some(done) => actions.push(Notify(PublishDone(request_id, done)))
        None => ()
      }
      Ok(actions)
    }
    None => {
      if self.subgroup_buffers.get(stream) is Some(_) {
        self.subgroup_buffers.remove(stream)
        return Err(Track(IncompleteObjectStream))
      }
      self.inbound_buffers.remove(stream)
      Ok([])
    }
  }
}

///|
fn ClientSession::stream_reset(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.subgroup_requests.get(stream) {
    Some(request_id) => {
      let subscriber = self.requests.get(request_id).unwrap().subscriber.unwrap()
      subscriber.reset_object_stream(stream)
      let request = self.requests.get(request_id).unwrap()
      request.active_object_streams = request.active_object_streams - 1UL
      self.subgroup_requests.remove(stream)
      self.subgroup_buffers.remove(stream)
      let actions : Array[ClientSessionAction] = [
        Notify(ObjectStreamReset(request_id, stream)),
      ]
      match self.take_ready_publish_done(request_id) {
        Some(done) => actions.push(Notify(PublishDone(request_id, done)))
        None => ()
      }
      Ok(actions)
    }
    None => {
      self.inbound_buffers.remove(stream)
      Ok([])
    }
  }
}

///|
fn ClientSession::send_complete(
  self : ClientSession,
  stream : Int64,
  canceled : Bool,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.outgoing_object_streams.get(stream) {
    Some(outgoing) if canceled => {
      let publisher = self.publisher(outgoing.publisher_request_id)
      match publisher.reset_object_stream(stream) {
        Ok(_) => ()
        Err(error) => return Err(Track(error))
      }
      self.outgoing_object_streams.remove(stream)
      Ok([Notify(SendFinished(stream, true))])
    }
    Some(outgoing) => {
      if outgoing.pending_send_count > 0UL {
        outgoing.pending_send_count = outgoing.pending_send_count - 1UL
      }
      if outgoing.finishing && outgoing.pending_send_count == 0UL {
        let publisher = self.publisher(outgoing.publisher_request_id)
        match publisher.finish_object_stream(stream) {
          Ok(_) => ()
          Err(error) => return Err(Track(error))
        }
        self.outgoing_object_streams.remove(stream)
        Ok([
          Notify(SendFinished(stream, false)),
          Notify(ObjectStreamFinished(stream)),
        ])
      } else {
        Ok([Notify(SendFinished(stream, false))])
      }
    }
    None => Ok([Notify(SendFinished(stream, canceled))])
  }
}

///|
pub extend ClientStreamDirection with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ClientStreamDirection with Eq::{equal, not_equal}

///|
pub extend ClientStreamPurpose with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ClientStreamPurpose with Eq::{equal, not_equal}

///|
pub extend ClientSessionEvent with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ClientSessionEvent with Eq::{equal, not_equal}

///|
pub extend ClientSessionNotice with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ClientSessionNotice with Eq::{equal, not_equal}

///|
pub extend ClientSessionAction with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ClientSessionAction with Eq::{equal, not_equal}

///|
pub extend ClientSessionError with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ClientSessionError with Eq::{equal, not_equal}
