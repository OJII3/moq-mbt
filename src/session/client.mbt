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
  InboundStreamStarted(Int64, ClientStreamDirection)
  StreamData(Int64, Bytes)
  StreamOpened(ClientStreamPurpose, Int64)
  StreamFinished(Int64)
  StreamReset(Int64, UInt64)
  SendStopped(Int64, UInt64)
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
  SubscriptionEnded(UInt64, Int64)
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
  AbortSend(Int64, UInt64)
  StopReceiving(Int64, UInt64)
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
priv enum InboundStreamKind {
  Control
  Request
  Subgroup
  Padding
}

///|
/// Draft 18 Sans-I/O client session. It owns requests, subscriptions, Object
/// streams, and track aliases; transport references remain opaque.
pub struct ClientSession {
  priv handshake : ClientHandshake
  priv mut started : Bool
  priv mut ready : Bool
  priv mut outbound_control_stream : Int64?
  priv mut next_request_id : UInt64
  priv mut next_object_token : UInt64
  priv mut peer_control_stream : Int64?
  priv mut control_buffer : Bytes
  priv inbound_stream_kinds : Map[Int64, InboundStreamKind]
  priv inbound_unclassified_buffers : Map[Int64, Bytes]
  priv requests : Map[UInt64, ClientRequest]
  priv request_streams : Map[Int64, UInt64]
  priv inbound_buffers : Map[Int64, Bytes]
  priv inbound_subscriptions : Map[Int64, InboundSubscription]
  priv peer_request_ids : Map[UInt64, Int64]
  priv published_requests : Array[UInt64]
  priv aliases : Map[UInt64, UInt64]
  priv incoming_object_streams : Map[Int64, IncomingObjectStream]
  priv published_subscriptions : Map[Int64, PublishedSubscription]
  priv ignored_object_streams : Map[Int64, IgnoredObjectStream]
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
        inbound_stream_kinds: {},
        inbound_unclassified_buffers: {},
        requests: {},
        request_streams: {},
        inbound_buffers: {},
        inbound_subscriptions: {},
        peer_request_ids: {},
        published_requests: [],
        aliases: {},
        incoming_object_streams: {},
        published_subscriptions: {},
        ignored_object_streams: {},
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
  }
  Ok((request_id, [OpenStream(Bidirectional, Request(request_id))]))
}

///|
pub fn ClientSession::receive(
  self : ClientSession,
  event : ClientSessionEvent,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match event {
    InboundStreamStarted(stream, direction) => {
      if self.inbound_stream_kinds.get(stream) is Some(_) ||
        self.inbound_unclassified_buffers.get(stream) is Some(_) ||
        self.inbound_buffers.get(stream) is Some(_) ||
        self.inbound_subscriptions.get(stream) is Some(_) ||
        self.request_streams.get(stream) is Some(_) ||
        self.incoming_object_streams.get(stream) is Some(_) {
        return Err(DuplicateStream)
      }
      match direction {
        Bidirectional => {
          self.inbound_stream_kinds[stream] = Request
          self.inbound_buffers[stream] = b""
        }
        Unidirectional => self.inbound_unclassified_buffers[stream] = b""
      }
      Ok([])
    }
    StreamData(stream, bytes) => self.receive_stream_data(stream, bytes)
    StreamOpened(purpose, stream) => self.stream_opened(purpose, stream)
    StreamFinished(stream) => self.stream_finished(stream)
    StreamReset(stream, code) => self.stream_reset(stream, code)
    SendStopped(stream, code) => Ok(self.end_request_stream(stream, code))
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
      if inbound.state != Active {
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
  let publisher_request = self.requests.get(publisher_request_id).unwrap()
  if header.track_alias != publisher_request.track_alias.unwrap() {
    return Err(Track(AliasMismatch))
  }
  let token = self.next_object_token
  self.next_object_token = token + 1UL
  self.pending_object_streams[token] = {
    subscription_stream,
    header,
    cancelled_error_code: None,
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
  if !self.subscription_is_active(outgoing.subscription_stream) {
    return Err(SubscriptionNotAccepted)
  }
  if outgoing.finishing {
    return Err(UnexpectedMessage)
  }
  let bytes = match encode_outgoing_object(outgoing, frame) {
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
  let (request_id, local_publish) = match
    self.inbound_subscriptions.get(subscription_stream) {
    Some(inbound) => {
      if inbound.state != Active {
        return Err(SubscriptionNotAccepted)
      }
      (inbound.request_id, false)
    }
    None =>
      match self.request_streams.get(subscription_stream) {
        Some(request_id) => {
          let request = self.requests.get(request_id).unwrap()
          if request.kind != Publishing || !request.accepted || request.terminal {
            return Err(SubscriptionNotAccepted)
          }
          (request_id, true)
        }
        None => return Err(UnknownStream)
      }
  }
  let subscription = match
    self.published_subscriptions.get(subscription_stream) {
    Some(value) => value
    None => return Err(UnexpectedMessage)
  }
  if subscription.open_stream_count != 0UL {
    return Err(Track(ObjectStreamsStillOpen))
  }
  let message : @protocol.PublishDoneMessage = {
    code,
    stream_count: subscription.opened_stream_count,
    reason,
  }
  let bytes = match @protocol.encode_publish_done(message) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  if local_publish {
    self.requests.get(request_id).unwrap().terminal = true
  } else {
    self.inbound_subscriptions.get(subscription_stream).unwrap().state = Ended
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
fn ClientSession::receive_stream_data(
  self : ClientSession,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.inbound_unclassified_buffers.get(stream) {
    Some(previous) => {
      if bytes.length() > 1024 * 1024 - previous.length() {
        return Err(BufferLimitExceeded)
      }
      let combined = previous + bytes
      match @protocol.decode_vi64(combined) {
        @protocol.Incomplete => {
          self.inbound_unclassified_buffers[stream] = combined
          Ok([])
        }
        @protocol.Invalid(_) => Err(UnexpectedMessage)
        @protocol.Complete(stream_type, _) => {
          self.inbound_unclassified_buffers.remove(stream)
          match stream_type {
            0x2f00UL => {
              self.inbound_stream_kinds[stream] = Control
              self.receive_control(stream, combined)
            }
            0x05UL => Err(UnexpectedMessage)
            0x132b3e28UL => {
              self.inbound_stream_kinds[stream] = Padding
              Ok([])
            }
            _ if is_subgroup_stream_type(stream_type) => {
              self.inbound_stream_kinds[stream] = Subgroup
              self.receive_subgroup(stream, combined)
            }
            _ => Err(UnexpectedMessage)
          }
        }
      }
    }
    None =>
      match self.inbound_stream_kinds.get(stream) {
        Some(Control) => self.receive_control(stream, bytes)
        Some(Request) => self.receive_request(stream, bytes)
        Some(Subgroup) => self.receive_subgroup(stream, bytes)
        Some(Padding) => Ok([])
        None =>
          match self.request_streams.get(stream) {
            Some(_) => self.receive_request(stream, bytes)
            None => Err(UnknownStream)
          }
      }
  }
}

///|
fn is_subgroup_stream_type(stream_type : UInt64) -> Bool {
  stream_type >= 0x10UL &&
  stream_type <= 0x7fUL &&
  (stream_type & 0x10UL) != 0UL
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
        Publishing =>
          @protocol.encode_publish({
            request_id,
            track: request.track,
            track_alias: request.track_alias.unwrap(),
            parameters: [],
            properties: b"",
          })
        Subscribing =>
          @protocol.encode_subscribe({
            request_id,
            track: request.track,
            parameters: [],
          })
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
      match pending.cancelled_error_code {
        Some(error_code) => {
          self.pending_object_streams.remove(token)
          return Ok([AbortSend(stream, error_code)])
        }
        None => ()
      }
      if !self.subscription_is_active(pending.subscription_stream) {
        self.pending_object_streams.remove(token)
        return Ok([AbortSend(stream, 0UL)])
      }
      let subscription = match
        self.published_subscriptions.get(pending.subscription_stream) {
        Some(value) => value
        None => return Err(SubscriptionNotAccepted)
      }
      if self.outgoing_object_streams.get(stream) is Some(_) {
        return Err(Track(UnexpectedMessage))
      }
      subscription.opened_stream_count = subscription.opened_stream_count + 1UL
      subscription.open_stream_count = subscription.open_stream_count + 1UL
      self.pending_object_streams.remove(token)
      self.outgoing_object_streams[stream] = {
        subscription_stream: pending.subscription_stream,
        header: pending.header,
        previous_object_id: None,
        finishing: false,
        pending_send_count: 1UL,
      }
      Ok([Send(stream, @protocol.encode_subgroup_header(pending.header), false)])
    }
  }
}

///|
fn ClientSession::end_request_stream(
  self : ClientSession,
  stream : Int64,
  error_code : UInt64,
) -> Array[ClientSessionAction] {
  let actions : Array[ClientSessionAction] = []
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

///|
fn ClientSession::stream_finished(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  if self.peer_control_stream == Some(stream) ||
    self.inbound_stream_kinds.get(stream) is Some(Control) {
    return Err(Handshake(self.handshake.control_closed()))
  }
  match self.ignored_object_streams.get(stream) {
    Some(ignored) => {
      self.ignored_object_streams.remove(stream)
      self.inbound_stream_kinds.remove(stream)
      if ignored.counted_active {
        let request = self.requests.get(ignored.request_id).unwrap()
        request.active_object_streams = request.active_object_streams - 1UL
      }
      return Ok([])
    }
    None => ()
  }
  match self.incoming_object_streams.get(stream) {
    Some(state) => {
      if state.header is None {
        self.incoming_object_streams.remove(stream)
        self.inbound_stream_kinds.remove(stream)
        return Err(Track(IncompleteObjectStream))
      }
      if !state.buffer.is_empty() {
        return Err(Track(IncompleteObjectStream))
      }
      let request_id = match state.request_id {
        Some(value) => value
        None => {
          self.incoming_object_streams.remove(stream)
          self.inbound_stream_kinds.remove(stream)
          return Err(Track(IncompleteObjectStream))
        }
      }
      let group_id = state.header.unwrap().group_id
      let group_finished = state.header.unwrap().end_of_group
      let request = self.requests.get(request_id).unwrap()
      request.active_object_streams = request.active_object_streams - 1UL
      self.incoming_object_streams.remove(stream)
      self.inbound_stream_kinds.remove(stream)
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
      if self.inbound_subscriptions.get(stream) is Some(_) ||
        self.request_streams.get(stream) is Some(_) {
        return Ok(self.end_request_stream(stream, 0UL))
      }
      if self.inbound_unclassified_buffers.get(stream) is Some(_) {
        self.inbound_unclassified_buffers.remove(stream)
        return Err(UnexpectedMessage)
      }
      self.inbound_buffers.remove(stream)
      self.inbound_stream_kinds.remove(stream)
      Ok([])
    }
  }
}

///|
fn ClientSession::stream_reset(
  self : ClientSession,
  stream : Int64,
  error_code : UInt64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  if self.peer_control_stream == Some(stream) ||
    self.inbound_stream_kinds.get(stream) is Some(Control) {
    return Err(Handshake(self.handshake.control_closed()))
  }
  match self.ignored_object_streams.get(stream) {
    Some(ignored) => {
      self.ignored_object_streams.remove(stream)
      self.inbound_stream_kinds.remove(stream)
      if ignored.counted_active {
        let request = self.requests.get(ignored.request_id).unwrap()
        request.active_object_streams = request.active_object_streams - 1UL
      }
      return Ok([])
    }
    None => ()
  }
  match self.incoming_object_streams.get(stream) {
    Some(state) => {
      let request_id = match state.request_id {
        Some(value) => value
        None => {
          self.incoming_object_streams.remove(stream)
          self.inbound_stream_kinds.remove(stream)
          return Ok([])
        }
      }
      let request = self.requests.get(request_id).unwrap()
      request.active_object_streams = request.active_object_streams - 1UL
      self.incoming_object_streams.remove(stream)
      self.inbound_stream_kinds.remove(stream)
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
      if self.inbound_subscriptions.get(stream) is Some(_) ||
        self.request_streams.get(stream) is Some(_) {
        return Ok(self.end_request_stream(stream, error_code))
      }
      self.inbound_buffers.remove(stream)
      self.incoming_object_streams.remove(stream)
      self.inbound_unclassified_buffers.remove(stream)
      self.inbound_stream_kinds.remove(stream)
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
    Some(_) if canceled => {
      self.close_outgoing_object_stream(stream)
      Ok([Notify(SendFinished(stream, true))])
    }
    Some(outgoing) => {
      if outgoing.pending_send_count > 0UL {
        outgoing.pending_send_count = outgoing.pending_send_count - 1UL
      }
      if outgoing.finishing && outgoing.pending_send_count == 0UL {
        self.close_outgoing_object_stream(stream)
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
