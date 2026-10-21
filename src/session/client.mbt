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
pub(all) enum ClientSessionState {
  Active
  Draining
  Closed
} derive(Debug, Eq)

///|
pub(all) enum ClientSessionEnd {
  Local(UInt64, Bytes)
  Peer(UInt64)
  Transport(Int64)
} derive(Debug, Eq)

///|
/// `Clock` values are monotonic milliseconds supplied by the adapter.
pub(all) enum ClientSessionEvent {
  InboundStreamStarted(Int64, ClientStreamDirection)
  StreamData(Int64, Bytes)
  DatagramReceived(Bytes)
  DatagramSendCapabilityChanged(Bool, Int32)
  StreamOpened(ClientStreamPurpose, Int64)
  StreamFinished(Int64)
  StreamReset(Int64, UInt64)
  SendStopped(Int64, UInt64)
  SendComplete(Int64, Bool)
  DatagramSendComplete
  Clock(UInt64)
  PeerClosed(UInt64)
  TransportFailed(Int64)
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
  SubscriptionUpdateRequested(
    UInt64,
    UInt64,
    Int64,
    Array[@protocol.MessageParameter],
    Array[@protocol.MessageParameter]
  )
  SubscriptionUpdateApplied(
    UInt64,
    UInt64,
    Int64,
    Array[@protocol.MessageParameter]
  )
  SubscriptionUpdateAccepted(UInt64, UInt64, @protocol.RequestOkMessage)
  SubscriptionUpdateRejected(UInt64, UInt64, @protocol.RequestErrorMessage)
  SubscriptionEnded(UInt64, Int64)
  ObjectReceived(
    UInt64,
    @protocol.SubgroupHeader,
    Array[@protocol.SubgroupFrame]
  )
  ObjectDatagramReceived(UInt64, @protocol.ObjectDatagram)
  ObjectStreamReset(UInt64, Int64)
  ObjectStreamFinished(Int64)
  GroupFinished(UInt64, UInt64)
  PublishDone(UInt64, @protocol.PublishDoneMessage)
  SendFinished(Int64, Bool)
  ControlGoAwayReceived(Bytes, UInt64, UInt64)
  RequestGoAwayReceived(UInt64, Int64, Bytes, UInt64)
  SessionEnded(ClientSessionEnd)
} derive(Debug, Eq)

///|
pub(all) enum ClientSessionAction {
  OpenStream(ClientStreamDirection, ClientStreamPurpose)
  Send(Int64, Bytes, Bool)
  SendDatagram(Bytes)
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
  SessionDraining
  SessionClosed
  TrackNotPublished
  AliasInUse
  SubscriptionNotAccepted
  RequestUpdatePending
  UnexpectedMessage
  BufferLimitExceeded
  DatagramSendUnavailable
  DatagramTooLarge(Int32, Int32)
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
  priv mut state : ClientSessionState
  priv mut outbound_control_stream : Int64?
  priv mut next_request_id : UInt64
  priv mut next_object_token : UInt64
  priv mut peer_control_stream : Int64?
  priv mut control_buffer : Bytes
  priv mut control_goaway_sent : Bool
  priv mut control_goaway_received : Bool
  priv mut peer_goaway_boundary : UInt64?
  priv mut control_goaway_deadline : UInt64?
  priv mut now_ms : UInt64
  priv mut clock_initialized : Bool
  priv request_goaway_streams : Map[Int64, Bool]
  priv request_goaway_deadlines : Map[Int64, UInt64]
  priv mut max_peer_request_id : UInt64?
  priv pending_sends : Map[Int64, UInt64]
  priv mut pending_datagram_sends : UInt64
  priv mut datagram_send_enabled : Bool
  priv mut max_datagram_length : Int32
  priv inbound_stream_kinds : Map[Int64, InboundStreamKind]
  priv inbound_unclassified_buffers : Map[Int64, Bytes]
  priv requests : Map[UInt64, ClientRequest]
  priv request_streams : Map[Int64, UInt64]
  priv inbound_buffers : Map[Int64, Bytes]
  priv inbound_subscriptions : Map[Int64, InboundSubscription]
  priv peer_request_ids : Map[UInt64, Int64]
  priv published_requests : Array[UInt64]
  priv largest_published_locations : Map[UInt64, (UInt64, UInt64)]
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
        state: Active,
        outbound_control_stream: None,
        next_request_id: 0UL,
        next_object_token: 0UL,
        peer_control_stream: None,
        control_buffer: b"",
        control_goaway_sent: false,
        control_goaway_received: false,
        peer_goaway_boundary: None,
        control_goaway_deadline: None,
        now_ms: 0UL,
        clock_initialized: false,
        request_goaway_streams: {},
        request_goaway_deadlines: {},
        max_peer_request_id: None,
        pending_sends: {},
        pending_datagram_sends: 0UL,
        datagram_send_enabled: false,
        max_datagram_length: 0,
        inbound_stream_kinds: {},
        inbound_unclassified_buffers: {},
        requests: {},
        request_streams: {},
        inbound_buffers: {},
        inbound_subscriptions: {},
        peer_request_ids: {},
        published_requests: [],
        largest_published_locations: {},
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
  if self.state == Closed {
    return Err(SessionClosed)
  }
  if self.started {
    return Err(UnexpectedMessage)
  }
  self.started = true
  Ok([OpenStream(Unidirectional, Control)])
}

///|
pub fn ClientSession::receive(
  self : ClientSession,
  event : ClientSessionEvent,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  if self.state == Closed {
    return Ok([])
  }
  let result = match event {
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
    DatagramReceived(bytes) => self.receive_object_datagram(bytes)
    DatagramSendCapabilityChanged(enabled, max_length) => {
      self.datagram_send_enabled = enabled
      self.max_datagram_length = max_length
      Ok([])
    }
    StreamOpened(purpose, stream) => self.stream_opened(purpose, stream)
    StreamFinished(stream) => self.stream_finished(stream)
    StreamReset(stream, code) => self.stream_reset(stream, code)
    SendStopped(stream, code) =>
      if self.outbound_control_stream == Some(stream) {
        Err(Handshake(self.handshake.control_closed()))
      } else {
        match self.outgoing_object_streams.get(stream) {
          Some(outgoing) => {
            let subscription_stream = outgoing.subscription_stream
            self.close_outgoing_object_stream(stream)
            Ok(
              [AbortSend(stream, code), Notify(SendFinished(stream, true))] +
              self.finish_failed_subscription(subscription_stream),
            )
          }
          None => Ok(self.end_request_stream(stream, PeerStop(code)))
        }
      }
    SendComplete(stream, canceled) => {
      self.note_send_complete(stream)
      self.send_complete(stream, canceled)
    }
    DatagramSendComplete => {
      if self.pending_datagram_sends > 0UL {
        self.pending_datagram_sends = self.pending_datagram_sends - 1UL
      }
      Ok([])
    }
    Clock(now_ms) => Ok(self.clock(now_ms))
    PeerClosed(code) => Ok(self.end_session(Peer(code), None, b""))
    TransportFailed(status) =>
      Ok(self.end_session(Transport(status), None, b""))
  }
  match result {
    Ok(actions) => Ok(self.finish_actions(actions))
    Err(error) =>
      match self.protocol_error_actions(event, error) {
        Some(actions) => Ok(self.finish_actions(actions))
        None => Err(error)
      }
  }
}

///|
/// Convert peer protocol violations into session actions. Errors from local
/// API methods remain errors; adapters only execute the actions returned here.
fn ClientSession::protocol_error_actions(
  self : ClientSession,
  event : ClientSessionEvent,
  error : ClientSessionError,
) -> Array[ClientSessionAction]? {
  let code = match error {
    Handshake(handshake_error) => handshake_error.close_code()
    UnexpectedMessage
    | Track(_)
    | UnknownStream
    | UnknownRequest
    | InvalidRequestId
    | DuplicateRequestId
    | DuplicateStream
    | SubscriptionNotAccepted
    | BufferLimitExceeded => close_protocol_violation
    _ => return None
  }
  match event {
    StreamData(stream, _)
    | StreamFinished(stream)
    | StreamReset(stream, _)
    | SendStopped(stream, _) => {
      let kind = self.inbound_stream_kinds.get(stream)
      if self.peer_control_stream == Some(stream) || kind is Some(Control) {
        Some(self.end_session(Local(code, b""), Some(code), b""))
      } else if self.request_streams.get(stream) is Some(_) ||
        self.inbound_subscriptions.get(stream) is Some(_) {
        Some(self.end_request_stream(stream, Cancel(code)))
      } else if kind is Some(Request) {
        self.inbound_buffers.remove(stream)
        Some([AbortSend(stream, code), StopReceiving(stream, code)])
      } else if kind is Some(Subgroup) {
        self.incoming_object_streams.remove(stream)
        self.inbound_stream_kinds.remove(stream)
        Some([StopReceiving(stream, code)])
      } else {
        Some(self.end_session(Local(code, b""), Some(code), b""))
      }
    }
    DatagramReceived(_) =>
      Some(self.end_session(Local(code, b""), Some(code), b""))
    InboundStreamStarted(_, _) =>
      Some(self.end_session(Local(code, b""), Some(code), b""))
    _ => None
  }
}

///|
/// Request a transport-level connection close.
pub fn ClientSession::close(
  self : ClientSession,
  code : UInt64,
  reason : Bytes,
) -> Array[ClientSessionAction] {
  self.end_session(Local(code, reason), Some(code), reason)
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
    match
      @protocol.decode_control_with_context(
        self.control_buffer,
        @protocol.ControlStream,
      ) {
      @protocol.Incomplete => return Ok(actions)
      @protocol.Invalid(_) => return Err(UnexpectedMessage)
      @protocol.Complete(message, consumed) => {
        self.control_buffer = self.control_buffer[consumed:].to_owned()
        match message {
          @protocol.GoAwayControl(goaway) => {
            if self.control_goaway_received || (goaway.request_id & 1UL) != 0UL {
              for action in self.receive_control_goaway(goaway) {
                actions.push(action)
              }
              self.control_buffer = b""
              return Ok(actions)
            }
            actions.push(Notify(ControlMessageReceived(message)))
            for action in self.receive_control_goaway(goaway) {
              actions.push(action)
            }
          }
          _ => actions.push(Notify(ControlMessageReceived(message)))
        }
        if self.state == Closed {
          self.control_buffer = b""
          return Ok(actions)
        }
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
      if request.terminal {
        if request.lifetime.cancelled {
          let error_code = match request.lifetime.cancel_error_code {
            Some(value) => value
            None => 1UL
          }
          return Ok(self.end_request_stream(stream, Cancel(error_code)))
        }
        return Ok([])
      }
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
          match self.published_subscriptions.get(pending.subscription_stream) {
            Some(subscription) =>
              subscription.opened_stream_count = subscription.opened_stream_count +
                1UL
            None => ()
          }
          return Ok(
            [AbortSend(stream, error_code)] +
            self.finish_failed_subscription(pending.subscription_stream),
          )
        }
        None => ()
      }
      if !self.subscription_is_active(pending.subscription_stream) {
        self.pending_object_streams.remove(token)
        match self.published_subscriptions.get(pending.subscription_stream) {
          Some(subscription) =>
            subscription.opened_stream_count = subscription.opened_stream_count +
              1UL
          None => ()
        }
        return Ok(
          [AbortSend(stream, 1UL)] +
          self.finish_failed_subscription(pending.subscription_stream),
        )
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
        abort_sent: false,
        pending_send_count: 1UL,
      }
      Ok([Send(stream, @protocol.encode_subgroup_header(pending.header), false)])
    }
  }
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
        match self.inbound_subscriptions.get(stream) {
          Some(inbound) => ignore(inbound.lifetime.transition(PeerFin, true))
          None =>
            match self.request_streams.get(stream) {
              Some(request_id) => {
                let request = self.requests.get(request_id).unwrap()
                ignore(request.lifetime.transition(PeerFin, true))
                request.response_buffer = b""
              }
              None => ()
            }
        }
        self.inbound_buffers.remove(stream)
        return Ok([])
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
        return Ok(self.end_request_stream(stream, PeerReset(error_code)))
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
    Some(outgoing) if canceled => {
      let subscription_stream = outgoing.subscription_stream
      self.close_outgoing_object_stream(stream)
      Ok(
        [Notify(SendFinished(stream, true))] +
        self.finish_failed_subscription(subscription_stream),
      )
    }
    Some(outgoing) => {
      if outgoing.pending_send_count > 0UL {
        outgoing.pending_send_count = outgoing.pending_send_count - 1UL
      }
      if outgoing.finishing && outgoing.pending_send_count == 0UL {
        let subscription_stream = outgoing.subscription_stream
        self.close_outgoing_object_stream(stream)
        Ok(
          [
            Notify(SendFinished(stream, false)),
            Notify(ObjectStreamFinished(stream)),
          ] +
          self.finish_failed_subscription(subscription_stream),
        )
      } else {
        Ok([Notify(SendFinished(stream, false))])
      }
    }
    None =>
      match self.request_streams.get(stream) {
        Some(request_id) if self.requests.get(request_id).unwrap().lifetime.cancelled =>
          Ok([])
        _ => Ok([Notify(SendFinished(stream, canceled))])
      }
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
