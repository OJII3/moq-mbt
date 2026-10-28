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
  FetchData(UInt64)
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
/// How a local stream send ended.
pub(all) enum SendCompletion {
  Sent
  Reset
} derive(Debug, Eq)

///|
/// `SendCompleted` means the backend's send operation has completed. The
/// completion point is backend-specific: Web Streams can report local write
/// and FIN completion, while a QUIC backend may report a stronger signal.
/// The common session contract does not promise peer receipt or QUIC ACK.
/// `Clock` values are monotonic milliseconds supplied by the adapter.
pub(all) enum SessionEvent {
  InboundStreamStarted(Int64, ClientStreamDirection)
  StreamData(Int64, Bytes)
  DatagramReceived(Bytes)
  DatagramSendCapabilityChanged(enabled~ : Bool, max_length~ : Int32)
  StreamOpened(ClientStreamPurpose, Int64)
  InboundStreamFinished(Int64)
  StreamReset(Int64, UInt64)
  SendStopped(Int64, UInt64)
  SendCompleted(Int64, SendCompletion)
  DatagramSendComplete(UInt64)
  DatagramSendFailed(UInt64, Int64)
  Clock(UInt64)
  PeerClosed(UInt64)
  TransportFailed(Int64)
} derive(Debug, Eq)

///|
pub(all) enum SessionNotice {
  HandshakeComplete(@protocol.Setup)
  ControlMessageReceived(@protocol.ControlMessage)
  PublishAccepted(UInt64, @protocol.TrackName, UInt64)
  PublishRejected(UInt64, @protocol.RequestErrorMessage)
  SubscribeEstablished(UInt64, UInt64)
  SubscribeRejected(UInt64, @protocol.RequestErrorMessage)
  SubscriptionRequested(UInt64, Int64, @protocol.TrackName)
  FetchRequested(
    UInt64,
    Int64,
    @protocol.TrackName,
    @protocol.FetchLocation,
    @protocol.FetchLocation,
    Array[@protocol.MessageParameter]
  )
  FetchAccepted(UInt64, Int64)
  FetchResponseAccepted(UInt64, Int64, @protocol.FetchOkMessage)
  FetchRejected(UInt64, Int64, @protocol.RequestErrorMessage)
  FetchObjectReceived(UInt64, @protocol.FetchObject)
  FetchRangeEnded(UInt64, @protocol.FetchRangeEnd)
  FetchEndOfTrack(UInt64)
  FetchFinished(UInt64)
  FetchFailed(UInt64, @protocol.RequestErrorMessage)
  FetchCancelled(UInt64, UInt64)
  FetchReset(UInt64, Int64, UInt64)
  FetchUpdateRequested(UInt64, UInt64, Int64, Array[@protocol.MessageParameter])
  FetchUpdateAccepted(UInt64, UInt64, @protocol.RequestOkMessage)
  FetchUpdateRejected(UInt64, UInt64, @protocol.RequestErrorMessage)
  NamespacePublicationRequested(
    UInt64,
    Int64,
    @protocol.TrackNamespace,
    Array[@protocol.MessageParameter]
  )
  NamespacePublicationAccepted(UInt64, Int64)
  NamespacePublicationRejected(UInt64, Int64, @protocol.RequestErrorMessage)
  NamespaceUpdateRequested(
    UInt64,
    UInt64,
    Int64,
    Array[@protocol.MessageParameter]
  )
  NamespaceUpdateAccepted(UInt64, UInt64, @protocol.RequestOkMessage)
  NamespaceUpdateRejected(UInt64, UInt64, @protocol.RequestErrorMessage)
  NamespaceWithdrawn(UInt64, Int64)
  NamespaceAcceptanceRevoked(UInt64, Int64)
  NamespaceSubscriptionRequested(UInt64, Int64, @protocol.TrackNamespace)
  NamespaceSubscriptionEstablished(UInt64, Int64)
  NamespaceSubscriptionRejected(UInt64, Int64, @protocol.RequestErrorMessage)
  NamespaceDiscovered(UInt64, @protocol.TrackNamespace)
  NamespaceNoLongerAvailable(UInt64, @protocol.TrackNamespace)
  NamespaceSubscriptionEnded(UInt64, Int64)
  NamespaceSubscriptionUpdateRequested(
    UInt64,
    UInt64,
    Int64,
    @protocol.TrackNamespace
  )
  NamespaceSubscriptionUpdateAccepted(
    UInt64,
    UInt64,
    @protocol.RequestOkMessage
  )
  NamespaceSubscriptionUpdateRejected(
    UInt64,
    UInt64,
    @protocol.RequestErrorMessage
  )
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
  SubscriptionEnded(UInt64, Int64, @protocol.TrackName)
  ObjectReceived(
    UInt64,
    @protocol.TrackName,
    @protocol.SubgroupHeader,
    Array[@protocol.SubgroupFrame]
  )
  ObjectDatagramReceived(UInt64, @protocol.TrackName, @protocol.ObjectDatagram)
  ObjectStreamReset(UInt64, Int64)
  ObjectStreamSendFinished(Int64)
  GroupFinished(UInt64, UInt64)
  PublishDone(UInt64, @protocol.PublishDoneMessage)
  StreamSendFinished(Int64, SendCompletion)
  DatagramQueued(UInt64)
  DatagramLocalWriteFailed(UInt64, Int64)
  ControlGoAwayReceived(Bytes, UInt64, UInt64)
  RequestGoAwayReceived(UInt64, Int64, Bytes, UInt64)
  SessionEnded(ClientSessionEnd)
} derive(Debug, Eq)

///|
pub(all) enum SessionAction {
  OpenStream(ClientStreamDirection, ClientStreamPurpose)
  SendChunk(Int64, Bytes)
  SendFin(Int64, Bytes)
  SendDatagram(UInt64, Bytes)
  AbortSend(Int64, UInt64)
  StopReceiving(Int64, UInt64)
  CloseConnection(UInt64, Bytes)
  EmitNotice(SessionNotice)
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
  InvalidFetchRange
  SessionDraining
  SessionClosed
  TrackNotPublished
  AliasInUse
  SubscriptionNotAccepted
  RequestUpdatePending
  PrefixOverlap
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
  FetchData
  Padding
  Discarded
}

///|
priv enum RequestStreamRoute {
  NamespaceSubscriptionResponse(UInt64)
  NamespacePublicationResponse(UInt64)
  FetchResponse(UInt64)
  TrackResponse(UInt64)
  UnknownRequestStream
}

///|
/// Draft 18 Sans-I/O client session. It owns requests, subscriptions, Object
/// streams, and track aliases; transport references remain opaque.
pub struct ClientSession {
  priv handshake : ClientHandshake
  priv mut started : Bool
  priv mut ready : Bool
  priv mut state : ClientSessionState
  priv mut desired_subscriptions : Array[DesiredSubscription]
  priv mut outbound_control_stream : Int64?
  priv mut next_request_id : UInt64
  priv mut next_object_token : UInt64
  priv mut next_datagram_token : UInt64
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
  priv pending_datagram_sends : Map[UInt64, Bool]
  priv mut datagram_send_enabled : Bool
  priv mut max_datagram_length : Int32
  priv inbound_stream_kinds : Map[Int64, InboundStreamKind]
  priv inbound_unclassified_buffers : Map[Int64, Bytes]
  priv requests : Map[UInt64, ClientRequest]
  priv fetches : Map[UInt64, ClientFetch]
  priv fetch_streams : Map[Int64, UInt64]
  priv inbound_fetch_streams : Map[Int64, InboundFetchDataStream]
  priv inbound_fetch_request_ids : Map[UInt64, Int64]
  priv pending_joining_fetches : Map[UInt64, Array[UInt64]]
  priv outgoing_fetch_streams : Map[Int64, OutgoingFetchDataStream]
  priv request_streams : Map[Int64, UInt64]
  priv inbound_buffers : Map[Int64, Bytes]
  priv inbound_subscriptions : Map[Int64, InboundSubscription]
  priv peer_request_ids : Map[UInt64, Int64]
  priv namespace_requests : Map[UInt64, NamespaceRequest]
  priv namespace_request_streams : Map[Int64, UInt64]
  priv namespace_subscriptions : Map[UInt64, NamespaceSubscription]
  priv namespace_subscription_streams : Map[Int64, UInt64]
  priv inbound_namespace_subscriptions : Map[
    Int64,
    InboundNamespaceSubscription,
  ]
  priv application_namespaces : Array[@protocol.TrackNamespace]
  priv published_requests : Array[UInt64]
  priv largest_published_locations : Map[UInt64, (UInt64, UInt64)]
  priv aliases : Map[UInt64, UInt64]
  priv incoming_object_streams : Map[Int64, IncomingObjectStream]
  priv published_subscriptions : Map[Int64, PublishedSubscription]
  priv ignored_object_streams : Map[Int64, IgnoredObjectStream]
  priv pending_object_streams : Map[UInt64, PendingObjectStream]
  priv outgoing_object_streams : Map[Int64, OutgoingObjectStream]
  priv group_streams : Map[UInt64, Int64]
}

///|
/// Create a session that restores these desired track subscriptions after
/// SETUP completes. Each restored request receives a fresh request ID.
/// Pass an empty list or omit the argument to start without restored
/// subscriptions.
pub fn ClientSession::new(
  version : Version,
  setup : @protocol.Setup,
  desired_subscriptions~ : Array[DesiredSubscription] = [],
) -> Result[ClientSession, @protocol.CodecError] {
  match ClientHandshake::new(version, setup) {
    Err(error) => Err(error)
    Ok(handshake) =>
      Ok({
        handshake,
        started: false,
        ready: false,
        state: Active,
        desired_subscriptions: copy_desired_subscriptions(desired_subscriptions),
        outbound_control_stream: None,
        next_request_id: 0UL,
        next_object_token: 0UL,
        next_datagram_token: 0UL,
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
        pending_datagram_sends: {},
        datagram_send_enabled: false,
        max_datagram_length: 0,
        inbound_stream_kinds: {},
        inbound_unclassified_buffers: {},
        requests: {},
        fetches: {},
        fetch_streams: {},
        inbound_fetch_streams: {},
        inbound_fetch_request_ids: {},
        pending_joining_fetches: {},
        outgoing_fetch_streams: {},
        request_streams: {},
        inbound_buffers: {},
        inbound_subscriptions: {},
        peer_request_ids: {},
        namespace_requests: {},
        namespace_request_streams: {},
        namespace_subscriptions: {},
        namespace_subscription_streams: {},
        inbound_namespace_subscriptions: {},
        application_namespaces: [],
        published_requests: [],
        largest_published_locations: {},
        aliases: {},
        incoming_object_streams: {},
        published_subscriptions: {},
        ignored_object_streams: {},
        pending_object_streams: {},
        outgoing_object_streams: {},
        group_streams: {},
      })
  }
}

///|
/// Ask the adapter to open the local unidirectional control stream.
pub fn ClientSession::start(
  self : ClientSession,
) -> Result[Array[SessionAction], ClientSessionError] {
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
  event : SessionEvent,
) -> Result[Array[SessionAction], ClientSessionError] {
  if self.state == Closed {
    return Ok([])
  }
  let result = match event {
    InboundStreamStarted(stream, direction) => {
      if self.inbound_stream_kinds.get(stream) is Some(_) ||
        self.inbound_unclassified_buffers.get(stream) is Some(_) ||
        self.inbound_buffers.get(stream) is Some(_) ||
        self.inbound_fetch_streams.get(stream) is Some(_) ||
        self.request_stream_exists(stream) ||
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
    DatagramSendCapabilityChanged(enabled~, max_length~) => {
      self.datagram_send_enabled = enabled
      self.max_datagram_length = max_length
      Ok([])
    }
    StreamOpened(purpose, stream) => self.stream_opened(purpose, stream)
    InboundStreamFinished(stream) => self.stream_finished(stream)
    StreamReset(stream, code) => self.stream_reset(stream, code)
    SendStopped(stream, code) =>
      if self.outbound_control_stream == Some(stream) {
        Err(Handshake(self.handshake.control_closed()))
      } else {
        match self.outgoing_fetch_streams.get(stream) {
          Some(outgoing) => {
            let request_id = outgoing.request_id
            let fetch = self.fetches.get(request_id).unwrap()
            Ok(
              self.terminate_fetch(
                fetch,
                [AbortSend(stream, code)],
                Some(EmitNotice(FetchReset(request_id, stream, code))),
                RemoveFetchData,
              ),
            )
          }
          None =>
            match self.outgoing_object_streams.get(stream) {
              Some(outgoing) => {
                let subscription_stream = outgoing.subscription_stream
                self.close_outgoing_object_stream(stream)
                Ok(
                  [
                    AbortSend(stream, code),
                    EmitNotice(StreamSendFinished(stream, Reset)),
                  ] +
                  self.finish_failed_subscription(subscription_stream),
                )
              }
              None => Ok(self.end_request_stream(stream, PeerStop(code)))
            }
        }
      }
    SendCompleted(stream, completion) => {
      self.note_send_complete(stream)
      self.send_complete(stream, completion)
    }
    DatagramSendComplete(token) => {
      self.pending_datagram_sends.remove(token)
      Ok([])
    }
    DatagramSendFailed(token, status) => {
      self.pending_datagram_sends.remove(token)
      Ok([EmitNotice(DatagramLocalWriteFailed(token, status))])
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
  event : SessionEvent,
  error : ClientSessionError,
) -> Array[SessionAction]? {
  let code = match error {
    InvalidRequestId | DuplicateRequestId => close_invalid_request_id
    Handshake(handshake_error) => handshake_error.close_code()
    UnexpectedMessage
    | Track(_)
    | UnknownStream
    | UnknownRequest
    | DuplicateStream
    | SubscriptionNotAccepted
    | BufferLimitExceeded => close_protocol_violation
    _ => return None
  }
  match error {
    InvalidRequestId | DuplicateRequestId =>
      return Some(self.end_session(Local(code, b""), Some(code), b""))
    _ => ()
  }
  match event {
    StreamData(stream, _)
    | InboundStreamFinished(stream)
    | StreamReset(stream, _)
    | SendStopped(stream, _) => {
      let kind = self.inbound_stream_kinds.get(stream)
      if self.peer_control_stream == Some(stream) || kind is Some(Control) {
        Some(self.end_session(Local(code, b""), Some(code), b""))
      } else if self.namespace_subscription_streams.get(stream) is Some(_) ||
        self.inbound_namespace_subscriptions.get(stream) is Some(_) {
        Some(self.end_session(Local(code, b""), Some(code), b""))
      } else if self.request_stream_requires_cancel(stream) {
        match self.namespace_request_streams.get(stream) {
          Some(request_id) if self.namespace_requests.get(request_id).unwrap().locally_initiated =>
            Some(self.end_session(Local(code, b""), Some(code), b""))
          _ => Some(self.end_request_stream(stream, Cancel(code)))
        }
      } else if kind is Some(Request) {
        self.inbound_buffers.remove(stream)
        Some([AbortSend(stream, code), StopReceiving(stream, code)])
      } else if kind is Some(Subgroup) {
        let actions : Array[SessionAction] = []
        match self.incoming_object_streams.get(stream) {
          Some(state) => {
            match state.request_id {
              Some(request_id) => {
                let request = self.requests.get(request_id).unwrap()
                request.object_drain.note_stream_closed()
                match self.take_ready_publish_done(request_id) {
                  Some(done) =>
                    actions.push(EmitNotice(PublishDone(request_id, done)))
                  None => ()
                }
              }
              None => ()
            }
            self.incoming_object_streams.remove(stream)
          }
          None => ()
        }
        // Retain a tombstone until FIN/reset so already queued data is ignored.
        self.inbound_stream_kinds[stream] = Discarded
        actions.push(StopReceiving(stream, code))
        Some(actions)
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
) -> Array[SessionAction] {
  self.end_session(Local(code, reason), Some(code), reason)
}

///|
fn ClientSession::receive_control(
  self : ClientSession,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[SessionAction], ClientSessionError] {
  if !self.ready {
    match self.handshake.receive(stream, bytes) {
      Err(error) => Err(Handshake(error))
      Ok(Pending) => Ok([])
      Ok(Ready(setup, trailing)) => {
        self.peer_control_stream = Some(stream)
        self.ready = true
        let mut actions = [EmitNotice(HandshakeComplete(setup))]
        if !trailing.is_empty() {
          match self.decode_control_messages(trailing) {
            Ok(more) => actions = actions + more
            Err(error) => return Err(error)
          }
        }
        if self.state == Active {
          match self.resubscribe_desired() {
            Ok(restored) => actions = actions + restored
            Err(error) => return Err(error)
          }
        }
        Ok(actions)
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
) -> Result[Array[SessionAction], ClientSessionError] {
  if first.length() > 1024 * 1024 - self.control_buffer.length() {
    return Err(BufferLimitExceeded)
  }
  self.control_buffer = self.control_buffer + first
  let actions : Array[SessionAction] = []
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
            actions.push(EmitNotice(ControlMessageReceived(message)))
            for action in self.receive_control_goaway(goaway) {
              actions.push(action)
            }
          }
          _ => actions.push(EmitNotice(ControlMessageReceived(message)))
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
pub extend ClientStreamDirection with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ClientStreamDirection with Eq::{equal, not_equal}

///|
pub extend ClientStreamPurpose with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ClientStreamPurpose with Eq::{equal, not_equal}

///|
pub extend SendCompletion with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend SendCompletion with Eq::{equal, not_equal}

///|
pub extend SessionEvent with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend SessionEvent with Eq::{equal, not_equal}

///|
pub extend SessionNotice with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend SessionNotice with Eq::{equal, not_equal}

///|
pub extend SessionAction with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend SessionAction with Eq::{equal, not_equal}

///|
pub extend ClientSessionError with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ClientSessionError with Eq::{equal, not_equal}
