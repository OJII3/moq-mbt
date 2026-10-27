///|
/// Result of handing one session action to a transport.
///
/// `Accepted` means the transport took ownership of the action and will report
/// its completion through the callback passed with the action. `Retry` means
/// the action was not taken and must be attempted again later, usually because
/// the transport queue is full. `Fatal` ends the session with `TransportFailed`.
pub(all) enum TransportAcceptance {
  Accepted
  Retry
  Fatal(Int64)
} derive(Debug, Eq)

///|
pub(all) enum TransportOpenOutcome {
  Opened(Int64)
  OpenFailed(Int64)
} derive(Debug, Eq)

///|
/// Completion of a stream write or datagram send.
///
/// `Abandoned` reports that the transport will never report this send again,
/// for example after a local abort. It only releases the driver's in-flight
/// slot and does not produce a session event.
pub(all) enum TransportWriteOutcome {
  Written
  WriteCanceled
  WriteReset(UInt64)
  WriteFailed(Int64)
  Abandoned
} derive(Debug, Eq)

///|
/// Transport operations required by `SessionDriver`.
///
/// Implementations must invoke the completion callback exactly once for every
/// `Accepted` action, either before returning or later. They must not invoke it
/// for `Retry` or `Fatal`. The callback may run from a transport thread or
/// promise; the driver serializes the resulting session events.
pub(open) trait SessionTransport {
  fn open_stream(
    Self,
    @session.ClientStreamDirection,
    @session.ClientStreamPurpose,
    (TransportOpenOutcome) -> Unit,
  ) -> TransportAcceptance
  fn send(Self, Int64, Bytes, Bool, (TransportWriteOutcome) -> Unit) -> TransportAcceptance
  fn send_datagram(Self, Bytes, (TransportWriteOutcome) -> Unit) -> TransportAcceptance
  fn abort_send(Self, Int64, UInt64) -> Unit
  fn stop_receiving(Self, Int64, UInt64) -> Unit
  fn close_connection(Self, UInt64, Bytes) -> Unit
}

///|
/// Queue limits injected by the adapter. A value of zero or less disables the
/// limit for that action kind.
pub(all) struct SessionDriverConfig {
  max_pending_opens : Int32
  max_pending_sends : Int32
  max_pending_datagrams : Int32
} derive(Debug, Eq)

///|
/// Portable pump for `SessionAction` values.
///
/// The driver keeps accepted actions in flight, retries temporarily refused
/// actions without blocking unrelated streams, and turns transport completions
/// back into session events. Adapters only implement `SessionTransport`; they
/// do not match on `SessionAction`.
pub struct SessionDriver[T] {
  priv transport : T
  priv session : @session.ClientSession
  priv config : SessionDriverConfig
  priv mut pending : Array[@session.SessionAction]
  priv mut events : Array[@session.SessionEvent]
  priv mut notices : Array[@session.SessionNotice]
  priv mut opened_streams : Array[(@session.ClientStreamPurpose, Int64)]
  priv mut pending_opens : Int32
  priv mut pending_sends : Int32
  priv send_streams : Map[Int64, Int32]
  priv mut pending_datagrams : Int32
  priv blocked_streams : Array[Int64]
  priv mut blocked_datagrams : Bool
  priv mut blocked_opens : Bool
  priv mut pumping : Bool
  priv mut repump : Bool
  priv mut failed : Bool
  priv mut failure : @session.ClientSessionError?
  priv mut update_handler : (() -> Unit)?
}

///|
pub fn[T] SessionDriver::new(
  transport : T,
  session : @session.ClientSession,
  config : SessionDriverConfig,
) -> SessionDriver[T] {
  {
    transport,
    session,
    config,
    pending: [],
    events: [],
    notices: [],
    opened_streams: [],
    pending_opens: 0,
    pending_sends: 0,
    send_streams: Map([]),
    pending_datagrams: 0,
    blocked_streams: [],
    blocked_datagrams: false,
    blocked_opens: false,
    pumping: false,
    repump: false,
    failed: false,
    failure: None,
    update_handler: None,
  }
}

///|
/// Register a callback invoked after the driver changes its notices, opened
/// streams, or failure state. Adapters use it to wake pending waiters.
pub fn[T] SessionDriver::set_update_handler(
  self : SessionDriver[T],
  handler : () -> Unit,
) -> Unit {
  self.update_handler = Some(handler)
}

///|
pub fn[T] SessionDriver::session(
  self : SessionDriver[T],
) -> @session.ClientSession {
  self.session
}

///|
pub fn[T] SessionDriver::failed(self : SessionDriver[T]) -> Bool {
  self.failed
}

///|
/// The session error that stopped the driver, if any.
pub fn[T] SessionDriver::failure(
  self : SessionDriver[T],
) -> @session.ClientSessionError? {
  self.failure
}

///|
/// Enqueue actions returned by a public `ClientSession` operation and pump
/// them through the transport.
pub fn[T : SessionTransport] SessionDriver::dispatch(
  self : SessionDriver[T],
  actions : Array[@session.SessionAction],
) -> Unit {
  if self.failed {
    return
  }
  self.pending = self.pending + actions
  self.pump()
}

///|
/// Feed a transport event to the session and pump the resulting actions.
pub fn[T : SessionTransport] SessionDriver::receive(
  self : SessionDriver[T],
  event : @session.SessionEvent,
) -> Unit {
  self.enqueue_event(event)
}

///|
/// Drain session notices produced since the last call.
pub fn[T] SessionDriver::take_notices(
  self : SessionDriver[T],
) -> Array[@session.SessionNotice] {
  let notices = self.notices
  self.notices = []
  notices
}

///|
/// Drain streams opened since the last call. The transport reference is
/// intentionally not exposed here; it stays opaque to the session.
pub fn[T] SessionDriver::take_opened_streams(
  self : SessionDriver[T],
) -> Array[(@session.ClientStreamPurpose, Int64)] {
  let opened = self.opened_streams
  self.opened_streams = []
  opened
}

///|
/// Whether a send accepted for this stream has not completed yet.
pub fn[T] SessionDriver::has_pending_sends(
  self : SessionDriver[T],
  stream : Int64,
) -> Bool {
  match self.send_streams.get(stream) {
    Some(count) => count > 0
    None => false
  }
}

///|
/// Number of actions still waiting for transport acceptance. Exposed for
/// adapter diagnostics and tests.
pub fn[T] SessionDriver::pending_action_count(self : SessionDriver[T]) -> Int32 {
  self.pending.length()
}

///|
fn[T] SessionDriver::notify_update(self : SessionDriver[T]) -> Unit {
  match self.update_handler {
    Some(handler) => handler()
    None => ()
  }
}

///|
fn[T : SessionTransport] SessionDriver::enqueue_event(
  self : SessionDriver[T],
  event : @session.SessionEvent,
) -> Unit {
  if self.failed {
    return
  }
  self.events.push(event)
  self.pump()
}

///|

///|
/// Retry actions that were temporarily refused by the transport. Adapters
/// call this after transport progress that did not arrive as a session event.
pub fn[T : SessionTransport] SessionDriver::pump(
  self : SessionDriver[T],
) -> Unit {
  if self.failed || self.pumping {
    return
  }
  self.pumping = true
  self.blocked_streams.clear()
  self.blocked_datagrams = false
  self.blocked_opens = false
  while true {
    self.repump = false
    self.attempt_pending()
    if !self.failed {
      self.deliver_events()
    }
    if self.failed || !self.repump {
      break
    }
  }
  self.pumping = false
  self.notify_update()
}

///|
fn[T : SessionTransport] SessionDriver::attempt_pending(
  self : SessionDriver[T],
) -> Unit {
  let remaining : Array[@session.SessionAction] = []
  let aborted_streams : Array[Int64] = []
  let mut index = 0
  while index < self.pending.length() && !self.failed {
    let action = self.pending[index]
    index = index + 1
    match action {
      @session.OpenStream(direction, purpose) =>
        if self.blocked_opens || self.at_open_limit() {
          remaining.push(action)
        } else {
          self.pending_opens = self.pending_opens + 1
          let accepted = self.transport.open_stream(direction, purpose, fn(
            outcome,
          ) {
            self.note_open(purpose, outcome)
          })
          match accepted {
            Accepted => self.repump = true
            Retry => {
              self.pending_opens = self.pending_opens - 1
              self.blocked_opens = true
              remaining.push(action)
            }
            Fatal(status) => {
              self.pending_opens = self.pending_opens - 1
              self.fail(status)
            }
          }
        }
      @session.SendChunk(stream, bytes) | @session.SendFin(stream, bytes) =>
        if self.blocked_streams.contains(stream) || self.at_send_limit() {
          remaining.push(action)
        } else {
          let fin = match action {
            @session.SendFin(_, _) => true
            _ => false
          }
          self.pending_sends = self.pending_sends + 1
          self.note_send_started(stream)
          let accepted = self.transport.send(stream, bytes, fin, fn(outcome) {
            self.note_send(stream, outcome)
          })
          match accepted {
            Accepted => self.repump = true
            Retry => {
              self.pending_sends = self.pending_sends - 1
              self.note_send_settled(stream)
              self.blocked_streams.push(stream)
              remaining.push(action)
            }
            Fatal(status) => {
              self.pending_sends = self.pending_sends - 1
              self.note_send_settled(stream)
              self.fail(status)
            }
          }
        }
      @session.SendDatagram(token, bytes) =>
        if self.blocked_datagrams || self.at_datagram_limit() {
          remaining.push(action)
        } else {
          self.pending_datagrams = self.pending_datagrams + 1
          let accepted = self.transport.send_datagram(bytes, fn(outcome) {
            self.note_datagram(token, outcome)
          })
          match accepted {
            Accepted => self.repump = true
            Retry => {
              self.pending_datagrams = self.pending_datagrams - 1
              self.blocked_datagrams = true
              remaining.push(action)
            }
            Fatal(status) => {
              self.pending_datagrams = self.pending_datagrams - 1
              self.fail(status)
            }
          }
        }
      @session.AbortSend(stream, code) => {
        aborted_streams.push(stream)
        self.transport.abort_send(stream, code)
        self.repump = true
      }
      @session.StopReceiving(stream, code) => {
        self.transport.stop_receiving(stream, code)
        self.repump = true
      }
      @session.CloseConnection(code, reason) => {
        self.transport.close_connection(code, reason)
        self.repump = true
      }
      @session.EmitNotice(notice) => {
        self.notices.push(notice)
        self.repump = true
      }
    }
  }
  if self.failed {
    self.pending = []
    return
  }
  let filtered : Array[@session.SessionAction] = []
  for action in remaining {
    match action {
      @session.SendChunk(stream, _) | @session.SendFin(stream, _) if aborted_streams.contains(
          stream,
        ) => self.repump = true
      _ => filtered.push(action)
    }
  }
  self.pending = filtered
}

///|
fn[T] SessionDriver::deliver_events(self : SessionDriver[T]) -> Unit {
  while self.events.length() > 0 && !self.failed {
    let event = self.events.remove(0)
    match self.session.receive(event) {
      Ok(actions) =>
        if actions.length() > 0 {
          self.pending = self.pending + actions
          self.repump = true
        }
      Err(error) => {
        self.failure = Some(error)
        self.abort()
      }
    }
  }
}

///|
fn[T : SessionTransport] SessionDriver::note_open(
  self : SessionDriver[T],
  purpose : @session.ClientStreamPurpose,
  outcome : TransportOpenOutcome,
) -> Unit {
  if self.pending_opens > 0 {
    self.pending_opens = self.pending_opens - 1
  }
  match outcome {
    Opened(stream) => {
      self.opened_streams.push((purpose, stream))
      self.events.push(@session.StreamOpened(purpose, stream))
    }
    OpenFailed(status) => self.events.push(@session.TransportFailed(status))
  }
  if !self.pumping {
    self.pump()
  }
}

///|
fn[T : SessionTransport] SessionDriver::note_send(
  self : SessionDriver[T],
  stream : Int64,
  outcome : TransportWriteOutcome,
) -> Unit {
  if self.pending_sends > 0 {
    self.pending_sends = self.pending_sends - 1
  }
  self.note_send_settled(stream)
  match outcome {
    Written => self.events.push(@session.SendCompleted(stream, @session.Sent))
    WriteCanceled =>
      self.events.push(@session.SendCompleted(stream, @session.Reset))
    WriteReset(code) => self.events.push(@session.StreamReset(stream, code))
    WriteFailed(status) => self.events.push(@session.TransportFailed(status))
    Abandoned => ()
  }
  if !self.pumping {
    self.pump()
  }
}

///|
fn[T] SessionDriver::note_send_started(
  self : SessionDriver[T],
  stream : Int64,
) -> Unit {
  let count = match self.send_streams.get(stream) {
    Some(value) => value
    None => 0
  }
  self.send_streams[stream] = count + 1
}

///|
fn[T] SessionDriver::note_send_settled(
  self : SessionDriver[T],
  stream : Int64,
) -> Unit {
  match self.send_streams.get(stream) {
    Some(count) if count > 1 => self.send_streams[stream] = count - 1
    Some(_) => self.send_streams.remove(stream)
    None => ()
  }
}

///|
fn[T : SessionTransport] SessionDriver::note_datagram(
  self : SessionDriver[T],
  token : UInt64,
  outcome : TransportWriteOutcome,
) -> Unit {
  if self.pending_datagrams > 0 {
    self.pending_datagrams = self.pending_datagrams - 1
  }
  match outcome {
    Written => self.events.push(@session.DatagramSendComplete(token))
    WriteCanceled => self.events.push(@session.DatagramSendFailed(token, 0))
    WriteReset(code) =>
      self.events.push(
        @session.DatagramSendFailed(token, code.reinterpret_as_int64()),
      )
    WriteFailed(status) =>
      self.events.push(@session.DatagramSendFailed(token, status))
    Abandoned => self.events.push(@session.DatagramSendComplete(token))
  }
  if !self.pumping {
    self.pump()
  }
}

///|
fn[T] SessionDriver::at_open_limit(self : SessionDriver[T]) -> Bool {
  self.config.max_pending_opens > 0 &&
  self.pending_opens >= self.config.max_pending_opens
}

///|
fn[T] SessionDriver::at_send_limit(self : SessionDriver[T]) -> Bool {
  self.config.max_pending_sends > 0 &&
  self.pending_sends >= self.config.max_pending_sends
}

///|
fn[T] SessionDriver::at_datagram_limit(self : SessionDriver[T]) -> Bool {
  self.config.max_pending_datagrams > 0 &&
  self.pending_datagrams >= self.config.max_pending_datagrams
}

///|
/// Stop without a transport event. Used when the session rejects an event.
fn[T] SessionDriver::abort(self : SessionDriver[T]) -> Unit {
  self.failed = true
  self.pending = []
  self.events = []
}

///|
/// Report an unrecoverable transport failure to the session and execute only
/// the terminal actions, since no further I/O can be accepted.
fn[T : SessionTransport] SessionDriver::fail(
  self : SessionDriver[T],
  status : Int64,
) -> Unit {
  if self.failed {
    return
  }
  self.failed = true
  self.pending = []
  self.events = []
  match self.session.receive(@session.TransportFailed(status)) {
    Ok(actions) =>
      for action in actions {
        match action {
          @session.EmitNotice(notice) => self.notices.push(notice)
          @session.CloseConnection(code, reason) =>
            self.transport.close_connection(code, reason)
          _ => ()
        }
      }
    Err(_) => ()
  }
}

///|
pub extend TransportAcceptance with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend TransportAcceptance with Eq::{equal, not_equal}

///|
pub extend TransportOpenOutcome with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend TransportOpenOutcome with Eq::{equal, not_equal}

///|
pub extend TransportWriteOutcome with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend TransportWriteOutcome with Eq::{equal, not_equal}

///|
pub extend SessionDriverConfig with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend SessionDriverConfig with Eq::{equal, not_equal}
