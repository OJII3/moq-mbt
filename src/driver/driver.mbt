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
/// Local completion of a stream write or datagram send, not peer delivery.
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
/// Reason a `SessionDriver` entered its terminal failure state.
pub(all) enum SessionDriverFailureReason {
  SessionError(@session.ClientSessionError)
  Transport(Int64)
  PendingOverflow
} derive(Debug, Eq)

///|
/// Transport operations required by `SessionDriver`.
///
/// Implementations must invoke the completion callback exactly once for every
/// `Accepted` action, either before returning or later. They must not invoke it
/// for `Retry` or `Fatal`. `SessionDriver` is single-thread confined: its public
/// methods and completion callbacks must run serially on the same thread. A
/// transport that receives a completion on another thread must marshal the
/// callback invocation back to the driver's thread.
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
/// Maximum number of transport-accepted operations awaiting completion.
/// In-flight limits may be disabled with zero. Pending action and byte limits
/// must be positive; control actions are exempt so aborts remain available.
/// Stream writes are divided into chunks of at most max_send_chunk_bytes.
pub(all) struct SessionDriverConfig {
  max_in_flight_opens : Int32
  max_in_flight_sends : Int32
  max_in_flight_datagrams : Int32
  max_pending_actions : Int32
  max_pending_bytes : Int64
  max_send_chunk_bytes : Int32
} derive(Debug, Eq)

///|
pub(all) enum DriverDispatchResult {
  Accepted
  QueueFull
  TooLarge
  DriverFailed
} derive(Debug, Eq)

///|
priv struct PendingAction {
  action : @session.SessionAction
  offset : Int32
  state : PendingState
}

///|
priv struct PendingState {
  mut failed : Bool
}

///|
/// Portable pump for `SessionAction` values.
///
/// The driver tracks transport-accepted operations in flight, retries
/// temporarily refused actions without blocking unrelated streams, and turns
/// transport completions back into session events. Pending data is bounded
/// by the configured action and byte limits. All calls,
/// including transport completion callbacks, must be serialized on one thread.
/// Adapters only implement `SessionTransport`; they do not match on
/// `SessionAction`.
pub struct SessionDriver[T] {
  priv transport : T
  priv session : @session.ClientSession
  priv config : SessionDriverConfig
  priv mut pending : Array[PendingAction]
  priv mut now_ms : UInt64
  priv mut clock : (() -> UInt64)?
  priv mut events : Array[@session.SessionEvent]
  priv mut notices : Array[@session.SessionNotice]
  priv mut opened_streams : Array[
    (@session.ClientStreamPurpose, @session.StreamId),
  ]
  priv mut in_flight_opens : Int32
  priv mut in_flight_sends : Int32
  priv send_streams : Map[Int64, Int32]
  priv mut in_flight_datagrams : Int32
  priv blocked_streams : Array[Int64]
  priv chunk_blocked_streams : Array[Int64]
  priv mut blocked_datagrams : Bool
  priv mut blocked_opens : Bool
  priv mut pumping : Bool
  priv mut repump : Bool
  priv mut failed : Bool
  priv mut failure_reason : SessionDriverFailureReason?
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
    now_ms: 0UL,
    clock: None,
    events: [],
    notices: [],
    opened_streams: [],
    in_flight_opens: 0,
    in_flight_sends: 0,
    send_streams: Map([]),
    in_flight_datagrams: 0,
    blocked_streams: [],
    chunk_blocked_streams: [],
    blocked_datagrams: false,
    blocked_opens: false,
    pumping: false,
    repump: false,
    failed: false,
    failure_reason: None,
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
/// The reason the driver entered its terminal failure state, if any.
pub fn[T] SessionDriver::failure(
  self : SessionDriver[T],
) -> SessionDriverFailureReason? {
  self.failure_reason
}

///|
/// Atomically accept and enqueue a complete action batch, then pump it through
/// the transport. `Accepted` means the batch was admitted, not that any bytes
/// reached the peer. When a transport asks for `Retry`, retain the entire batch
/// and dispatch it again after progress; never repeat the session operation
/// that produced it. `TooLarge` cannot succeed even when the queue is empty.
/// Control-only batches remain admissible when the data queue is full.
pub fn[T : SessionTransport] SessionDriver::dispatch(
  self : SessionDriver[T],
  actions : Array[@session.SessionAction],
) -> DriverDispatchResult {
  if self.failed {
    return DriverFailed
  }
  let (count, bytes) = action_size(actions)
  if count > self.config.max_pending_actions ||
    bytes > self.config.max_pending_bytes {
    return TooLarge
  }
  let existing = self.pending.map(fn(item) { item.action })
  let (pending_count, pending_bytes) = action_size(existing)
  if count + pending_count > self.config.max_pending_actions ||
    bytes + pending_bytes > self.config.max_pending_bytes {
    return QueueFull
  }
  self.append_actions(actions)
  self.pump()
  Accepted
}

///|
fn action_size(actions : Array[@session.SessionAction]) -> (Int32, Int64) {
  let mut count = 0
  let mut bytes = 0L
  for action in actions {
    match action {
      @session.SendChunk(_, data)
      | @session.SendFin(_, data)
      | @session.SendObjectChunk(_, data, _)
      | @session.SendDatagram(_, data)
      | @session.SendObjectDatagram(_, data, _) => {
        count = count + 1
        bytes = bytes + data.length().to_int64()
      }
      @session.OpenStream(_, _) => count = count + 1
      _ => ()
    }
  }
  (count, bytes)
}

///|
fn[T] SessionDriver::append_actions(
  self : SessionDriver[T],
  actions : Array[@session.SessionAction],
) -> Unit {
  for action in actions {
    self.pending.push({ action, offset: 0, state: { failed: false, }, })
  }
}

///|
/// Update the monotonic time used to expire queued object writes.
pub fn[T] SessionDriver::set_now_ms(
  self : SessionDriver[T],
  now_ms : UInt64,
) -> Unit {
  self.now_ms = now_ms
}

///|
/// Supply the monotonic clock. It is read immediately before queued writes,
/// including writes resumed by completion callbacks.
pub fn[T] SessionDriver::set_clock(
  self : SessionDriver[T],
  clock : () -> UInt64,
) -> Unit {
  self.clock = Some(clock)
}

///|
fn[T] SessionDriver::current_time(self : SessionDriver[T]) -> UInt64 {
  match self.clock {
    Some(clock) => clock()
    None => self.now_ms
  }
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
) -> Array[(@session.ClientStreamPurpose, @session.StreamId)] {
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
    self.chunk_blocked_streams.clear()
    self.deliver_events()
    if !self.failed {
      self.attempt_pending()
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
  let remaining : Array[PendingAction] = []
  let aborted_streams : Array[Int64] = []
  // Cancellation takes precedence over data already waiting in the queue.
  for item in self.pending {
    if item.action is @session.AbortSend(stream, _) {
      aborted_streams.push(stream)
    }
  }
  let mut index = 0
  while index < self.pending.length() && !self.failed {
    // Process synchronous completions before attempting another action. They
    // can terminate the session or cancel a stream.
    if !self.events.is_empty() {
      self.repump = true
      break
    }
    let item = self.pending[index]
    let action = item.action
    index = index + 1
    if item.state.failed {
      continue
    }
    match action {
      @session.OpenStream(direction, purpose) =>
        if self.blocked_opens || self.at_open_limit() {
          remaining.push(item)
        } else {
          self.in_flight_opens = self.in_flight_opens + 1
          let accepted = self.transport.open_stream(direction, purpose, fn(
            outcome,
          ) {
            self.note_open(purpose, outcome)
          })
          match accepted {
            Accepted => self.repump = true
            Retry => {
              self.in_flight_opens = self.in_flight_opens - 1
              self.blocked_opens = true
              remaining.push(item)
            }
            Fatal(status) => {
              self.in_flight_opens = self.in_flight_opens - 1
              self.fail(status)
            }
          }
        }
      @session.SendChunk(stream, bytes)
      | @session.SendFin(stream, bytes)
      | @session.SendObjectChunk(stream, bytes, _) =>
        if aborted_streams.contains(stream) {
          self.repump = true
        } else if action is @session.SendObjectChunk(_, _, deadline) &&
          self.current_time() > deadline {
          aborted_streams.push(stream)
          self.events.push(@session.SendStopped(stream, 2UL))
          self.repump = true
        } else if self.blocked_streams.contains(stream) ||
          self.chunk_blocked_streams.contains(stream) ||
          self.at_send_limit() ||
          (
            bytes.length() > self.config.max_send_chunk_bytes &&
            self.has_pending_sends(stream)
          ) {
          remaining.push(item)
          self.chunk_blocked_streams.push(stream)
        } else {
          let fin = match action {
            @session.SendFin(_, _) => true
            _ => false
          }
          let end = Int32::min(
            item.offset + Int32::max(1, self.config.max_send_chunk_bytes),
            bytes.length(),
          )
          let last = end == bytes.length()
          let chunk = bytes[item.offset:end].to_owned()
          self.in_flight_sends = self.in_flight_sends + 1
          self.note_send_started(stream)
          let accepted = self.transport.send(stream, chunk, fin && last, fn(
            outcome,
          ) {
            if outcome != Written {
              item.state.failed = true
            }
            self.note_send(
              stream,
              if outcome == Written && !last {
                Abandoned
              } else {
                outcome
              },
            )
          })
          match accepted {
            Accepted => {
              if !last {
                remaining.push({ action, offset: end, state: item.state, })
                self.chunk_blocked_streams.push(stream)
              }
              self.repump = true
            }
            Retry => {
              self.in_flight_sends = self.in_flight_sends - 1
              self.note_send_settled(stream)
              self.blocked_streams.push(stream)
              remaining.push(item)
            }
            Fatal(status) => {
              self.in_flight_sends = self.in_flight_sends - 1
              self.note_send_settled(stream)
              self.fail(status)
            }
          }
        }
      @session.SendDatagram(token, bytes)
      | @session.SendObjectDatagram(token, bytes, _) =>
        if action is @session.SendObjectDatagram(_, _, deadline) &&
          self.current_time() > deadline {
          self.events.push(@session.DatagramSendComplete(token))
          self.repump = true
        } else if self.blocked_datagrams || self.at_datagram_limit() {
          remaining.push(item)
        } else {
          self.in_flight_datagrams = self.in_flight_datagrams + 1
          let accepted = self.transport.send_datagram(bytes, fn(outcome) {
            self.note_datagram(token, outcome)
          })
          match accepted {
            Accepted => self.repump = true
            Retry => {
              self.in_flight_datagrams = self.in_flight_datagrams - 1
              self.blocked_datagrams = true
              remaining.push(item)
            }
            Fatal(status) => {
              self.in_flight_datagrams = self.in_flight_datagrams - 1
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
  while index < self.pending.length() {
    remaining.push(self.pending[index])
    index = index + 1
  }
  let filtered : Array[PendingAction] = []
  for item in remaining {
    match item.action {
      @session.SendChunk(stream, _)
      | @session.SendFin(stream, _)
      | @session.SendObjectChunk(stream, _, _) if aborted_streams.contains(
          stream,
        ) => self.repump = true
      _ => filtered.push(item)
    }
  }
  self.pending = filtered
}

///|
fn[T : SessionTransport] SessionDriver::deliver_events(
  self : SessionDriver[T],
) -> Unit {
  while self.events.length() > 0 && !self.failed {
    let event = self.events.remove(0)
    match event {
      @session.TransportFailed(status) => self.fail(status)
      _ =>
        match self.session.receive(event) {
          Ok(actions) =>
            if actions.length() > 0 {
              let existing = self.pending.map(fn(item) { item.action })
              let (count, bytes) = action_size(existing + actions)
              if count > self.config.max_pending_actions ||
                bytes > self.config.max_pending_bytes {
                self.failure_reason = Some(PendingOverflow)
                self.transport.close_connection(
                  1UL, b"driver pending queue overflow",
                )
                self.abort()
              } else {
                self.append_actions(actions)
              }
              self.repump = true
            }
          Err(error) => {
            self.failure_reason = Some(SessionError(error))
            self.abort()
          }
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
  if self.in_flight_opens > 0 {
    self.in_flight_opens = self.in_flight_opens - 1
  }
  if self.failed {
    return
  }
  match outcome {
    Opened(stream) => {
      self.opened_streams.push((purpose, @session.StreamId(stream)))
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
  if self.in_flight_sends > 0 {
    self.in_flight_sends = self.in_flight_sends - 1
  }
  self.note_send_settled(stream)
  if self.failed {
    return
  }
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
  if self.in_flight_datagrams > 0 {
    self.in_flight_datagrams = self.in_flight_datagrams - 1
  }
  if self.failed {
    return
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
  self.config.max_in_flight_opens > 0 &&
  self.in_flight_opens >= self.config.max_in_flight_opens
}

///|
fn[T] SessionDriver::at_send_limit(self : SessionDriver[T]) -> Bool {
  self.config.max_in_flight_sends > 0 &&
  self.in_flight_sends >= self.config.max_in_flight_sends
}

///|
fn[T] SessionDriver::at_datagram_limit(self : SessionDriver[T]) -> Bool {
  self.config.max_in_flight_datagrams > 0 &&
  self.in_flight_datagrams >= self.config.max_in_flight_datagrams
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
  self.failure_reason = Some(Transport(status))
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
pub extend SessionDriverFailureReason with @moonbitlang/core/debug.Debug::{
  to_repr,
}

///|
pub extend SessionDriverFailureReason with Eq::{equal, not_equal}

///|
pub extend SessionDriverConfig with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend SessionDriverConfig with Eq::{equal, not_equal}

///|
pub extend DriverDispatchResult with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend DriverDispatchResult with Eq::{equal, not_equal}
