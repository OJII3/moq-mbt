///|
/// Scripted transport for driver contract tests.
priv struct FakeTransport {
  open_acceptance : TransportAcceptance
  mut send_acceptance : TransportAcceptance
  mut sync_sends : Bool
  retry_streams : Array[Int64]
  mut datagram_acceptance : TransportAcceptance
  opens : Array[(@session.ClientStreamDirection, @session.ClientStreamPurpose)]
  sends : Array[(Int64, Bytes, Bool)]
  datagrams : Array[Bytes]
  aborts : Array[(Int64, UInt64)]
  open_callbacks : Array[(TransportOpenOutcome) -> Unit]
  send_callbacks : Array[(Int64, (TransportWriteOutcome) -> Unit)]
  datagram_callbacks : Array[(TransportWriteOutcome) -> Unit]
}

///|
fn FakeTransport::new() -> FakeTransport {
  {
    open_acceptance: Accepted,
    send_acceptance: Accepted,
    sync_sends: false,
    retry_streams: [],
    datagram_acceptance: Accepted,
    opens: [],
    sends: [],
    datagrams: [],
    aborts: [],
    open_callbacks: [],
    send_callbacks: [],
    datagram_callbacks: [],
  }
}

///|
impl SessionTransport for FakeTransport with fn open_stream(
  self,
  direction,
  purpose,
  on_done,
) {
  self.opens.push((direction, purpose))
  match self.open_acceptance {
    Accepted => self.open_callbacks.push(on_done)
    _ => ()
  }
  self.open_acceptance
}

///|
impl SessionTransport for FakeTransport with fn send(
  self,
  stream,
  bytes,
  fin,
  on_done,
) {
  self.sends.push((stream, bytes, fin))
  let acceptance = if self.retry_streams.contains(stream) {
    Retry
  } else {
    self.send_acceptance
  }
  match acceptance {
    Accepted =>
      if self.sync_sends {
        on_done(Written)
      } else {
        self.send_callbacks.push((stream, on_done))
      }
    _ => ()
  }
  acceptance
}

///|
impl SessionTransport for FakeTransport with fn send_datagram(
  self,
  bytes,
  on_done,
) {
  self.datagrams.push(bytes)
  match self.datagram_acceptance {
    Accepted => self.datagram_callbacks.push(on_done)
    _ => ()
  }
  self.datagram_acceptance
}

///|
impl SessionTransport for FakeTransport with fn abort_send(self, stream, code) {
  self.aborts.push((stream, code))
}

///|
impl SessionTransport for FakeTransport with fn stop_receiving(
  _self,
  _stream,
  _code,
) -> Unit {

}

///|
impl SessionTransport for FakeTransport with fn close_connection(
  _self,
  _code,
  _reason,
) -> Unit {

}

///|
fn FakeTransport::complete_open(
  self : FakeTransport,
  outcome : TransportOpenOutcome,
) -> Unit {
  if self.open_callbacks.length() > 0 {
    let callback = self.open_callbacks.remove(0)
    callback(outcome)
  }
}

///|
fn FakeTransport::complete_send(
  self : FakeTransport,
  stream : Int64,
  outcome : TransportWriteOutcome,
) -> Unit {
  let mut index = 0
  while index < self.send_callbacks.length() {
    let (candidate, callback) = self.send_callbacks[index]
    if candidate == stream {
      ignore(self.send_callbacks.remove(index))
      callback(outcome)
      return
    }
    index = index + 1
  }
  abort("no pending send callback for stream \{stream}")
}

///|
fn driver_session() -> @session.ClientSession {
  @session.ClientSession::new(@session.Draft18, { options: [], }).unwrap()
}

///|
fn driver_config(
  opens : Int32,
  sends : Int32,
  datagrams : Int32,
) -> SessionDriverConfig {
  {
    max_in_flight_opens: opens,
    max_in_flight_sends: sends,
    max_in_flight_datagrams: datagrams,
  }
}

///|
test "driver retries a refused send in order until it is accepted" {
  let transport = FakeTransport::new()
  transport.send_acceptance = Retry
  let driver = SessionDriver::new(
    transport,
    driver_session(),
    driver_config(8, 8, 8),
  )
  driver.dispatch([
    @session.SendChunk(4L, b"first"),
    @session.SendChunk(5L, b"other"),
  ])
  assert_eq(transport.sends.length(), 2)
  assert_eq(driver.pending_action_count(), 2)
  transport.send_acceptance = Accepted
  driver.pump()
  assert_eq(driver.pending_action_count(), 0)
  assert_eq(transport.sends.length(), 4)
  assert_eq(transport.sends[2].0, 4L)
  assert_eq(transport.sends[3].0, 5L)
}

///|
test "driver keeps per-stream order after a temporary refusal" {
  let transport = FakeTransport::new()
  let driver = SessionDriver::new(
    transport,
    driver_session(),
    driver_config(8, 8, 8),
  )
  transport.retry_streams.push(4L)
  driver.dispatch([
    @session.SendChunk(4L, b"first"),
    @session.SendChunk(4L, b"second"),
  ])
  // The second action on the refused stream is not attempted out of order.
  assert_eq(transport.sends.length(), 1)
  assert_eq(driver.pending_action_count(), 2)
  driver.dispatch([@session.SendChunk(5L, b"other")])
  // Stream 4 is still blocked, but stream 5 is accepted independently.
  assert_eq(transport.sends.length(), 3)
  assert_eq(transport.sends[2].0, 5L)
  assert_eq(driver.pending_action_count(), 2)
}

///|
test "driver drops queued sends for an aborted stream" {
  let transport = FakeTransport::new()
  transport.send_acceptance = Retry
  let driver = SessionDriver::new(
    transport,
    driver_session(),
    driver_config(8, 8, 8),
  )
  driver.dispatch([
    @session.SendChunk(4L, b"queued"),
    @session.AbortSend(4L, 7UL),
    @session.SendChunk(5L, b"kept"),
  ])
  assert_eq(transport.aborts, [(4L, 7UL)])
  assert_eq(driver.pending_action_count(), 1)
  transport.send_acceptance = Accepted
  driver.pump()
  assert_eq(transport.sends.length(), 3)
  assert_eq(transport.sends[2].0, 5L)
}

///|
test "driver respects the configured in-flight send limit" {
  let transport = FakeTransport::new()
  let driver = SessionDriver::new(
    transport,
    driver_session(),
    driver_config(8, 1, 8),
  )
  driver.dispatch([
    @session.SendChunk(4L, b"first"),
    @session.SendChunk(5L, b"second"),
  ])
  assert_eq(transport.sends.length(), 1)
  assert_eq(driver.pending_action_count(), 1)
  transport.complete_send(4L, Written)
  assert_eq(transport.sends.length(), 2)
  assert_eq(driver.pending_action_count(), 0)
}

///|
test "driver reports an opened stream and forwards its purpose" {
  let transport = FakeTransport::new()
  let session = driver_session()
  let driver = SessionDriver::new(transport, session, driver_config(8, 8, 8))
  driver.dispatch(session.start().unwrap())
  assert_eq(transport.opens.length(), 1)
  assert_eq(transport.opens[0].1, @session.Control)
  transport.complete_open(Opened(10L))
  assert_eq(driver.take_opened_streams(), [(@session.Control, 10L)])
  assert_eq(transport.sends.length(), 1)
  assert_eq(transport.sends[0].0, 10L)
}

///|
test "driver terminates on an asynchronous stream open failure" {
  let transport = FakeTransport::new()
  let session = driver_session()
  let driver = SessionDriver::new(transport, session, driver_config(8, 8, 8))
  driver.dispatch(session.start().unwrap())
  transport.complete_open(OpenFailed(-10L))
  assert_true(driver.failed())
  assert_eq(driver.take_notices(), [
    @session.SessionEnded(@session.Transport(-10L)),
  ])
}

///|
test "driver terminates on an asynchronous stream write failure" {
  let transport = FakeTransport::new()
  let driver = SessionDriver::new(
    transport,
    driver_session(),
    driver_config(8, 8, 8),
  )
  driver.dispatch([@session.SendChunk(4L, b"payload")])
  transport.complete_send(4L, WriteFailed(-11L))
  assert_true(driver.failed())
  assert_eq(driver.take_notices(), [
    @session.SessionEnded(@session.Transport(-11L)),
  ])
}

///|
test "driver treats a transport failure event as terminal" {
  let driver = SessionDriver::new(
    FakeTransport::new(),
    driver_session(),
    driver_config(8, 8, 8),
  )
  driver.receive(@session.TransportFailed(-12L))
  assert_true(driver.failed())
  assert_eq(driver.take_notices(), [
    @session.SessionEnded(@session.Transport(-12L)),
  ])
}

///|
test "driver turns a fatal transport result into a session end" {
  let transport = FakeTransport::new()
  transport.send_acceptance = Fatal(-9L)
  let driver = SessionDriver::new(
    transport,
    driver_session(),
    driver_config(8, 8, 8),
  )
  driver.dispatch([@session.SendChunk(4L, b"payload")])
  assert_true(driver.failed())
  assert_eq(driver.take_notices(), [
    @session.SessionEnded(@session.Transport(-9L)),
  ])
}

///|
test "driver retries datagrams independently of sends" {
  let transport = FakeTransport::new()
  transport.datagram_acceptance = Retry
  let driver = SessionDriver::new(
    transport,
    driver_session(),
    driver_config(8, 8, 8),
  )
  driver.dispatch([
    @session.SendDatagram(0UL, b"datagram"),
    @session.SendChunk(4L, b"payload"),
  ])
  assert_eq(transport.datagrams.length(), 1)
  assert_eq(transport.sends.length(), 1)
  assert_eq(driver.pending_action_count(), 1)
}

///|
test "driver caps datagrams independently" {
  let transport = FakeTransport::new()
  let driver = SessionDriver::new(
    transport,
    driver_session(),
    driver_config(8, 8, 1),
  )
  driver.dispatch([
    @session.SendDatagram(0UL, b"one"),
    @session.SendDatagram(1UL, b"two"),
  ])
  assert_eq(transport.datagrams.length(), 1)
  assert_eq(driver.pending_action_count(), 1)
  transport.datagram_callbacks[0](Written)
  assert_eq(transport.datagrams.length(), 2)
  assert_eq(driver.pending_action_count(), 0)
}

///|
test "driver reports datagram write failure without failing the session" {
  let transport = FakeTransport::new()
  let driver = SessionDriver::new(
    transport,
    driver_session(),
    driver_config(8, 8, 8),
  )
  driver.dispatch([@session.SendDatagram(7UL, b"payload")])
  assert_eq(transport.datagrams.length(), 1)
  transport.datagram_callbacks[0](WriteFailed(-4L))
  assert_false(driver.failed())
  assert_eq(driver.take_notices(), [@session.DatagramLocalWriteFailed(7UL, -4L)])
}

///|
test "driver releases the in-flight slot of a synchronous send" {
  let transport = FakeTransport::new()
  transport.sync_sends = true
  let driver = SessionDriver::new(
    transport,
    driver_session(),
    driver_config(8, 8, 8),
  )
  driver.dispatch([@session.SendChunk(4L, b"payload")])
  assert_eq(driver.pending_action_count(), 0)
  assert_false(driver.has_pending_sends(4L))
}
