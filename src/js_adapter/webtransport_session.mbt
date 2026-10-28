///|
/// Browser WebTransport backend for the portable session driver.
priv struct WebTransportTransport {
  connection : WebTransportConnection
}

///|
impl @driver.SessionTransport for WebTransportTransport with fn open_stream(
  self,
  direction,
  _purpose,
  on_done,
) {
  let opening = match direction {
    @session.Unidirectional => self.connection.open_unidirectional()
    @session.Bidirectional => self.connection.open_bidirectional()
  }
  ignore(
    opening.then(fn(result) {
      match result {
        Ok(stream) => on_done(@driver.Opened(stream))
        Err(_) => on_done(@driver.OpenFailed(-1L))
      }
      @js.Promise::resolve(())
    }),
  )
  @driver.Accepted
}

///|
impl @driver.SessionTransport for WebTransportTransport with fn send(
  self,
  stream,
  bytes,
  fin,
  on_done,
) {
  let writing = self.connection.write_stream(stream, bytes, fin~)
  ignore(
    writing.then(fn(result) {
      match result {
        Ok(_) => on_done(@driver.Written)
        Err(failure) =>
          match failure.stream_error_code {
            Some(code) =>
              on_done(@driver.WriteReset(code.reinterpret_as_uint64()))
            None => on_done(@driver.WriteFailed(-1L))
          }
      }
      @js.Promise::resolve(())
    }),
  )
  @driver.Accepted
}

///|
impl @driver.SessionTransport for WebTransportTransport with fn send_datagram(
  self,
  bytes,
  on_done,
) {
  let sending = self.connection.send_datagram(bytes)
  ignore(
    sending.then(fn(result) {
      match result {
        Ok(_) => on_done(@driver.Written)
        Err(_) => on_done(@driver.WriteFailed(-4L))
      }
      @js.Promise::resolve(())
    }),
  )
  @driver.Accepted
}

///|
impl @driver.SessionTransport for WebTransportTransport with fn abort_send(
  self,
  stream,
  code,
) -> Unit {
  ignore(
    self.connection
    .abort_stream_write(stream, code.reinterpret_as_int64())
    .then(fn(_) { @js.Promise::resolve(()) }),
  )
}

///|
impl @driver.SessionTransport for WebTransportTransport with fn stop_receiving(
  self,
  stream,
  code,
) -> Unit {
  ignore(
    self.connection
    .cancel_stream_read(stream, code.reinterpret_as_int64())
    .then(fn(_) { @js.Promise::resolve(()) }),
  )
}

///|
impl @driver.SessionTransport for WebTransportTransport with fn close_connection(
  self,
  code,
  reason,
) -> Unit {
  let reason = @utf8.decode(reason) catch { _ => "" }
  ignore(self.connection.close(code.reinterpret_as_int64(), reason))
}

///|
/// Drives a Sans-I/O client session over a WebTransport connection. Session
/// mutations happen synchronously on the JS event loop; I/O promises feed
/// their results back as events and never block the receive pumps.
pub struct WebTransportSessionDriver {
  connection : WebTransportConnection
  priv driver : @driver.SessionDriver[WebTransportTransport]
  notices : Array[@session.SessionNotice]
  notice_waiters : Array[(@session.SessionNotice?) -> Unit]
  opened_streams : Array[(@session.ClientStreamPurpose, Int64)]
  opened_stream_waiters : Array[
    ((@session.ClientStreamPurpose, Int64)?) -> Unit,
  ]
  mut active : Bool
  mut clock_timer : Int32?
  mut datagram_read_ended : Bool
}

///|
/// Bounded pending actions and transport operations for browser backpressure.
fn WebTransportSessionDriver::config() -> @driver.SessionDriverConfig {
  {
    max_in_flight_opens: 8,
    max_in_flight_sends: 128,
    max_in_flight_datagrams: 32,
    max_pending_actions: 1024,
    max_pending_bytes: 16777216L,
    max_send_chunk_bytes: 65536,
  }
}

///|
pub fn WebTransportSessionDriver::new(
  connection : WebTransportConnection,
  session : @session.ClientSession,
) -> WebTransportSessionDriver {
  session.set_stream_delivery_capability(committed=true, abort_after_fin=false)
  let driver = @driver.SessionDriver::new(
    WebTransportTransport::{ connection, },
    session,
    WebTransportSessionDriver::config(),
  )
  driver.set_clock(fn() { ffi_webtransport_monotonic_now().to_uint64() })
  let result : WebTransportSessionDriver = {
    connection,
    driver,
    notices: [],
    notice_waiters: [],
    opened_streams: [],
    opened_stream_waiters: [],
    active: false,
    clock_timer: None,
    datagram_read_ended: false,
  }
  result.driver.set_update_handler(fn() { result.flush() })
  result
}

///|
/// Create a WebTransport driver and remove the Native-only PATH and AUTHORITY
/// setup parameters. Their WebTransport equivalents come from the URL. Pass
/// `desired_subscriptions` to restore those subscriptions after SETUP.
pub fn WebTransportSessionDriver::from_setup(
  connection : WebTransportConnection,
  setup : @protocol.Setup,
  desired_subscriptions~ : Array[@session.DesiredSubscription] = [],
) -> Result[WebTransportSessionDriver, @protocol.CodecError] {
  match
    @session.ClientSession::new(
      @session.Draft18,
      setup.for_webtransport(),
      desired_subscriptions~,
    ) {
    Ok(session) => Ok(WebTransportSessionDriver::new(connection, session))
    Err(error) => Err(error)
  }
}

///|
/// Start the session, advertise Datagram capability, then process all
/// transport events until the session or connection ends.
pub fn WebTransportSessionDriver::start(
  self : WebTransportSessionDriver,
) -> Result[Unit, @driver.SessionDriverFailureReason] {
  if self.active {
    return Ok(())
  }
  self.active = true
  let capability = self.connection.datagram_capability()
  self.driver.receive(
    @session.DatagramSendCapabilityChanged(
      enabled=capability.enabled,
      max_length=capability.max_length,
    ),
  )
  match self.driver.failure() {
    Some(error) => {
      self.fail_close()
      return Err(error)
    }
    None => ()
  }
  self.read_stream_events()
  self.read_datagrams()
  self.read_close()
  self.run_clock()
  ignore(
    self.connection
    .ready()
    .then(fn(result) {
      if self.active {
        match result {
          Ok(_) => {
            self.refresh_datagram_capability()
            match self.driver.session().start() {
              Ok(actions) =>
                match self.driver.dispatch(actions) {
                  Dispatched => ()
                  QueueFull | TooLarge | DriverFailed => {
                    self.receive_event(@session.TransportFailed(-7L))
                    self.fail_close()
                  }
                }
              Err(_) => self.fail_close()
            }
            self.flush()
          }
          Err(_) => {
            self.receive_event(@session.TransportFailed(-1L))
            self.fail_close()
          }
        }
      }
      @js.Promise::resolve(())
    }),
  )
  Ok(())
}

///|
/// Apply actions returned by a public ClientSession operation. On QueueFull, retain
/// these actions and retry dispatch after progress; do not repeat the session
/// operation that produced them. TooLarge requires a smaller action batch.
pub fn WebTransportSessionDriver::dispatch(
  self : WebTransportSessionDriver,
  actions : Array[@session.SessionAction],
) -> @driver.DriverDispatchResult {
  let result = self.driver.dispatch(actions)
  self.flush()
  if result == @driver.DriverFailed {
    self.fail_close()
  }
  result
}

///|
/// Access the driven session to invoke its Sans-I/O operations.
pub fn WebTransportSessionDriver::session(
  self : WebTransportSessionDriver,
) -> @session.ClientSession {
  self.driver.session()
}

///|
/// Receive the next session notice. The returned Promise remains pending
/// until a notice arrives or the driver ends.
pub fn WebTransportSessionDriver::next_notice(
  self : WebTransportSessionDriver,
) -> @js.Promise[@session.SessionNotice?] {
  self.flush()
  if self.notices.length() > 0 {
    let notice = self.notices[0]
    ignore(self.notices.remove(0))
    return @js.Promise::resolve(Some(notice))
  }
  if !self.active {
    return @js.Promise::resolve(None)
  }
  @js.Promise::new(fn(resolve, _reject) { self.notice_waiters.push(resolve) })
}

///|
/// Receive the next stream opened for a session action. This lets application
/// code retain the transport reference needed by later session operations.
pub fn WebTransportSessionDriver::next_opened_stream(
  self : WebTransportSessionDriver,
) -> @js.Promise[(@session.ClientStreamPurpose, Int64)?] {
  self.flush()
  if self.opened_streams.length() > 0 {
    let stream = self.opened_streams[0]
    ignore(self.opened_streams.remove(0))
    return @js.Promise::resolve(Some(stream))
  }
  if !self.active {
    return @js.Promise::resolve(None)
  }
  @js.Promise::new(fn(resolve, _reject) {
    self.opened_stream_waiters.push(resolve)
  })
}

///|
/// Move driver notices and opened streams into the adapter's waiter queues.
fn WebTransportSessionDriver::flush(self : WebTransportSessionDriver) -> Unit {
  for notice in self.driver.take_notices() {
    self.enqueue_notice(notice)
  }
  for (purpose, stream) in self.driver.take_opened_streams() {
    self.notify_opened_stream(purpose, stream)
  }
  if self.driver.failed() {
    self.finish()
  }
}

///|
fn WebTransportSessionDriver::notify_opened_stream(
  self : WebTransportSessionDriver,
  purpose : @session.ClientStreamPurpose,
  stream : Int64,
) -> Unit {
  if self.opened_stream_waiters.length() > 0 {
    let waiter = self.opened_stream_waiters[0]
    ignore(self.opened_stream_waiters.remove(0))
    waiter(Some((purpose, stream)))
  } else {
    self.opened_streams.push((purpose, stream))
  }
}

///|
fn WebTransportSessionDriver::receive_event(
  self : WebTransportSessionDriver,
  event : @session.SessionEvent,
) -> Unit {
  if !self.active {
    return
  }
  self.driver.receive(event)
  self.flush()
  match event {
    @session.PeerClosed(_) => self.finish()
    _ => ()
  }
}

///|
fn WebTransportSessionDriver::enqueue_notice(
  self : WebTransportSessionDriver,
  notice : @session.SessionNotice,
) -> Unit {
  if self.notice_waiters.length() > 0 {
    let waiter = self.notice_waiters[0]
    ignore(self.notice_waiters.remove(0))
    waiter(Some(notice))
  } else {
    self.notices.push(notice)
  }
}

///|
fn WebTransportSessionDriver::read_stream_events(
  self : WebTransportSessionDriver,
) -> Unit {
  if !self.active {
    return
  }
  ignore(
    self.connection
    .next_stream_event()
    .then(fn(event) {
      let mut connection_closed = false
      match event {
        LocalStreamOpened(_, _) => ()
        StreamStarted(stream, direction) =>
          self.receive_event(
            @session.InboundStreamStarted(
              stream,
              match direction {
                Unidirectional => @session.Unidirectional
                Bidirectional => @session.Bidirectional
              },
            ),
          )
        Data(stream, bytes) =>
          self.receive_event(@session.StreamData(stream, bytes))
        StreamFinished(stream) =>
          self.receive_event(@session.InboundStreamFinished(stream))
        StreamDataCommitted(stream) => {
          self.driver.receive(@session.StreamDataCommitted(stream))
          self.flush()
        }
        StreamReset(stream, failure) =>
          match failure.stream_error_code {
            Some(code) =>
              self.receive_event(
                @session.StreamReset(stream, code.reinterpret_as_uint64()),
              )
            None => self.receive_event(@session.TransportFailed(-5L))
          }
        SendStopped(stream, failure) =>
          match failure.stream_error_code {
            Some(code) =>
              self.receive_event(
                @session.SendStopped(stream, code.reinterpret_as_uint64()),
              )
            None => self.receive_event(@session.TransportFailed(-6L))
          }
        StreamQueueOverflow(_) =>
          self.receive_event(@session.TransportFailed(-2L))
        // The connection.closed() promise carries the close cause into the
        // session state machine. Stop this reader while it is being handled.
        ConnectionClosed(_) => connection_closed = true
      }
      if !connection_closed {
        self.read_stream_events()
      }
      @js.Promise::resolve(())
    }),
  )
}

///|
fn WebTransportSessionDriver::read_datagrams(
  self : WebTransportSessionDriver,
) -> Unit {
  if !self.active {
    return
  }
  ignore(
    self.connection
    .next_datagram()
    .then(fn(event) {
      let mut continue_reading = true
      match event {
        DatagramReceived(bytes) =>
          self.receive_event(@session.DatagramReceived(bytes))
        DatagramReceiveQueueFull =>
          self.receive_event(@session.TransportFailed(-3L))
        DatagramsClosed => {
          continue_reading = false
          self.datagram_read_ended = true
          self.receive_event(
            @session.DatagramSendCapabilityChanged(enabled=false, max_length=0),
          )
        }
      }
      if continue_reading {
        self.read_datagrams()
      }
      @js.Promise::resolve(())
    }),
  )
}

///|
fn WebTransportSessionDriver::refresh_datagram_capability(
  self : WebTransportSessionDriver,
) -> Unit {
  if !self.datagram_read_ended {
    let capability = self.connection.datagram_capability()
    self.receive_event(
      @session.DatagramSendCapabilityChanged(
        enabled=capability.enabled,
        max_length=capability.max_length,
      ),
    )
  }
}

///|
fn WebTransportSessionDriver::read_close(
  self : WebTransportSessionDriver,
) -> Unit {
  ignore(
    self.connection
    .closed()
    .then(fn(close) {
      match close {
        PeerClose(info) =>
          self.receive_event(
            @session.PeerClosed(info.close_code.reinterpret_as_uint64()),
          )
        Failed(_) => self.receive_event(@session.TransportFailed(-1L))
        LocalClose => self.finish()
      }
      self.finish()
      @js.Promise::resolve(())
    }),
  )
}

///|
fn WebTransportSessionDriver::run_clock(
  self : WebTransportSessionDriver,
) -> Unit {
  self.schedule_clock()
}

///|
fn WebTransportSessionDriver::schedule_clock(
  self : WebTransportSessionDriver,
) -> Unit {
  if self.active {
    self.clock_timer = Some(
      ffi_webtransport_set_timeout(
        fn() {
          self.clock_timer = None
          if self.active {
            self.refresh_datagram_capability()
            let now = ffi_webtransport_monotonic_now().to_uint64()
            self.receive_event(@session.Clock(now))
            self.schedule_clock()
          }
        },
        25,
      ),
    )
  }
}

///|
fn WebTransportSessionDriver::finish(self : WebTransportSessionDriver) -> Unit {
  if !self.active {
    return
  }
  self.active = false
  match self.clock_timer {
    Some(timer) => ffi_webtransport_clear_timeout(timer)
    None => ()
  }
  self.clock_timer = None
  while self.notice_waiters.length() > 0 {
    let waiter = self.notice_waiters[0]
    ignore(self.notice_waiters.remove(0))
    waiter(None)
  }
  while self.opened_stream_waiters.length() > 0 {
    let waiter = self.opened_stream_waiters[0]
    ignore(self.opened_stream_waiters.remove(0))
    waiter(None)
  }
}

///|
fn WebTransportSessionDriver::fail_close(
  self : WebTransportSessionDriver,
) -> Unit {
  self.finish()
  ignore(self.connection.close(0L, "session driver failed"))
}

///|
extern "js" fn ffi_webtransport_monotonic_now() -> Double =
  #| () => globalThis.performance.now();

///|
extern "js" fn ffi_webtransport_set_timeout(
  callback : () -> Unit,
  delay_ms : Int32,
) -> Int32 =
  #| (callback, delay) => globalThis.setTimeout(callback, delay);

///|
extern "js" fn ffi_webtransport_clear_timeout(timer : Int32) -> Unit =
  #| timer => globalThis.clearTimeout(timer);
