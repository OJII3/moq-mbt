///|
/// Drives a Sans-I/O client session over a WebTransport connection. Session
/// mutations happen synchronously on the JS event loop; I/O promises feed
/// their results back as events and never block the receive pumps.
pub struct WebTransportSessionDriver {
  connection : WebTransportConnection
  session : @session.ClientSession
  mut pending_actions : Array[@session.ClientSessionAction]
  mut pending_opens : Int32
  mut pending_sends : Int32
  mut pending_datagrams : Int32
  notices : Array[@session.ClientSessionNotice]
  notice_waiters : Array[(@session.ClientSessionNotice?) -> Unit]
  mut active : Bool
  mut clock_timer : Int32?
}

///|
pub fn WebTransportSessionDriver::new(
  connection : WebTransportConnection,
  session : @session.ClientSession,
) -> WebTransportSessionDriver {
  {
    connection,
    session,
    pending_actions: [],
    pending_opens: 0,
    pending_sends: 0,
    pending_datagrams: 0,
    notices: [],
    notice_waiters: [],
    active: false,
    clock_timer: None,
  }
}

///|
/// Create a WebTransport driver and remove the Native-only PATH and AUTHORITY
/// setup parameters. Their WebTransport equivalents come from the URL.
pub fn WebTransportSessionDriver::from_setup(
  connection : WebTransportConnection,
  setup : @protocol.Setup,
) -> Result[WebTransportSessionDriver, @protocol.CodecError] {
  let options : Array[@protocol.SetupOption] = []
  for option in setup.options {
    if option.kind != 1UL && option.kind != 5UL {
      options.push(option)
    }
  }
  match @session.ClientSession::new(@session.Draft18, { options, }) {
    Ok(session) => Ok(WebTransportSessionDriver::new(connection, session))
    Err(error) => Err(error)
  }
}

///|
/// Start the session, advertise Datagram capability, then process all
/// transport events until the session or connection ends.
pub fn WebTransportSessionDriver::start(
  self : WebTransportSessionDriver,
) -> Result[Unit, @session.ClientSessionError] {
  if self.active {
    return Ok(())
  }
  self.active = true
  let capability = self.connection.datagram_capability()
  match
    self.session.receive(
      @session.DatagramSendCapabilityChanged(
        capability.enabled,
        capability.max_length,
      ),
    ) {
    Err(error) => return Err(error)
    Ok(actions) => self.process_actions(actions)
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
          Ok(_) =>
            match self.session.start() {
              Ok(actions) => self.process_actions(actions)
              Err(_) => self.finish()
            }
          Err(_) => self.receive_event(@session.TransportFailed(-1L))
        }
      }
      @js.Promise::resolve(())
    }),
  )
  Ok(())
}

///|
/// Apply actions returned by a public ClientSession operation.
pub fn WebTransportSessionDriver::dispatch(
  self : WebTransportSessionDriver,
  actions : Array[@session.ClientSessionAction],
) -> Unit {
  self.process_actions(actions)
}

///|
/// Access the driven session to invoke its Sans-I/O operations.
pub fn WebTransportSessionDriver::session(
  self : WebTransportSessionDriver,
) -> @session.ClientSession {
  self.session
}

///|
/// Receive the next session notice. The returned Promise remains pending
/// until a notice arrives or the driver ends.
pub fn WebTransportSessionDriver::next_notice(
  self : WebTransportSessionDriver,
) -> @js.Promise[@session.ClientSessionNotice?] {
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
fn WebTransportSessionDriver::receive_event(
  self : WebTransportSessionDriver,
  event : @session.ClientSessionEvent,
) -> Unit {
  if !self.active {
    return
  }
  match self.session.receive(event) {
    Err(_) => self.finish()
    Ok(actions) => {
      self.process_actions(actions)
      match event {
        @session.PeerClosed(_) | @session.TransportFailed(_) => self.finish()
        _ => ()
      }
    }
  }
}

///|
fn WebTransportSessionDriver::process_actions(
  self : WebTransportSessionDriver,
  actions : Array[@session.ClientSessionAction],
) -> Unit {
  if !self.active {
    return
  }
  if actions.length() > 512 - self.pending_actions.length() {
    self.receive_event(@session.TransportFailed(-7L))
    return
  }
  self.pending_actions = self.pending_actions + actions
  self.pump_actions()
}

///|
fn WebTransportSessionDriver::pump_actions(
  self : WebTransportSessionDriver,
) -> Unit {
  if !self.active {
    return
  }
  let mut blocked = false
  while self.pending_actions.length() > 0 && !blocked && self.active {
    match self.pending_actions[0] {
      @session.OpenStream(direction, purpose) =>
        if self.pending_opens >= 8 {
          blocked = true
        } else {
          ignore(self.pending_actions.remove(0))
          self.pending_opens += 1
          let opening = match direction {
            @session.Unidirectional => self.connection.open_unidirectional()
            @session.Bidirectional => self.connection.open_bidirectional()
          }
          ignore(
            opening.then(fn(result) {
              self.pending_opens -= 1
              match result {
                Ok(stream) =>
                  self.receive_event(@session.StreamOpened(purpose, stream))
                Err(_) => self.receive_event(@session.TransportFailed(-1L))
              }
              self.pump_actions()
              @js.Promise::resolve(())
            }),
          )
        }
      @session.Send(stream, bytes, fin) =>
        if self.pending_sends >= 128 {
          blocked = true
        } else {
          ignore(self.pending_actions.remove(0))
          self.pending_sends += 1
          let writing = self.connection.write_stream(stream, bytes, fin~)
          ignore(
            writing.then(fn(result) {
              self.pending_sends -= 1
              match result {
                Ok(_) =>
                  self.receive_event(@session.SendComplete(stream, false))
                Err(failure) =>
                  match failure.stream_error_code {
                    Some(code) =>
                      self.receive_event(
                        @session.StreamReset(
                          stream,
                          code.reinterpret_as_uint64(),
                        ),
                      )
                    None => self.receive_event(@session.TransportFailed(-1L))
                  }
              }
              self.pump_actions()
              @js.Promise::resolve(())
            }),
          )
        }
      @session.SendDatagram(bytes) =>
        if self.pending_datagrams >= 32 {
          blocked = true
        } else {
          ignore(self.pending_actions.remove(0))
          self.pending_datagrams += 1
          let sending = self.connection.send_datagram(bytes)
          ignore(
            sending.then(fn(result) {
              self.pending_datagrams -= 1
              match result {
                Ok(_) => self.receive_event(@session.DatagramSendComplete)
                Err(_) => self.receive_event(@session.TransportFailed(-4L))
              }
              self.pump_actions()
              @js.Promise::resolve(())
            }),
          )
        }
      action => {
        ignore(self.pending_actions.remove(0))
        self.execute_immediate(action)
      }
    }
  }
}

///|
fn WebTransportSessionDriver::execute_immediate(
  self : WebTransportSessionDriver,
  action : @session.ClientSessionAction,
) -> Unit {
  match action {
    @session.AbortSend(stream, code) =>
      ignore(
        self.connection
        .abort_stream_write(stream, code.reinterpret_as_int64())
        .then(fn(_) { @js.Promise::resolve(()) }),
      )
    @session.StopReceiving(stream, code) =>
      ignore(
        self.connection
        .cancel_stream_read(stream, code.reinterpret_as_int64())
        .then(fn(_) { @js.Promise::resolve(()) }),
      )
    @session.CloseConnection(code, reason) => {
      let reason = @utf8.decode(reason) catch { _ => "" }
      ignore(self.connection.close(code.reinterpret_as_int64(), reason))
    }
    @session.Notify(notice) => self.enqueue_notice(notice)
    @session.OpenStream(_, _)
    | @session.Send(_, _, _)
    | @session.SendDatagram(_) => ()
  }
}

///|
fn WebTransportSessionDriver::enqueue_notice(
  self : WebTransportSessionDriver,
  notice : @session.ClientSessionNotice,
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
          self.receive_event(@session.StreamFinished(stream))
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
        ConnectionClosed(_) => self.finish()
      }
      self.read_stream_events()
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
          self.receive_event(@session.DatagramSendCapabilityChanged(false, 0))
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
  self.pending_actions.clear()
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
