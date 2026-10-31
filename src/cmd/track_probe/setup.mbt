///|
fn probe_payload(group_id : UInt64, object_id : UInt64) -> Bytes {
  @utf8.encode("group-\{group_id}-object-\{object_id}")
}

///|
fn probe_properties() -> Bytes {
  b"\x02\x2a"
}

///|
fn client_stream_direction(
  direction : @transport.StreamDirection,
) -> @session.ClientStreamDirection {
  match direction {
    @transport.Unidirectional => @session.Unidirectional
    @transport.Bidirectional => @session.Bidirectional
  }
}

///|
/// Native transport backend for the portable session driver. Stream opens are
/// synchronous; send completions are paired with the transport's SendComplete
/// events so the driver can release its in-flight slots.
priv struct ProbeTransport {
  client : @transport.Client
  send_waiters : Map[Int64, Array[(@driver.TransportWriteOutcome) -> Unit]]
  datagram_waiters : Array[(@driver.TransportWriteOutcome) -> Unit]
}

///|
fn ProbeTransport::new(client : @transport.Client) -> ProbeTransport {
  { client, send_waiters: Map([]), datagram_waiters: [], }
}

///|
impl @driver.SessionTransport for ProbeTransport with fn open_stream(
  self,
  direction,
  _purpose,
  on_done,
) {
  let opened = try {
    match direction {
      @session.Unidirectional => self.client.open_uni()
      @session.Bidirectional => self.client.open_bidi()
    }
  } catch {
    _ => return @driver.Fatal(-1L)
  }
  on_done(@driver.Opened(opened))
  @driver.Accepted
}

///|
impl @driver.SessionTransport for ProbeTransport with fn send(
  self,
  stream,
  bytes,
  fin,
  on_done,
) {
  let accepted = try {
    self.client.send(stream, bytes, fin~)
    true
  } catch {
    @transport.SendQueueFull => false
    _ => return @driver.Fatal(-1L)
  }
  if !accepted {
    return @driver.Retry
  }
  let waiters = match self.send_waiters.get(stream) {
    Some(waiters) => waiters
    None => {
      let waiters : Array[(@driver.TransportWriteOutcome) -> Unit] = []
      self.send_waiters[stream] = waiters
      waiters
    }
  }
  waiters.push(on_done)
  @driver.Accepted
}

///|
impl @driver.SessionTransport for ProbeTransport with fn send_datagram(
  self,
  bytes,
  on_done,
) {
  let accepted = try {
    ignore(self.client.send_datagram(bytes))
    true
  } catch {
    @transport.DatagramSendQueueFull => false
    _ => return @driver.Fatal(-1L)
  }
  if !accepted {
    return @driver.Retry
  }
  self.datagram_waiters.push(on_done)
  @driver.Accepted
}

///|
impl @driver.SessionTransport for ProbeTransport with fn abort_send(
  self,
  stream,
  code,
) -> Unit {
  // The session already closed this stream, so late SendComplete events are
  // ignored. Release the in-flight slots without producing session events.
  self.abandon_send(stream)
  self.client.abort_send(stream, code.reinterpret_as_int64()) catch {
    _ => ()
  }
}

///|
impl @driver.SessionTransport for ProbeTransport with fn stop_receiving(
  self,
  stream,
  code,
) -> Unit {
  self.client.stop_receiving(stream, code.reinterpret_as_int64()) catch {
    _ => ()
  }
}

///|
impl @driver.SessionTransport for ProbeTransport with fn close_connection(
  self,
  code,
  _reason,
) -> Unit {
  self.client.close(error_code=code.reinterpret_as_int64())
}

///|
fn ProbeTransport::complete_send(
  self : ProbeTransport,
  stream : Int64,
  canceled : Bool,
) -> Unit {
  match self.send_waiters.get(stream) {
    Some(waiters) if waiters.length() > 0 => {
      let callback = waiters.remove(0)
      if waiters.is_empty() {
        self.send_waiters.remove(stream)
      }
      callback(if canceled { @driver.WriteCanceled } else { @driver.Written })
    }
    _ => ()
  }
}

///|
fn ProbeTransport::complete_datagram(self : ProbeTransport) -> Unit {
  if self.datagram_waiters.length() > 0 {
    let callback = self.datagram_waiters.remove(0)
    callback(@driver.Written)
  }
}

///|
fn ProbeTransport::abandon_send(self : ProbeTransport, stream : Int64) -> Unit {
  match self.send_waiters.get(stream) {
    Some(waiters) => {
      self.send_waiters.remove(stream)
      for waiter in waiters {
        waiter(@driver.Abandoned)
      }
    }
    None => ()
  }
}

///|
/// Probe-side wrapper around the shared driver. The session is bound on the
/// first action so callers can keep constructing it next to the connection.
priv struct SessionActionDriver {
  mut transport : ProbeTransport?
  mut inner : @driver.SessionDriver[ProbeTransport]?
}

///|
fn SessionActionDriver::new() -> SessionActionDriver {
  { transport: None, inner: None, }
}

///|
fn SessionActionDriver::ensure(
  self : SessionActionDriver,
  client : @transport.Client,
  session : @session.ClientSession,
) -> @driver.SessionDriver[ProbeTransport] {
  match self.inner {
    Some(inner) => inner
    None => {
      let transport = ProbeTransport::new(client)
      session.set_stream_delivery_capability(
        committed=true,
        abort_after_fin=true,
      )
      let inner = @driver.SessionDriver::new(transport, session, {
        max_in_flight_opens: 8,
        max_in_flight_sends: 128,
        max_in_flight_datagrams: 32,
        max_pending_actions: 1024,
        max_pending_bytes: 16777216L,
        max_send_chunk_bytes: 65536,
      })
      inner.set_clock(fn() { @env.now() })
      self.transport = Some(transport)
      self.inner = Some(inner)
      inner
    }
  }
}

///|
fn SessionActionDriver::complete_send(
  self : SessionActionDriver,
  stream : Int64,
  canceled : Bool,
) -> Unit {
  match self.transport {
    Some(transport) => transport.complete_send(stream, canceled)
    None => ()
  }
}

///|
fn SessionActionDriver::committed(
  self : SessionActionDriver,
  stream : Int64,
) -> Unit {
  match self.inner {
    Some(inner) => inner.receive(@session.StreamDataCommitted(stream))
    None => ()
  }
}

///|
fn SessionActionDriver::complete_datagram(self : SessionActionDriver) -> Unit {
  match self.transport {
    Some(transport) => transport.complete_datagram()
    None => ()
  }
}

///|
fn SessionActionDriver::has_pending_sends(
  self : SessionActionDriver,
  stream : Int64,
) -> Bool {
  match self.inner {
    Some(inner) => inner.has_pending_sends(stream)
    None => false
  }
}

///|
fn take_driver_events(
  driver : SessionActionDriver,
) -> (
  Array[@session.SessionNotice],
  Array[(@session.ClientStreamPurpose, Int64)],
) {
  match driver.inner {
    Some(inner) =>
      (
        inner.take_notices(),
        inner
        .take_opened_streams()
        .map(fn(entry) { (entry.0, entry.1.to_int64()) }),
      )
    None => ([], [])
  }
}

///|
fn drive_session_actions(
  client : @transport.Client,
  session : @session.ClientSession,
  initial : Array[@session.SessionAction],
  driver : SessionActionDriver,
) -> (
  Array[@session.SessionNotice],
  Array[(@session.ClientStreamPurpose, Int64)],
) raise {
  let inner = driver.ensure(client, session)
  match inner.dispatch(initial) {
    @driver.Accepted => ()
    result => fail("driver rejected actions: \{result.to_repr()}")
  }
  match inner.failure() {
    Some(error) => fail("client session error: \{error.to_repr()}")
    None => ()
  }
  (
    inner.take_notices(),
    inner.take_opened_streams().map(fn(entry) { (entry.0, entry.1.to_int64()) }),
  )
}

///|
fn drive_session_result(
  client : @transport.Client,
  session : @session.ClientSession,
  result : Result[Array[@session.SessionAction], @session.ClientSessionError],
  driver : SessionActionDriver,
) -> (
  Array[@session.SessionNotice],
  Array[(@session.ClientStreamPurpose, Int64)],
) raise {
  match result {
    Ok(actions) => drive_session_actions(client, session, actions, driver)
    Err(error) => fail("client session error: \{error.to_repr()}")
  }
}

///|
fn receive_session_event(
  session : @session.ClientSession,
  event : @session.SessionEvent,
) -> Result[Array[@session.SessionAction], @session.ClientSessionError] {
  match session.receive(@session.Clock(@env.now())) {
    Err(error) => Err(error)
    Ok(clock_actions) =>
      match session.receive(event) {
        Err(error) => Err(error)
        Ok(actions) => Ok(clock_actions + actions)
      }
  }
}

///|
fn receive_session_clock(
  session : @session.ClientSession,
) -> Result[Array[@session.SessionAction], @session.ClientSessionError] {
  session.receive(@session.Clock(@env.now()))
}

///|
fn setup_client(
  host : String,
  port : Int32,
  path : String,
  insecure : Bool,
  timeout_ms~ : UInt64 = 10000UL,
) -> (@transport.Client, @session.ClientSession) raise {
  let authority = if host.contains(":") {
    "[\{host}]:\{port}"
  } else {
    "\{host}:\{port}"
  }
  let session = @session.ClientSession::new(Draft18, {
    options: [
      { kind: 1, value: Data(@utf8.encode(path)), },
      { kind: 5, value: Data(@utf8.encode(authority)), },
      { kind: 7, value: Data(b"moq-mbt/0.1.0"), },
    ],
  }).unwrap()
  let client = @transport.Client::connect(host, port, insecure~)
  let capability = client.datagram_send_capability()
  ignore(
    session.receive(
      @session.DatagramSendCapabilityChanged(
        enabled=capability.enabled,
        max_length=capability.max_length,
      ),
    ),
  )
  let driver = SessionActionDriver::new()
  let mut setup_stream : Int64? = None
  let mut setup_received = false
  let mut setup_acknowledged = false
  let deadline = @env.now() + timeout_ms
  while @env.now() < deadline {
    match client.next_event(100) {
      Connected => {
        let (notices, opened) = drive_session_actions(
          client,
          session,
          session.start().unwrap(),
          driver,
        )
        ignore(notices)
        for (purpose, stream) in opened {
          if purpose is @session.Control {
            setup_stream = Some(stream)
          }
        }
      }
      StreamStarted(stream, direction) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.InboundStreamStarted(
              stream,
              client_stream_direction(direction),
            ),
          ),
          driver,
        )
        ignore(notices)
      }
      Data(stream, bytes) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(session, @session.StreamData(stream, bytes)),
          driver,
        )
        for notice in notices {
          match notice {
            @session.HandshakeComplete(setup) => {
              eprintln(
                "MoQT Draft 18 SETUP complete (\{setup.options.length()} options)",
              )
              setup_received = true
            }
            _ => ()
          }
        }
      }
      Datagram(bytes) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(session, @session.DatagramReceived(bytes)),
          driver,
        )
        ignore(notices)
      }
      SendComplete(stream, canceled) => {
        driver.complete_send(stream, canceled)
        let (notices, _) = take_driver_events(driver)
        ignore(notices)
        if setup_stream == Some(stream) {
          if canceled {
            fail("client SETUP send was canceled")
          }
          setup_acknowledged = true
        }
      }
      StreamDataCommitted(stream) => {
        driver.committed(stream)
        let (notices, _) = take_driver_events(driver)
        ignore(notices)
      }
      DatagramStateChanged(capability) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.DatagramSendCapabilityChanged(
              enabled=capability.enabled,
              max_length=capability.max_length,
            ),
          ),
          driver,
        )
        ignore(notices)
      }
      DatagramSendStateChanged(_, state) =>
        if state.is_final() {
          driver.complete_datagram()
          let (notices, _) = take_driver_events(driver)
          ignore(notices)
        }
      StreamFinished(_)
      | StreamClosed(_)
      | StreamReset(_, _)
      | SendStopped(_, _) => fail("peer control stream ended during SETUP")
      Closed(code) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.PeerClosed(code.reinterpret_as_uint64()),
          ),
          driver,
        )
        ignore(notices)
        fail("peer closed during SETUP (application code \{code})")
      }
      TransportClosed(status) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.TransportFailed(status.to_int64()),
          ),
          driver,
        )
        ignore(notices)
        fail("QUIC failed during SETUP (MsQuic status \{status})")
      }
      QueueOverflow => fail("transport event buffer overflow")
      Timeout => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_clock(session),
          driver,
        )
        ignore(notices)
      }
    }
    if setup_received && setup_acknowledged {
      return (client, session)
    }
  }
  client.close()
  fail("timed out waiting for peer SETUP")
}
