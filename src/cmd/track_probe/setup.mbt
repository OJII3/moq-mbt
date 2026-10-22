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
priv struct SessionActionDriver {
  send_counts : Map[Int64, UInt64]
  mut pending : Array[@session.ClientSessionAction]
}

///|
fn SessionActionDriver::new() -> SessionActionDriver {
  { send_counts: Map([]), pending: [], }
}

///|
fn attempt_stream_send(
  stream : Int64,
  bytes : Bytes,
  fin : Bool,
  send : (Int64, Bytes, Bool) -> Unit raise @transport.TransportError,
) -> Bool raise @transport.TransportError {
  try {
    send(stream, bytes, fin)
    true
  } catch {
    @transport.SendQueueFull => false
    error => raise error
  }
}

///|
fn SessionActionDriver::send_head(
  self : SessionActionDriver,
  send : (Int64, Bytes, Bool) -> Unit raise @transport.TransportError,
) -> Bool raise @transport.TransportError {
  match self.pending[0] {
    @session.Send(stream, bytes, fin) => {
      let accepted = attempt_stream_send(stream, bytes, fin, send)
      if accepted {
        ignore(self.pending.remove(0))
        let count = match self.send_counts.get(stream) {
          Some(value) => value
          None => 0UL
        }
        self.send_counts[stream] = count + 1UL
      }
      accepted
    }
    _ => false
  }
}

///|
fn SessionActionDriver::send_datagram_head(
  self : SessionActionDriver,
  send : (Bytes) -> Unit raise @transport.TransportError,
) -> Bool raise @transport.TransportError {
  match self.pending[0] {
    @session.SendDatagram(bytes) => {
      let accepted = try {
        send(bytes)
        true
      } catch {
        @transport.DatagramSendQueueFull => false
        error => raise error
      }
      if accepted {
        ignore(self.pending.remove(0))
      }
      accepted
    }
    _ => false
  }
}

///|
fn end_driver_session(
  client : @transport.Client,
  session : @session.ClientSession,
) -> Array[@session.ClientSessionNotice] {
  client.close()
  let notices : Array[@session.ClientSessionNotice] = []
  match session.receive(@session.TransportFailed(-1L)) {
    Ok(actions) =>
      for action in actions {
        match action {
          @session.Notify(notice) => notices.push(notice)
          _ => ()
        }
      }
    Err(_) => ()
  }
  notices
}

///|
fn drive_session_actions(
  client : @transport.Client,
  session : @session.ClientSession,
  initial : Array[@session.ClientSessionAction],
  driver : SessionActionDriver,
) -> (
  Array[@session.ClientSessionNotice],
  Array[(@session.ClientStreamPurpose, Int64)],
) raise {
  let mut notices : Array[@session.ClientSessionNotice] = []
  let opened : Array[(@session.ClientStreamPurpose, Int64)] = []
  driver.pending = driver.pending + initial
  let mut blocked = false
  while driver.pending.length() > 0 && !blocked {
    let action = driver.pending[0]
    match action {
      @session.OpenStream(direction, purpose) => {
        let stream = try {
          match direction {
            @session.Unidirectional => client.open_uni()
            @session.Bidirectional => client.open_bidi()
          }
        } catch {
          _ => {
            driver.pending = []
            notices = notices + end_driver_session(client, session)
            return (notices, opened)
          }
        }
        opened.push((purpose, stream))
        let next = match
          session.receive(@session.StreamOpened(purpose, stream)) {
          Ok(value) => value
          Err(error) =>
            fail("client session rejected opened stream: \{error.to_repr()}")
        }
        driver.pending = driver.pending + next
      }
      @session.Send(_, _, _) => {
        let accepted = driver.send_head(fn(
          s,
          b,
          f,
        ) raise @transport.TransportError {
          client.send(s, b, fin=f)
        }) catch {
          _ => {
            driver.pending = []
            notices = notices + end_driver_session(client, session)
            return (notices, opened)
          }
        }
        if !accepted {
          blocked = true
        }
      }
      @session.SendDatagram(_) => {
        let accepted = driver.send_datagram_head(fn(
          b : Bytes,
        ) raise @transport.TransportError {
          ignore(client.send_datagram(b))
        }) catch {
          _ => {
            driver.pending = []
            notices = notices + end_driver_session(client, session)
            return (notices, opened)
          }
        }
        if !accepted {
          blocked = true
        }
      }
      @session.AbortSend(stream, error_code) =>
        client.abort_send(stream, error_code.reinterpret_as_int64()) catch {
          _ => {
            driver.pending = []
            notices = notices + end_driver_session(client, session)
            return (notices, opened)
          }
        }
      @session.StopReceiving(stream, error_code) =>
        client.stop_receiving(stream, error_code.reinterpret_as_int64()) catch {
          _ => {
            driver.pending = []
            notices = notices + end_driver_session(client, session)
            return (notices, opened)
          }
        }
      @session.CloseConnection(code, _reason) =>
        client.close(error_code=code.reinterpret_as_int64())
      @session.Notify(notice) => notices.push(notice)
    }
    if !blocked &&
      !(action is @session.Send(_, _, _)) &&
      !(action is @session.SendDatagram(_)) {
      ignore(driver.pending.remove(0))
    }
  }
  (notices, opened)
}

///|
fn note_send_complete(driver : SessionActionDriver, stream : Int64) -> Unit {
  match driver.send_counts.get(stream) {
    Some(count) if count > 1UL => driver.send_counts[stream] = count - 1UL
    Some(_) => driver.send_counts.remove(stream)
    None => ()
  }
}

///|
fn drive_session_result(
  client : @transport.Client,
  session : @session.ClientSession,
  result : Result[
    Array[@session.ClientSessionAction],
    @session.ClientSessionError,
  ],
  driver : SessionActionDriver,
) -> (
  Array[@session.ClientSessionNotice],
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
  event : @session.ClientSessionEvent,
) -> Result[Array[@session.ClientSessionAction], @session.ClientSessionError] {
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
) -> Result[Array[@session.ClientSessionAction], @session.ClientSessionError] {
  session.receive(@session.Clock(@env.now()))
}

///|
fn setup_client(
  host : String,
  port : Int32,
  path : String,
  insecure : Bool,
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
        capability.enabled,
        capability.max_length,
      ),
    ),
  )
  let driver = SessionActionDriver::new()
  let mut setup_stream : Int64? = None
  let mut setup_received = false
  let mut setup_acknowledged = false
  let deadline = @env.now() + 10000
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
              println(
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
        note_send_complete(driver, stream)
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.SendComplete(stream, canceled),
          ),
          driver,
        )
        ignore(notices)
        if setup_stream == Some(stream) {
          if canceled {
            fail("client SETUP send was canceled")
          }
          setup_acknowledged = true
        }
      }
      DatagramStateChanged(capability) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.DatagramSendCapabilityChanged(
              capability.enabled,
              capability.max_length,
            ),
          ),
          driver,
        )
        ignore(notices)
      }
      DatagramSendStateChanged(_, state) =>
        if state.is_final() {
          let (notices, _) = drive_session_result(
            client,
            session,
            receive_session_event(session, @session.DatagramSendComplete),
            driver,
          )
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
