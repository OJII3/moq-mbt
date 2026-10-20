///|
fn probe_payload(group_id : UInt64, object_id : UInt64) -> Bytes {
  @utf8.encode("group-\{group_id}-object-\{object_id}")
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
fn drive_session_actions(
  client : @transport.Client,
  session : @session.ClientSession,
  initial : Array[@session.ClientSessionAction],
  send_counts : Map[Int64, UInt64],
) -> (
  Array[@session.ClientSessionNotice],
  Array[(@session.ClientStreamPurpose, Int64)],
) raise {
  let notices : Array[@session.ClientSessionNotice] = []
  let opened : Array[(@session.ClientStreamPurpose, Int64)] = []
  let mut pending = initial
  let mut index = 0
  while index < pending.length() {
    let action = pending[index]
    index += 1
    match action {
      @session.OpenStream(direction, purpose) => {
        let stream = match direction {
          @session.Unidirectional => client.open_uni()
          @session.Bidirectional => client.open_bidi()
        }
        opened.push((purpose, stream))
        let next = match
          session.receive(@session.StreamOpened(purpose, stream)) {
          Ok(value) => value
          Err(error) =>
            fail("client session rejected opened stream: \{error.to_repr()}")
        }
        pending = pending + next
      }
      @session.Send(stream, bytes, fin) => {
        client.send(stream, bytes, fin~)
        let count = match send_counts.get(stream) {
          Some(value) => value
          None => 0UL
        }
        send_counts[stream] = count + 1UL
      }
      @session.SendDatagram(bytes) => client.send_datagram(bytes)
      @session.AbortSend(stream, error_code) =>
        client.abort_send(stream, error_code.reinterpret_as_int64())
      @session.StopReceiving(stream, error_code) =>
        client.stop_receiving(stream, error_code.reinterpret_as_int64())
      @session.CloseConnection(code, _reason) =>
        client.close(error_code=code.reinterpret_as_int64())
      @session.Notify(notice) => notices.push(notice)
    }
  }
  (notices, opened)
}

///|
fn note_send_complete(send_counts : Map[Int64, UInt64], stream : Int64) -> Unit {
  match send_counts.get(stream) {
    Some(count) if count > 1UL => send_counts[stream] = count - 1UL
    Some(_) => send_counts.remove(stream)
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
  send_counts : Map[Int64, UInt64],
) -> (
  Array[@session.ClientSessionNotice],
  Array[(@session.ClientStreamPurpose, Int64)],
) raise {
  match result {
    Ok(actions) => drive_session_actions(client, session, actions, send_counts)
    Err(error) => fail("client session error: \{error.to_repr()}")
  }
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
  let send_counts : Map[Int64, UInt64] = Map([])
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
          send_counts,
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
          session.receive(
            @session.InboundStreamStarted(
              stream,
              client_stream_direction(direction),
            ),
          ),
          send_counts,
        )
        ignore(notices)
      }
      Data(stream, bytes) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          session.receive(@session.StreamData(stream, bytes)),
          send_counts,
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
          session.receive(@session.DatagramReceived(bytes)),
          send_counts,
        )
        ignore(notices)
      }
      SendComplete(stream, canceled) => {
        note_send_complete(send_counts, stream)
        let (notices, _) = drive_session_result(
          client,
          session,
          session.receive(@session.SendComplete(stream, canceled)),
          send_counts,
        )
        ignore(notices)
        if setup_stream == Some(stream) {
          if canceled {
            fail("client SETUP send was canceled")
          }
          setup_acknowledged = true
        }
      }
      StreamFinished(_) | StreamReset(_, _) | SendStopped(_, _) =>
        fail("peer control stream ended during SETUP")
      Closed(code) =>
        fail("peer closed during SETUP (application code \{code})")
      TransportClosed(status) =>
        fail("QUIC failed during SETUP (MsQuic status \{status})")
      QueueOverflow => fail("transport event buffer overflow")
      Timeout => ()
    }
    if setup_received && setup_acknowledged {
      return (client, session)
    }
  }
  client.close()
  fail("timed out waiting for peer SETUP")
}
