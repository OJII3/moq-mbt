///|
fn setup_client(
  host : String,
  port : Int32,
  path : String,
  insecure : Bool,
) -> @transport.Client raise {
  let authority = if host.contains(":") {
    "[\{host}]:\{port}"
  } else {
    "\{host}:\{port}"
  }
  let handshake = @session.ClientHandshake::new(Draft18, {
    options: [
      { kind: 1, value: Data(@utf8.encode(path)), },
      { kind: 5, value: Data(@utf8.encode(authority)), },
      { kind: 7, value: Data(b"moq-mbt/0.1.0"), },
    ],
  }).unwrap()
  let client = @transport.Client::connect(host, port, insecure~)
  let mut setup_stream : Int64? = None
  let mut setup_received = false
  let mut setup_acknowledged = false
  let deadline = @env.now() + 10000
  while @env.now() < deadline {
    match client.next_event(100) {
      Connected => {
        let stream = client.open_uni()
        setup_stream = Some(stream)
        client.send(stream, handshake.outbound_setup())
      }
      StreamStarted(_) => ()
      Data(stream, bytes) if !setup_received =>
        match handshake.receive(stream, bytes) {
          Ok(Pending) => ()
          Ok(Ready(setup, _trailing)) => {
            println(
              "MoQT Draft 18 SETUP complete (\{setup.options.length()} options)",
            )
            setup_received = true
          }
          Err(error) => fail("invalid peer SETUP: \{error.to_repr()}")
        }
      Data(_, _) => ()
      SendComplete(stream, canceled) if setup_stream == Some(stream) => {
        if canceled {
          fail("client SETUP send was canceled")
        }
        setup_acknowledged = true
      }
      SendComplete(_, _) => ()
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
      return client
    }
  }
  client.close()
  fail("timed out waiting for peer SETUP")
}
