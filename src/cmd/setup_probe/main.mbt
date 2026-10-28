///|
extern "C" fn exit(status : Int32) -> Unit = "exit"

///|
fn probe(
  host : String,
  port : Int32,
  path : String,
  insecure : Bool,
) -> Unit raise {
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
  let mut close_code = 0L
  defer client.close(error_code=close_code)
  let mut outbound_stream : Int64? = None
  let mut setup_received = false
  let mut setup_acknowledged = false
  let deadline = @env.now() + 10000
  while @env.now() < deadline {
    match client.next_event(100) {
      Connected => {
        println("QUIC connected: ALPN moqt-18, DATAGRAM negotiated")
        let stream = client.open_uni()
        outbound_stream = Some(stream)
        client.send(stream, handshake.outbound_setup())
      }
      StreamStarted(_, _)
      | StreamDataCommitted(_)
      | Datagram(_)
      | DatagramStateChanged(_) => ()
      DatagramSendStateChanged(_, _) => ()
      Data(stream, bytes) if !setup_received =>
        match handshake.receive(stream, bytes) {
          Ok(Pending) => ()
          Ok(Ready(setup, trailing)) => {
            println(
              "MoQT Draft 18 SETUP received (\{setup.options.length()} options)",
            )
            if !trailing.is_empty() {
              println("\{trailing.length()} bytes follow SETUP")
            }
            setup_received = true
          }
          Err(error) => {
            close_code = error.close_code().reinterpret_as_int64()
            fail("invalid peer SETUP: \{error.to_repr()}")
          }
        }
      // This probe processes only the initial SETUP, then waits for its own
      // reliable send to be acknowledged before shutting down the connection.
      Data(_, _) => ()
      SendComplete(stream, canceled) =>
        if outbound_stream == Some(stream) {
          if canceled {
            fail("client SETUP send was canceled")
          }
          setup_acknowledged = true
        }
      StreamFinished(_)
      | StreamClosed(_)
      | StreamReset(_, _)
      | SendStopped(_, _) => {
        close_code = handshake
          .control_closed()
          .close_code()
          .reinterpret_as_int64()
        fail("peer control stream ended")
      }
      Closed(code) =>
        fail("peer closed before SETUP (application code \{code})")
      TransportClosed(status) =>
        fail("QUIC failed before SETUP (MsQuic status \{status})")
      QueueOverflow => fail("transport event buffer overflow")
      Timeout => ()
    }
    if setup_received && setup_acknowledged {
      println("Client SETUP acknowledged")
      return
    }
  }
  fail("timed out waiting for peer SETUP")
}

///|
fn main {
  let args = @env.args()
  guard args.length() == 4 || (args.length() == 5 && args[4] == "--insecure") else {
    eprintln("usage: setup_probe HOST PORT PATH [--insecure]")
    exit(2)
    return
  }
  try {
    let port = @string.parse_int(args[2])
    guard args[3].is_empty() ||
      args[3].has_prefix("/") ||
      args[3].has_prefix("?") else {
      fail("PATH must be empty or start with / or ?")
    }
    probe(args[1], port, args[3], args.length() == 5)
  } catch {
    error => {
      eprintln(error.to_string())
      exit(1)
    }
  }
}
