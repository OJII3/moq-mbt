///|
extern "C" fn exit(status : Int32) -> Unit = "exit"

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

///|
fn run_publisher(
  host : String,
  port : Int32,
  path : String,
  namespace_field : String,
  track_name : String,
  payload : String,
  insecure : Bool,
) -> Unit raise {
  let track : @protocol.TrackName = {
    namespace_fields: [@utf8.encode(namespace_field)],
    name: @utf8.encode(track_name),
  }
  let publisher = @session.SingleTrackPublisher::new(track, 1)
  let client = setup_client(host, port, path, insecure)
  defer client.close()

  let publish_stream = client.open_bidi()
  client.send(publish_stream, publisher.publish_request().unwrap())
  let mut publish_sent = false
  let mut publish_buffer = b""
  let mut publish_accepted = false
  let mut subscribe_stream : Int64? = None
  let mut object_stream : Int64? = None
  let mut object_request_stream : Int64? = None
  let mut done_stream : Int64? = None
  let deadline = @env.now() + 60000
  while @env.now() < deadline {
    match client.next_event(100) {
      Connected | StreamStarted(_) => ()
      Data(stream, bytes) if stream == publish_stream =>
        if !publish_accepted {
          publish_buffer = publish_buffer + bytes
          match @protocol.decode_control(publish_buffer) {
            @protocol.Incomplete => ()
            @protocol.Invalid(error) =>
              fail("invalid PUBLISH response: \{error.to_repr()}")
            @protocol.Complete(@protocol.RequestOk(_), _) => {
              publish_accepted = true
              publisher.publish_accepted()
              eprintln("PUBLISH_OK received")
              println("Track publication accepted")
              if object_stream is None {
                let data_stream = client.open_uni()
                let data = publisher
                  .object_stream(0, 0, @utf8.encode(payload))
                  .unwrap()
                object_stream = Some(data_stream)
                object_request_stream = Some(publish_stream)
                client.send(data_stream, data, fin=true)
                println("Sent one Object")
              }
            }
            @protocol.Complete(@protocol.RequestError(error), _) =>
              fail("PUBLISH rejected (\{error.code})")
            @protocol.Complete(_, _) => fail("unexpected PUBLISH response")
          }
        }
      Data(stream, bytes) =>
        match publisher.receive_subscribe(stream, bytes) {
          Ok(@session.Pending) => ()
          Ok(@session.SubscribeRequested(request_stream, _, _trailing)) => {
            let response = publisher.accept_subscribe(request_stream).unwrap()
            subscribe_stream = Some(request_stream)
            client.send(request_stream, response)
          }
          Err(error) => fail("invalid SUBSCRIBE: \{error.to_repr()}")
        }
      SendComplete(stream, canceled) => {
        if canceled {
          fail("publisher send was canceled")
        }
        if stream == publish_stream && !publish_sent {
          publish_sent = true
          eprintln("PUBLISH request sent")
        } else if subscribe_stream == Some(stream) && object_stream is None {
          let data_stream = client.open_uni()
          let data = publisher
            .object_stream(0, 0, @utf8.encode(payload))
            .unwrap()
          object_stream = Some(data_stream)
          object_request_stream = Some(stream)
          client.send(data_stream, data, fin=true)
          println("Sent one Object")
        } else if object_stream == Some(stream) {
          let request_stream = object_request_stream.unwrap()
          let done = @protocol.encode_publish_done({
            code: 3,
            stream_count: 1,
            reason: b"",
          }).unwrap()
          done_stream = Some(request_stream)
          client.send(request_stream, done, fin=true)
        } else if done_stream == Some(stream) {
          println("Subscription finished")
          return
        }
      }
      StreamFinished(_) | StreamReset(_, _) | SendStopped(_, _) =>
        fail("publisher stream ended before object delivery")
      Closed(code) => fail("peer closed (application code \{code})")
      TransportClosed(status) => fail("QUIC failed (MsQuic status \{status})")
      QueueOverflow => fail("transport event buffer overflow")
      Timeout => ()
    }
  }
  fail("timed out waiting for a subscriber")
}

///|
fn run_subscriber(
  host : String,
  port : Int32,
  path : String,
  namespace_field : String,
  track_name : String,
  insecure : Bool,
) -> Unit raise {
  let track : @protocol.TrackName = {
    namespace_fields: [@utf8.encode(namespace_field)],
    name: @utf8.encode(track_name),
  }
  let subscriber = @session.SingleTrackSubscriber::new(track)
  let client = setup_client(host, port, path, insecure)
  defer client.close()

  let request_stream = client.open_bidi()
  client.send(request_stream, subscriber.subscribe_request().unwrap())
  let deadline = @env.now() + 15000
  while @env.now() < deadline {
    match client.next_event(100) {
      Connected | StreamStarted(_) => ()
      Data(stream, bytes) if stream == request_stream =>
        match subscriber.receive_response(stream, bytes) {
          Ok(@session.Pending) => ()
          Ok(@session.Established(track_alias, trailing)) => {
            eprintln("SUBSCRIBE_OK received")
            println("Subscription accepted (track alias \{track_alias})")
            if !trailing.is_empty() {
              match subscriber.receive_response(stream, trailing) {
                Ok(@session.Pending | @session.Finished(_, _)) => ()
                Ok(_) => fail("unexpected control message after SUBSCRIBE_OK")
                Err(error) =>
                  fail(
                    "invalid control message after SUBSCRIBE_OK: \{error.to_repr()}",
                  )
              }
            }
          }
          Ok(@session.Rejected(error, _trailing)) =>
            fail("SUBSCRIBE rejected (\{error.code})")
          Ok(@session.Finished(_, _)) => ()
          Err(error) => fail("invalid SUBSCRIBE response: \{error.to_repr()}")
        }
      Data(stream, bytes) =>
        match subscriber.receive_object(stream, bytes) {
          Ok(@session.Pending) => ()
          Ok(@session.Received(object, _trailing)) => {
            println(
              "Received Object (group \{object.group_id}, object \{object.object_id}, \{object.payload.length()} bytes)",
            )
            return
          }
          Err(error) => fail("invalid Object: \{error.to_repr()}")
        }
      StreamFinished(_) => ()
      StreamReset(_, code) => fail("stream reset (code \{code})")
      SendComplete(stream, canceled) if stream == request_stream =>
        if canceled {
          fail("SUBSCRIBE send was canceled")
        }
      SendComplete(_, _) | SendStopped(_, _) => ()
      Closed(code) => fail("peer closed (application code \{code})")
      TransportClosed(status) => fail("QUIC failed (MsQuic status \{status})")
      QueueOverflow => fail("transport event buffer overflow")
      Timeout => ()
    }
  }
  fail("timed out waiting for an Object")
}

///|
fn main {
  let args = @env.args()
  guard args.length() >= 2 else {
    eprintln(
      "usage: track_probe publish HOST PORT PATH NAMESPACE TRACK OBJECT [--insecure]\n       track_probe subscribe HOST PORT PATH NAMESPACE TRACK [--insecure]",
    )
    exit(2)
    return
  }
  let insecure = args[args.length() - 1] == "--insecure"
  let expected_publisher_args = if insecure { 9 } else { 8 }
  let expected_subscriber_args = if insecure { 8 } else { 7 }
  try {
    let port = @string.parse_int(args[3])
    guard args[4].is_empty() ||
      args[4].has_prefix("/") ||
      args[4].has_prefix("?") else {
      fail("PATH must be empty or start with / or ?")
    }
    match args[1] {
      "publish" if args.length() == expected_publisher_args =>
        run_publisher(
          args[2],
          port,
          args[4],
          args[5],
          args[6],
          args[7],
          insecure,
        )
      "subscribe" if args.length() == expected_subscriber_args =>
        run_subscriber(args[2], port, args[4], args[5], args[6], insecure)
      _ => {
        eprintln(
          "usage: track_probe publish HOST PORT PATH NAMESPACE TRACK OBJECT [--insecure]\n       track_probe subscribe HOST PORT PATH NAMESPACE TRACK [--insecure]",
        )
        exit(2)
      }
    }
  } catch {
    error => {
      eprintln(error.to_string())
      exit(1)
    }
  }
}
