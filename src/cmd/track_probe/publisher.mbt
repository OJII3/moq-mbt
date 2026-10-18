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
