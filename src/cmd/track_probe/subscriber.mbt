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
          Ok(@session.Finished(_, _)) => return
          Err(error) => fail("invalid SUBSCRIBE response: \{error.to_repr()}")
        }
      Data(stream, bytes) =>
        match subscriber.receive_object(stream, bytes) {
          Ok(@session.Pending) => ()
          Ok(@session.Received(header, frames)) =>
            for frame in frames {
              println(
                "Received Object (group \{header.group_id}, object \{frame.object_id}, \{frame.payload.length()} bytes)",
              )
            }
          Err(error) => fail("invalid Object: \{error.to_repr()}")
        }
      StreamFinished(stream) if stream == request_stream => ()
      StreamFinished(stream) =>
        match subscriber.finish_object_stream(stream) {
          Ok(true) => eprintln("Group finished")
          Ok(false) => ()
          Err(error) =>
            fail("invalid finished Object stream: \{error.to_repr()}")
        }
      StreamReset(stream, _) if stream != request_stream =>
        subscriber.reset_object_stream(stream)
      StreamReset(_, _) => ()
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
