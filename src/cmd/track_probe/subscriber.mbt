///|
fn handle_subscriber_notices(
  notices : Array[@session.ClientSessionNotice],
) -> Bool raise {
  let mut finished = false
  for notice in notices {
    match notice {
      @session.SubscribeEstablished(_, track_alias) => {
        eprintln("SUBSCRIBE_OK received")
        println("Subscription accepted (track alias \{track_alias})")
      }
      @session.SubscribeRejected(_, error) =>
        fail("SUBSCRIBE rejected (\{error.code})")
      @session.ObjectReceived(_, header, frames) =>
        for frame in frames {
          println(
            "Received Object (group \{header.group_id}, object \{frame.object_id}, \{frame.payload.length()} bytes)",
          )
        }
      @session.GroupFinished(_, _) => eprintln("Group finished")
      @session.ObjectStreamReset(_, _) => eprintln("Object stream reset")
      @session.PublishDone(_, _) => {
        println("Subscription finished")
        finished = true
      }
      _ => ()
    }
  }
  finished
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
  let (client, session, control_stream) = setup_client(
    host, port, path, insecure,
  )
  defer client.close()
  let send_counts : Map[Int64, UInt64] = Map([])
  let (request_id, actions) = session.subscribe_track(track).unwrap()
  let (notices, opened) = drive_session_actions(
    client, session, actions, send_counts,
  )
  ignore(handle_subscriber_notices(notices))
  let mut request_stream : Int64? = None
  for (purpose, stream) in opened {
    match purpose {
      @session.Request(opened_request_id) if opened_request_id == request_id =>
        request_stream = Some(stream)
      _ => ()
    }
  }
  let request_stream = request_stream.unwrap()
  let deadline = @env.now() + 15000
  while @env.now() < deadline {
    match client.next_event(100) {
      Connected => ()
      StreamStarted(stream) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          session.receive(@session.SubgroupStreamStarted(stream)),
          send_counts,
        )
        if handle_subscriber_notices(notices) {
          return
        }
      }
      Data(stream, bytes) => {
        let event = if stream == control_stream {
          @session.ControlData(stream, bytes)
        } else if stream == request_stream {
          @session.RequestData(stream, bytes)
        } else {
          @session.SubgroupData(stream, bytes)
        }
        let (notices, _) = drive_session_result(
          client,
          session,
          session.receive(event),
          send_counts,
        )
        if handle_subscriber_notices(notices) {
          return
        }
      }
      StreamFinished(stream) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          session.receive(@session.StreamFinished(stream)),
          send_counts,
        )
        if handle_subscriber_notices(notices) {
          return
        }
      }
      StreamReset(stream, code) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          session.receive(
            @session.StreamReset(stream, code.reinterpret_as_uint64()),
          ),
          send_counts,
        )
        if handle_subscriber_notices(notices) {
          return
        }
      }
      SendComplete(stream, canceled) => {
        note_send_complete(send_counts, stream)
        if canceled && stream == request_stream {
          fail("SUBSCRIBE send was canceled")
        }
        let (notices, _) = drive_session_result(
          client,
          session,
          session.receive(@session.SendComplete(stream, canceled)),
          send_counts,
        )
        if handle_subscriber_notices(notices) {
          return
        }
      }
      SendStopped(_, _) => ()
      Closed(code) => fail("peer closed (application code \{code})")
      TransportClosed(status) => fail("QUIC failed (MsQuic status \{status})")
      QueueOverflow => fail("transport event buffer overflow")
      Timeout => ()
    }
  }
  fail("timed out waiting for an Object")
}
