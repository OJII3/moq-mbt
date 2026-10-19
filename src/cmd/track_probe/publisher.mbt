///|
priv struct PublisherProbeState {
  mut subscription_stream : Int64?
  mut object_stream : Int64?
  mut done_stream : Int64?
}

///|
fn handle_publisher_notices(
  client : @transport.Client,
  session : @session.ClientSession,
  notices : Array[@session.ClientSessionNotice],
  payload : Bytes,
  send_counts : Map[Int64, UInt64],
  state : PublisherProbeState,
) -> Unit raise {
  let mut pending = notices
  let mut index = 0
  while index < pending.length() {
    let notice = pending[index]
    index += 1
    match notice {
      @session.PublishAccepted(_) => {
        eprintln("PUBLISH request accepted")
        println("Track publication accepted; waiting for a subscriber")
      }
      @session.PublishRejected(_, error) =>
        fail("PUBLISH rejected (\{error.code})")
      @session.SubscriptionRequested(request_id, stream, _) => {
        eprintln("SUBSCRIBE request \{request_id} received")
        let (more, _) = drive_session_result(
          client,
          session,
          session.accept_subscription(stream),
          send_counts,
        )
        pending = pending + more
      }
      @session.SubscriptionAccepted(_, stream, track_alias) => {
        state.subscription_stream = Some(stream)
        let header : @protocol.SubgroupHeader = {
          track_alias,
          group_id: 0,
          subgroup_id_mode: @protocol.Zero,
          publisher_priority: None,
          end_of_group: true,
          first_object: true,
          has_properties: false,
        }
        let (more, opened) = drive_session_result(
          client,
          session,
          session.open_object_stream(stream, header),
          send_counts,
        )
        pending = pending + more
        let mut object_stream : Int64? = None
        for (purpose, opened_stream) in opened {
          match purpose {
            @session.Object(_) => object_stream = Some(opened_stream)
            _ => ()
          }
        }
        let object_stream = object_stream.unwrap()
        state.object_stream = Some(object_stream)
        let frame : @protocol.SubgroupFrame = {
          object_id: 0,
          status: @protocol.Normal,
          properties: b"",
          payload,
        }
        let (object_notices, _) = drive_session_result(
          client,
          session,
          session.send_object(object_stream, frame),
          send_counts,
        )
        pending = pending + object_notices
        let (finish_notices, _) = drive_session_result(
          client,
          session,
          session.finish_object_stream(object_stream),
          send_counts,
        )
        pending = pending + finish_notices
        println("Sent one Object")
      }
      @session.ObjectStreamFinished(stream) if state.object_stream ==
        Some(stream) => {
        let subscription_stream = state.subscription_stream.unwrap()
        let (more, _) = drive_session_result(
          client,
          session,
          session.publish_done(subscription_stream, 3, b""),
          send_counts,
        )
        pending = pending + more
        state.done_stream = Some(subscription_stream)
      }
      @session.PublishDone(_, _) => println("Subscription finished")
      _ => ()
    }
  }
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
  let (client, session, control_stream) = setup_client(
    host, port, path, insecure,
  )
  defer client.close()
  let send_counts : Map[Int64, UInt64] = Map([])
  let (request_id, actions) = session.publish_track(track, 1).unwrap()
  let (notices, _) = drive_session_actions(
    client, session, actions, send_counts,
  )
  let state : PublisherProbeState = {
    subscription_stream: None,
    object_stream: None,
    done_stream: None,
  }
  handle_publisher_notices(
    client,
    session,
    notices,
    @utf8.encode(payload),
    send_counts,
    state,
  )
  let deadline = @env.now() + 60000
  while @env.now() < deadline {
    match client.next_event(100) {
      Connected => ()
      StreamStarted(stream) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          session.receive(@session.RequestStreamStarted(stream)),
          send_counts,
        )
        handle_publisher_notices(
          client,
          session,
          notices,
          @utf8.encode(payload),
          send_counts,
          state,
        )
      }
      Data(stream, bytes) => {
        let event = if stream == control_stream {
          @session.ControlData(stream, bytes)
        } else {
          @session.RequestData(stream, bytes)
        }
        let (notices, _) = drive_session_result(
          client,
          session,
          session.receive(event),
          send_counts,
        )
        handle_publisher_notices(
          client,
          session,
          notices,
          @utf8.encode(payload),
          send_counts,
          state,
        )
      }
      SendComplete(stream, canceled) => {
        note_send_complete(send_counts, stream)
        if canceled {
          fail("publisher send was canceled")
        }
        let (notices, _) = drive_session_result(
          client,
          session,
          session.receive(@session.SendComplete(stream, canceled)),
          send_counts,
        )
        handle_publisher_notices(
          client,
          session,
          notices,
          @utf8.encode(payload),
          send_counts,
          state,
        )
        if state.done_stream == Some(stream) && send_counts.get(stream) is None {
          println("Subscription finished")
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
        handle_publisher_notices(
          client,
          session,
          notices,
          @utf8.encode(payload),
          send_counts,
          state,
        )
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
        handle_publisher_notices(
          client,
          session,
          notices,
          @utf8.encode(payload),
          send_counts,
          state,
        )
      }
      SendStopped(_, _) => fail("publisher send was stopped")
      Closed(code) => fail("peer closed (application code \{code})")
      TransportClosed(status) => fail("QUIC failed (MsQuic status \{status})")
      QueueOverflow => fail("transport event buffer overflow")
      Timeout => ()
    }
  }
  fail("timed out waiting for a subscriber (PUBLISH request \{request_id})")
}
