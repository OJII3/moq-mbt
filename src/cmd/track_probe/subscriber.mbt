///|
priv struct SubscriberProbeState {
  group_count : UInt64
  objects_per_group : UInt64
  next_object_ids : Map[UInt64, UInt64]
  finished_groups : Map[UInt64, Bool]
  mut received_objects : UInt64
}

///|
fn handle_subscriber_notices(
  notices : Array[@session.ClientSessionNotice],
  state : SubscriberProbeState,
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
      @session.ControlGoAwayReceived(uri, timeout_ms, request_id) =>
        eprintln(
          "session GOAWAY received (URI \{uri.length()} bytes, timeout \{timeout_ms} ms, request ID \{request_id})",
        )
      @session.RequestGoAwayReceived(request_id, stream, uri, timeout_ms) =>
        eprintln(
          "request GOAWAY received (request \{request_id}, stream \{stream}, URI \{uri.length()} bytes, timeout \{timeout_ms} ms)",
        )
      @session.SessionEnded(end) => fail("session ended: \{end.to_repr()}")
      @session.ObjectReceived(_, header, frames) => {
        guard header.group_id < state.group_count else {
          fail("received unexpected group \{header.group_id}")
        }
        for frame in frames {
          let expected_object_id = match
            state.next_object_ids.get(header.group_id) {
            Some(value) => value
            None => 0UL
          }
          guard frame.object_id == expected_object_id else {
            fail(
              "unexpected object order in group \{header.group_id}: got \{frame.object_id}, expected \{expected_object_id}",
            )
          }
          let expected_payload = probe_payload(header.group_id, frame.object_id)
          guard frame.payload == expected_payload else {
            fail(
              "payload mismatch for group \{header.group_id}, object \{frame.object_id}",
            )
          }
          state.next_object_ids[header.group_id] = expected_object_id + 1UL
          state.received_objects = state.received_objects + 1UL
          println(
            "Received Object (group \{header.group_id}, object \{frame.object_id}, \{frame.payload.length()} bytes)",
          )
        }
      }
      @session.GroupFinished(_, group_id) => {
        guard group_id < state.group_count else {
          fail("finished unexpected group \{group_id}")
        }
        guard state.finished_groups.get(group_id) is None else {
          fail("group \{group_id} finished more than once")
        }
        let next_object_id = match state.next_object_ids.get(group_id) {
          Some(value) => value
          None => 0UL
        }
        guard next_object_id == state.objects_per_group else {
          fail(
            "group \{group_id} ended after \{next_object_id} Objects; expected \{state.objects_per_group}",
          )
        }
        state.finished_groups[group_id] = true
        println("Group finished (group \{group_id})")
      }
      @session.ObjectStreamReset(_, stream) =>
        fail("object stream \{stream} was reset")
      @session.PublishDone(_, done) => {
        guard done.stream_count == state.group_count else {
          fail(
            "PUBLISH_DONE reported \{done.stream_count} streams; expected \{state.group_count}",
          )
        }
        let expected_object_count = state.group_count * state.objects_per_group
        guard state.received_objects == expected_object_count else {
          fail(
            "received \{state.received_objects} Objects; expected \{expected_object_count}",
          )
        }
        guard state.finished_groups.length().to_uint64() == state.group_count else {
          fail("PUBLISH_DONE arrived before all groups finished")
        }
        println(
          "Verified \{state.received_objects} Objects across \{state.group_count} groups; PUBLISH_DONE received",
        )
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
  group_count : Int32,
  objects_per_group : Int32,
  insecure : Bool,
) -> Unit raise {
  let track : @protocol.TrackName = {
    namespace_fields: [@utf8.encode(namespace_field)],
    name: @utf8.encode(track_name),
  }
  let (client, session) = setup_client(host, port, path, insecure)
  defer client.close()
  let send_counts : Map[Int64, UInt64] = Map([])
  let (request_id, actions) = session.subscribe_track(track).unwrap()
  let (notices, opened) = drive_session_actions(
    client, session, actions, send_counts,
  )
  let state : SubscriberProbeState = {
    group_count: group_count.to_uint64(),
    objects_per_group: objects_per_group.to_uint64(),
    next_object_ids: {},
    finished_groups: {},
    received_objects: 0UL,
  }
  ignore(handle_subscriber_notices(notices, state))
  let mut request_stream : Int64? = None
  for (purpose, stream) in opened {
    match purpose {
      @session.Request(opened_request_id) if opened_request_id == request_id =>
        request_stream = Some(stream)
      _ => ()
    }
  }
  let request_stream = request_stream.unwrap()
  let deadline = @env.now() + 30000
  while @env.now() < deadline {
    match client.next_event(100) {
      Connected => ()
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
          send_counts,
        )
        if handle_subscriber_notices(notices, state) {
          return
        }
      }
      Data(stream, bytes) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(session, @session.StreamData(stream, bytes)),
          send_counts,
        )
        if handle_subscriber_notices(notices, state) {
          return
        }
      }
      Datagram(bytes) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(session, @session.DatagramReceived(bytes)),
          send_counts,
        )
        if handle_subscriber_notices(notices, state) {
          return
        }
      }
      StreamFinished(stream) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(session, @session.StreamFinished(stream)),
          send_counts,
        )
        if handle_subscriber_notices(notices, state) {
          return
        }
      }
      StreamReset(stream, code) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.StreamReset(stream, code.reinterpret_as_uint64()),
          ),
          send_counts,
        )
        if handle_subscriber_notices(notices, state) {
          return
        }
      }
      SendComplete(stream, canceled) => {
        note_send_complete(send_counts, stream)
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.SendComplete(stream, canceled),
          ),
          send_counts,
        )
        if handle_subscriber_notices(notices, state) {
          return
        }
        if canceled && stream == request_stream {
          fail("SUBSCRIBE send was canceled")
        }
      }
      SendStopped(stream, code) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.SendStopped(stream, code.reinterpret_as_uint64()),
          ),
          send_counts,
        )
        if handle_subscriber_notices(notices, state) {
          return
        }
      }
      DatagramSendComplete => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(session, @session.DatagramSendComplete),
          send_counts,
        )
        if handle_subscriber_notices(notices, state) {
          return
        }
      }
      Closed(code) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.PeerClosed(code.reinterpret_as_uint64()),
          ),
          send_counts,
        )
        if handle_subscriber_notices(notices, state) {
          return
        }
      }
      TransportClosed(status) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.TransportFailed(status.to_int64()),
          ),
          send_counts,
        )
        if handle_subscriber_notices(notices, state) {
          return
        }
      }
      QueueOverflow => fail("transport event buffer overflow")
      Timeout => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_clock(session),
          send_counts,
        )
        if handle_subscriber_notices(notices, state) {
          return
        }
      }
    }
  }
  fail(
    "timed out waiting for PUBLISH_DONE after \{state.received_objects} Objects",
  )
}
