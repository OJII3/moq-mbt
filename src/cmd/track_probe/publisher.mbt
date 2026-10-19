///|
priv struct PublisherProbeState {
  publish_stream : Int64
  mut done_stream : Int64?
  group_count : UInt64
  objects_per_group : Int32
  object_stream_groups : Map[Int64, UInt64]
  finished_object_streams : Map[Int64, Bool]
  mut finished_stream_count : UInt64
  mut publish_after : UInt64
  mut publish_accepted : Bool
  mut objects_started : Bool
  mut publish_done_sent : Bool
}

///|
fn start_probe_publication(
  client : @transport.Client,
  session : @session.ClientSession,
  send_counts : Map[Int64, UInt64],
  state : PublisherProbeState,
) -> Array[@session.ClientSessionNotice] raise {
  let notices : Array[@session.ClientSessionNotice] = []
  for group_index in 0..<state.group_count.to_int() {
    let group_id = group_index.to_uint64()
    let header : @protocol.SubgroupHeader = {
      track_alias: 1,
      group_id,
      subgroup_id_mode: @protocol.Zero,
      publisher_priority: None,
      end_of_group: true,
      first_object: true,
      has_properties: false,
    }
    let (more, opened) = drive_session_result(
      client,
      session,
      session.open_object_stream(state.publish_stream, header),
      send_counts,
    )
    for notice in more {
      notices.push(notice)
    }
    let mut object_stream : Int64? = None
    for (purpose, opened_stream) in opened {
      match purpose {
        @session.Object(_) => object_stream = Some(opened_stream)
        _ => ()
      }
    }
    let object_stream = object_stream.unwrap()
    state.object_stream_groups[object_stream] = group_id
    for object_index in 0..<state.objects_per_group {
      let frame : @protocol.SubgroupFrame = {
        object_id: object_index.to_uint64(),
        status: @protocol.Normal,
        properties: b"",
        payload: probe_payload(group_id, object_index.to_uint64()),
      }
      let (object_notices, _) = drive_session_result(
        client,
        session,
        session.send_object(object_stream, frame),
        send_counts,
      )
      for notice in object_notices {
        notices.push(notice)
      }
    }
    let (finish_notices, _) = drive_session_result(
      client,
      session,
      session.finish_object_stream(object_stream),
      send_counts,
    )
    for notice in finish_notices {
      notices.push(notice)
    }
    println("Sent group \{group_id} (\{state.objects_per_group} Objects)")
  }
  notices
}

///|
fn handle_publisher_notices(
  client : @transport.Client,
  session : @session.ClientSession,
  notices : Array[@session.ClientSessionNotice],
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
        state.publish_accepted = true
        // Let a separately launched probe subscriber establish its track
        // subscription before this finite publication reaches PUBLISH_DONE.
        state.publish_after = @env.now() + 1000UL
      }
      @session.PublishRejected(_, error) =>
        fail("PUBLISH rejected (\{error.code})")
      @session.ObjectStreamFinished(stream) =>
        match state.object_stream_groups.get(stream) {
          Some(group_id) => {
            guard state.finished_object_streams.get(stream) is None else {
              fail("object stream finished more than once")
            }
            state.finished_object_streams[stream] = true
            state.finished_stream_count = state.finished_stream_count + 1UL
            println("Group stream finished (group \{group_id})")
            if state.finished_stream_count == state.group_count &&
              !state.publish_done_sent {
              let (more, _) = drive_session_result(
                client,
                session,
                session.publish_done(state.publish_stream, 3, b""),
                send_counts,
              )
              pending = pending + more
              state.publish_done_sent = true
              state.done_stream = Some(state.publish_stream)
            }
          }
          None => fail("unknown object stream finished")
        }
      @session.PublishDone(_, done) => {
        guard done.stream_count == state.group_count else {
          fail(
            "PUBLISH_DONE reported \{done.stream_count} streams; expected \{state.group_count}",
          )
        }
        println("PUBLISH_DONE sent (\{done.stream_count} object streams)")
      }
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
  let (request_id, actions) = session.publish_track(track, 1).unwrap()
  let (notices, opened) = drive_session_actions(
    client, session, actions, send_counts,
  )
  eprintln("PUBLISH request sent")
  let mut request_stream : Int64? = None
  for (purpose, stream) in opened {
    match purpose {
      @session.Request(opened_request_id) if opened_request_id == request_id =>
        request_stream = Some(stream)
      _ => ()
    }
  }
  let state : PublisherProbeState = {
    publish_stream: request_stream.unwrap(),
    done_stream: None,
    group_count: group_count.to_uint64(),
    objects_per_group,
    object_stream_groups: {},
    finished_object_streams: {},
    finished_stream_count: 0UL,
    publish_after: 0UL,
    publish_accepted: false,
    objects_started: false,
    publish_done_sent: false,
  }
  handle_publisher_notices(client, session, notices, send_counts, state)
  let deadline = @env.now() + 60000
  while @env.now() < deadline {
    match client.next_event(100) {
      Connected => ()
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
        handle_publisher_notices(client, session, notices, send_counts, state)
      }
      Data(stream, bytes) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          session.receive(@session.StreamData(stream, bytes)),
          send_counts,
        )
        handle_publisher_notices(client, session, notices, send_counts, state)
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
        handle_publisher_notices(client, session, notices, send_counts, state)
        if state.done_stream == Some(stream) && send_counts.get(stream) is None {
          println("PUBLISH_DONE send completed")
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
        handle_publisher_notices(client, session, notices, send_counts, state)
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
        handle_publisher_notices(client, session, notices, send_counts, state)
      }
      SendStopped(stream, code) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          session.receive(
            @session.SendStopped(stream, code.reinterpret_as_uint64()),
          ),
          send_counts,
        )
        handle_publisher_notices(client, session, notices, send_counts, state)
      }
      Closed(code) => fail("peer closed (application code \{code})")
      TransportClosed(status) => fail("QUIC failed (MsQuic status \{status})")
      QueueOverflow => fail("transport event buffer overflow")
      Timeout => ()
    }
    if state.publish_accepted &&
      !state.objects_started &&
      @env.now() >= state.publish_after {
      state.objects_started = true
      let notices = start_probe_publication(client, session, send_counts, state)
      handle_publisher_notices(client, session, notices, send_counts, state)
    }
  }
  fail("timed out waiting for a subscriber (PUBLISH request \{request_id})")
}
