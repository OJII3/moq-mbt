///|
priv struct PublisherProbeState {
  publish_stream : Int64
  mut done_stream : Int64?
  group_count : UInt64
  objects_per_group : Int32
  object_stream_groups : Map[Int64, UInt64]
  finished_object_streams : Map[Int64, Bool]
  mut finished_stream_count : UInt64
  mut publish_accepted : Bool
  mut publish_after : UInt64
  mut peer_ready : Bool
  wait_for_start : Bool
  datagrams : Bool
  mut datagram_send_count : UInt64
  mut datagram_send_final_count : UInt64
  mut next_group_index : Int32
  mut publish_done_sent : Bool
}

///|
fn start_probe_group(
  client : @transport.Client,
  session : @session.ClientSession,
  driver : SessionActionDriver,
  state : PublisherProbeState,
  group_index : Int32,
) -> Array[@session.ClientSessionNotice] raise {
  let notices : Array[@session.ClientSessionNotice] = []
  let group_id = group_index.to_uint64()
  let header : @protocol.SubgroupHeader = {
    track_alias: 1,
    group_id,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: true,
    first_object: true,
    has_properties: true,
  }
  let (more, opened) = drive_session_result(
    client,
    session,
    session.open_object_stream(state.publish_stream, header),
    driver,
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
      properties: probe_properties(),
      payload: probe_payload(group_id, object_index.to_uint64()),
    }
    let (object_notices, _) = drive_session_result(
      client,
      session,
      session.send_object(object_stream, frame),
      driver,
    )
    for notice in object_notices {
      notices.push(notice)
    }
  }
  let (finish_notices, _) = drive_session_result(
    client,
    session,
    session.finish_object_stream(object_stream),
    driver,
  )
  for notice in finish_notices {
    notices.push(notice)
  }
  state.next_group_index = group_index + 1
  println("Sent group \{group_id} (\{state.objects_per_group} Objects)")
  notices
}

///|
fn probe_datagrams(group_count : UInt64) -> Array[@protocol.ObjectDatagram] {
  [
    {
      track_alias: 1UL,
      group_id: group_count,
      object_id: 0UL,
      publisher_priority: None,
      end_of_group: false,
      properties: b"",
      status: @protocol.Normal,
      payload: b"track-probe-object-datagram",
    },
    {
      track_alias: 1UL,
      group_id: group_count,
      object_id: 1UL,
      publisher_priority: None,
      end_of_group: true,
      properties: b"",
      status: @protocol.Normal,
      payload: b"track-probe-last-in-group",
    },
    {
      track_alias: 1UL,
      group_id: group_count + 1UL,
      object_id: 0UL,
      publisher_priority: None,
      end_of_group: false,
      properties: b"",
      status: @protocol.EndOfGroup,
      payload: b"",
    },
    {
      track_alias: 1UL,
      group_id: group_count + 2UL,
      object_id: 0UL,
      publisher_priority: None,
      end_of_group: false,
      properties: b"",
      status: @protocol.EndOfTrack,
      payload: b"",
    },
  ]
}

///|
fn update_enables_forwarding(
  parameters : Array[@protocol.MessageParameter],
) -> Bool {
  for parameter in parameters {
    if parameter.kind == 0x10 {
      match parameter.value {
        @protocol.Byte(value) => return value != 0
        _ => ()
      }
    }
  }
  true
}

///|
fn handle_publisher_notices(
  _client : @transport.Client,
  _session : @session.ClientSession,
  notices : Array[@session.ClientSessionNotice],
  _driver : SessionActionDriver,
  state : PublisherProbeState,
) -> Unit raise {
  for notice in notices {
    match notice {
      @session.PublishAccepted(_) => {
        eprintln("PUBLISH request accepted")
        state.publish_accepted = true
        state.publish_after = @env.now() + 1000UL
      }
      @session.PublishRejected(_, error) =>
        fail("PUBLISH rejected (\{error.code})")
      @session.SubscriptionUpdateRequested(
        request_id,
        update_id,
        stream,
        _,
        parameters
      ) => {
        eprintln(
          "PUBLISH update requested (request \{request_id}, update \{update_id}, stream \{stream})",
        )
        if update_enables_forwarding(parameters) {
          state.peer_ready = true
          eprintln("PUBLISH subscriber forwarding enabled")
          if state.wait_for_start {
            eprintln("E2E publisher released by relay subscription update")
          }
        }
        let response : @protocol.RequestOkMessage = {
          parameters: [],
          properties: [],
        }
        let (update_notices, _) = drive_session_result(
          _client,
          _session,
          _session.accept_subscription_update(stream, update_id, response),
          _driver,
        )
        for update_notice in update_notices {
          match update_notice {
            @session.SubscriptionUpdateApplied(_, applied_id, _, _) =>
              eprintln(
                "PUBLISH subscription update accepted (request \{applied_id})",
              )
            _ => ()
          }
        }
      }
      @session.ControlGoAwayReceived(uri, timeout_ms, request_id) =>
        eprintln(
          "session GOAWAY received (URI \{uri.length()} bytes, timeout \{timeout_ms} ms, request ID \{request_id})",
        )
      @session.RequestGoAwayReceived(request_id, stream, uri, timeout_ms) =>
        eprintln(
          "request GOAWAY received (request \{request_id}, stream \{stream}, URI \{uri.length()} bytes, timeout \{timeout_ms} ms)",
        )
      @session.SessionEnded(end) => fail("session ended: \{end.to_repr()}")
      @session.ObjectStreamFinished(stream) =>
        match state.object_stream_groups.get(stream) {
          Some(group_id) => {
            guard state.finished_object_streams.get(stream) is None else {
              fail("object stream finished more than once")
            }
            state.finished_object_streams[stream] = true
            state.finished_stream_count = state.finished_stream_count + 1UL
            println("Group stream finished (group \{group_id})")
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
  datagrams : Bool,
  insecure : Bool,
  hold_open : Bool,
  wait_for_start : Bool,
) -> Unit raise {
  let track : @protocol.TrackName = {
    namespace_fields: [@utf8.encode(namespace_field)],
    name: @utf8.encode(track_name),
  }
  let (client, session) = setup_client(host, port, path, insecure)
  defer client.close()
  let driver = SessionActionDriver::new()
  let (request_id, actions) = session.publish_track(track, 1).unwrap()
  let (notices, opened) = drive_session_actions(
    client, session, actions, driver,
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
    datagrams,
    object_stream_groups: {},
    finished_object_streams: {},
    finished_stream_count: 0UL,
    publish_accepted: false,
    publish_after: 0UL,
    peer_ready: false,
    wait_for_start,
    datagram_send_count: 0UL,
    datagram_send_final_count: 0UL,
    next_group_index: 0,
    publish_done_sent: false,
  }
  handle_publisher_notices(client, session, notices, driver, state)
  let deadline = @env.now() + 60000
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
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
      Data(stream, bytes) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(session, @session.StreamData(stream, bytes)),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
      Datagram(bytes) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(session, @session.DatagramReceived(bytes)),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
      SendComplete(stream, canceled) => {
        note_send_complete(driver, stream)
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.SendComplete(stream, canceled),
          ),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
        if canceled {
          fail("publisher send was canceled")
        }
        if state.done_stream == Some(stream) &&
          driver.send_counts.get(stream) is None &&
          (
            !state.datagrams ||
            state.datagram_send_final_count == state.datagram_send_count
          ) {
          println("PUBLISH_DONE send completed")
          if hold_open {
            println("Holding publisher connection open for peer verification")
            let deadline = @env.now() + 120000UL
            while @env.now() < deadline {
              match client.next_event(100) {
                Closed(_) | TransportClosed(_) => return
                _ => ()
              }
            }
            fail("timed out waiting for peer verification to close")
          }
          return
        }
      }
      StreamFinished(stream) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(session, @session.StreamFinished(stream)),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
      StreamClosed(_) => ()
      StreamReset(stream, code) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.StreamReset(stream, code.reinterpret_as_uint64()),
          ),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
      SendStopped(stream, code) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.SendStopped(stream, code.reinterpret_as_uint64()),
          ),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
      DatagramStateChanged(capability) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.DatagramSendCapabilityChanged(
              capability.enabled,
              capability.max_length,
            ),
          ),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
      DatagramSendStateChanged(id, send_state) => {
        println("Datagram send \{id} state: \{send_state.to_repr()}")
        if send_state.is_final() {
          if state.datagrams {
            state.datagram_send_final_count = state.datagram_send_final_count +
              1UL
          }
          let (notices, _) = drive_session_result(
            client,
            session,
            receive_session_event(session, @session.DatagramSendComplete),
            driver,
          )
          handle_publisher_notices(client, session, notices, driver, state)
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
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
      TransportClosed(status) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            @session.TransportFailed(status.to_int64()),
          ),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
      QueueOverflow => fail("transport event buffer overflow")
      Timeout => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_clock(session),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
    }
    if state.publish_accepted &&
      state.peer_ready &&
      @env.now() >= state.publish_after &&
      state.datagrams &&
      state.datagram_send_count == 0UL {
      for datagram in probe_datagrams(state.group_count) {
        let (notices, _) = drive_session_result(
          client,
          session,
          session.send_object_datagram(state.publish_stream, datagram),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
        state.datagram_send_count = state.datagram_send_count + 1UL
      }
      println("Sent four Object Datagrams (best effort)")
    }
    if state.publish_accepted &&
      state.peer_ready &&
      @env.now() >= state.publish_after &&
      state.next_group_index < state.group_count.to_int() &&
      client.has_stream_capacity() {
      let notices = start_probe_group(
        client,
        session,
        driver,
        state,
        state.next_group_index,
      )
      handle_publisher_notices(client, session, notices, driver, state)
    } else if state.publish_accepted &&
      state.peer_ready &&
      @env.now() >= state.publish_after &&
      state.next_group_index < state.group_count.to_int() {
      eprintln("Waiting for capacity to open the next object stream")
    }
    if !state.publish_done_sent &&
      state.finished_stream_count == state.group_count &&
      (
        !state.datagrams ||
        state.datagram_send_final_count == state.datagram_send_count
      ) {
      state.publish_done_sent = true
      state.done_stream = Some(state.publish_stream)
      let (notices, _) = drive_session_result(
        client,
        session,
        session.publish_done(state.publish_stream, 2, b""),
        driver,
      )
      handle_publisher_notices(client, session, notices, driver, state)
    }
  }
  fail("timed out waiting for a subscriber (PUBLISH request \{request_id})")
}
