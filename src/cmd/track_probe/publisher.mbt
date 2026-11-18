///|
priv struct PublisherProbeState {
  track : @protocol.TrackName
  publish_stream : Int64
  mut done_stream : Int64?
  group_count : UInt64
  objects_per_group : Int32
  object_bytes : Int32
  object_stream_groups : Map[Int64, UInt64]
  finished_object_streams : Map[Int64, Bool]
  mut finished_stream_count : UInt64
  mut publish_accepted : Bool
  mut publish_after : UInt64
  mut peer_ready : Bool
  wait_for_start : Bool
  wait_for_fetch : Bool
  fetch_after_groups : Int32
  mut fetch_served : Bool
  mut waiting_for_fetch : Bool
  datagrams : Bool
  mut datagram_send_count : UInt64
  mut datagram_send_final_count : UInt64
  mut next_group_index : Int32
  mut publish_done_sent : Bool
}

///|
extern "C" fn ffi_wait_for_start() -> Int32 = "getchar"

///|
fn start_probe_group(
  client : @transport.Client,
  session : @session.ClientSession,
  driver : SessionActionDriver,
  state : PublisherProbeState,
  group_index : Int32,
) -> Array[@session.SessionNotice] raise {
  let notices : Array[@session.SessionNotice] = []
  let group_id = group_index.to_uint64()
  let (writer, open_actions) = match
    session.open_object_group(
      StreamId(state.publish_stream),
      group_id,
      end_of_group=true,
      has_properties=true,
    ) {
    Ok(value) => value
    Err(error) => fail("could not open group \{group_id}: \{error.to_repr()}")
  }
  let (more, opened) = drive_session_result(
    client,
    session,
    Ok(open_actions),
    driver,
  )
  for notice in more {
    notices.push(notice)
  }
  let mut object_stream : Int64? = None
  for (purpose, opened_stream) in opened {
    match purpose {
      Object(_) => object_stream = Some(opened_stream)
      _ => ()
    }
  }
  let object_stream = object_stream.unwrap()
  state.object_stream_groups[object_stream] = group_id
  for object_index in 0..<state.objects_per_group {
    if state.object_bytes <= 0 {
      let frame = @protocol.SubgroupFrame::{
        object_id: object_index.to_uint64(),
        status: Normal,
        properties: probe_properties(),
        payload: probe_payload(group_id, object_index.to_uint64()),
      }
      let (object_notices, _) = drive_session_result(
        client,
        session,
        writer.send_frame(frame),
        driver,
      )
      for notice in object_notices {
        notices.push(notice)
      }
    } else {
      let object_id = object_index.to_uint64()
      let payload = probe_object_payload(
        group_id,
        object_id,
        state.object_bytes,
      )
      let (object, header_actions) = match
        writer.begin_object(
          payload.length().to_uint64(),
          properties=probe_properties(),
        ) {
        Ok(value) => value
        Err(error) =>
          fail(
            "could not begin group \{group_id} object \{object_id}: \{error.to_repr()}",
          )
      }
      let (header_notices, _) = drive_session_result(
        client,
        session,
        Ok(header_actions),
        driver,
      )
      for notice in header_notices {
        notices.push(notice)
      }
      let chunk_bytes = 64 * 1024
      let mut offset = 0
      while offset < payload.length() {
        let end = if offset + chunk_bytes < payload.length() {
          offset + chunk_bytes
        } else {
          payload.length()
        }
        let (write_notices, _) = drive_session_result(
          client,
          session,
          object.write(payload[offset:end].to_owned()),
          driver,
        )
        for notice in write_notices {
          notices.push(notice)
        }
        offset = end
      }
      let (object_finish_notices, _) = drive_session_result(
        client,
        session,
        object.finish(),
        driver,
      )
      for notice in object_finish_notices {
        notices.push(notice)
      }
    }
  }
  let (finish_notices, _) = drive_session_result(
    client,
    session,
    writer.finish(),
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
      status: Normal,
      payload: b"track-probe-object-datagram",
    },
    {
      track_alias: 1UL,
      group_id: group_count,
      object_id: 1UL,
      publisher_priority: None,
      end_of_group: true,
      properties: b"",
      status: Normal,
      payload: b"track-probe-last-in-group",
    },
    {
      track_alias: 1UL,
      group_id: group_count + 1UL,
      object_id: 0UL,
      publisher_priority: None,
      end_of_group: false,
      properties: b"",
      status: EndOfGroup,
      payload: b"",
    },
    {
      track_alias: 1UL,
      group_id: group_count + 2UL,
      object_id: 0UL,
      publisher_priority: None,
      end_of_group: false,
      properties: b"",
      status: EndOfTrack,
      payload: b"",
    },
  ]
}

///|
fn update_enables_forwarding(
  parameters : Array[@protocol.MessageParameter],
) -> Bool {
  @protocol.forward_requested(parameters)
}

///|
fn serve_publisher_fetch(
  client : @transport.Client,
  session : @session.ClientSession,
  driver : SessionActionDriver,
  state : PublisherProbeState,
  request_id : @session.RequestId,
  track : @protocol.TrackName,
  start : @protocol.FetchLocation,
  end : @protocol.FetchLocation,
) -> Unit raise {
  guard track == state.track else {
    fail("relay FETCH requested an unexpected track")
  }
  guard start.group <= end.group else {
    fail("relay FETCH requested an invalid group range")
  }
  eprintln(
    "Relay FETCH range: group \{start.group} object \{start.object} through group \{end.group} object \{end.object}",
  )
  let last_group = if state.fetch_after_groups > 0 {
    (state.fetch_after_groups - 1).to_uint64()
  } else {
    end.group
  }
  guard start.group <= last_group else {
    fail("relay FETCH requested groups before the retained range")
  }
  let fetch_end_object = if state.fetch_after_groups > 0 || end.object == 0UL {
    state.objects_per_group.to_uint64()
  } else {
    end.object
  }
  let response_end = @protocol.FetchLocation::{
    group: last_group,
    object: fetch_end_object,
  }
  ignore(
    drive_session_result(
      client,
      session,
      session.accept_fetch(request_id, {
        end_of_track: 0,
        end_location: response_end,
        parameters: [],
        track_properties: b"",
      }),
      driver,
    ),
  )
  let mut group = start.group
  while group <= last_group {
    let first_object = if group == start.group { start.object } else { 0UL }
    let end_object = if group == last_group {
      fetch_end_object
    } else {
      state.objects_per_group.to_uint64()
    }
    let mut object = first_object
    while object < end_object {
      ignore(
        drive_session_result(
          client,
          session,
          session.send_fetch_object(request_id, {
            group_id: group,
            subgroup_id: Some(0UL),
            object_id: object,
            publisher_priority: 128,
            datagram_forwarding: false,
            properties: probe_properties(),
            payload: probe_object_payload(group, object, state.object_bytes),
          }),
          driver,
        ),
      )
      object = object + 1UL
    }
    group = group + 1UL
  }
  ignore(
    drive_session_result(
      client,
      session,
      session.finish_fetch(request_id),
      driver,
    ),
  )
  state.fetch_served = true
  if state.fetch_after_groups > 0 {
    state.publish_after = @env.now() + 2000UL
  }
  eprintln("Served relay FETCH request")
}

///|
fn handle_publisher_notices(
  client : @transport.Client,
  session : @session.ClientSession,
  notices : Array[@session.SessionNotice],
  driver : SessionActionDriver,
  state : PublisherProbeState,
) -> Unit raise {
  for notice in notices {
    match notice {
      PublishAccepted(_, _, _) => {
        eprintln("PUBLISH request accepted")
        state.publish_accepted = true
        state.publish_after = @env.now() + 1000UL
      }
      PublishRejected(_, error) => fail("PUBLISH rejected (\{error.code})")
      SubscriptionUpdateRequested(request_id, update_id, stream, _, parameters) => {
        eprintln(
          "PUBLISH update requested (request \{request_id}, update \{update_id}, stream \{stream})",
        )
        if update_enables_forwarding(parameters) && !state.wait_for_start {
          state.peer_ready = true
          eprintln("PUBLISH subscriber forwarding enabled")
        }
        let response = @protocol.RequestOkMessage::{
          parameters: [],
          properties: [],
        }
        let (update_notices, _) = drive_session_result(
          client,
          session,
          session.accept_subscription_update(stream, update_id, response),
          driver,
        )
        for update_notice in update_notices {
          match update_notice {
            SubscriptionUpdateApplied(_, applied_id, _, _) =>
              eprintln(
                "PUBLISH subscription update accepted (request \{applied_id})",
              )
            _ => ()
          }
        }
      }
      FetchRequested(request_id, _, track, start, end, _) =>
        serve_publisher_fetch(
          client, session, driver, state, request_id, track, start, end,
        )
      ControlGoAwayReceived(uri, timeout_ms, request_id) =>
        eprintln(
          "session GOAWAY received (URI \{uri.length()} bytes, timeout \{timeout_ms} ms, request ID \{request_id})",
        )
      RequestGoAwayReceived(request_id, stream, uri, timeout_ms) =>
        eprintln(
          "request GOAWAY received (request \{request_id}, stream \{stream}, URI \{uri.length()} bytes, timeout \{timeout_ms} ms)",
        )
      SessionEnded(end) => fail("session ended: \{end.to_repr()}")
      ObjectStreamSendFinished(stream) =>
        match state.object_stream_groups.get(stream.to_int64()) {
          Some(group_id) => {
            guard state.finished_object_streams.get(stream.to_int64()) is None else {
              fail("object stream finished more than once")
            }
            state.finished_object_streams[stream.to_int64()] = true
            state.finished_stream_count = state.finished_stream_count + 1UL
            println("Group stream finished (group \{group_id})")
          }
          None => fail("unknown object stream finished")
        }
      PublishDone(_, done) => {
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
  object_bytes : Int32,
  datagrams : Bool,
  insecure : Bool,
  hold_open : Bool,
  wait_for_start : Bool,
  wait_for_fetch : Bool,
  fetch_after_groups : Int32,
) -> Unit raise {
  let track = @protocol.TrackName::{
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
      Request(opened_request_id) if opened_request_id == request_id =>
        request_stream = Some(stream)
      _ => ()
    }
  }
  let state = PublisherProbeState::{
    track,
    publish_stream: request_stream.unwrap(),
    done_stream: None,
    group_count: group_count.to_uint64(),
    objects_per_group,
    object_bytes,
    datagrams,
    object_stream_groups: {},
    finished_object_streams: {},
    finished_stream_count: 0UL,
    publish_accepted: false,
    publish_after: 0UL,
    peer_ready: !wait_for_start,
    wait_for_start,
    wait_for_fetch,
    fetch_after_groups,
    fetch_served: false,
    waiting_for_fetch: false,
    datagram_send_count: 0UL,
    datagram_send_final_count: 0UL,
    next_group_index: 0,
    publish_done_sent: false,
  }
  handle_publisher_notices(client, session, notices, driver, state)
  let mut waiting_for_start = false
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
            InboundStreamStarted(stream, client_stream_direction(direction)),
          ),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
      Data(stream, bytes) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(session, StreamData(stream, bytes)),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
      Datagram(bytes) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(session, DatagramReceived(bytes)),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
      SendComplete(stream, canceled) => {
        driver.complete_send(stream, canceled)
        let (notices, _) = take_driver_events(driver)
        handle_publisher_notices(client, session, notices, driver, state)
        if canceled {
          fail("publisher send was canceled")
        }
        if state.done_stream == Some(stream) &&
          !driver.has_pending_sends(stream) &&
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
      StreamDataCommitted(stream) => {
        driver.committed(stream)
        let (notices, _) = take_driver_events(driver)
        handle_publisher_notices(client, session, notices, driver, state)
      }
      StreamFinished(stream) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(session, InboundStreamFinished(stream)),
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
            StreamReset(stream, code.reinterpret_as_uint64()),
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
            SendStopped(stream, code.reinterpret_as_uint64()),
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
            DatagramSendCapabilityChanged(
              enabled=capability.enabled,
              max_length=capability.max_length,
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
          driver.complete_datagram()
          let (notices, _) = take_driver_events(driver)
          handle_publisher_notices(client, session, notices, driver, state)
        }
      }
      Closed(code) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(
            session,
            PeerClosed(code.reinterpret_as_uint64()),
          ),
          driver,
        )
        handle_publisher_notices(client, session, notices, driver, state)
      }
      TransportClosed(status) => {
        let (notices, _) = drive_session_result(
          client,
          session,
          receive_session_event(session, TransportFailed(status.to_int64())),
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
    if state.publish_accepted && wait_for_start && !waiting_for_start {
      waiting_for_start = true
      eprintln("PUBLISH accepted; waiting for subscriber readiness")
      ignore(ffi_wait_for_start())
      state.peer_ready = true
      eprintln("Publisher released by subscriber readiness")
    }
    if state.publish_accepted &&
      state.peer_ready &&
      @env.now() >= state.publish_after &&
      state.datagrams &&
      state.datagram_send_count == 0UL {
      for datagram in probe_datagrams(state.group_count) {
        let result = match
          session.send_object_datagram(state.publish_stream, datagram) {
          Ok((_, actions)) => Ok(actions)
          Err(error) => Err(error)
        }
        let (notices, _) = drive_session_result(client, session, result, driver)
        handle_publisher_notices(client, session, notices, driver, state)
        state.datagram_send_count = state.datagram_send_count + 1UL
      }
      println("Sent four Object Datagrams (best effort)")
    }
    let fetch_gate = state.fetch_after_groups > 0 &&
      state.next_group_index >= state.fetch_after_groups &&
      !state.fetch_served
    if fetch_gate && !state.waiting_for_fetch {
      state.waiting_for_fetch = true
      eprintln("Waiting for relay FETCH before sending the remaining groups")
    }
    if state.publish_accepted &&
      state.peer_ready &&
      @env.now() >= state.publish_after &&
      state.next_group_index < state.group_count.to_int() &&
      !fetch_gate &&
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
      state.next_group_index < state.group_count.to_int() &&
      !fetch_gate {
      eprintln("Waiting for capacity to open the next object stream")
    }
    if !state.publish_done_sent &&
      state.finished_stream_count == state.group_count &&
      (!state.wait_for_fetch || state.fetch_served) &&
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
