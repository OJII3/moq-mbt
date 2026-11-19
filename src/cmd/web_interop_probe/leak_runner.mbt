///|
extern "js" fn ffi_register_leak_runner(
  runner : (@js.Any) -> @js.Promise[@js.Any],
) -> Unit =
  #| (runner) => { globalThis.__moqRunLeakScenario = runner; }

///|
extern "js" fn ffi_track_weakref(target : @js.Any) -> Unit =
  #| (target) => {
  #|   globalThis.__moqWeakRefs ??= [];
  #|   globalThis.__moqWeakRefs.push(new WeakRef(target));
  #| }

///|
extern "js" fn ffi_config_string(
  config : @js.Any,
  key : String,
  fallback : String,
) -> String =
  #| (config, key, fallback) => config[key] ?? fallback

///|
extern "js" fn ffi_config_int(
  config : @js.Any,
  key : String,
  fallback : Int32,
) -> Int32 =
  #| (config, key, fallback) => config[key] ?? fallback

///|
extern "js" fn ffi_config_bool(
  config : @js.Any,
  key : String,
  fallback : Bool,
) -> Bool =
  #| (config, key, fallback) => config[key] ?? fallback

///|
extern "js" fn ffi_config_hash(config : @js.Any) -> @arraybuffer.Uint8Array =
  #| (config) => new Uint8Array(config.certificateHash ?? [])

///|
extern "js" fn ffi_config_has_hash(config : @js.Any) -> Bool =
  #| (config) => (config.certificateHash?.length ?? 0) > 0

///|
extern "js" fn ffi_notify_callback(config : @js.Any, key : String) -> Unit =
  #| (config, key) => { config[key]?.(); }

///|
extern "js" fn ffi_wait_callback_promise(
  config : @js.Any,
  key : String,
) -> @js.Promise[Unit] =
  #| (config, key) => config[key] ?? Promise.resolve()

///|
extern "js" fn ffi_leak_ok(
  active_incoming : Int32,
  active_outgoing : Int32,
  count_overflows : Int32,
  byte_overflows : Int32,
  limit_overflows : Int32,
) -> @js.Any =
  #| (activeIncoming, activeOutgoing, eventQueueCountOverflows, eventQueueByteOverflows, activeStreamLimitOverflows) => ({
  #|   ok: true,
  #|   streamStats: {
  #|     activeIncoming,
  #|     activeOutgoing,
  #|     eventQueueCountOverflows,
  #|     eventQueueByteOverflows,
  #|     activeStreamLimitOverflows,
  #|   },
  #| })

///|
extern "js" fn ffi_leak_error(message : String) -> @js.Any =
  #| (message) => ({ ok: false, error: message })

///|
fn leak_stats_result(
  stats : @js_adapter.WebTransportStreamStatistics,
) -> @js.Any {
  ffi_leak_ok(
    stats.active_incoming,
    stats.active_outgoing,
    stats.event_queue_count_overflows,
    stats.event_queue_byte_overflows,
    stats.active_stream_limit_overflows,
  )
}

///|
async fn close_leak_client(client : @js_adapter.BrowserClient) -> String? {
  match client.close(0L, "leak check complete") {
    Err(InvalidCloseCode(code)) => Some("invalid close code \{code}")
    Err(CloseReasonTooLong(length)) => Some("close reason too long \{length}")
    Ok(_) =>
      match client.closed().wait() {
        LocalClose => None
        PeerClose(_) => Some("connection closed by the peer")
        Failed(error) => Some("connection close failed: \{error.message}")
      }
  }
}

///|
fn dispatch_leak_driver(
  driver : @js_adapter.WebTransportSessionDriver,
  actions : Array[@session.SessionAction],
) -> String? {
  match driver.dispatch(actions) {
    Accepted => None
    QueueFull => Some("action queue full")
    TooLarge => Some("action batch too large")
    DriverFailed => Some("session driver failed")
  }
}

///|
async fn run_leak_subscribe(config : @js.Any) -> @js.Any {
  let uri = ffi_config_string(config, "uri", "")
  let setup = @protocol.Setup::{ options: [], }
  let client_result = if ffi_config_has_hash(config) {
    @js_adapter.BrowserClient::connect(uri, setup, certificate_hashes=[
      ffi_config_hash(config),
    ])
  } else {
    @js_adapter.BrowserClient::connect(uri, setup)
  }
  let client = match client_result {
    Ok(client) => client
    Err(message) => return ffi_leak_error(message.to_string())
  }
  ffi_track_weakref(@js.any(client))
  match client.start() {
    Ok(_) => ()
    Err(error) => {
      ignore(client.close(0L, "start failed"))
      ignore(client.closed().wait())
      return ffi_leak_error("\{error.to_repr()}")
    }
  }
  let track = @protocol.TrackName::{
    namespace_fields: [
      @utf8.encode(ffi_config_string(config, "namespace", "interop")),
    ],
    name: @utf8.encode(ffi_config_string(config, "track", "multi-object")),
  }
  let groups = ffi_config_int(config, "groups", 2)
  let objects_per_group = ffi_config_int(config, "objectsPerGroup", 2)
  let object_bytes = ffi_config_int(config, "objectBytes", 0)
  let expect_datagrams = ffi_config_bool(config, "datagrams", false)
  let mut request_id = @session.RequestId(0UL)
  let mut subscribed = false
  let mut received_objects = 0
  let mut finished_groups = 0
  let next_object_ids : Map[UInt64, UInt64] = Map([])
  let finished_group_ids : Map[UInt64, Bool] = Map([])
  let mut track_alias : UInt64? = None
  let mut received_sample_datagram = false
  for ;; {
    match client.next_notice().wait() {
      None => return ffi_leak_error("WebTransport session ended")
      Some(HandshakeComplete(_)) if !subscribed => {
        ffi_notify_callback(config, "onReady")
        ffi_wait_callback_promise(config, "startPromise").wait()
        match client.next_opened_stream().wait() {
          Some((Control, _)) => ()
          Some(_) => return ffi_leak_error("expected the control stream first")
          None => return ffi_leak_error("session ended opening control stream")
        }
        match client.session().subscribe_track(track) {
          Ok((id, actions)) => {
            request_id = id
            if !dispatch_actions(client, actions) {
              return ffi_leak_error("failed to dispatch SUBSCRIBE")
            }
            match client.next_opened_stream().wait() {
              Some((Request(opened_id), _)) if opened_id == id => ()
              Some(_) =>
                return ffi_leak_error("expected SUBSCRIBE request stream")
              None => return ffi_leak_error("session ended opening SUBSCRIBE")
            }
          }
          Err(error) => return ffi_leak_error("\{error.to_repr()}")
        }
      }
      Some(SubscribeEstablished(id, track_alias_value)) if id == request_id => {
        track_alias = Some(track_alias_value.to_uint64())
        subscribed = true
        ffi_notify_callback(config, "onSubscribed")
      }
      Some(ObjectReceived(id, _, header, frames)) if id == request_id => {
        if Some(header.track_alias) != track_alias {
          return ffi_leak_error("Object Track Alias did not match SUBSCRIBE_OK")
        }
        if header.group_id >= groups.to_uint64() {
          return ffi_leak_error("received unexpected group \{header.group_id}")
        }
        for frame in frames {
          let expected_object_id = next_object_ids
            .get(header.group_id)
            .unwrap_or(0UL)
          if frame.object_id != expected_object_id {
            return ffi_leak_error(
              "unexpected object order in group \{header.group_id}",
            )
          }
          let expected = browser_object_payload(
            header.group_id,
            frame.object_id,
            object_bytes,
          )
          if frame.payload != expected || frame.properties != b"\x02\x2a" {
            return ffi_leak_error("Object payload or Properties mismatch")
          }
          next_object_ids[header.group_id] = expected_object_id + 1UL
          received_objects += 1
        }
      }
      Some(ObjectDatagramReceived(id, _, datagram)) if id == request_id => {
        if !expect_datagrams {
          return ffi_leak_error("received unexpected Object Datagram")
        }
        if Some(datagram.track_alias) != track_alias {
          return ffi_leak_error(
            "Object Datagram Alias did not match SUBSCRIBE_OK",
          )
        }
        match (datagram.status, datagram.end_of_group, datagram.payload) {
          (Normal, false, b"track-probe-object-datagram") => {
            if datagram.group_id != groups.to_uint64() ||
              datagram.object_id != 0UL {
              return ffi_leak_error(
                "normal Object Datagram fields did not match",
              )
            }
            received_sample_datagram = true
          }
          (Normal, true, b"track-probe-last-in-group") =>
            if datagram.group_id != groups.to_uint64() ||
              datagram.object_id != 1UL {
              return ffi_leak_error(
                "end-of-group Object Datagram fields did not match",
              )
            }
          (EndOfGroup, false, b"") =>
            if datagram.group_id != groups.to_uint64() + 1UL ||
              datagram.object_id != 0UL {
              return ffi_leak_error(
                "EndOfGroup Object Datagram fields did not match",
              )
            }
          (EndOfTrack, false, b"") =>
            if datagram.group_id != groups.to_uint64() + 2UL ||
              datagram.object_id != 0UL {
              return ffi_leak_error(
                "EndOfTrack Object Datagram fields did not match",
              )
            }
          _ =>
            return ffi_leak_error(
              "Object Datagram status or payload did not match",
            )
        }
      }
      Some(GroupFinished(id, group_id)) if id == request_id => {
        if group_id >= groups.to_uint64() {
          return ffi_leak_error("finished unexpected group \{group_id}")
        }
        if finished_group_ids.get(group_id) is Some(_) {
          return ffi_leak_error("group \{group_id} finished more than once")
        }
        let next_object_id = next_object_ids.get(group_id).unwrap_or(0UL)
        if next_object_id != objects_per_group.to_uint64() {
          return ffi_leak_error(
            "group \{group_id} ended after \{next_object_id} objects",
          )
        }
        finished_group_ids[group_id] = true
        finished_groups += 1
      }
      Some(PublishDone(id, done)) if id == request_id => {
        let expected_objects = groups * objects_per_group
        if done.stream_count != groups.to_uint64() ||
          received_objects != expected_objects ||
          finished_groups != groups ||
          (expect_datagrams && !received_sample_datagram) {
          return ffi_leak_error(
            "PUBLISH_DONE mismatch: streams=\{done.stream_count}, objects=\{received_objects}, groups=\{finished_groups}",
          )
        }
        let stats = client.stream_statistics()
        match close_leak_client(client) {
          Some(error) => return ffi_leak_error(error)
          None => ()
        }
        return leak_stats_result(stats)
      }
      Some(SubscribeRejected(id, error)) if id == request_id =>
        return ffi_leak_error("SUBSCRIBE rejected \{error.code}")
      Some(SessionEnded(end)) =>
        return ffi_leak_error("session ended \{end.to_repr()}")
      _ => ()
    }
  }
}

///|
async fn run_leak_driver_subscribe(config : @js.Any) -> @js.Any {
  let uri = ffi_config_string(config, "uri", "")
  let setup = @protocol.Setup::{ options: [], }
  let connection = match
    (if ffi_config_has_hash(config) {
      @js_adapter.WebTransportConnection::connect(uri, certificate_hashes=[
        ffi_config_hash(config),
      ])
    } else {
      @js_adapter.WebTransportConnection::connect(uri)
    }) {
    Ok(connection) => connection
    Err(message) => return ffi_leak_error(message)
  }
  let driver = match
    @js_adapter.WebTransportSessionDriver::from_setup(connection, setup) {
    Ok(driver) => driver
    Err(error) => {
      ignore(connection.close(0L, "invalid setup"))
      return ffi_leak_error("\{error.to_repr()}")
    }
  }
  ffi_track_weakref(@js.any(connection))
  ffi_track_weakref(@js.any(driver))
  match driver.start() {
    Ok(_) => ()
    Err(error) => {
      ignore(connection.close(0L, "start failed"))
      ignore(connection.closed().wait())
      return ffi_leak_error("\{error.to_repr()}")
    }
  }
  let track = @protocol.TrackName::{
    namespace_fields: [
      @utf8.encode(ffi_config_string(config, "namespace", "interop")),
    ],
    name: @utf8.encode(ffi_config_string(config, "track", "multi-object")),
  }
  let groups = ffi_config_int(config, "groups", 2)
  let objects_per_group = ffi_config_int(config, "objectsPerGroup", 2)
  let object_bytes = ffi_config_int(config, "objectBytes", 0)
  let mut request_id = @session.RequestId(0UL)
  let mut subscribed = false
  let mut received_objects = 0
  let mut finished_groups = 0
  let next_object_ids : Map[UInt64, UInt64] = Map([])
  let mut track_alias : UInt64? = None
  for ;; {
    match driver.next_notice().wait() {
      None => return ffi_leak_error("WebTransport session ended")
      Some(HandshakeComplete(_)) if !subscribed => {
        ffi_notify_callback(config, "onReady")
        ffi_wait_callback_promise(config, "startPromise").wait()
        match driver.next_opened_stream().wait() {
          Some((Control, _)) => ()
          _ => return ffi_leak_error("expected control stream")
        }
        match driver.session().subscribe_track(track) {
          Ok((id, actions)) => {
            request_id = id
            match dispatch_leak_driver(driver, actions) {
              Some(error) => return ffi_leak_error(error)
              None => ()
            }
            match driver.next_opened_stream().wait() {
              Some((Request(opened_id), _)) if opened_id == id => ()
              _ => return ffi_leak_error("expected SUBSCRIBE request stream")
            }
          }
          Err(error) => return ffi_leak_error("\{error.to_repr()}")
        }
      }
      Some(SubscribeEstablished(id, track_alias_value)) if id == request_id => {
        track_alias = Some(track_alias_value.to_uint64())
        subscribed = true
        ffi_notify_callback(config, "onSubscribed")
      }
      Some(ObjectReceived(id, _, header, frames)) if id == request_id => {
        if Some(header.track_alias) != track_alias {
          return ffi_leak_error("Object Track Alias did not match")
        }
        for frame in frames {
          let expected = browser_object_payload(
            header.group_id,
            frame.object_id,
            object_bytes,
          )
          if frame.payload != expected || frame.properties != b"\x02\x2a" {
            return ffi_leak_error("Object payload mismatch")
          }
          let next_id = next_object_ids.get(header.group_id).unwrap_or(0UL)
          if frame.object_id != next_id {
            return ffi_leak_error("unexpected object order")
          }
          next_object_ids[header.group_id] = next_id + 1UL
          received_objects += 1
        }
      }
      Some(GroupFinished(id, _)) if id == request_id => finished_groups += 1
      Some(PublishDone(id, done)) if id == request_id => {
        let expected_objects = groups * objects_per_group
        if done.stream_count != groups.to_uint64() ||
          received_objects != expected_objects ||
          finished_groups != groups {
          return ffi_leak_error("PUBLISH_DONE mismatch")
        }
        let stats = driver.stream_statistics()
        ignore(connection.close(0L, "driver leak check complete"))
        ignore(connection.closed().wait())
        while !(driver.next_notice().wait() is None) {
          ()
        }
        return leak_stats_result(stats)
      }
      Some(SubscribeRejected(id, error)) if id == request_id =>
        return ffi_leak_error("SUBSCRIBE rejected \{error.code}")
      Some(SessionEnded(end)) =>
        return ffi_leak_error("session ended \{end.to_repr()}")
      _ => ()
    }
  }
}

///|
async fn run_leak_publish(config : @js.Any) -> @js.Any {
  let uri = ffi_config_string(config, "uri", "")
  let setup = @protocol.Setup::{ options: [], }
  let client_result = if ffi_config_has_hash(config) {
    @js_adapter.BrowserClient::connect(uri, setup, certificate_hashes=[
      ffi_config_hash(config),
    ])
  } else {
    @js_adapter.BrowserClient::connect(uri, setup)
  }
  let client = match client_result {
    Ok(client) => client
    Err(message) => return ffi_leak_error(message.to_string())
  }
  ffi_track_weakref(@js.any(client))
  match client.start() {
    Ok(_) => ()
    Err(error) => {
      ignore(client.close(0L, "start failed"))
      ignore(client.closed().wait())
      return ffi_leak_error("\{error.to_repr()}")
    }
  }
  let track = @protocol.TrackName::{
    namespace_fields: [
      @utf8.encode(ffi_config_string(config, "namespace", "interop")),
    ],
    name: @utf8.encode(ffi_config_string(config, "track", "browser-publisher")),
  }
  let groups = ffi_config_int(config, "groups", 2)
  let objects_per_group = ffi_config_int(config, "objectsPerGroup", 2)
  let object_bytes = ffi_config_int(config, "objectBytes", 0)
  let mut publish_id = @session.RequestId(0UL)
  let mut publish_stream = None
  let mut start_requested = false
  let mut forward_enabled = false
  let mut publishing = false
  for ;; {
    match client.next_notice().wait() {
      None => return ffi_leak_error("WebTransport session ended")
      Some(HandshakeComplete(_)) if publish_stream is None => {
        match client.next_opened_stream().wait() {
          Some((Control, _)) => ()
          _ => return ffi_leak_error("expected control stream")
        }
        match client.session().publish_track(track, 1UL) {
          Ok((id, actions)) => {
            publish_id = id
            if !dispatch_actions(client, actions) {
              return ffi_leak_error("failed to dispatch PUBLISH")
            }
            match client.next_opened_stream().wait() {
              Some((Request(request_id), stream)) if request_id == id =>
                publish_stream = Some(stream)
              _ => return ffi_leak_error("expected PUBLISH request stream")
            }
          }
          Err(error) => return ffi_leak_error("\{error.to_repr()}")
        }
      }
      Some(PublishAccepted(id, _, _)) if id == publish_id => {
        ffi_notify_callback(config, "onPublished")
        ffi_wait_callback_promise(config, "startPromise").wait()
        start_requested = true
      }
      Some(SubscriptionUpdateRequested(_, update_id, stream, _, parameters)) => {
        let response = @protocol.RequestOkMessage::{
          parameters: [],
          properties: [],
        }
        match
          client
          .session()
          .accept_subscription_update(stream, update_id, response) {
          Ok(actions) =>
            if !dispatch_actions(client, actions) {
              return ffi_leak_error("failed to dispatch update response")
            }
          Err(error) => return ffi_leak_error("\{error.to_repr()}")
        }
        if @protocol.forward_requested(parameters) {
          forward_enabled = true
        }
      }
      Some(PublishRejected(id, error)) if id == publish_id =>
        return ffi_leak_error("PUBLISH rejected \{error.code}")
      Some(SessionEnded(end)) =>
        return ffi_leak_error("session ended \{end.to_repr()}")
      _ => ()
    }
    if start_requested && forward_enabled && !publishing {
      publishing = true
      let stream = publish_stream.unwrap()
      for group_id in 0UL..<groups.to_uint64() {
        if !publish_group(
            client, stream, group_id, objects_per_group, object_bytes,
          ) {
          return ffi_leak_error("publish_group failed")
        }
      }
      match client.session().publish_done(stream, 0UL, b"") {
        Ok(actions) =>
          if !dispatch_actions(client, actions) {
            return ffi_leak_error("failed to dispatch PUBLISH_DONE")
          }
        Err(error) => return ffi_leak_error("\{error.to_repr()}")
      }
      let mut done_sent = false
      while !done_sent {
        match client.next_notice().wait() {
          Some(StreamSendFinished(sent_stream, Sent)) if sent_stream == stream =>
            done_sent = true
          Some(SessionEnded(end)) =>
            return ffi_leak_error("session ended \{end.to_repr()}")
          None => return ffi_leak_error("session ended before PUBLISH_DONE FIN")
          _ => ()
        }
      }
      ffi_notify_callback(config, "onPublishingDone")
      ffi_wait_callback_promise(config, "finishPromise").wait()
      let stats = client.stream_statistics()
      match close_leak_client(client) {
        Some(error) => return ffi_leak_error(error)
        None => ()
      }
      return leak_stats_result(stats)
    }
  }
}

///|
async fn run_leak_bad_hash(config : @js.Any) -> @js.Any {
  let uri = ffi_config_string(config, "uri", "")
  let setup = @protocol.Setup::{ options: [], }
  let connection = match
    @js_adapter.WebTransportConnection::connect(uri, certificate_hashes=[
      ffi_config_hash(config),
    ]) {
    Ok(connection) => connection
    Err(message) => return ffi_leak_error(message)
  }
  let driver = match
    @js_adapter.WebTransportSessionDriver::from_setup(connection, setup) {
    Ok(driver) => driver
    Err(error) => {
      ignore(connection.close(0L, "invalid setup"))
      return ffi_leak_error("\{error.to_repr()}")
    }
  }
  ffi_track_weakref(@js.any(connection))
  ffi_track_weakref(@js.any(driver))
  match driver.start() {
    Ok(_) => ()
    Err(error) => return ffi_leak_error("\{error.to_repr()}")
  }
  let mut saw_ended = false
  for ;; {
    match driver.next_notice().wait() {
      Some(SessionEnded(Transport(_))) => saw_ended = true
      Some(HandshakeComplete(_)) =>
        return ffi_leak_error(
          "incorrect certificate hash unexpectedly connected",
        )
      Some(_) => ()
      None => break
    }
  }
  ignore(connection.closed().wait())
  if !saw_ended {
    return ffi_leak_error("expected SessionEnded(Transport) on bad hash")
  }
  leak_stats_result(driver.stream_statistics())
}

///|
async fn run_leak_early_close(config : @js.Any) -> @js.Any {
  let uri = ffi_config_string(config, "uri", "")
  let setup = @protocol.Setup::{ options: [], }
  let connection = match
    (if ffi_config_has_hash(config) {
      @js_adapter.WebTransportConnection::connect(uri, certificate_hashes=[
        ffi_config_hash(config),
      ])
    } else {
      @js_adapter.WebTransportConnection::connect(uri)
    }) {
    Ok(connection) => connection
    Err(message) => return ffi_leak_error(message)
  }
  let driver = match
    @js_adapter.WebTransportSessionDriver::from_setup(connection, setup) {
    Ok(driver) => driver
    Err(error) => {
      ignore(connection.close(0L, "invalid setup"))
      return ffi_leak_error("\{error.to_repr()}")
    }
  }
  ffi_track_weakref(@js.any(connection))
  ffi_track_weakref(@js.any(driver))
  match driver.start() {
    Ok(_) => ()
    Err(error) => return ffi_leak_error("\{error.to_repr()}")
  }
  ignore(connection.close(0L, "early-close"))
  ignore(connection.closed().wait())
  while !(driver.next_notice().wait() is None) {
    ()
  }
  leak_stats_result(driver.stream_statistics())
}

///|
fn register_leak_runner() -> Unit {
  ffi_register_leak_runner(fn(config) {
    @js.from_async(async fn() {
      match ffi_config_string(config, "mode", "") {
        "subscribe" => run_leak_subscribe(config)
        "driver-subscribe" => run_leak_driver_subscribe(config)
        "publish" => run_leak_publish(config)
        "bad-hash" => run_leak_bad_hash(config)
        "early-close" => run_leak_early_close(config)
        other => ffi_leak_error("unknown leak scenario: \{other}")
      }
    })
  })
  ffi_set_status("runner-ready")
}
