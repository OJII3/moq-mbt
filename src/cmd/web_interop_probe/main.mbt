///|
extern "js" fn ffi_moq_uri() -> String =
  #|() => globalThis.__moqE2EConfig.uri

///|
extern "js" fn ffi_certificate_hash() -> @arraybuffer.Uint8Array =
  #|() => new Uint8Array(globalThis.__moqE2EConfig.certificateHash)

///|
extern "js" fn ffi_has_certificate_hash() -> Bool =
  #|() => globalThis.__moqE2EConfig.certificateHash.length > 0

///|
extern "js" fn ffi_mode() -> String =
  #|() => globalThis.__moqE2EConfig.mode

///|
extern "js" fn ffi_namespace() -> String =
  #|() => globalThis.__moqE2EConfig.namespace ?? "interop"

///|
extern "js" fn ffi_track() -> String =
  #|() => globalThis.__moqE2EConfig.track ?? "multi-object"

///|
extern "js" fn ffi_groups() -> Int32 =
  #|() => globalThis.__moqE2EConfig.groups ?? 2

///|
extern "js" fn ffi_objects_per_group() -> Int32 =
  #|() => globalThis.__moqE2EConfig.objectsPerGroup ?? 2

///|
extern "js" fn ffi_object_bytes() -> Int32 =
  #|() => globalThis.__moqE2EConfig.objectBytes ?? 0

///|
extern "js" fn ffi_datagrams() -> Bool =
  #|() => globalThis.__moqE2EConfig.datagrams ?? false

///|
/// Match the native probe payload so both transports can share one scenario.
fn browser_object_payload(
  group_id : UInt64,
  object_id : UInt64,
  object_bytes : Int32,
) -> Bytes {
  if object_bytes <= 0 {
    return @utf8.encode("group-\{group_id}-object-\{object_id}")
  }
  let seed = @utf8.encode("group-\{group_id}-object-\{object_id}|")
  let payload : Array[Byte] = Array::make(object_bytes, b'.')
  for index in 0..<object_bytes {
    payload[index] = seed[index % seed.length()]
  }
  Bytes::from_array(payload)
}

///|
extern "js" fn ffi_wait_to_start() -> @js.Promise[Unit] =
  #|() => globalThis.__moqE2EStartPromise

///|
extern "js" fn ffi_wait_to_finish() -> @js.Promise[Unit] =
  #|() => globalThis.__moqE2EFinishPromise

///|
extern "js" fn ffi_set_status(value : String) -> Unit =
  #|(value) => globalThis.__moqE2EResult = { status: value }

///|
fn main {
  ignore(
    @js.from_async(async fn() {
      ffi_set_status("connecting")
      let setup = @protocol.Setup::{ options: [], }
      let client_result = if ffi_has_certificate_hash() {
        @js_adapter.BrowserClient::connect(ffi_moq_uri(), setup, certificate_hashes=[
          ffi_certificate_hash(),
        ])
      } else {
        @js_adapter.BrowserClient::connect(ffi_moq_uri(), setup)
      }
      let client = match client_result {
        Ok(client) => client
        Err(message) => {
          ffi_set_status("failed: \{message}")
          return
        }
      }
      match client.start() {
        Ok(_) => ()
        Err(error) => {
          ffi_set_status("failed: \{error.to_repr()}")
          return
        }
      }
      let track = @protocol.TrackName::{
        namespace_fields: [@utf8.encode(ffi_namespace())],
        name: @utf8.encode(ffi_track()),
      }
      ffi_set_status("waiting-handshake")
      if ffi_mode() == "publish" {
        run_publisher(client, track)
      } else {
        run_subscriber(client, track)
      }
    }),
  )
}

///|
async fn run_subscriber(
  client : @js_adapter.BrowserClient,
  track : @protocol.TrackName,
) -> Unit {
  let groups = ffi_groups()
  let objects_per_group = ffi_objects_per_group()
  let object_bytes = ffi_object_bytes()
  let expect_datagrams = ffi_datagrams()
  let mut request_id = @session.RequestId(0UL)
  let mut subscribed = false
  let mut received_objects = 0
  let mut finished_groups = 0
  let next_object_ids : Map[UInt64, UInt64] = Map([])
  let finished_group_ids : Map[UInt64, Bool] = Map([])
  let mut track_alias : UInt64? = None
  let mut received_sample_datagram = false
  let mut finished = false
  while !finished {
    match client.next_notice().wait() {
      None => {
        ffi_set_status("failed: WebTransport session ended")
        return
      }
      Some(HandshakeComplete(_)) if !subscribed => {
        ffi_set_status("ready")
        ffi_wait_to_start().wait()
        match client.next_opened_stream().wait() {
          Some((Control, _)) => ()
          Some(_) => {
            ffi_set_status("failed: expected the control stream first")
            return
          }
          None => {
            ffi_set_status("failed: session ended opening the control stream")
            return
          }
        }
        match client.session().subscribe_track(track) {
          Ok((id, actions)) => {
            request_id = id
            if !dispatch_actions(client, actions) {
              return
            }
            match client.next_opened_stream().wait() {
              Some((Request(opened_id), _)) if opened_id == id => ()
              Some(_) => {
                ffi_set_status("failed: expected the SUBSCRIBE request stream")
                return
              }
              None => {
                ffi_set_status("failed: session ended opening SUBSCRIBE")
                return
              }
            }
            ffi_set_status("subscribing")
          }
          Err(error) => {
            ffi_set_status("failed: \{error.to_repr()}")
            return
          }
        }
      }
      Some(SubscribeEstablished(id, track_alias_value)) if id == request_id => {
        track_alias = Some(track_alias_value.to_uint64())
        subscribed = true
        ffi_set_status("subscribed")
      }
      Some(ObjectReceived(id, _, header, frames)) if id == request_id => {
        if Some(header.track_alias) != track_alias {
          ffi_set_status(
            "failed: Object Track Alias did not match SUBSCRIBE_OK",
          )
          return
        }
        if header.group_id >= groups.to_uint64() {
          ffi_set_status(
            "failed: received an unexpected group \{header.group_id}",
          )
          return
        }
        for frame in frames {
          let expected_object_id = next_object_ids
            .get(header.group_id)
            .unwrap_or(0UL)
          if frame.object_id != expected_object_id {
            ffi_set_status(
              "failed: unexpected object order in group \{header.group_id}",
            )
            return
          }
          let expected = browser_object_payload(
            header.group_id,
            frame.object_id,
            object_bytes,
          )
          if frame.payload != expected || frame.properties != b"\x02\x2a" {
            ffi_set_status("failed: Object payload or Properties mismatch")
            return
          }
          next_object_ids[header.group_id] = expected_object_id + 1UL
          received_objects += 1
          ffi_set_status("objects-\{received_objects}")
        }
      }
      Some(ObjectDatagramReceived(id, _, datagram)) if id == request_id => {
        if !expect_datagrams {
          ffi_set_status("failed: received an unexpected Object Datagram")
          return
        }
        if Some(datagram.track_alias) != track_alias {
          ffi_set_status(
            "failed: Object Datagram Alias did not match SUBSCRIBE_OK",
          )
          return
        }
        match (datagram.status, datagram.end_of_group, datagram.payload) {
          (Normal, false, b"track-probe-object-datagram") => {
            if datagram.group_id != groups.to_uint64() ||
              datagram.object_id != 0UL {
              ffi_set_status(
                "failed: normal Object Datagram fields did not match",
              )
              return
            }
            received_sample_datagram = true
            ffi_set_status("datagram-sample")
          }
          (Normal, true, b"track-probe-last-in-group") =>
            if datagram.group_id != groups.to_uint64() ||
              datagram.object_id != 1UL {
              ffi_set_status(
                "failed: end-of-group Object Datagram fields did not match",
              )
              return
            }
          (EndOfGroup, false, b"") =>
            if datagram.group_id != groups.to_uint64() + 1UL ||
              datagram.object_id != 0UL {
              ffi_set_status(
                "failed: EndOfGroup Object Datagram fields did not match",
              )
              return
            }
          (EndOfTrack, false, b"") =>
            if datagram.group_id != groups.to_uint64() + 2UL ||
              datagram.object_id != 0UL {
              ffi_set_status(
                "failed: EndOfTrack Object Datagram fields did not match",
              )
              return
            }
          _ => {
            ffi_set_status(
              "failed: Object Datagram status or payload did not match",
            )
            return
          }
        }
      }
      Some(GroupFinished(id, group_id)) if id == request_id => {
        if group_id >= groups.to_uint64() {
          ffi_set_status("failed: finished an unexpected group \{group_id}")
          return
        }
        if finished_group_ids.get(group_id) is Some(_) {
          ffi_set_status("failed: group \{group_id} finished more than once")
          return
        }
        let next_object_id = next_object_ids.get(group_id).unwrap_or(0UL)
        if next_object_id != objects_per_group.to_uint64() {
          ffi_set_status(
            "failed: group \{group_id} ended after \{next_object_id} objects",
          )
          return
        }
        finished_group_ids[group_id] = true
        finished_groups += 1
        ffi_set_status("groups-\{finished_groups}")
      }
      Some(PublishDone(id, done)) if id == request_id => {
        let expected_objects = groups * objects_per_group
        if done.stream_count != groups.to_uint64() ||
          received_objects != expected_objects ||
          finished_groups != groups ||
          (expect_datagrams && !received_sample_datagram) {
          ffi_set_status(
            "failed: PUBLISH_DONE streams=\{done.stream_count}, objects=\{received_objects}, groups=\{finished_groups}, datagram=\{received_sample_datagram}",
          )
          return
        }
        finished = true
        ffi_set_status("passed")
        close_connection(client)
      }
      Some(SubscribeRejected(id, error)) if id == request_id => {
        ffi_set_status("failed: SUBSCRIBE rejected \{error.code}")
        return
      }
      Some(SessionEnded(end)) => {
        ffi_set_status("failed: session ended \{end.to_repr()}")
        return
      }
      _ => ()
    }
  }
}

///|
async fn publish_group(
  client : @js_adapter.BrowserClient,
  subscription_stream : @session.StreamId,
  group_id : UInt64,
  objects_per_group : Int32,
  object_bytes : Int32,
) -> Bool {
  let header = @protocol.SubgroupHeader::{
    track_alias: 1UL,
    group_id,
    subgroup_id_mode: Zero,
    publisher_priority: None,
    end_of_group: true,
    first_object: true,
    has_properties: true,
  }
  let actions = match
    client.session().open_object_stream(subscription_stream, header) {
    Ok(actions) => actions
    Err(error) => {
      ffi_set_status("failed: \{error.to_repr()}")
      return false
    }
  }
  ffi_set_status("opening-group-\{group_id}")
  if !dispatch_actions(client, actions) {
    return false
  }
  let object_stream = match client.next_opened_stream().wait() {
    Some((Object(_), stream)) => stream
    Some(_) => {
      ffi_set_status("failed: unexpected stream opened for Object")
      return false
    }
    None => {
      ffi_set_status("failed: WebTransport session ended opening Object")
      return false
    }
  }
  ffi_set_status("sending-group-\{group_id}")
  for object_id in 0UL..<objects_per_group.to_uint64() {
    let frame = @protocol.SubgroupFrame::{
      object_id,
      status: Normal,
      properties: b"\x02\x2a",
      payload: browser_object_payload(group_id, object_id, object_bytes),
    }
    match client.session().send_object(object_stream, frame) {
      Ok(actions) => if !dispatch_actions(client, actions) { return false }
      Err(error) => {
        ffi_set_status("failed: \{error.to_repr()}")
        return false
      }
    }
  }
  match client.session().finish_object_stream(object_stream) {
    Ok(actions) => if !dispatch_actions(client, actions) { return false }
    Err(error) => {
      ffi_set_status("failed: \{error.to_repr()}")
      return false
    }
  }
  let mut finished = false
  while !finished {
    match client.next_notice().wait() {
      Some(ObjectStreamSendFinished(stream)) if stream == object_stream =>
        finished = true
      Some(SessionEnded(end)) => {
        ffi_set_status("failed: session ended \{end.to_repr()}")
        return false
      }
      None => {
        ffi_set_status("failed: WebTransport session ended finishing Object")
        return false
      }
      _ => ()
    }
  }
  true
}

///|
async fn close_connection(client : @js_adapter.BrowserClient) -> Unit {
  match client.close(0L, "interop complete") {
    Err(InvalidCloseCode(code)) =>
      ffi_set_status("failed: invalid close code \{code}")
    Err(CloseReasonTooLong(length)) =>
      ffi_set_status("failed: close reason too long \{length}")
    Ok(_) =>
      match client.closed().wait() {
        LocalClose => ()
        PeerClose(_) => ffi_set_status("failed: connection closed by the peer")
        Failed(error) =>
          ffi_set_status("failed: connection close failed: \{error.message}")
      }
  }
}

///|
fn dispatch_actions(
  client : @js_adapter.BrowserClient,
  actions : Array[@session.SessionAction],
) -> Bool {
  match client.dispatch(actions) {
    Accepted => true
    QueueFull => {
      ffi_set_status("failed: action queue full")
      false
    }
    TooLarge => {
      ffi_set_status("failed: action batch too large")
      false
    }
    DriverFailed => {
      ffi_set_status("failed: session driver failed")
      false
    }
  }
}

///|
async fn run_publisher(
  client : @js_adapter.BrowserClient,
  track : @protocol.TrackName,
) -> Unit {
  let groups = ffi_groups()
  let objects_per_group = ffi_objects_per_group()
  let object_bytes = ffi_object_bytes()
  let mut publish_id = @session.RequestId(0UL)
  let mut publish_stream = None
  let mut finished = false
  while !finished {
    match client.next_notice().wait() {
      None => {
        ffi_set_status("failed: WebTransport session ended")
        return
      }
      Some(HandshakeComplete(_)) if publish_stream is None => {
        ffi_set_status("handshake")
        match client.next_opened_stream().wait() {
          Some((Control, _)) => ()
          Some(_) => {
            ffi_set_status("failed: expected the control stream first")
            return
          }
          None => {
            ffi_set_status("failed: session ended opening the control stream")
            return
          }
        }
        match client.session().publish_track(track, 1UL) {
          Ok((id, actions)) => {
            publish_id = id
            if !dispatch_actions(client, actions) {
              return
            }
            match client.next_opened_stream().wait() {
              Some((Request(request_id), stream)) if request_id == id =>
                publish_stream = Some(stream)
              Some(_) => {
                ffi_set_status("failed: expected the PUBLISH request stream")
                return
              }
              None => {
                ffi_set_status("failed: session ended opening PUBLISH")
                return
              }
            }
          }
          Err(error) => {
            ffi_set_status("failed: \{error.to_repr()}")
            return
          }
        }
      }
      Some(PublishAccepted(id, _, _)) if id == publish_id => {
        ffi_set_status("published")
        ffi_wait_to_start().wait()
        let stream = publish_stream.unwrap()
        for group_id in 0UL..<groups.to_uint64() {
          if !publish_group(
              client, stream, group_id, objects_per_group, object_bytes,
            ) {
            return
          }
        }
        match client.session().publish_done(stream, 0UL, b"") {
          Ok(actions) => if !dispatch_actions(client, actions) { return }
          Err(error) => {
            ffi_set_status("failed: \{error.to_repr()}")
            return
          }
        }
        let mut done_sent = false
        while !done_sent {
          match client.next_notice().wait() {
            Some(StreamSendFinished(sent_stream, Sent)) if sent_stream == stream =>
              done_sent = true
            Some(SessionEnded(end)) => {
              ffi_set_status("failed: session ended \{end.to_repr()}")
              return
            }
            None => {
              ffi_set_status("failed: session ended before PUBLISH_DONE FIN")
              return
            }
            _ => ()
          }
        }
        ffi_set_status("publishing-done")
        ffi_wait_to_finish().wait()
        finished = true
        ffi_set_status("passed")
        close_connection(client)
      }
      Some(PublishRejected(id, error)) if id == publish_id => {
        ffi_set_status("failed: PUBLISH rejected \{error.code}")
        return
      }
      Some(SessionEnded(end)) => {
        ffi_set_status("failed: session ended \{end.to_repr()}")
        return
      }
      _ => ()
    }
  }
}
