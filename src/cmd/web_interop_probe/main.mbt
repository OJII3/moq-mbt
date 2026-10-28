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
        @js_adapter.browser_connect(ffi_moq_uri(), setup, certificate_hashes=[
          ffi_certificate_hash(),
        ])
      } else {
        @js_adapter.browser_connect(ffi_moq_uri(), setup)
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
      let track : @protocol.TrackName = {
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
  let mut request_id = 0UL
  let mut subscribed = false
  let mut received_objects = 0
  let mut finished_groups = 0
  let mut finished = false
  while !finished {
    match client.next_notice().wait() {
      None => {
        ffi_set_status("failed: WebTransport session ended")
        return
      }
      Some(@session.HandshakeComplete(_)) if !subscribed => {
        ffi_set_status("ready")
        ffi_wait_to_start().wait()
        match client.next_opened_stream().wait() {
          Some((@session.Control, _)) => ()
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
              Some((@session.Request(opened_id), _)) if opened_id == id => ()
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
      Some(@session.SubscribeEstablished(id, _alias)) if id == request_id => {
        subscribed = true
        ffi_set_status("subscribed")
      }
      Some(@session.ObjectReceived(id, _, header, frames)) if id == request_id =>
        for frame in frames {
          let expected = @utf8.encode(
            "group-\{header.group_id}-object-\{frame.object_id}",
          )
          if frame.payload != expected || frame.properties != b"\x02\x2a" {
            ffi_set_status("failed: Object payload or Properties mismatch")
            return
          }
          received_objects += 1
          ffi_set_status("objects-\{received_objects}")
        }
      Some(@session.GroupFinished(id, _group_id)) if id == request_id => {
        finished_groups += 1
        ffi_set_status("groups-\{finished_groups}")
      }
      Some(@session.PublishDone(id, done)) if id == request_id => {
        if done.stream_count != 2UL ||
          received_objects != 4 ||
          finished_groups != 2 {
          ffi_set_status(
            "failed: PUBLISH_DONE streams=\{done.stream_count}, objects=\{received_objects}, groups=\{finished_groups}",
          )
          return
        }
        finished = true
        ffi_set_status("passed")
        close_connection(client)
      }
      Some(@session.SubscribeRejected(id, error)) if id == request_id => {
        ffi_set_status("failed: SUBSCRIBE rejected \{error.code}")
        return
      }
      Some(@session.SessionEnded(end)) => {
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
  subscription_stream : Int64,
  group_id : UInt64,
) -> Bool {
  let header : @protocol.SubgroupHeader = {
    track_alias: 1UL,
    group_id,
    subgroup_id_mode: @protocol.Zero,
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
    Some((@session.Object(_), stream)) => stream
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
  for object_id in 0UL..<2UL {
    let frame : @protocol.SubgroupFrame = {
      object_id,
      status: @protocol.Normal,
      properties: b"\x02\x2a",
      payload: @utf8.encode("group-\{group_id}-object-\{object_id}"),
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
      Some(@session.ObjectStreamSendFinished(stream)) if stream == object_stream =>
        finished = true
      Some(@session.SessionEnded(end)) => {
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
    Err(@js_adapter.InvalidCloseCode(code)) =>
      ffi_set_status("failed: invalid close code \{code}")
    Err(@js_adapter.CloseReasonTooLong(length)) =>
      ffi_set_status("failed: close reason too long \{length}")
    Ok(_) =>
      match client.closed().wait() {
        @js_adapter.LocalClose => ()
        @js_adapter.PeerClose(_) =>
          ffi_set_status("failed: connection closed by the peer")
        @js_adapter.Failed(error) =>
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
    @driver.Dispatched => true
    @driver.QueueFull => {
      ffi_set_status("failed: action queue full")
      false
    }
    @driver.TooLarge => {
      ffi_set_status("failed: action batch too large")
      false
    }
    @driver.DriverFailed => {
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
  let mut publish_id = 0UL
  let mut publish_stream = None
  let mut finished = false
  while !finished {
    match client.next_notice().wait() {
      None => {
        ffi_set_status("failed: WebTransport session ended")
        return
      }
      Some(@session.HandshakeComplete(_)) if publish_stream is None => {
        ffi_set_status("handshake")
        match client.next_opened_stream().wait() {
          Some((@session.Control, _)) => ()
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
              Some((@session.Request(request_id), stream)) if request_id == id =>
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
      Some(@session.PublishAccepted(id, _, _)) if id == publish_id => {
        ffi_set_status("published")
        ffi_wait_to_start().wait()
        let stream = publish_stream.unwrap()
        if !publish_group(client, stream, 0UL) ||
          !publish_group(client, stream, 1UL) {
          return
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
            Some(@session.StreamSendFinished(sent_stream, @session.Sent)) if sent_stream ==
              stream => done_sent = true
            Some(@session.SessionEnded(end)) => {
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
      Some(@session.PublishRejected(id, error)) if id == publish_id => {
        ffi_set_status("failed: PUBLISH rejected \{error.code}")
        return
      }
      Some(@session.SessionEnded(end)) => {
        ffi_set_status("failed: session ended \{end.to_repr()}")
        return
      }
      _ => ()
    }
  }
}
