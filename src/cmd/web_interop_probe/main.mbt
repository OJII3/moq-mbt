///|
extern "js" fn ffi_moq_uri() -> String =
  #|() => globalThis.__moqE2EConfig.uri

///|
extern "js" fn ffi_certificate_hash() -> @arraybuffer.Uint8Array =
  #|() => new Uint8Array(globalThis.__moqE2EConfig.certificateHash)

///|
extern "js" fn ffi_mode() -> String =
  #|() => globalThis.__moqE2EConfig.mode

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
      let connection = match
        @js_adapter.webtransport_connect_with_certificate_hashes(
          ffi_moq_uri(),
          [ffi_certificate_hash()],
        ) {
        Ok(connection) => connection
        Err(message) => {
          ffi_set_status("failed: \{message}")
          return
        }
      }
      let driver = match
        @js_adapter.WebTransportSessionDriver::from_setup(connection, @protocol.Setup::{
          options: [],
        }) {
        Ok(driver) => driver
        Err(error) => {
          ffi_set_status("failed: \{error.to_repr()}")
          return
        }
      }
      match driver.start() {
        Ok(_) => ()
        Err(error) => {
          ffi_set_status("failed: \{error.to_repr()}")
          return
        }
      }
      let track : @protocol.TrackName = {
        namespace_fields: [b"interop"],
        name: b"multi-object",
      }
      ffi_set_status("waiting-handshake")
      if ffi_mode() == "publish" {
        run_publisher(driver, connection, track)
      } else {
        run_subscriber(driver, connection, track)
      }
    }),
  )
}

///|
async fn run_subscriber(
  driver : @js_adapter.WebTransportSessionDriver,
  connection : @js_adapter.WebTransportConnection,
  track : @protocol.TrackName,
) -> Unit {
  let mut request_id = 0UL
  let mut subscribed = false
  let mut received_objects = 0
  let mut finished_groups = 0
  let mut finished = false
  while !finished {
    match driver.next_notice().wait() {
      None => {
        ffi_set_status("failed: WebTransport session ended")
        return
      }
      Some(@session.HandshakeComplete(_)) if !subscribed => {
        ffi_set_status("ready")
        ffi_wait_to_start().wait()
        match driver.next_opened_stream().wait() {
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
        match driver.session().subscribe_track(track) {
          Ok((id, actions)) => {
            request_id = id
            driver.dispatch(actions)
            match driver.next_opened_stream().wait() {
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
            match driver.session().finish_subscription_updates(id) {
              Ok(actions) => driver.dispatch(actions)
              Err(error) => {
                ffi_set_status("failed: \{error.to_repr()}")
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
      Some(@session.ObjectReceived(id, header, frames)) if id == request_id =>
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
          ffi_set_status("failed: incomplete Object or PUBLISH_DONE counts")
          return
        }
        finished = true
        ffi_set_status("passed")
        close_connection(connection)
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
  driver : @js_adapter.WebTransportSessionDriver,
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
    driver.session().open_object_stream(subscription_stream, header) {
    Ok(actions) => actions
    Err(error) => {
      ffi_set_status("failed: \{error.to_repr()}")
      return false
    }
  }
  ffi_set_status("opening-group-\{group_id}")
  driver.dispatch(actions)
  let object_stream = match driver.next_opened_stream().wait() {
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
    match driver.session().send_object(object_stream, frame) {
      Ok(actions) => driver.dispatch(actions)
      Err(error) => {
        ffi_set_status("failed: \{error.to_repr()}")
        return false
      }
    }
  }
  match driver.session().finish_object_stream(object_stream) {
    Ok(actions) => driver.dispatch(actions)
    Err(error) => {
      ffi_set_status("failed: \{error.to_repr()}")
      return false
    }
  }
  let mut finished = false
  while !finished {
    match driver.next_notice().wait() {
      Some(@session.ObjectStreamFinished(stream)) if stream == object_stream =>
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
async fn close_connection(
  connection : @js_adapter.WebTransportConnection,
) -> Unit {
  match connection.close(0L, "interop complete") {
    Err(@js_adapter.InvalidCloseCode(code)) =>
      ffi_set_status("failed: invalid close code \{code}")
    Err(@js_adapter.CloseReasonTooLong(length)) =>
      ffi_set_status("failed: close reason too long \{length}")
    Ok(_) =>
      match connection.closed().wait() {
        @js_adapter.LocalClose => ()
        @js_adapter.PeerClose(_) =>
          ffi_set_status("failed: connection closed by the peer")
        @js_adapter.Failed(error) =>
          ffi_set_status("failed: connection close failed: \{error.message}")
      }
  }
}

///|
async fn run_publisher(
  driver : @js_adapter.WebTransportSessionDriver,
  connection : @js_adapter.WebTransportConnection,
  track : @protocol.TrackName,
) -> Unit {
  let mut publish_id = 0UL
  let mut publish_stream = None
  let mut finished = false
  while !finished {
    match driver.next_notice().wait() {
      None => {
        ffi_set_status("failed: WebTransport session ended")
        return
      }
      Some(@session.HandshakeComplete(_)) if publish_stream is None => {
        ffi_set_status("handshake")
        match driver.next_opened_stream().wait() {
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
        match driver.session().publish_track(track, 1UL) {
          Ok((id, actions)) => {
            publish_id = id
            driver.dispatch(actions)
            match driver.next_opened_stream().wait() {
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
      Some(@session.PublishAccepted(id)) if id == publish_id => {
        ffi_set_status("published")
        ffi_wait_to_start().wait()
        let stream = publish_stream.unwrap()
        if !publish_group(driver, stream, 0UL) ||
          !publish_group(driver, stream, 1UL) {
          return
        }
        match driver.session().publish_done(stream, 0UL, b"") {
          Ok(actions) => driver.dispatch(actions)
          Err(error) => {
            ffi_set_status("failed: \{error.to_repr()}")
            return
          }
        }
        let mut done_sent = false
        while !done_sent {
          match driver.next_notice().wait() {
            Some(@session.SendFinished(sent_stream, false)) if sent_stream ==
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
        close_connection(connection)
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
