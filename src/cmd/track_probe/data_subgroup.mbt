///|
/// The 16-field MoQT Test tuple for the canonical `data-subgroup-basic` case.
let interop_data_subgroup_tuple : Array[String] = [
  "moq-test-00", "0", "0", "0", "2", "4", "5", "64", "32", "2", "1", "1", "0", "-1",
  "-1", "0",
]

///|
/// Build a collision-resistant run identifier: 16 random bytes as lowercase hex.
fn interop_run_id() -> String {
  let generator = @random.Rand()
  let digits = b"0123456789abcdef"
  let encoded : Array[Byte] = []
  for _ in 0..<16 {
    let value = generator.uint(limit=256).reinterpret_as_int()
    encoded.push(digits[value / 16])
    encoded.push(digits[value % 16])
  }
  @utf8.decode_lossy(Bytes::from_array(encoded))
}

///|
fn interop_data_payload(length : Int32) -> Bytes {
  let payload : Array[Byte] = []
  for _ in 0..<length {
    payload.push(b't')
  }
  Bytes::from_array(payload)
}

///|
fn interop_bytes_are(bytes : Bytes, expected : Byte) -> Bool {
  for byte in bytes {
    if byte != expected {
      return false
    }
  }
  true
}

///|
/// Wait between data objects so the publisher paces delivery as the tuple asks.
fn interop_pace(milliseconds : UInt64) -> Unit {
  let deadline = @env.now() + milliseconds
  while @env.now() < deadline {
    ()
  }
}

///|
fn interop_finished_streams(
  notices : Array[@session.SessionNotice],
) -> Int32 raise {
  let mut finished = 0
  for notice in notices {
    match notice {
      @session.ObjectStreamSendFinished(_) => finished += 1
      @session.SessionEnded(end) =>
        fail("relay ended the publisher session: \{end.to_repr()}")
      _ => ()
    }
  }
  finished
}

///|
priv struct DataSubgroupObservation {
  lengths : Map[UInt64, Int32]
  mut count : Int32
  mut established : Bool
  mut done : @protocol.PublishDoneMessage?
}

///|
fn DataSubgroupObservation::new() -> DataSubgroupObservation {
  { lengths: {}, count: 0, established: false, done: None, }
}

///|
fn DataSubgroupObservation::observe_notices(
  self : DataSubgroupObservation,
  notices : Array[@session.SessionNotice],
  subscription : UInt64,
) -> Unit raise {
  for notice in notices {
    match notice {
      @session.SubscribeEstablished(id, _) if id.to_uint64() == subscription =>
        self.established = true
      @session.SubscribeRejected(_, error) =>
        fail("SUBSCRIBE rejected: \{error.to_repr()}")
      @session.ObjectReceived(_, _, header, frames) =>
        self.observe(header, frames)
      @session.PublishDone(_, done) => self.done = Some(done)
      @session.SessionEnded(end) =>
        fail("relay ended the subscriber session: \{end.to_repr()}")
      _ => ()
    }
  }
}

///|
fn DataSubgroupObservation::observe(
  self : DataSubgroupObservation,
  header : @protocol.SubgroupHeader,
  frames : Array[@protocol.SubgroupFrame],
) -> Unit raise {
  match header.subgroup_id_mode {
    @protocol.Zero => ()
    @protocol.Explicit(0UL) => ()
    _ => fail("object stream used a subgroup other than 0")
  }
  let priority = match header.publisher_priority {
    Some(value) => value
    None => 128
  }
  guard priority == 128 else {
    fail("object stream used publisher priority \{priority}; expected 128")
  }
  guard header.group_id < 3UL else {
    fail("received group \{header.group_id}; expected groups 0 through 2")
  }
  for frame in frames {
    guard frame.object_id < 5UL else {
      fail("received object \{frame.object_id}; expected objects 0 through 4")
    }
    let key = header.group_id * 5UL + frame.object_id
    guard self.lengths.get(key) is None else {
      fail(
        "received group \{header.group_id} object \{frame.object_id} more than once",
      )
    }
    let expected_length = if frame.object_id == 0UL { 64 } else { 32 }
    guard frame.payload.length() == expected_length else {
      fail(
        "group \{header.group_id} object \{frame.object_id} has \{frame.payload.length()} bytes; expected \{expected_length}",
      )
    }
    guard interop_bytes_are(frame.payload, b't') else {
      fail(
        "group \{header.group_id} object \{frame.object_id} has an unexpected payload",
      )
    }
    self.lengths[key] = expected_length
    self.count += 1
  }
}

///|
/// Accept the upstream SUBSCRIBE so the relay can establish the downstream one.
fn interop_accept_data_subscription(
  peer : LifecyclePeer,
  subscription_stream : @session.StreamId,
) -> Unit raise {
  let (notices, _) = drive_session_result(
    peer.client,
    peer.session,
    peer.session.accept_subscription(subscription_stream),
    peer.driver,
  )
  for notice in notices {
    match notice {
      @session.SessionEnded(end) =>
        fail("relay ended the publisher session: \{end.to_repr()}")
      _ => ()
    }
  }
}

///|
/// Send the canonical object set and finish the track, as the controlled
/// publisher in the `data-subgroup-basic` specification.
fn interop_publish_data_subgroup(
  peer : LifecyclePeer,
  subscription_stream : @session.StreamId,
) -> Unit raise {
  let mut finished = 0
  for group_id in 0..<3 {
    let (writer, actions) = match
      peer.session.open_object_group(subscription_stream, group_id.to_uint64()) {
      Ok(value) => value
      Err(error) => fail("could not open a subgroup stream: \{error.to_repr()}")
    }
    let (notices, _) = drive_session_actions(
      peer.client,
      peer.session,
      actions,
      peer.driver,
    )
    finished += interop_finished_streams(notices)
    for object_id in 0..<5 {
      let length = if object_id == 0 { 64 } else { 32 }
      let (_, _) = drive_session_result(
        peer.client,
        peer.session,
        writer.send(interop_data_payload(length)),
        peer.driver,
      )
      interop_pace(2UL)
    }
    let (notices, _) = drive_session_result(
      peer.client,
      peer.session,
      writer.finish(),
      peer.driver,
    )
    finished += interop_finished_streams(notices)
  }
  let deadline = @env.now() + 2000UL
  while finished < 3 && @env.now() < deadline {
    finished += interop_finished_streams(lifecycle_poll(peer, 10))
  }
  eprintln("publisher finished \{finished} subgroup streams")
  guard finished == 3 else {
    fail("timed out waiting for the subgroup streams to finish")
  }
  let (notices, _) = drive_session_result(
    peer.client,
    peer.session,
    peer.session.publish_done(subscription_stream, 2UL, b""),
    peer.driver,
  )
  for notice in notices {
    match notice {
      @session.PublishDone(_, done) => {
        guard done.code == 2UL else {
          fail("PUBLISH_DONE used code \{done.code}; expected TRACK_ENDED (2)")
        }
        guard done.stream_count == 3UL else {
          fail("PUBLISH_DONE reported \{done.stream_count} streams; expected 3")
        }
      }
      @session.SessionEnded(end) =>
        fail("relay ended the publisher session: \{end.to_repr()}")
      _ => ()
    }
  }
}

///|
fn interop_data_subgroup_basic(context : InteropContext) -> Unit raise {
  let deadline = @env.now() + 10000UL
  let run_id = interop_run_id()
  let routing_prefix : @protocol.TrackNamespace = {
    fields: [b"moq-test", b"data-subgroup-basic", @utf8.encode(run_id)],
  }
  let namespace_fields = routing_prefix.fields.copy()
  for field in interop_data_subgroup_tuple {
    namespace_fields.push(@utf8.encode(field))
  }
  let track : @protocol.TrackName = { namespace_fields, name: b"track", }

  let publisher = context.connect("publisher")
  defer publisher.client.close()
  let (publication, actions) = match
    publisher.session.publish_namespace(routing_prefix, []) {
    Ok(value) => value
    Err(error) => fail("could not publish the namespace: \{error.to_repr()}")
  }
  let publication = publication.to_uint64()
  let (notices, _) = drive_session_actions(
    publisher.client,
    publisher.session,
    actions,
    publisher.driver,
  )
  interop_wait_namespace_accepted(publisher, publication, notices, 5000UL)

  let subscriber = context.connect("subscriber")
  defer subscriber.client.close()
  let (subscription, actions) = match
    subscriber.session.subscribe_track(track, parameters=[
      { kind: 0x22UL, value: @protocol.Byte(1), },
    ]) {
    Ok(value) => value
    Err(error) => fail("could not subscribe: \{error.to_repr()}")
  }
  let subscription = subscription.to_uint64()
  let (notices, _) = drive_session_actions(
    subscriber.client,
    subscriber.session,
    actions,
    subscriber.driver,
  )
  let observation = DataSubgroupObservation::new()
  observation.observe_notices(notices, subscription)

  let mut subscription_stream : @session.StreamId? = None
  let mut published = false
  while @env.now() < deadline && observation.done is None {
    if subscription_stream is None {
      for notice in lifecycle_poll(publisher, 10) {
        match notice {
          @session.SubscriptionRequested(_, stream, subscribed_track) => {
            guard subscribed_track == track else {
              fail(
                "upstream SUBSCRIBE used \{subscribed_track.to_display_string()}; expected the 19-field namespace",
              )
            }
            interop_accept_data_subscription(publisher, stream)
            subscription_stream = Some(stream)
          }
          @session.SessionEnded(end) =>
            fail("relay ended the publisher session: \{end.to_repr()}")
          _ => ()
        }
      }
    }
    observation.observe_notices(lifecycle_poll(subscriber, 10), subscription)
    if !published && observation.established {
      match subscription_stream {
        Some(stream) => {
          interop_publish_data_subgroup(publisher, stream)
          published = true
        }
        None => ()
      }
    }
  }
  guard observation.established else {
    fail("timed out waiting for SUBSCRIBE_OK")
  }
  let done = match observation.done {
    Some(done) => done
    None =>
      fail(
        "timed out waiting for PUBLISH_DONE after \{observation.count} objects",
      )
  }
  guard done.code == 2UL else {
    fail("PUBLISH_DONE used code \{done.code}; expected TRACK_ENDED (2)")
  }
  guard observation.count == 15 else {
    fail("subscriber received \{observation.count} objects; expected 15")
  }
  for group_id in 0..<3 {
    for object_id in 0..<5 {
      let key = group_id.to_uint64() * 5UL + object_id.to_uint64()
      guard observation.lengths.get(key) is Some(_) else {
        fail("missing group \{group_id} object \{object_id}")
      }
    }
  }
}
