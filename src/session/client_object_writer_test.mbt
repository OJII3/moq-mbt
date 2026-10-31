///|
fn incremental_send_bytes(actions : Array[@session.SessionAction]) -> Bytes {
  let mut bytes = b""
  for action in actions {
    match action {
      @session.SendChunk(_, data) | @session.SendObjectChunk(_, data, _) =>
        bytes = bytes + data
      _ => ()
    }
  }
  bytes
}

///|
fn incremental_decode_frames(
  bytes : Bytes,
) -> Array[@protocol.SubgroupFrame] raise {
  let (header, used) = match @protocol.decode_subgroup_header(bytes) {
    @protocol.Complete(header, used) => (header, used)
    _ => fail("expected subgroup header")
  }
  let decoder = @protocol.SubgroupDecoder::new(header)
  let frames : Array[@protocol.SubgroupFrame] = []
  let mut remaining = bytes[used:].to_owned()
  while !remaining.is_empty() {
    match decoder.decode_next(remaining) {
      @protocol.Complete(frame, used) => {
        frames.push(frame)
        remaining = remaining[used:].to_owned()
      }
      _ => fail("expected complete Object")
    }
  }
  frames
}

///|
test "incremental Object cannot begin before stream open" {
  let publisher = publisher_started()
  let (group, _) = writer_group_open(publisher, 0UL)
  assert_true(group.begin_object(2UL) is Err(@session.ObjectStreamNotReady))
}

///|
test "incremental Object enforces exact payload length and blocks group FIN" {
  let publisher = publisher_started()
  let (group, token) = writer_group_open(publisher, 0UL)
  let actions = publisher
    .receive(@session.StreamOpened(@session.Object(token), 50))
    .unwrap()
  let (object, header) = group.begin_object(3UL).unwrap()
  assert_true(group.begin_object(1UL) is Err(@session.UnexpectedMessage))
  assert_eq(group.send(b"blocked"), Err(@session.UnexpectedMessage))
  assert_eq(group.finish(), Err(@session.UnexpectedMessage))
  assert_eq(object.finish(), Err(@session.ObjectPayloadLengthMismatch))
  assert_eq(object.write(b"excess"), Err(@session.ObjectPayloadLengthMismatch))
  let first = object.write(b"ab").unwrap()
  assert_eq(object.finish(), Err(@session.ObjectPayloadLengthMismatch))
  let last = object.write(b"c").unwrap()
  assert_eq(object.finish(), Ok([]))
  assert_eq(object.write(b"late"), Err(@session.UnexpectedMessage))
  assert_eq(object.finish(), Err(@session.UnexpectedMessage))
  let bytes = incremental_send_bytes(actions + header + first + last)
  let frames = incremental_decode_frames(bytes)
  assert_eq(frames.length(), 1)
  assert_eq(frames[0].payload, b"abc")
  assert_true(group.send(b"next") is Ok(_))
  assert_true(group.finish() is Ok(_))
}

///|
test "incremental Object starts timeout at first nonempty payload chunk" {
  let publisher = delivery_publisher()
  ignore(publisher.receive(@session.Clock(100UL)).unwrap())
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(
    publisher
    .receive(@session.StreamOpened(@session.Object(token), 50))
    .unwrap(),
  )
  let (object, header) = group.begin_object(3UL).unwrap()
  assert_true(header[0] is @session.SendChunk(50, _))
  assert_eq(object.write(b""), Ok([]))
  ignore(publisher.receive(@session.Clock(120UL)).unwrap())
  let first = object.write(b"a").unwrap()
  assert_true(first[0] is @session.SendObjectChunk(50, b"a", 130UL))
  ignore(publisher.receive(@session.Clock(125UL)).unwrap())
  let next = object.write(b"bc").unwrap()
  assert_true(next[0] is @session.SendObjectChunk(50, b"bc", 130UL))
}

///|
test "aborting group invalidates incremental Object writer" {
  let publisher = publisher_started()
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(
    publisher
    .receive(@session.StreamOpened(@session.Object(token), 50))
    .unwrap(),
  )
  let (object, _) = group.begin_object(2UL).unwrap()
  ignore(object.write(b"a").unwrap())
  ignore(group.abort(2UL).unwrap())
  assert_eq(object.write(b"b"), Err(@session.UnknownStream))
  assert_eq(object.finish(), Err(@session.UnknownStream))
}

///|
test "incremental Object carries properties and zero length payload" {
  let publisher = publisher_started()
  let (group, actions) = publisher
    .open_object_group(@session.StreamId(40), 0UL, has_properties=true)
    .unwrap()
  let token = match actions[0] {
    @session.OpenStream(_, @session.Object(value)) => value
    _ => fail("expected open")
  }
  let stream_header = publisher
    .receive(@session.StreamOpened(@session.Object(token), 50))
    .unwrap()
  let (object, header) = group
    .begin_object(0UL, properties=b"\x02\x01")
    .unwrap()
  assert_eq(object.finish(), Ok([]))
  let bytes = incremental_send_bytes(stream_header + header)
  let frames = incremental_decode_frames(bytes)
  assert_eq(frames[0].payload, b"")
  assert_eq(frames[0].properties, b"\x02\x01")
  assert_eq(frames[0].status, @protocol.Normal)
}

///|
test "incremental Object delivers two MiB using independent bounded chunks" {
  let publisher = publisher_started()
  let subscriber = subscriber_started()
  subscriber_start_stream(subscriber, 50)
  let (group, token) = writer_group_open(publisher, 0UL)
  let header = publisher
    .receive(@session.StreamOpened(@session.Object(token), 50))
    .unwrap()
  ignore(
    subscriber
    .receive(@session.StreamData(50, incremental_send_bytes(header)))
    .unwrap(),
  )
  let (object, actions) = group.begin_object(2UL * 1024UL * 1024UL).unwrap()
  ignore(
    subscriber
    .receive(@session.StreamData(50, incremental_send_bytes(actions)))
    .unwrap(),
  )
  let chunk = Bytes::make(64 * 1024, 42)
  let mut received_count = 0
  for _ in 0..<32 {
    let actions = object.write(chunk).unwrap()
    assert_eq(actions.length(), 1)
    let notices = subscriber
      .receive(@session.StreamData(50, incremental_send_bytes(actions)))
      .unwrap()
    for frame in collect_frames(notices) {
      assert_eq(frame.payload.length(), 2 * 1024 * 1024)
      received_count = received_count + 1
    }
  }
  assert_eq(received_count, 1)
  assert_eq(object.finish(), Ok([]))
}

///|
test "Object receive buffer rejects configured limit" {
  let subscriber = client_session_ready(max_object_buffer_bytes=8UL)
  ignore(subscriber.subscribe_track(subscriber_track()).unwrap())
  ignore(
    subscriber
    .receive(@session.StreamOpened(@session.Request(0UL), 30))
    .unwrap(),
  )
  ignore(
    subscriber
    .receive(@session.StreamData(30, b"\x04\x00\x02\x01\x00"))
    .unwrap(),
  )
  subscriber_start_stream(subscriber, 9)
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        9,
        @protocol.encode_subgroup_header(subscriber_header(0UL, false)) +
        b"\x00\x10payload",
      ),
    ),
    Ok([@session.StopReceiving(9, 3UL)]),
  )
}

///|
test "incremental Object encodes the full UInt64 payload length with Draft18 vi64" {
  for
    (length, encoded_length) in [
      (0x4000000000000000UL, b"\xff\x40\x00\x00\x00\x00\x00\x00\x00"),
      (0xffffffffffffffffUL, b"\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
    ] {
    let publisher = publisher_started()
    let (group, token) = writer_group_open(publisher, 0UL)
    ignore(
      publisher
      .receive(@session.StreamOpened(@session.Object(token), 50L))
      .unwrap(),
    )
    let (object, actions) = group.begin_object(length).unwrap()
    // The first Object ID is zero; the declared length occupies nine bytes.
    assert_eq(actions, [@session.SendChunk(50L, b"\x00" + encoded_length)])
    assert_eq(object.write(b"a"), Ok([@session.SendChunk(50L, b"a")]))
    assert_eq(object.finish(), Err(@session.ObjectPayloadLengthMismatch))
    ignore(group.abort(2UL).unwrap())
  }
}
