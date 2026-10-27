///|
fn subscriber_track() -> @protocol.TrackName {
  { namespace_fields: [b"demo"], name: b"video", }
}

///|
fn subscriber_header(
  group_id : UInt64,
  end_of_group : Bool,
) -> @protocol.SubgroupHeader {
  {
    track_alias: 1,
    group_id,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group,
    first_object: true,
    has_properties: false,
  }
}

///|
fn subscriber_frame(
  object_id : UInt64,
  payload : Bytes,
) -> @protocol.SubgroupFrame {
  { object_id, status: @protocol.Normal, properties: b"", payload, }
}

///|
fn subscriber_pending() -> @session.ClientSession raise {
  let subscriber = client_session_ready()
  ignore(subscriber.subscribe_track(subscriber_track()).unwrap())
  ignore(
    subscriber.receive(@session.StreamOpened(@session.Request(0), 30)).unwrap(),
  )
  subscriber
}

///|
fn subscriber_started() -> @session.ClientSession raise {
  let subscriber = subscriber_pending()
  assert_eq(
    subscriber.receive(@session.StreamData(30, b"\x04\x00\x02\x01\x00")),
    Ok([@session.Notify(@session.SubscribeEstablished(0, 1))]),
  )
  subscriber
}

///|
fn subscriber_start_stream(
  subscriber : @session.ClientSession,
  stream : Int64,
) -> Unit raise {
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(stream, @session.Unidirectional),
    ),
    Ok([]),
  )
}

///|
fn collect_frames(
  actions : Array[@session.ClientSessionAction],
) -> Array[@protocol.SubgroupFrame] {
  let frames : Array[@protocol.SubgroupFrame] = []
  for action in actions {
    match action {
      @session.Notify(
        @session.ObjectReceived(
          _,
          { namespace_fields: [b"demo"], name: b"video", },
          _,
          received
        )
      ) =>
        for frame in received {
          frames.push(frame)
        }
      _ => ()
    }
  }
  frames
}

///|
test "subscriber decodes multiple objects from one stream chunk" {
  let frames = [subscriber_frame(2, b"first"), subscriber_frame(5, b"second")]
  let header = subscriber_header(4, true)
  let wire = @protocol.encode_subgroup_stream(header, frames).unwrap()
  let subscriber = subscriber_started()
  subscriber_start_stream(subscriber, 9)
  assert_eq(
    subscriber.receive(@session.StreamData(9, wire)),
    Ok([
      @session.Notify(
        @session.ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          header,
          frames,
        ),
      ),
    ]),
  )
}

///|
test "subscriber handles every split across a header and multiple frames" {
  let frames = [subscriber_frame(0, b"one"), subscriber_frame(1, b"two")]
  let wire = @protocol.encode_subgroup_stream(
    subscriber_header(4, false),
    frames,
  ).unwrap()
  for split = 0; split < wire.length(); split = split + 1 {
    let subscriber = subscriber_started()
    subscriber_start_stream(subscriber, 9)
    let first = subscriber
      .receive(@session.StreamData(9, wire[:split].to_owned()))
      .unwrap()
    let second = subscriber
      .receive(@session.StreamData(9, wire[split:].to_owned()))
      .unwrap()
    assert_eq(collect_frames(first) + collect_frames(second), frames)
  }
}

///|
test "subscriber isolates interleaved subgroup streams" {
  let subscriber = subscriber_started()
  let header_a = subscriber_header(10, false)
  let header_b = subscriber_header(11, true)
  let wire_a = @protocol.encode_subgroup_stream(header_a, [
    subscriber_frame(1, b"a1"),
    subscriber_frame(3, b"a2"),
  ]).unwrap()
  let wire_b = @protocol.encode_subgroup_stream(header_b, [
    subscriber_frame(7, b"b1"),
  ]).unwrap()
  let header_length_a = @protocol.encode_subgroup_header(header_a).length()
  let header_length_b = @protocol.encode_subgroup_header(header_b).length()
  let split_a = header_length_a + 4
  let split_b = header_length_b + 1
  subscriber_start_stream(subscriber, 21)
  subscriber_start_stream(subscriber, 23)
  assert_eq(
    subscriber.receive(@session.StreamData(21, wire_a[:split_a].to_owned())),
    Ok([
      @session.Notify(
        @session.ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          header_a,
          [subscriber_frame(1, b"a1")],
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.StreamData(23, wire_b[:split_b].to_owned())),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(@session.StreamData(21, wire_a[split_a:].to_owned())),
    Ok([
      @session.Notify(
        @session.ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          header_a,
          [subscriber_frame(3, b"a2")],
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.StreamData(23, wire_b[split_b:].to_owned())),
    Ok([
      @session.Notify(
        @session.ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          header_b,
          [subscriber_frame(7, b"b1")],
        ),
      ),
    ]),
  )
}

///|
test "subscriber rejects unknown aliases and invalid object status" {
  let subscriber = subscriber_started()
  let bad_alias : @protocol.SubgroupHeader = {
    ..subscriber_header(1, false),
    track_alias: 2,
  }
  subscriber_start_stream(subscriber, 40)
  let alias_wire = @protocol.encode_subgroup_header(bad_alias) + b"\x00\x01x"
  assert_eq(
    subscriber.receive(@session.StreamData(40, alias_wire)),
    Ok([@session.StopReceiving(40, 3)]),
  )
  subscriber_start_stream(subscriber, 41)
  let invalid_status = @protocol.encode_subgroup_header(
      subscriber_header(1, false),
    ) +
    b"\x00\x00\x01"
  assert_eq(
    subscriber.receive(@session.StreamData(41, invalid_status)),
    Ok([@session.StopReceiving(41, 3)]),
  )
  assert_eq(subscriber.receive(@session.StreamData(41, b"queued")), Ok([]))
  assert_eq(subscriber.receive(@session.StreamReset(41, 1)), Ok([]))
  assert_eq(subscriber.state(), @session.Active)
}

///|
test "subscriber distinguishes FIN, reset, and incomplete frames" {
  let subscriber = subscriber_started()
  let header = subscriber_header(1, true)
  let header_bytes = @protocol.encode_subgroup_header(header)
  subscriber_start_stream(subscriber, 40)
  assert_eq(subscriber.receive(@session.StreamData(40, header_bytes)), Ok([]))
  assert_eq(
    subscriber.receive(@session.StreamFinished(40)),
    Ok([@session.Notify(@session.GroupFinished(0, 1))]),
  )
  assert_eq(subscriber.receive(@session.StreamFinished(40)), Ok([]))
  subscriber_start_stream(subscriber, 41)
  assert_eq(
    subscriber.receive(@session.StreamData(41, header_bytes + b"\x00\x01x")),
    Ok([
      @session.Notify(
        @session.ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          header,
          [subscriber_frame(0, b"x")],
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.StreamReset(41, 1)),
    Ok([@session.Notify(@session.ObjectStreamReset(0, 41))]),
  )
  assert_eq(subscriber.receive(@session.StreamFinished(41)), Ok([]))
  subscriber_start_stream(subscriber, 42)
  assert_eq(
    subscriber.receive(@session.StreamData(42, header_bytes[:1].to_owned())),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(@session.StreamFinished(42)),
    Ok([
      @session.CloseConnection(3, b""),
      @session.Notify(@session.SessionEnded(@session.Local(3, b""))),
    ]),
  )
  let malformed_subscriber = subscriber_started()
  subscriber_start_stream(malformed_subscriber, 43)
  assert_eq(
    malformed_subscriber.receive(
      @session.StreamData(43, header_bytes + b"\x00\x02x"),
    ),
    Ok([]),
  )
  assert_eq(
    malformed_subscriber.receive(@session.StreamFinished(43)),
    Ok([@session.StopReceiving(43, 3)]),
  )
}

///|
test "EndOfGroup object status does not imply header End of Group" {
  let subscriber = subscriber_started()
  let header = subscriber_header(1, false)
  let frame : @protocol.SubgroupFrame = {
    object_id: 0,
    status: @protocol.EndOfGroup,
    properties: b"",
    payload: b"",
  }
  subscriber_start_stream(subscriber, 44)
  let wire = @protocol.encode_subgroup_stream(header, [frame]).unwrap()
  assert_eq(
    subscriber.receive(@session.StreamData(44, wire)),
    Ok([
      @session.Notify(
        @session.ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          header,
          [frame],
        ),
      ),
    ]),
  )
  assert_eq(subscriber.receive(@session.StreamFinished(44)), Ok([]))
}

///|
test "subscriber accepts concatenated SUBSCRIBE_OK and PUBLISH_DONE while objects drain" {
  let subscriber = subscriber_pending()
  let done : @protocol.PublishDoneMessage = {
    code: 3,
    stream_count: 1,
    reason: b"",
  }
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        30,
        b"\x04\x00\x02\x01\x00" + @protocol.encode_publish_done(done).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(@session.SubscribeEstablished(0, 1)),
      @session.Send(30, b"", true),
    ]),
  )
  let header = subscriber_header(1, true)
  let frames = [subscriber_frame(0, b"last")]
  subscriber_start_stream(subscriber, 44)
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        44,
        @protocol.encode_subgroup_stream(header, frames).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(
        @session.ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          header,
          frames,
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.StreamFinished(44)),
    Ok([
      @session.Notify(@session.GroupFinished(0, 1)),
      @session.Notify(@session.PublishDone(0, done)),
    ]),
  )
}

///|
test "subscriber resets incomplete subgroup headers without counting an unbound stream" {
  let subscriber = subscriber_started()
  let header = subscriber_header(1, false)
  subscriber_start_stream(subscriber, 40)
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        40,
        @protocol.encode_subgroup_header(header)[:1].to_owned(),
      ),
    ),
    Ok([]),
  )
  assert_eq(subscriber.receive(@session.StreamReset(40, 1)), Ok([]))
  assert_eq(subscriber.receive(@session.StreamReset(40, 1)), Ok([]))
  subscriber_start_stream(subscriber, 41)
  ignore(
    subscriber
    .receive(
      @session.StreamData(
        41,
        @protocol.encode_subgroup_stream(header, [subscriber_frame(0, b"last")]).unwrap(),
      ),
    )
    .unwrap(),
  )
  let done : @protocol.PublishDoneMessage = {
    code: 3,
    stream_count: 1,
    reason: b"",
  }
  assert_eq(
    subscriber.receive(
      @session.StreamData(30, @protocol.encode_publish_done(done).unwrap()),
    ),
    Ok([@session.Send(30, b"", true)]),
  )
  assert_eq(
    subscriber.receive(@session.StreamFinished(41)),
    Ok([@session.Notify(@session.PublishDone(0, done))]),
  )
}

///|
test "subscriber can reset an incomplete object stream after FIN validation fails" {
  let subscriber = subscriber_started()
  subscriber_start_stream(subscriber, 40)
  let header = subscriber_header(1, false)
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        40,
        @protocol.encode_subgroup_header(header) + b"\x00\x02x",
      ),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(@session.StreamFinished(40)),
    Ok([@session.StopReceiving(40, 3)]),
  )
  assert_eq(subscriber.receive(@session.StreamReset(40, 1)), Ok([]))
  let done : @protocol.PublishDoneMessage = {
    code: 3,
    stream_count: 1,
    reason: b"",
  }
  assert_eq(
    subscriber.receive(
      @session.StreamData(30, @protocol.encode_publish_done(done).unwrap()),
    ),
    Ok([
      @session.Send(30, b"", true),
      @session.Notify(@session.PublishDone(0, done)),
    ]),
  )
}
