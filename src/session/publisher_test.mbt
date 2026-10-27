///|
fn publisher_track() -> @protocol.TrackName {
  { namespace_fields: [b"demo"], name: b"video", }
}

///|
fn publisher_header(
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
fn publisher_frame(
  object_id : UInt64,
  payload : Bytes,
) -> @protocol.SubgroupFrame {
  { object_id, status: @protocol.Normal, properties: b"", payload, }
}

///|
fn publisher_announced() -> @session.ClientSession raise {
  let publisher = client_session_ready()
  ignore(publisher.publish_track(publisher_track(), 1).unwrap())
  ignore(
    publisher.receive(@session.StreamOpened(@session.Request(0), 20)).unwrap(),
  )
  assert_eq(
    publisher.receive(@session.StreamData(20, b"\x07\x00\x01\x00")),
    Ok([
      @session.EmitNotice(
        @session.PublishAccepted(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          1UL,
        ),
      ),
    ]),
  )
  publisher
}

///|
fn publisher_started() -> @session.ClientSession raise {
  let publisher = publisher_announced()
  ignore(
    publisher
    .receive(@session.InboundStreamStarted(40, @session.Bidirectional))
    .unwrap(),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(
        40,
        @protocol.encode_subscribe({
          request_id: 1,
          track: publisher_track(),
          parameters: [],
        }).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.SubscriptionRequested(1, 40, publisher_track()),
      ),
    ]),
  )
  assert_eq(
    publisher.accept_subscription(40),
    Ok([
      @session.SendChunk(40, b"\x04\x00\x02\x01\x00"),
      @session.EmitNotice(@session.SubscriptionAccepted(1, 40, 1)),
    ]),
  )
  publisher
}

///|
fn publisher_open(
  publisher : @session.ClientSession,
  subscription_stream : Int64,
  stream : Int64,
  header : @protocol.SubgroupHeader,
) -> Bytes raise {
  let token = match
    publisher.open_object_stream(subscription_stream, header).unwrap() {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("expected object stream request")
  }
  client_send_bytes(
    publisher
    .receive(@session.StreamOpened(@session.Object(token), stream))
    .unwrap(),
  )
}

///|
fn publisher_finish(
  publisher : @session.ClientSession,
  stream : Int64,
  object_count : Int32,
) -> Unit raise {
  assert_eq(
    publisher.finish_object_stream(stream),
    Ok([@session.SendFin(stream, b"")]),
  )
  // Header, Objects, then FIN each produce a send completion.
  for _ in 0..<(object_count + 1) {
    assert_eq(
      publisher.receive(@session.SendCompleted(stream, @session.Sent)),
      Ok([
        @session.EmitNotice(@session.StreamSendFinished(stream, @session.Sent)),
      ]),
    )
  }
  assert_eq(
    publisher.receive(@session.SendCompleted(stream, @session.Sent)),
    Ok([
      @session.EmitNotice(@session.StreamSendFinished(stream, @session.Sent)),
      @session.EmitNotice(@session.ObjectStreamSendFinished(stream)),
    ]),
  )
}

///|
fn publisher_done_message(
  publisher : @session.ClientSession,
  stream : Int64,
) -> @protocol.PublishDoneMessage raise {
  let bytes = match publisher.publish_done(stream, 3, b"").unwrap() {
    [
      @session.SendFin(_, bytes),
      @session.EmitNotice(@session.PublishDone(_, _)),
    ] => bytes
    _ => fail("expected PUBLISH_DONE and FIN")
  }
  match @protocol.decode_control(bytes) {
    @protocol.Complete(@protocol.PublishDone(message), _) => message
    _ => fail("expected encoded PUBLISH_DONE")
  }
}

///|
test "publisher encodes one header and increasing object deltas" {
  let publisher = client_session_ready()
  ignore(publisher.publish_track(publisher_track(), 1).unwrap())
  assert_eq(
    client_send_bytes(
      publisher.receive(@session.StreamOpened(@session.Request(0), 20)).unwrap(),
    ),
    b"\x1d\x00\x0f\x00\x01\x04demo\x05video\x01\x00",
  )
  let subscribe = @protocol.encode_subscribe({
    request_id: 1,
    track: publisher_track(),
    parameters: [],
  }).unwrap()
  assert_eq(subscribe, b"\x03\x00\x0e\x01\x01\x04demo\x05video\x00")
  for split = 0; split < subscribe.length(); split = split + 1 {
    let publisher = publisher_announced()
    ignore(
      publisher
      .receive(@session.InboundStreamStarted(40, @session.Bidirectional))
      .unwrap(),
    )
    assert_eq(
      publisher.receive(@session.StreamData(40, subscribe[:split].to_owned())),
      Ok([]),
    )
    assert_eq(
      publisher.receive(@session.StreamData(40, subscribe[split:].to_owned())),
      Ok([
        @session.EmitNotice(
          @session.SubscriptionRequested(1, 40, publisher_track()),
        ),
      ]),
    )
  }
  let publisher = publisher_started()
  let header = publisher_header(0, true)
  assert_eq(
    publisher_open(publisher, 40, 50, header),
    @protocol.encode_subgroup_header(header),
  )
  assert_eq(
    client_send_bytes(
      publisher.send_object(50, publisher_frame(0, b"payload")).unwrap(),
    ),
    b"\x00\x07payload",
  )
  assert_eq(
    client_send_bytes(
      publisher.send_object(50, publisher_frame(2, b"second")).unwrap(),
    ),
    b"\x01\x06second",
  )
  assert_eq(
    publisher.send_object(50, publisher_frame(2, b"duplicate")),
    Err(@session.Track(@session.ObjectIdNotIncreasing)),
  )
  publisher_finish(publisher, 50, 2)
  assert_eq(publisher_done_message(publisher, 40), {
    code: 3,
    stream_count: 1,
    reason: b"",
  })
}

///|
test "publisher counts empty and parallel streams per subscription" {
  let publisher = publisher_started()
  let header_a = publisher_header(1, false)
  let header_b = publisher_header(2, true)
  ignore(publisher_open(publisher, 40, 60, header_a))
  ignore(publisher_open(publisher, 40, 61, header_b))
  ignore(publisher_open(publisher, 20, 62, header_a))
  assert_eq(
    client_send_bytes(
      publisher.send_object(60, publisher_frame(3, b"a")).unwrap(),
    ),
    b"\x03\x01a",
  )
  assert_eq(
    client_send_bytes(
      publisher.send_object(60, publisher_frame(4, b"b")).unwrap(),
    ),
    b"\x00\x01b",
  )
  assert_eq(
    publisher.publish_done(40, 3, b""),
    Err(@session.Track(@session.ObjectStreamsStillOpen)),
  )
  publisher_finish(publisher, 60, 2)
  assert_eq(
    publisher.receive(@session.SendCompleted(61, @session.Reset)),
    Ok([@session.EmitNotice(@session.StreamSendFinished(61, @session.Reset))]),
  )
  publisher_finish(publisher, 62, 0)
  assert_eq(publisher_done_message(publisher, 40), {
    code: 3,
    stream_count: 2,
    reason: b"",
  })
  assert_eq(publisher_done_message(publisher, 20), {
    code: 3,
    stream_count: 1,
    reason: b"",
  })
  let no_stream_publisher = publisher_announced()
  assert_eq(publisher_done_message(no_stream_publisher, 20), {
    code: 3,
    stream_count: 0,
    reason: b"",
  })
}

///|
test "publisher validates stream alias and allows only one publish done" {
  let publisher = publisher_announced()
  let invalid_header : @protocol.SubgroupHeader = {
    ..publisher_header(0, false),
    track_alias: 2,
  }
  assert_eq(
    publisher.open_object_stream(20, invalid_header),
    Err(@session.Track(@session.AliasMismatch)),
  )
  assert_eq(publisher_done_message(publisher, 20), {
    code: 3,
    stream_count: 0,
    reason: b"",
  })
  assert_eq(
    publisher.publish_done(20, 3, b""),
    Err(@session.SubscriptionNotAccepted),
  )
}

///|
test "publisher keeps EndOfGroup status separate from header End of Group" {
  let publisher = publisher_started()
  let header = publisher_header(1, false)
  let header_bytes = publisher_open(publisher, 40, 80, header)
  let frame : @protocol.SubgroupFrame = {
    object_id: 4,
    status: @protocol.EndOfGroup,
    properties: b"",
    payload: b"",
  }
  let wire = header_bytes +
    client_send_bytes(publisher.send_object(80, frame).unwrap())
  let subscriber = subscriber_started()
  ignore(
    subscriber
    .receive(@session.InboundStreamStarted(80, @session.Unidirectional))
    .unwrap(),
  )
  assert_eq(
    subscriber.receive(@session.StreamData(80, wire)),
    Ok([
      @session.EmitNotice(
        @session.ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          header,
          [frame],
        ),
      ),
    ]),
  )
  publisher_finish(publisher, 80, 1)
  assert_eq(subscriber.receive(@session.InboundStreamFinished(80)), Ok([]))
}

///|
test "publisher rejects duplicate object stream handles without replacing writer state or counting twice" {
  let publisher = publisher_announced()
  ignore(publisher_open(publisher, 20, 50, publisher_header(1, false)))
  assert_eq(
    client_send_bytes(
      publisher.send_object(50, publisher_frame(3, b"a")).unwrap(),
    ),
    b"\x03\x01a",
  )
  let token = match
    publisher.open_object_stream(20, publisher_header(2, false)).unwrap() {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("expected object stream request")
  }
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(token), 50)),
    Err(@session.Track(@session.UnexpectedMessage)),
  )
  assert_eq(
    client_send_bytes(
      publisher.send_object(50, publisher_frame(4, b"b")).unwrap(),
    ),
    b"\x00\x01b",
  )
  ignore(
    publisher
    .receive(@session.StreamOpened(@session.Object(token), 51))
    .unwrap(),
  )
  publisher_finish(publisher, 50, 2)
  publisher_finish(publisher, 51, 0)
  assert_eq(publisher_done_message(publisher, 20), {
    code: 3,
    stream_count: 2,
    reason: b"",
  })
}
