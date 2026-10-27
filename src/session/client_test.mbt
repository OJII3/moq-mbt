///|
fn client_session_ready() -> @session.ClientSession raise {
  let client = @session.ClientSession::new(@session.Draft18, { options: [], }).unwrap()
  assert_eq(
    client.receive(
      @session.DatagramSendCapabilityChanged(enabled=true, max_length=65535),
    ),
    Ok([]),
  )
  assert_eq(
    client.start(),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Control)]),
  )
  assert_eq(
    client.receive(@session.StreamOpened(@session.Control, 10)),
    Ok([@session.SendChunk(10, b"\xaf\x00\x00\x00")]),
  )
  assert_eq(
    client.receive(@session.InboundStreamStarted(11, @session.Unidirectional)),
    Ok([]),
  )
  assert_eq(
    client.receive(@session.StreamData(11, b"\xaf\x00\x00\x00")),
    Ok([@session.EmitNotice(@session.HandshakeComplete({ options: [], }))]),
  )
  ignore(client.receive(@session.SendCompleted(10, @session.Sent)))
  client
}

///|
fn client_track(name : Bytes) -> @protocol.TrackName {
  { namespace_fields: [b"demo"], name, }
}

///|
fn client_subgroup_header(
  track_alias : UInt64,
  group_id : UInt64,
) -> @protocol.SubgroupHeader {
  {
    track_alias,
    group_id,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: true,
    first_object: true,
    has_properties: false,
  }
}

///|
fn client_object(
  object_id : UInt64,
  payload : Bytes,
) -> @protocol.SubgroupFrame {
  { object_id, status: @protocol.Normal, properties: b"", payload, }
}

///|
fn client_send_bytes(actions : Array[@session.SessionAction]) -> Bytes raise {
  match actions {
    [@session.SendChunk(_, bytes)] | [@session.SendFin(_, bytes)] => bytes
    _ => fail("expected one send action")
  }
}

///|
test "ClientSession shares even request IDs across tracks and routes interleaved objects" {
  let publisher = client_session_ready()
  let subscriber = client_session_ready()
  let track_a = client_track(b"video")
  let track_b = client_track(b"audio")
  let track_c = client_track(b"captions")

  let (publish_a, open_publish_a) = publisher.publish_track(track_a, 1).unwrap()
  let (publish_b, open_publish_b) = publisher.publish_track(track_b, 2).unwrap()
  assert_eq(publish_a, 0UL)
  assert_eq(publish_b, 2UL)
  assert_eq(open_publish_a, [
    @session.OpenStream(@session.Bidirectional, @session.Request(0)),
  ])
  assert_eq(open_publish_b, [
    @session.OpenStream(@session.Bidirectional, @session.Request(2)),
  ])
  assert_eq(publisher.publish_track(track_c, 1), Err(@session.AliasInUse))
  let (mixed_request, mixed_actions) = publisher
    .subscribe_track(track_b)
    .unwrap()
  assert_eq(mixed_request, 4UL)
  assert_eq(mixed_actions, [
    @session.OpenStream(@session.Bidirectional, @session.Request(4)),
  ])
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Request(2), 22)),
    Ok([
      @session.SendChunk(
        22,
        @protocol.encode_publish({
          request_id: 2,
          track: track_b,
          track_alias: 2,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Request(0), 20)),
    Ok([
      @session.SendChunk(
        20,
        @protocol.encode_publish({
          request_id: 0,
          track: track_a,
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ]),
  )

  let publish_ok = @protocol.encode_request_ok({
    parameters: [],
    properties: b"",
  }).unwrap()
  assert_eq(
    publisher.receive(@session.StreamData(22, publish_ok)),
    Ok([@session.EmitNotice(@session.PublishAccepted(2, track_b, 2UL))]),
  )
  assert_eq(
    publisher.receive(@session.StreamData(20, publish_ok)),
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

  let (subscribe_a, open_subscribe_a) = subscriber
    .subscribe_track(track_a)
    .unwrap()
  let (subscribe_b, open_subscribe_b) = subscriber
    .subscribe_track(track_b)
    .unwrap()
  assert_eq((subscribe_a, subscribe_b), (0UL, 2UL))
  assert_eq(open_subscribe_a, [
    @session.OpenStream(@session.Bidirectional, @session.Request(0)),
  ])
  assert_eq(open_subscribe_b, [
    @session.OpenStream(@session.Bidirectional, @session.Request(2)),
  ])
  assert_eq(
    subscriber.receive(@session.StreamOpened(@session.Request(2), 32)),
    Ok([
      @session.SendChunk(
        32,
        @protocol.encode_subscribe({
          request_id: 2,
          track: track_b,
          parameters: [],
        }).unwrap(),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.StreamOpened(@session.Request(0), 30)),
    Ok([
      @session.SendChunk(
        30,
        @protocol.encode_subscribe({
          request_id: 0,
          track: track_a,
          parameters: [],
        }).unwrap(),
      ),
    ]),
  )

  let subscribe_a_from_relay = @protocol.encode_subscribe({
    request_id: 1,
    track: track_a,
    parameters: [],
  }).unwrap()
  let subscribe_b_from_relay = @protocol.encode_subscribe({
    request_id: 3,
    track: track_b,
    parameters: [],
  }).unwrap()
  assert_eq(
    publisher.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
    Ok([]),
  )
  assert_eq(
    publisher.receive(@session.StreamData(40, subscribe_a_from_relay)),
    Ok([@session.EmitNotice(@session.SubscriptionRequested(1, 40, track_a))]),
  )
  assert_eq(
    publisher.receive(@session.InboundStreamStarted(42, @session.Bidirectional)),
    Ok([]),
  )
  assert_eq(
    publisher.receive(@session.StreamData(42, subscribe_b_from_relay)),
    Ok([@session.EmitNotice(@session.SubscriptionRequested(3, 42, track_b))]),
  )

  assert_eq(
    publisher.accept_subscription(40),
    Ok([
      @session.SendChunk(
        40,
        @protocol.encode_subscribe_ok({
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
      @session.EmitNotice(@session.SubscriptionAccepted(1, 40, 1)),
    ]),
  )
  assert_eq(
    publisher.accept_subscription(42),
    Ok([
      @session.SendChunk(
        42,
        @protocol.encode_subscribe_ok({
          track_alias: 2,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
      @session.EmitNotice(@session.SubscriptionAccepted(3, 42, 2)),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        32,
        @protocol.encode_subscribe_ok({
          track_alias: 20,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([@session.EmitNotice(@session.SubscribeEstablished(2, 20))]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        30,
        @protocol.encode_subscribe_ok({
          track_alias: 10,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([@session.EmitNotice(@session.SubscribeEstablished(0, 10))]),
  )

  let header_a = client_subgroup_header(1, 5)
  let header_b = client_subgroup_header(2, 7)
  let subscriber_header_a = client_subgroup_header(10, 5)
  let subscriber_header_b = client_subgroup_header(20, 7)
  assert_eq(
    publisher.open_object_stream(40, header_a),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Object(0))]),
  )
  assert_eq(
    publisher.open_object_stream(42, header_b),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Object(1))]),
  )
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(0), 50)),
    Ok([@session.SendChunk(50, @protocol.encode_subgroup_header(header_a))]),
  )
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(1), 52)),
    Ok([@session.SendChunk(52, @protocol.encode_subgroup_header(header_b))]),
  )
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(60, @session.Unidirectional),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(62, @session.Unidirectional),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        60,
        @protocol.encode_subgroup_header(subscriber_header_a),
      ),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        62,
        @protocol.encode_subgroup_header(subscriber_header_b),
      ),
    ),
    Ok([]),
  )

  let frame_a0 = client_object(0, b"a0")
  let frame_b0 = client_object(0, b"b0")
  let frame_a1 = client_object(1, b"a1")
  let frame_b1 = client_object(1, b"b1")
  let object_a0 = client_send_bytes(
    publisher.send_object(50, frame_a0).unwrap(),
  )
  let object_b0 = client_send_bytes(
    publisher.send_object(52, frame_b0).unwrap(),
  )
  let object_a1 = client_send_bytes(
    publisher.send_object(50, frame_a1).unwrap(),
  )
  let object_b1 = client_send_bytes(
    publisher.send_object(52, frame_b1).unwrap(),
  )
  assert_eq(
    subscriber.receive(@session.StreamData(60, object_a0)),
    Ok([
      @session.EmitNotice(
        @session.ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          subscriber_header_a,
          [frame_a0],
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.StreamData(62, object_b0)),
    Ok([
      @session.EmitNotice(
        @session.ObjectReceived(2, track_b, subscriber_header_b, [frame_b0]),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.StreamData(60, object_a1)),
    Ok([
      @session.EmitNotice(
        @session.ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          subscriber_header_a,
          [frame_a1],
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.StreamData(62, object_b1)),
    Ok([
      @session.EmitNotice(
        @session.ObjectReceived(2, track_b, subscriber_header_b, [frame_b1]),
      ),
    ]),
  )

  assert_eq(publisher.finish_object_stream(50), Ok([@session.SendFin(50, b"")]))
  assert_eq(publisher.finish_object_stream(52), Ok([@session.SendFin(52, b"")]))
  for _ in 0..<3 {
    assert_eq(
      publisher.receive(@session.SendCompleted(50, @session.Sent)),
      Ok([@session.EmitNotice(@session.StreamSendFinished(50, @session.Sent))]),
    )
    assert_eq(
      publisher.receive(@session.SendCompleted(52, @session.Sent)),
      Ok([@session.EmitNotice(@session.StreamSendFinished(52, @session.Sent))]),
    )
  }
  assert_eq(
    publisher.receive(@session.SendCompleted(50, @session.Sent)),
    Ok([
      @session.EmitNotice(@session.StreamSendFinished(50, @session.Sent)),
      @session.EmitNotice(@session.ObjectStreamSendFinished(50)),
    ]),
  )
  assert_eq(
    publisher.receive(@session.SendCompleted(52, @session.Sent)),
    Ok([
      @session.EmitNotice(@session.StreamSendFinished(52, @session.Sent)),
      @session.EmitNotice(@session.ObjectStreamSendFinished(52)),
    ]),
  )
  assert_eq(
    publisher.publish_done(40, 3, b""),
    Ok([
      @session.SendFin(
        40,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
      ),
      @session.EmitNotice(
        @session.PublishDone(1, { code: 3, stream_count: 1, reason: b"", }),
      ),
    ]),
  )
  assert_eq(
    publisher.publish_done(42, 3, b""),
    Ok([
      @session.SendFin(
        42,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
      ),
      @session.EmitNotice(
        @session.PublishDone(3, { code: 3, stream_count: 1, reason: b"", }),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        32,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
      ),
    ),
    Ok([@session.SendFin(32, b"")]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        30,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
      ),
    ),
    Ok([@session.SendFin(30, b"")]),
  )
  assert_eq(
    subscriber.receive(@session.InboundStreamFinished(60)),
    Ok([
      @session.EmitNotice(@session.GroupFinished(0, 5)),
      @session.EmitNotice(
        @session.PublishDone(0, { code: 3, stream_count: 1, reason: b"", }),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.InboundStreamFinished(62)),
    Ok([
      @session.EmitNotice(@session.GroupFinished(2, 7)),
      @session.EmitNotice(
        @session.PublishDone(2, { code: 3, stream_count: 1, reason: b"", }),
      ),
    ]),
  )
}

///|
test "ClientSession preserves SETUP trailing control bytes" {
  let client = @session.ClientSession::new(@session.Draft18, { options: [], }).unwrap()
  assert_eq(
    client.receive(@session.InboundStreamStarted(11, @session.Unidirectional)),
    Ok([]),
  )
  assert_eq(client.receive(@session.StreamData(11, b"\xaf\x00")), Ok([]))
  assert_eq(
    client.receive(
      @session.StreamData(11, b"\x00\x00" + b"\x10\x00\x03\x00\x00\x00"),
    ),
    Ok([
      @session.EmitNotice(@session.HandshakeComplete({ options: [], })),
      @session.EmitNotice(
        @session.ControlMessageReceived(
          @protocol.GoAwayControl({ uri: b"", timeout_ms: 0, request_id: 0, }),
        ),
      ),
      @session.EmitNotice(@session.ControlGoAwayReceived(b"", 0UL, 0UL)),
      @session.CloseConnection(0UL, b""),
      @session.EmitNotice(@session.SessionEnded(@session.Local(0UL, b""))),
    ]),
  )
}

///|
test "peer request ID parity and duplicate violations close the session" {
  let track = publisher_track()
  let invalid = publisher_announced()
  assert_eq(
    invalid.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
    Ok([]),
  )
  assert_eq(
    invalid.receive(
      @session.StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 2, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([
      @session.CloseConnection(4, b""),
      @session.EmitNotice(@session.SessionEnded(@session.Local(4, b""))),
    ]),
  )
  assert_eq(invalid.receive(@session.StreamData(40, b"")), Ok([]))
  assert_eq(invalid.state(), @session.Closed)

  let duplicate = publisher_announced()
  assert_eq(
    duplicate.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
    Ok([]),
  )
  let first = @protocol.encode_subscribe({
    request_id: 1,
    track,
    parameters: [],
  }).unwrap()
  assert_eq(
    duplicate.receive(@session.StreamData(40, first)),
    Ok([@session.EmitNotice(@session.SubscriptionRequested(1, 40, track))]),
  )
  assert_eq(
    duplicate.receive(@session.InboundStreamStarted(41, @session.Bidirectional)),
    Ok([]),
  )
  assert_eq(
    duplicate.receive(@session.StreamData(41, first)),
    Ok([
      @session.CloseConnection(4, b""),
      @session.EmitNotice(@session.SessionEnded(@session.Local(4, b""))),
    ]),
  )
  assert_eq(duplicate.receive(@session.StreamData(41, b"")), Ok([]))
  assert_eq(duplicate.state(), @session.Closed)
}

///|
test "peer request ID parity accepts distinct odd IDs" {
  let publisher = publisher_announced()
  let track = publisher_track()
  for (stream, request_id) in [(40L, 1UL), (41L, 3UL)] {
    assert_eq(
      publisher.receive(
        @session.InboundStreamStarted(stream, @session.Bidirectional),
      ),
      Ok([]),
    )
    assert_eq(
      publisher.receive(
        @session.StreamData(
          stream,
          @protocol.encode_subscribe({ request_id, track, parameters: [], }).unwrap(),
        ),
      ),
      Ok([
        @session.EmitNotice(
          @session.SubscriptionRequested(request_id, stream, track),
        ),
      ]),
    )
  }
  assert_eq(
    publisher.accept_subscription(40),
    Ok([
      @session.SendChunk(
        40,
        @protocol.encode_subscribe_ok({
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
      @session.EmitNotice(@session.SubscriptionAccepted(1, 40, 1)),
    ]),
  )
  assert_eq(
    publisher.accept_subscription(41),
    Ok([
      @session.SendChunk(
        41,
        @protocol.encode_subscribe_ok({
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
      @session.EmitNotice(@session.SubscriptionAccepted(3, 41, 1)),
    ]),
  )
}

///|
test "ClientSession sends Objects and PUBLISH_DONE on a local PUBLISH stream" {
  let publisher = client_session_ready()
  let track = client_track(b"video")
  let (request_id, _) = publisher.publish_track(track, 1).unwrap()
  assert_eq(request_id, 0UL)
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Request(0), 20)),
    Ok([
      @session.SendChunk(
        20,
        @protocol.encode_publish({
          request_id: 0,
          track,
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(
        20,
        @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
      ),
    ),
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

  let header = client_subgroup_header(1, 7)
  assert_eq(
    publisher.open_object_stream(20, header),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Object(0))]),
  )
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(0), 22)),
    Ok([@session.SendChunk(22, @protocol.encode_subgroup_header(header))]),
  )
  assert_true(
    match publisher.send_object(22, client_object(0, b"payload")) {
      Ok([@session.SendChunk(22, bytes)]) => !bytes.is_empty()
      _ => false
    },
  )
  assert_eq(publisher.finish_object_stream(22), Ok([@session.SendFin(22, b"")]))
  assert_eq(
    publisher.receive(@session.SendCompleted(22, @session.Sent)),
    Ok([@session.EmitNotice(@session.StreamSendFinished(22, @session.Sent))]),
  )
  assert_eq(
    publisher.receive(@session.SendCompleted(22, @session.Sent)),
    Ok([@session.EmitNotice(@session.StreamSendFinished(22, @session.Sent))]),
  )
  assert_eq(
    publisher.receive(@session.SendCompleted(22, @session.Sent)),
    Ok([
      @session.EmitNotice(@session.StreamSendFinished(22, @session.Sent)),
      @session.EmitNotice(@session.ObjectStreamSendFinished(22)),
    ]),
  )
  assert_eq(
    publisher.publish_done(20, 3, b""),
    Ok([
      @session.SendFin(
        20,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
      ),
      @session.EmitNotice(
        @session.PublishDone(0, { code: 3, stream_count: 1, reason: b"", }),
      ),
    ]),
  )
}

///|
test "ClientSession ends inbound subscriptions when request streams terminate" {
  let publisher = client_session_ready()
  let track = client_track(b"video")
  let (publish_id, _) = publisher.publish_track(track, 1).unwrap()
  assert_eq(publish_id, 0UL)
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Request(0), 20)),
    Ok([
      @session.SendChunk(
        20,
        @protocol.encode_publish({
          request_id: 0,
          track,
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(
        20,
        @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
      ),
    ),
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

  assert_eq(
    publisher.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
    Ok([]),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 1, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([@session.EmitNotice(@session.SubscriptionRequested(1, 40, track))]),
  )
  assert_eq(
    publisher.accept_subscription(40),
    Ok([
      @session.SendChunk(
        40,
        @protocol.encode_subscribe_ok({
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
      @session.EmitNotice(@session.SubscriptionAccepted(1, 40, 1)),
    ]),
  )

  let header = client_subgroup_header(1, 7)
  assert_eq(
    publisher.open_object_stream(40, header),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Object(0))]),
  )
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(0), 50)),
    Ok([@session.SendChunk(50, @protocol.encode_subgroup_header(header))]),
  )
  assert_eq(
    publisher.receive(@session.StreamReset(40, 9UL)),
    Ok([
      @session.AbortSend(40, 9UL),
      @session.AbortSend(50, 9UL),
      @session.EmitNotice(
        @session.SubscriptionEnded(1, 40, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    publisher.open_object_stream(40, header),
    Err(@session.SubscriptionNotAccepted),
  )
  assert_eq(
    publisher.send_object(50, client_object(0, b"late")),
    Err(@session.UnknownStream),
  )

  assert_eq(
    publisher.receive(@session.InboundStreamStarted(42, @session.Bidirectional)),
    Ok([]),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(
        42,
        @protocol.encode_subscribe({ request_id: 3, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([@session.EmitNotice(@session.SubscriptionRequested(3, 42, track))]),
  )
  assert_eq(
    publisher.accept_subscription(42),
    Ok([
      @session.SendChunk(
        42,
        @protocol.encode_subscribe_ok({
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
      @session.EmitNotice(@session.SubscriptionAccepted(3, 42, 1)),
    ]),
  )
  assert_eq(
    publisher.open_object_stream(42, header),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Object(1))]),
  )
  assert_eq(
    publisher.receive(@session.SendStopped(42, 5UL)),
    Ok([
      @session.StopReceiving(42, 5UL),
      @session.EmitNotice(
        @session.SubscriptionEnded(3, 42, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(1), 52)),
    Ok([@session.AbortSend(52, 5UL)]),
  )
}

///|
test "ClientSession stops incoming object streams when a subscription request ends" {
  let subscriber = client_session_ready()
  let track = client_track(b"video")
  let (request_id, _) = subscriber.subscribe_track(track).unwrap()
  assert_eq(request_id, 0UL)
  assert_eq(
    subscriber.receive(@session.StreamOpened(@session.Request(0), 30)),
    Ok([
      @session.SendChunk(
        30,
        @protocol.encode_subscribe({ request_id: 0, track, parameters: [], }).unwrap(),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        30,
        @protocol.encode_subscribe_ok({
          track_alias: 10,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([@session.EmitNotice(@session.SubscribeEstablished(0, 10))]),
  )
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(60, @session.Unidirectional),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(62, @session.Unidirectional),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        60,
        @protocol.encode_subgroup_header(client_subgroup_header(10, 7)),
      ),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(@session.StreamReset(30, 1UL)),
    Ok([
      @session.AbortSend(30, 1UL),
      @session.StopReceiving(60, 1UL),
      @session.EmitNotice(
        @session.SubscriptionEnded(0, 30, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        62,
        @protocol.encode_subgroup_header(client_subgroup_header(10, 8)),
      ),
    ),
    Ok([@session.StopReceiving(62, 1UL)]),
  )
  assert_eq(subscriber.receive(@session.StreamData(60, b"ignored")), Ok([]))
  assert_eq(subscriber.receive(@session.InboundStreamFinished(60)), Ok([]))
  assert_eq(subscriber.receive(@session.InboundStreamFinished(62)), Ok([]))
}

///|
test "ClientSession releases a completed subscription alias" {
  let subscriber = client_session_ready()
  let first_track = client_track(b"first")
  let second_track = client_track(b"second")
  assert_eq(subscriber.subscribe_track(first_track).unwrap().0, 0UL)
  assert_eq(
    subscriber.receive(@session.StreamOpened(@session.Request(0), 30)),
    Ok([
      @session.SendChunk(
        30,
        @protocol.encode_subscribe({
          request_id: 0,
          track: first_track,
          parameters: [],
        }).unwrap(),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        30,
        @protocol.encode_subscribe_ok({
          track_alias: 10,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([@session.EmitNotice(@session.SubscribeEstablished(0, 10))]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        30,
        @protocol.encode_publish_done({ code: 0, stream_count: 0, reason: b"", }).unwrap(),
      ),
    ),
    Ok([
      @session.SendFin(30, b""),
      @session.EmitNotice(
        @session.PublishDone(0, { code: 0, stream_count: 0, reason: b"", }),
      ),
    ]),
  )
  assert_eq(subscriber.receive(@session.InboundStreamFinished(30)), Ok([]))

  assert_eq(subscriber.subscribe_track(second_track).unwrap().0, 2UL)
  assert_eq(
    subscriber.receive(@session.StreamOpened(@session.Request(2), 32)),
    Ok([
      @session.SendChunk(
        32,
        @protocol.encode_subscribe({
          request_id: 2,
          track: second_track,
          parameters: [],
        }).unwrap(),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        32,
        @protocol.encode_subscribe_ok({
          track_alias: 10,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([@session.EmitNotice(@session.SubscribeEstablished(2, 10))]),
  )
}

///|
test "ClientSession ends local PUBLISH subscriptions on STOP_SENDING" {
  let publisher = client_session_ready()
  let track = client_track(b"video")
  assert_eq(publisher.publish_track(track, 1).unwrap().0, 0UL)
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Request(0), 20)),
    Ok([
      @session.SendChunk(
        20,
        @protocol.encode_publish({
          request_id: 0,
          track,
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(
        20,
        @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
      ),
    ),
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

  let header = client_subgroup_header(1, 7)
  assert_eq(
    publisher.open_object_stream(20, header),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Object(0))]),
  )
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(0), 50)),
    Ok([@session.SendChunk(50, @protocol.encode_subgroup_header(header))]),
  )
  assert_eq(
    publisher.receive(@session.SendStopped(20, 4UL)),
    Ok([
      @session.StopReceiving(20, 4UL),
      @session.AbortSend(50, 4UL),
      @session.EmitNotice(
        @session.SubscriptionEnded(0, 20, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    publisher.open_object_stream(20, header),
    Err(@session.SubscriptionNotAccepted),
  )
  assert_eq(
    publisher.send_object(50, client_object(0, b"late")),
    Err(@session.UnknownStream),
  )
}
