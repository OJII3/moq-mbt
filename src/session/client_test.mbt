///|
fn client_session_ready(
  max_object_buffer_bytes~ : UInt64 = 16UL * 1024UL * 1024UL,
) -> @session.ClientSession raise {
  let client = @session.ClientSession::new(
    Draft18,
    { options: [], },
    max_object_buffer_bytes~,
  ).unwrap()
  client.set_stream_delivery_capability(committed=true, abort_after_fin=true)
  assert_eq(
    client.receive(
      DatagramSendCapabilityChanged(enabled=true, max_length=65535),
    ),
    Ok([]),
  )
  assert_eq(client.start(), Ok([OpenStream(Unidirectional, Control)]))
  assert_eq(
    client.receive(StreamOpened(Control, 10)),
    Ok([SendChunk(10, b"\xaf\x00\x00\x00")]),
  )
  assert_eq(client.receive(InboundStreamStarted(11, Unidirectional)), Ok([]))
  assert_eq(
    client.receive(StreamData(11, b"\xaf\x00\x00\x00")),
    Ok([EmitNotice(HandshakeComplete({ options: [], }))]),
  )
  ignore(client.receive(SendCompleted(10, Sent)))
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
    subgroup_id_mode: Zero,
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
  { object_id, status: Normal, properties: b"", payload, }
}

///|
fn client_send_bytes(actions : Array[@session.SessionAction]) -> Bytes raise {
  match actions {
    [SendChunk(_, bytes)] | [SendFin(_, bytes)] => bytes
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

  let (publish_a, open_publish_a) = publisher
    .publish_track(track_a, TrackAlias(1))
    .unwrap()
  let (publish_b, open_publish_b) = publisher
    .publish_track(track_b, TrackAlias(2))
    .unwrap()
  assert_eq(publish_a, RequestId(0UL))
  assert_eq(publish_b, RequestId(2UL))
  assert_eq(open_publish_a, [OpenStream(Bidirectional, Request(0))])
  assert_eq(open_publish_b, [OpenStream(Bidirectional, Request(2))])
  assert_eq(publisher.publish_track(track_c, TrackAlias(1)), Err(AliasInUse))
  let (mixed_request, mixed_actions) = publisher
    .subscribe_track(track_b)
    .unwrap()
  assert_eq(mixed_request, RequestId(4UL))
  assert_eq(mixed_actions, [OpenStream(Bidirectional, Request(4))])
  assert_eq(
    publisher.receive(StreamOpened(Request(2), 22)),
    Ok([
      SendChunk(
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
    publisher.receive(StreamOpened(Request(0), 20)),
    Ok([
      SendChunk(
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
    publisher.receive(StreamData(22, publish_ok)),
    Ok([EmitNotice(PublishAccepted(2, track_b, 2UL))]),
  )
  assert_eq(
    publisher.receive(StreamData(20, publish_ok)),
    Ok([
      EmitNotice(
        PublishAccepted(
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
  assert_eq(open_subscribe_a, [OpenStream(Bidirectional, Request(0))])
  assert_eq(open_subscribe_b, [OpenStream(Bidirectional, Request(2))])
  assert_eq(
    subscriber.receive(StreamOpened(Request(2), 32)),
    Ok([
      SendChunk(
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
    subscriber.receive(StreamOpened(Request(0), 30)),
    Ok([
      SendChunk(
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
  assert_eq(publisher.receive(InboundStreamStarted(40, Bidirectional)), Ok([]))
  assert_eq(
    publisher.receive(StreamData(40, subscribe_a_from_relay)),
    Ok([EmitNotice(SubscriptionRequested(1, 40, track_a))]),
  )
  assert_eq(publisher.receive(InboundStreamStarted(42, Bidirectional)), Ok([]))
  assert_eq(
    publisher.receive(StreamData(42, subscribe_b_from_relay)),
    Ok([EmitNotice(SubscriptionRequested(3, 42, track_b))]),
  )

  assert_eq(
    publisher.accept_subscription(StreamId(40)),
    Ok([
      SendChunk(
        40,
        @protocol.encode_subscribe_ok({
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
      EmitNotice(SubscriptionAccepted(1, 40, 1)),
    ]),
  )
  assert_eq(
    publisher.accept_subscription(StreamId(42)),
    Ok([
      SendChunk(
        42,
        @protocol.encode_subscribe_ok({
          track_alias: 2,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
      EmitNotice(SubscriptionAccepted(3, 42, 2)),
    ]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(
        32,
        @protocol.encode_subscribe_ok({
          track_alias: 20,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscribeEstablished(2, 20))]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(
        30,
        @protocol.encode_subscribe_ok({
          track_alias: 10,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscribeEstablished(0, 10))]),
  )

  let header_a = client_subgroup_header(1, 5)
  let header_b = client_subgroup_header(2, 7)
  let subscriber_header_a = client_subgroup_header(10, 5)
  let subscriber_header_b = client_subgroup_header(20, 7)
  assert_eq(
    publisher.open_object_stream(StreamId(40), header_a),
    Ok([OpenStream(Unidirectional, Object(0))]),
  )
  assert_eq(
    publisher.open_object_stream(StreamId(42), header_b),
    Ok([OpenStream(Unidirectional, Object(1))]),
  )
  assert_eq(
    publisher.receive(StreamOpened(Object(0), 50)),
    Ok([SendChunk(50, @protocol.encode_subgroup_header(header_a))]),
  )
  assert_eq(
    publisher.receive(StreamOpened(Object(1), 52)),
    Ok([SendChunk(52, @protocol.encode_subgroup_header(header_b))]),
  )
  assert_eq(
    subscriber.receive(InboundStreamStarted(60, Unidirectional)),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(InboundStreamStarted(62, Unidirectional)),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(60, @protocol.encode_subgroup_header(subscriber_header_a)),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(62, @protocol.encode_subgroup_header(subscriber_header_b)),
    ),
    Ok([]),
  )

  let frame_a0 = client_object(0, b"a0")
  let frame_b0 = client_object(0, b"b0")
  let frame_a1 = client_object(1, b"a1")
  let frame_b1 = client_object(1, b"b1")
  let object_a0 = client_send_bytes(
    publisher.send_object(StreamId(50), frame_a0).unwrap(),
  )
  let object_b0 = client_send_bytes(
    publisher.send_object(StreamId(52), frame_b0).unwrap(),
  )
  let object_a1 = client_send_bytes(
    publisher.send_object(StreamId(50), frame_a1).unwrap(),
  )
  let object_b1 = client_send_bytes(
    publisher.send_object(StreamId(52), frame_b1).unwrap(),
  )
  assert_eq(
    subscriber.receive(StreamData(60, object_a0)),
    Ok([
      EmitNotice(
        ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          subscriber_header_a,
          [frame_a0],
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(StreamData(62, object_b0)),
    Ok([EmitNotice(ObjectReceived(2, track_b, subscriber_header_b, [frame_b0]))]),
  )
  assert_eq(
    subscriber.receive(StreamData(60, object_a1)),
    Ok([
      EmitNotice(
        ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          subscriber_header_a,
          [frame_a1],
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(StreamData(62, object_b1)),
    Ok([EmitNotice(ObjectReceived(2, track_b, subscriber_header_b, [frame_b1]))]),
  )

  assert_eq(
    publisher.finish_object_stream(StreamId(50)),
    Ok([SendFin(50, b"")]),
  )
  assert_eq(
    publisher.finish_object_stream(StreamId(52)),
    Ok([SendFin(52, b"")]),
  )
  for _ in 0..<3 {
    assert_eq(
      publisher.receive(SendCompleted(50, Sent)),
      Ok([EmitNotice(StreamSendFinished(50, Sent))]),
    )
    assert_eq(
      publisher.receive(SendCompleted(52, Sent)),
      Ok([EmitNotice(StreamSendFinished(52, Sent))]),
    )
  }
  assert_eq(
    publisher.receive(SendCompleted(50, Sent)),
    Ok([EmitNotice(StreamSendFinished(50, Sent))]),
  )
  assert_eq(
    publisher.receive(StreamDataCommitted(50)),
    Ok([EmitNotice(ObjectStreamSendFinished(50))]),
  )
  assert_eq(
    publisher.receive(SendCompleted(52, Sent)),
    Ok([EmitNotice(StreamSendFinished(52, Sent))]),
  )
  assert_eq(
    publisher.receive(StreamDataCommitted(52)),
    Ok([EmitNotice(ObjectStreamSendFinished(52))]),
  )
  assert_eq(
    publisher.publish_done(StreamId(40), 3, b""),
    Ok([
      SendFin(
        40,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
      ),
      EmitNotice(PublishDone(1, { code: 3, stream_count: 1, reason: b"", })),
    ]),
  )
  assert_eq(
    publisher.publish_done(StreamId(42), 3, b""),
    Ok([
      SendFin(
        42,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
      ),
      EmitNotice(PublishDone(3, { code: 3, stream_count: 1, reason: b"", })),
    ]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(
        32,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
      ),
    ),
    Ok([SendFin(32, b"")]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(
        30,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
      ),
    ),
    Ok([SendFin(30, b"")]),
  )
  assert_eq(
    subscriber.receive(InboundStreamFinished(60)),
    Ok([
      EmitNotice(GroupFinished(0, 5)),
      EmitNotice(PublishDone(0, { code: 3, stream_count: 1, reason: b"", })),
    ]),
  )
  assert_eq(
    subscriber.receive(InboundStreamFinished(62)),
    Ok([
      EmitNotice(GroupFinished(2, 7)),
      EmitNotice(PublishDone(2, { code: 3, stream_count: 1, reason: b"", })),
    ]),
  )
}

///|
test "ClientSession preserves SETUP trailing control bytes" {
  let client = @session.ClientSession::new(Draft18, { options: [], }).unwrap()
  assert_eq(client.receive(InboundStreamStarted(11, Unidirectional)), Ok([]))
  assert_eq(client.receive(StreamData(11, b"\xaf\x00")), Ok([]))
  assert_eq(
    client.receive(StreamData(11, b"\x00\x00" + b"\x10\x00\x03\x00\x00\x00")),
    Ok([
      EmitNotice(HandshakeComplete({ options: [], })),
      EmitNotice(
        ControlMessageReceived(
          GoAwayControl({ uri: b"", timeout_ms: 0, request_id: 0, }),
        ),
      ),
      EmitNotice(ControlGoAwayReceived(b"", 0UL, 0UL)),
      CloseConnection(0UL, b""),
      EmitNotice(SessionEnded(Local(0UL, b""))),
    ]),
  )
}

///|
test "peer request ID parity and duplicate violations close the session" {
  let track = publisher_track()
  let invalid = publisher_announced()
  assert_eq(invalid.receive(InboundStreamStarted(40, Bidirectional)), Ok([]))
  assert_eq(
    invalid.receive(
      StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 2, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([CloseConnection(4, b""), EmitNotice(SessionEnded(Local(4, b"")))]),
  )
  assert_eq(invalid.receive(StreamData(40, b"")), Ok([]))
  assert_eq(invalid.state(), Closed)

  let duplicate = publisher_announced()
  assert_eq(duplicate.receive(InboundStreamStarted(40, Bidirectional)), Ok([]))
  let first = @protocol.encode_subscribe({
    request_id: 1,
    track,
    parameters: [],
  }).unwrap()
  assert_eq(
    duplicate.receive(StreamData(40, first)),
    Ok([EmitNotice(SubscriptionRequested(1, 40, track))]),
  )
  assert_eq(duplicate.receive(InboundStreamStarted(41, Bidirectional)), Ok([]))
  assert_eq(
    duplicate.receive(StreamData(41, first)),
    Ok([CloseConnection(4, b""), EmitNotice(SessionEnded(Local(4, b"")))]),
  )
  assert_eq(duplicate.receive(StreamData(41, b"")), Ok([]))
  assert_eq(duplicate.state(), Closed)
}

///|
test "peer request ID parity accepts distinct odd IDs" {
  let publisher = publisher_announced()
  let track = publisher_track()
  for (stream, request_id) in [(40L, 1UL), (41L, 3UL)] {
    assert_eq(
      publisher.receive(InboundStreamStarted(stream, Bidirectional)),
      Ok([]),
    )
    assert_eq(
      publisher.receive(
        StreamData(
          stream,
          @protocol.encode_subscribe({ request_id, track, parameters: [], }).unwrap(),
        ),
      ),
      Ok([EmitNotice(SubscriptionRequested(request_id, stream, track))]),
    )
  }
  assert_eq(
    publisher.accept_subscription(StreamId(40)),
    Ok([
      SendChunk(
        40,
        @protocol.encode_subscribe_ok({
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
      EmitNotice(SubscriptionAccepted(1, 40, 1)),
    ]),
  )
  assert_eq(
    publisher.accept_subscription(StreamId(41)),
    Ok([
      SendChunk(
        41,
        @protocol.encode_subscribe_ok({
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
      EmitNotice(SubscriptionAccepted(3, 41, 1)),
    ]),
  )
}

///|
test "ClientSession sends Objects and PUBLISH_DONE on a local PUBLISH stream" {
  let publisher = client_session_ready()
  let track = client_track(b"video")
  let (request_id, _) = publisher.publish_track(track, TrackAlias(1)).unwrap()
  assert_eq(request_id, RequestId(0UL))
  assert_eq(
    publisher.receive(StreamOpened(Request(0), 20)),
    Ok([
      SendChunk(
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
      StreamData(
        20,
        @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
      ),
    ),
    Ok([
      EmitNotice(
        PublishAccepted(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          1UL,
        ),
      ),
    ]),
  )

  let header = client_subgroup_header(1, 7)
  assert_eq(
    publisher.open_object_stream(StreamId(20), header),
    Ok([OpenStream(Unidirectional, Object(0))]),
  )
  assert_eq(
    publisher.receive(StreamOpened(Object(0), 22)),
    Ok([SendChunk(22, @protocol.encode_subgroup_header(header))]),
  )
  assert_true(
    match publisher.send_object(StreamId(22), client_object(0, b"payload")) {
      Ok([SendChunk(22, bytes)]) => !bytes.is_empty()
      _ => false
    },
  )
  assert_eq(
    publisher.finish_object_stream(StreamId(22)),
    Ok([SendFin(22, b"")]),
  )
  assert_eq(
    publisher.receive(SendCompleted(22, Sent)),
    Ok([EmitNotice(StreamSendFinished(22, Sent))]),
  )
  assert_eq(
    publisher.receive(SendCompleted(22, Sent)),
    Ok([EmitNotice(StreamSendFinished(22, Sent))]),
  )
  assert_eq(
    publisher.receive(SendCompleted(22, Sent)),
    Ok([EmitNotice(StreamSendFinished(22, Sent))]),
  )
  assert_eq(
    publisher.receive(StreamDataCommitted(22)),
    Ok([EmitNotice(ObjectStreamSendFinished(22))]),
  )
  assert_eq(
    publisher.publish_done(StreamId(20), 3, b""),
    Ok([
      SendFin(
        20,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
      ),
      EmitNotice(PublishDone(0, { code: 3, stream_count: 1, reason: b"", })),
    ]),
  )
}

///|
test "ClientSession ends inbound subscriptions when request streams terminate" {
  let publisher = client_session_ready()
  let track = client_track(b"video")
  let (publish_id, _) = publisher.publish_track(track, TrackAlias(1)).unwrap()
  assert_eq(publish_id, RequestId(0UL))
  assert_eq(
    publisher.receive(StreamOpened(Request(0), 20)),
    Ok([
      SendChunk(
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
      StreamData(
        20,
        @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
      ),
    ),
    Ok([
      EmitNotice(
        PublishAccepted(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          1UL,
        ),
      ),
    ]),
  )

  assert_eq(publisher.receive(InboundStreamStarted(40, Bidirectional)), Ok([]))
  assert_eq(
    publisher.receive(
      StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 1, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscriptionRequested(1, 40, track))]),
  )
  assert_eq(
    publisher.accept_subscription(StreamId(40)),
    Ok([
      SendChunk(
        40,
        @protocol.encode_subscribe_ok({
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
      EmitNotice(SubscriptionAccepted(1, 40, 1)),
    ]),
  )

  let header = client_subgroup_header(1, 7)
  assert_eq(
    publisher.open_object_stream(StreamId(40), header),
    Ok([OpenStream(Unidirectional, Object(0))]),
  )
  assert_eq(
    publisher.receive(StreamOpened(Object(0), 50)),
    Ok([SendChunk(50, @protocol.encode_subgroup_header(header))]),
  )
  assert_eq(
    publisher.receive(StreamReset(40, 9UL)),
    Ok([
      AbortSend(40, 9UL),
      AbortSend(50, 9UL),
      EmitNotice(
        SubscriptionEnded(1, 40, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    publisher.open_object_stream(StreamId(40), header),
    Err(SubscriptionNotAccepted),
  )
  assert_eq(
    publisher.send_object(StreamId(50), client_object(0, b"late")),
    Err(UnknownStream),
  )

  assert_eq(publisher.receive(InboundStreamStarted(42, Bidirectional)), Ok([]))
  assert_eq(
    publisher.receive(
      StreamData(
        42,
        @protocol.encode_subscribe({ request_id: 3, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscriptionRequested(3, 42, track))]),
  )
  assert_eq(
    publisher.accept_subscription(StreamId(42)),
    Ok([
      SendChunk(
        42,
        @protocol.encode_subscribe_ok({
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
      EmitNotice(SubscriptionAccepted(3, 42, 1)),
    ]),
  )
  assert_eq(
    publisher.open_object_stream(StreamId(42), header),
    Ok([OpenStream(Unidirectional, Object(1))]),
  )
  assert_eq(
    publisher.receive(SendStopped(42, 5UL)),
    Ok([
      StopReceiving(42, 5UL),
      EmitNotice(
        SubscriptionEnded(3, 42, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(StreamOpened(Object(1), 52)),
    Ok([AbortSend(52, 5UL)]),
  )
}

///|
test "ClientSession stops incoming object streams when a subscription request ends" {
  let subscriber = client_session_ready()
  let track = client_track(b"video")
  let (request_id, _) = subscriber.subscribe_track(track).unwrap()
  assert_eq(request_id, RequestId(0UL))
  assert_eq(
    subscriber.receive(StreamOpened(Request(0), 30)),
    Ok([
      SendChunk(
        30,
        @protocol.encode_subscribe({ request_id: 0, track, parameters: [], }).unwrap(),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(
        30,
        @protocol.encode_subscribe_ok({
          track_alias: 10,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscribeEstablished(0, 10))]),
  )
  assert_eq(
    subscriber.receive(InboundStreamStarted(60, Unidirectional)),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(InboundStreamStarted(62, Unidirectional)),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(
        60,
        @protocol.encode_subgroup_header(client_subgroup_header(10, 7)),
      ),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(StreamReset(30, 1UL)),
    Ok([
      AbortSend(30, 1UL),
      StopReceiving(60, 1UL),
      EmitNotice(
        SubscriptionEnded(0, 30, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(
        62,
        @protocol.encode_subgroup_header(client_subgroup_header(10, 8)),
      ),
    ),
    Ok([StopReceiving(62, 1UL)]),
  )
  assert_eq(subscriber.receive(StreamData(60, b"ignored")), Ok([]))
  assert_eq(subscriber.receive(InboundStreamFinished(60)), Ok([]))
  assert_eq(subscriber.receive(InboundStreamFinished(62)), Ok([]))
}

///|
test "ClientSession releases a completed subscription alias" {
  let subscriber = client_session_ready()
  let first_track = client_track(b"first")
  let second_track = client_track(b"second")
  assert_eq(subscriber.subscribe_track(first_track).unwrap().0, 0UL)
  assert_eq(
    subscriber.receive(StreamOpened(Request(0), 30)),
    Ok([
      SendChunk(
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
      StreamData(
        30,
        @protocol.encode_subscribe_ok({
          track_alias: 10,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscribeEstablished(0, 10))]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(
        30,
        @protocol.encode_publish_done({ code: 0, stream_count: 0, reason: b"", }).unwrap(),
      ),
    ),
    Ok([
      SendFin(30, b""),
      EmitNotice(PublishDone(0, { code: 0, stream_count: 0, reason: b"", })),
    ]),
  )
  assert_eq(subscriber.receive(InboundStreamFinished(30)), Ok([]))

  assert_eq(subscriber.subscribe_track(second_track).unwrap().0, 2UL)
  assert_eq(
    subscriber.receive(StreamOpened(Request(2), 32)),
    Ok([
      SendChunk(
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
      StreamData(
        32,
        @protocol.encode_subscribe_ok({
          track_alias: 10,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscribeEstablished(2, 10))]),
  )
}

///|
test "ClientSession ends local PUBLISH subscriptions on STOP_SENDING" {
  let publisher = client_session_ready()
  let track = client_track(b"video")
  assert_eq(publisher.publish_track(track, TrackAlias(1)).unwrap().0, 0UL)
  assert_eq(
    publisher.receive(StreamOpened(Request(0), 20)),
    Ok([
      SendChunk(
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
      StreamData(
        20,
        @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
      ),
    ),
    Ok([
      EmitNotice(
        PublishAccepted(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          1UL,
        ),
      ),
    ]),
  )

  let header = client_subgroup_header(1, 7)
  assert_eq(
    publisher.open_object_stream(StreamId(20), header),
    Ok([OpenStream(Unidirectional, Object(0))]),
  )
  assert_eq(
    publisher.receive(StreamOpened(Object(0), 50)),
    Ok([SendChunk(50, @protocol.encode_subgroup_header(header))]),
  )
  assert_eq(
    publisher.receive(SendStopped(20, 4UL)),
    Ok([
      StopReceiving(20, 4UL),
      AbortSend(50, 4UL),
      EmitNotice(
        SubscriptionEnded(0, 20, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    publisher.open_object_stream(StreamId(20), header),
    Err(SubscriptionNotAccepted),
  )
  assert_eq(
    publisher.send_object(StreamId(50), client_object(0, b"late")),
    Err(UnknownStream),
  )
}

///|
test "dropping the control stream ends the session locally" {
  let client = client_session_ready()
  let actions = client.receive(StreamDropped(11, EventQueueCount)).unwrap()
  assert_true(actions.contains(CloseConnection(3, b"")))
  assert_true(actions.contains(EmitNotice(SessionEnded(Local(3, b"")))))
  assert_eq(client.state(), Closed)
}
