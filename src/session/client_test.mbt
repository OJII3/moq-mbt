///|
fn client_session_ready() -> @session.ClientSession raise {
  let client = @session.ClientSession::new(@session.Draft18, { options: [], }).unwrap()
  assert_eq(
    client.start(),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Control)]),
  )
  assert_eq(
    client.receive(@session.StreamOpened(@session.Control, 10)),
    Ok([@session.Send(10, b"\xaf\x00\x00\x00", false)]),
  )
  assert_eq(
    client.receive(@session.ControlData(11, b"\xaf\x00\x00\x00")),
    Ok([@session.Notify(@session.HandshakeComplete({ options: [], }))]),
  )
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
fn client_send_bytes(
  actions : Array[@session.ClientSessionAction],
) -> Bytes raise {
  match actions {
    [@session.Send(_, bytes, _)] => bytes
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
      @session.Send(
        22,
        @protocol.encode_publish({
          request_id: 2,
          track: track_b,
          track_alias: 2,
          parameters: [],
          properties: b"",
        }).unwrap(),
        false,
      ),
    ]),
  )
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Request(0), 20)),
    Ok([
      @session.Send(
        20,
        @protocol.encode_publish({
          request_id: 0,
          track: track_a,
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
        false,
      ),
    ]),
  )

  let publish_ok = @protocol.encode_request_ok({
    parameters: [],
    properties: b"",
  }).unwrap()
  assert_eq(
    publisher.receive(@session.RequestData(22, publish_ok)),
    Ok([@session.Notify(@session.PublishAccepted(2))]),
  )
  assert_eq(
    publisher.receive(@session.RequestData(20, publish_ok)),
    Ok([@session.Notify(@session.PublishAccepted(0))]),
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
      @session.Send(
        32,
        @protocol.encode_subscribe({
          request_id: 2,
          track: track_b,
          parameters: [],
        }).unwrap(),
        false,
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.StreamOpened(@session.Request(0), 30)),
    Ok([
      @session.Send(
        30,
        @protocol.encode_subscribe({
          request_id: 0,
          track: track_a,
          parameters: [],
        }).unwrap(),
        false,
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
  assert_eq(publisher.receive(@session.RequestStreamStarted(40)), Ok([]))
  assert_eq(
    publisher.receive(@session.RequestData(40, subscribe_a_from_relay)),
    Ok([@session.Notify(@session.SubscriptionRequested(1, 40, track_a))]),
  )
  assert_eq(publisher.receive(@session.RequestStreamStarted(42)), Ok([]))
  assert_eq(
    publisher.receive(@session.RequestData(42, subscribe_b_from_relay)),
    Ok([@session.Notify(@session.SubscriptionRequested(3, 42, track_b))]),
  )

  assert_eq(
    publisher.accept_subscription(40),
    Ok([
      @session.Send(
        40,
        @protocol.encode_subscribe_ok({
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
        false,
      ),
      @session.Notify(@session.SubscriptionAccepted(1, 40, 1)),
    ]),
  )
  assert_eq(
    publisher.accept_subscription(42),
    Ok([
      @session.Send(
        42,
        @protocol.encode_subscribe_ok({
          track_alias: 2,
          parameters: [],
          properties: b"",
        }).unwrap(),
        false,
      ),
      @session.Notify(@session.SubscriptionAccepted(3, 42, 2)),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.RequestData(
        32,
        @protocol.encode_subscribe_ok({
          track_alias: 20,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.SubscribeEstablished(2, 20))]),
  )
  assert_eq(
    subscriber.receive(
      @session.RequestData(
        30,
        @protocol.encode_subscribe_ok({
          track_alias: 10,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.SubscribeEstablished(0, 10))]),
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
    Ok([@session.Send(50, @protocol.encode_subgroup_header(header_a), false)]),
  )
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(1), 52)),
    Ok([@session.Send(52, @protocol.encode_subgroup_header(header_b), false)]),
  )
  assert_eq(subscriber.receive(@session.SubgroupStreamStarted(60)), Ok([]))
  assert_eq(subscriber.receive(@session.SubgroupStreamStarted(62)), Ok([]))
  assert_eq(
    subscriber.receive(
      @session.SubgroupData(
        60,
        @protocol.encode_subgroup_header(subscriber_header_a),
      ),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.SubgroupData(
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
    subscriber.receive(@session.SubgroupData(60, object_a0)),
    Ok([
      @session.Notify(
        @session.ObjectReceived(0, subscriber_header_a, [frame_a0]),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.SubgroupData(62, object_b0)),
    Ok([
      @session.Notify(
        @session.ObjectReceived(2, subscriber_header_b, [frame_b0]),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.SubgroupData(60, object_a1)),
    Ok([
      @session.Notify(
        @session.ObjectReceived(0, subscriber_header_a, [frame_a1]),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.SubgroupData(62, object_b1)),
    Ok([
      @session.Notify(
        @session.ObjectReceived(2, subscriber_header_b, [frame_b1]),
      ),
    ]),
  )

  assert_eq(
    publisher.finish_object_stream(50),
    Ok([@session.Send(50, b"", true)]),
  )
  assert_eq(
    publisher.finish_object_stream(52),
    Ok([@session.Send(52, b"", true)]),
  )
  for _ in 0..<3 {
    assert_eq(
      publisher.receive(@session.SendComplete(50, false)),
      Ok([@session.Notify(@session.SendFinished(50, false))]),
    )
    assert_eq(
      publisher.receive(@session.SendComplete(52, false)),
      Ok([@session.Notify(@session.SendFinished(52, false))]),
    )
  }
  assert_eq(
    publisher.receive(@session.SendComplete(50, false)),
    Ok([
      @session.Notify(@session.SendFinished(50, false)),
      @session.Notify(@session.ObjectStreamFinished(50)),
    ]),
  )
  assert_eq(
    publisher.receive(@session.SendComplete(52, false)),
    Ok([
      @session.Notify(@session.SendFinished(52, false)),
      @session.Notify(@session.ObjectStreamFinished(52)),
    ]),
  )
  assert_eq(
    publisher.publish_done(40, 3, b""),
    Ok([
      @session.Send(
        40,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
        true,
      ),
      @session.Notify(
        @session.PublishDone(1, { code: 3, stream_count: 1, reason: b"", }),
      ),
    ]),
  )
  assert_eq(
    publisher.publish_done(42, 3, b""),
    Ok([
      @session.Send(
        42,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
        true,
      ),
      @session.Notify(
        @session.PublishDone(3, { code: 3, stream_count: 1, reason: b"", }),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.RequestData(
        32,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
      ),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.RequestData(
        30,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
      ),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(@session.StreamFinished(60)),
    Ok([
      @session.Notify(@session.GroupFinished(0, 5)),
      @session.Notify(
        @session.PublishDone(0, { code: 3, stream_count: 1, reason: b"", }),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.StreamFinished(62)),
    Ok([
      @session.Notify(@session.GroupFinished(2, 7)),
      @session.Notify(
        @session.PublishDone(2, { code: 3, stream_count: 1, reason: b"", }),
      ),
    ]),
  )
}

///|
test "ClientSession validates peer request IDs and preserves SETUP trailing control bytes" {
  let client = @session.ClientSession::new(@session.Draft18, { options: [], }).unwrap()
  assert_eq(client.receive(@session.ControlData(11, b"\xaf\x00")), Ok([]))
  assert_eq(
    client.receive(
      @session.ControlData(
        11,
        b"\x00\x00" +
        @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(@session.HandshakeComplete({ options: [], })),
      @session.Notify(
        @session.ControlMessageReceived(
          @protocol.RequestOk({ parameters: [], properties: b"", }),
        ),
      ),
    ]),
  )

  let publisher = client_session_ready()
  let track = client_track(b"video")
  let (request_id, _) = publisher.publish_track(track, 1).unwrap()
  assert_eq(request_id, 0UL)
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Request(0), 20)),
    Ok([
      @session.Send(
        20,
        @protocol.encode_publish({
          request_id: 0,
          track,
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
        false,
      ),
    ]),
  )
  assert_eq(
    publisher.receive(
      @session.RequestData(
        20,
        @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.PublishAccepted(0))]),
  )
  assert_eq(publisher.receive(@session.RequestStreamStarted(44)), Ok([]))
  assert_eq(
    publisher.receive(
      @session.RequestData(
        44,
        @protocol.encode_subscribe({ request_id: 2, track, parameters: [], }).unwrap(),
      ),
    ),
    Err(@session.InvalidRequestId),
  )
  assert_eq(
    publisher.receive(
      @session.RequestData(
        44,
        @protocol.encode_subscribe({ request_id: 1, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.SubscriptionRequested(1, 44, track))]),
  )
  assert_eq(
    publisher.receive(
      @session.RequestData(
        45,
        @protocol.encode_subscribe({ request_id: 1, track, parameters: [], }).unwrap(),
      ),
    ),
    Err(@session.DuplicateRequestId),
  )
  assert_eq(publisher.receive(@session.RequestStreamStarted(46)), Ok([]))
  assert_eq(
    publisher.receive(
      @session.RequestData(
        46,
        @protocol.encode_subscribe({ request_id: 3, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.SubscriptionRequested(3, 46, track))]),
  )
  assert_eq(
    publisher.accept_subscription(44),
    Ok([
      @session.Send(
        44,
        @protocol.encode_subscribe_ok({
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
        false,
      ),
      @session.Notify(@session.SubscriptionAccepted(1, 44, 1)),
    ]),
  )
  assert_eq(
    publisher.accept_subscription(46),
    Ok([
      @session.Send(
        46,
        @protocol.encode_subscribe_ok({
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
        false,
      ),
      @session.Notify(@session.SubscriptionAccepted(3, 46, 1)),
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
      @session.Send(
        20,
        @protocol.encode_publish({
          request_id: 0,
          track,
          track_alias: 1,
          parameters: [],
          properties: b"",
        }).unwrap(),
        false,
      ),
    ]),
  )
  assert_eq(
    publisher.receive(
      @session.RequestData(
        20,
        @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.PublishAccepted(0))]),
  )

  let header = client_subgroup_header(1, 7)
  assert_eq(
    publisher.open_object_stream(20, header),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Object(0))]),
  )
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(0), 22)),
    Ok([@session.Send(22, @protocol.encode_subgroup_header(header), false)]),
  )
  assert_true(
    match publisher.send_object(22, client_object(0, b"payload")) {
      Ok([@session.Send(22, bytes, false)]) => !bytes.is_empty()
      _ => false
    },
  )
  assert_eq(
    publisher.finish_object_stream(22),
    Ok([@session.Send(22, b"", true)]),
  )
  assert_eq(
    publisher.receive(@session.SendComplete(22, false)),
    Ok([@session.Notify(@session.SendFinished(22, false))]),
  )
  assert_eq(
    publisher.receive(@session.SendComplete(22, false)),
    Ok([@session.Notify(@session.SendFinished(22, false))]),
  )
  assert_eq(
    publisher.receive(@session.SendComplete(22, false)),
    Ok([
      @session.Notify(@session.SendFinished(22, false)),
      @session.Notify(@session.ObjectStreamFinished(22)),
    ]),
  )
  assert_eq(
    publisher.publish_done(20, 3, b""),
    Ok([
      @session.Send(
        20,
        @protocol.encode_publish_done({ code: 3, stream_count: 1, reason: b"", }).unwrap(),
        true,
      ),
      @session.Notify(
        @session.PublishDone(0, { code: 3, stream_count: 1, reason: b"", }),
      ),
    ]),
  )
}
