///|
test "cancellation before Request stream open closes the delayed stream without sending the request" {
  let subscriber = client_session_ready()
  let (request_id, _) = subscriber
    .subscribe_track(client_track(b"video"))
    .unwrap()
  assert_eq(request_id, 0UL)
  assert_eq(subscriber.cancel_request(request_id), Ok([]))
  assert_eq(subscriber.cancel_request(request_id), Ok([]))
  assert_eq(
    subscriber.receive(@session.StreamOpened(@session.Request(request_id), 30)),
    Ok([
      @session.AbortSend(30, 1UL),
      @session.StopReceiving(30, 1UL),
      @session.Notify(@session.SubscriptionEnded(request_id, 30)),
    ]),
  )
  assert_eq(subscriber.receive(@session.StreamData(30, b"late")), Ok([]))
  assert_eq(subscriber.receive(@session.SendComplete(30, false)), Ok([]))
  assert_eq(subscriber.receive(@session.StreamReset(30, 1UL)), Ok([]))
}

///|
test "cancellation closes pending local and peer requests before acceptance" {
  let subscriber = client_session_ready()
  let (local_request_id, _) = subscriber
    .subscribe_track(client_track(b"video"))
    .unwrap()
  assert_eq(
    subscriber.receive(
      @session.StreamOpened(@session.Request(local_request_id), 30),
    ),
    Ok([
      @session.Send(
        30,
        @protocol.encode_subscribe({
          request_id: local_request_id,
          track: client_track(b"video"),
          parameters: [],
        }).unwrap(),
        false,
      ),
    ]),
  )
  assert_eq(
    subscriber.cancel_request(local_request_id),
    Ok([
      @session.AbortSend(30, 1UL),
      @session.StopReceiving(30, 1UL),
      @session.Notify(@session.SubscriptionEnded(local_request_id, 30)),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        30,
        @protocol.encode_subscribe_ok({
          track_alias: 1UL,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(31, @session.Unidirectional),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        31,
        @protocol.encode_subgroup_header(subscriber_header(1, true)),
      ),
    ),
    Ok([@session.StopReceiving(31, 1UL)]),
  )
  assert_eq(subscriber.receive(@session.StreamFinished(31)), Ok([]))

  let publisher = publisher_announced()
  let track = publisher_track()
  assert_eq(
    publisher.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
    Ok([]),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.SubscriptionRequested(1UL, 40, track))]),
  )
  assert_eq(
    publisher.cancel_request(1UL),
    Ok([
      @session.AbortSend(40, 1UL),
      @session.StopReceiving(40, 1UL),
      @session.Notify(@session.SubscriptionEnded(1UL, 40)),
    ]),
  )
  assert_eq(publisher.accept_subscription(40), Err(@session.UnexpectedMessage))
}

///|
test "cancellation stops active and late incoming Object streams once" {
  let subscriber = subscriber_started()
  for stream in [40L, 41L] {
    assert_eq(
      subscriber.receive(
        @session.InboundStreamStarted(stream, @session.Unidirectional),
      ),
      Ok([]),
    )
  }
  let header = subscriber_header(1, true)
  assert_eq(
    subscriber.receive(
      @session.StreamData(40, @protocol.encode_subgroup_header(header)),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.cancel_request(0UL),
    Ok([
      @session.AbortSend(30, 1UL),
      @session.StopReceiving(30, 1UL),
      @session.StopReceiving(40, 1UL),
      @session.Notify(@session.SubscriptionEnded(0, 30)),
    ]),
  )
  assert_eq(subscriber.cancel_request(0UL), Ok([]))
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        40,
        @protocol.encode_subgroup_stream(header, [subscriber_frame(0, b"late")]).unwrap(),
      ),
    ),
    Ok([]),
  )
  assert_eq(subscriber.receive(@session.StreamFinished(40)), Ok([]))
  assert_eq(
    subscriber.receive(
      @session.StreamData(41, @protocol.encode_subgroup_header(header)),
    ),
    Ok([@session.StopReceiving(41, 1UL)]),
  )
  assert_eq(subscriber.receive(@session.StreamFinished(41)), Ok([]))
  assert_eq(subscriber.receive(@session.StreamReset(30, 1UL)), Ok([]))
}

///|
test "cancellation of a peer SUBSCRIBE is idempotent and leaves its PUBLISH active" {
  let publisher = publisher_announced()
  let track = publisher_track()
  let header = publisher_header(1, false)
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
    Ok([@session.Notify(@session.SubscriptionRequested(1, 40, track))]),
  )
  ignore(publisher.accept_subscription(40).unwrap())
  let token = match publisher.open_object_stream(40, header).unwrap() {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("expected object stream request")
  }
  ignore(
    publisher
    .receive(@session.StreamOpened(@session.Object(token), 50))
    .unwrap(),
  )
  let pending_token = match publisher.open_object_stream(40, header).unwrap() {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("expected pending object stream request")
  }
  assert_eq(
    publisher.cancel_request(1UL),
    Ok([
      @session.AbortSend(40, 1UL),
      @session.StopReceiving(40, 1UL),
      @session.AbortSend(50, 1UL),
      @session.Notify(@session.SubscriptionEnded(1, 40)),
    ]),
  )
  assert_eq(publisher.cancel_request(1UL), Ok([]))
  assert_eq(publisher.receive(@session.SendStopped(40, 1UL)), Ok([]))
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(pending_token), 51)),
    Ok([@session.AbortSend(51, 1UL)]),
  )
  assert_eq(
    publisher.open_object_stream(40, header),
    Err(@session.SubscriptionNotAccepted),
  )
  assert_eq(
    publisher.open_object_stream(20, header),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Object(2))]),
  )
}

///|
fn cancellation_publish_track(
  publisher : @session.ClientSession,
  track : @protocol.TrackName,
  track_alias : UInt64,
  stream : Int64,
) -> UInt64 raise {
  let (request_id, _) = publisher.publish_track(track, track_alias).unwrap()
  ignore(
    publisher
    .receive(@session.StreamOpened(@session.Request(request_id), stream))
    .unwrap(),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(
        stream,
        @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.PublishAccepted(request_id))]),
  )
  request_id
}

///|
fn cancellation_accept_peer_subscription(
  publisher : @session.ClientSession,
  request_id : UInt64,
  stream : Int64,
  track : @protocol.TrackName,
  track_alias : UInt64,
) -> Unit raise {
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
      @session.Notify(@session.SubscriptionRequested(request_id, stream, track)),
    ]),
  )
  assert_eq(
    publisher.accept_subscription(stream),
    Ok([
      @session.Send(
        stream,
        @protocol.encode_subscribe_ok({
          track_alias,
          parameters: [],
          properties: b"",
        }).unwrap(),
        false,
      ),
      @session.Notify(
        @session.SubscriptionAccepted(request_id, stream, track_alias),
      ),
    ]),
  )
}

///|
test "cancelling a local PUBLISH tears down its subscriptions but preserves other tracks" {
  let publisher = client_session_ready()
  let first_track = client_track(b"first")
  let second_track = client_track(b"second")
  let first_request = cancellation_publish_track(
    publisher, first_track, 1UL, 20,
  )
  ignore(cancellation_publish_track(publisher, second_track, 2UL, 22))
  cancellation_accept_peer_subscription(publisher, 1UL, 40, first_track, 1UL)
  cancellation_accept_peer_subscription(publisher, 3UL, 42, second_track, 2UL)
  let first_header = client_subgroup_header(1UL, 1UL)
  let token = match publisher.open_object_stream(40, first_header).unwrap() {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("expected object stream request")
  }
  ignore(
    publisher
    .receive(@session.StreamOpened(@session.Object(token), 50))
    .unwrap(),
  )
  assert_eq(
    publisher.cancel_request(first_request),
    Ok([
      @session.AbortSend(20, 1UL),
      @session.StopReceiving(20, 1UL),
      @session.AbortSend(40, 1UL),
      @session.StopReceiving(40, 1UL),
      @session.AbortSend(50, 1UL),
      @session.Notify(@session.SubscriptionEnded(1UL, 40)),
      @session.Notify(@session.SubscriptionEnded(first_request, 20)),
    ]),
  )
  let second_header = client_subgroup_header(2UL, 2UL)
  let second_token = match
    publisher.open_object_stream(42, second_header).unwrap() {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("unrelated track subscription should remain active")
  }
  ignore(
    publisher
    .receive(@session.StreamOpened(@session.Object(second_token), 52))
    .unwrap(),
  )
  assert_true(
    publisher.send_object(52, client_object(0UL, b"still active")) is Ok(_),
  )
}
