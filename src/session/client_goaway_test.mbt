///|
fn client_goaway_control(
  request_id : UInt64,
  timeout_ms : UInt64,
  uri : Bytes,
) -> Bytes {
  @protocol.encode_goaway_control({ uri, timeout_ms, request_id, }).unwrap()
}

///|
test "client control GOAWAY sends an empty URI and rejects late peer requests" {
  let publisher = client_session_ready()
  let track = client_track(b"video")
  let (publish_id, _) = publisher.publish_track(track, TrackAlias(1)).unwrap()
  assert_eq(
    publisher.receive(StreamOpened(Request(publish_id), 20)),
    Ok([
      SendChunk(
        20,
        @protocol.encode_publish({
          request_id: publish_id.to_uint64(),
          track,
          track_alias: 1UL,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(SendCompleted(20, Sent)),
    Ok([EmitNotice(StreamSendFinished(20, Sent))]),
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
          publish_id,
          { namespace_fields: [b"demo"], name: b"video", },
          1UL,
        ),
      ),
    ]),
  )
  assert_eq(publisher.receive(InboundStreamStarted(30, Bidirectional)), Ok([]))
  assert_eq(
    publisher.receive(
      StreamData(
        30,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscriptionRequested(1UL, 30, track))]),
  )
  assert_eq(
    publisher.accept_subscription(StreamId(30)),
    Ok([
      SendChunk(
        30,
        @protocol.encode_subscribe_ok({
          track_alias: 1UL,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
      EmitNotice(SubscriptionAccepted(1UL, 30, 1UL)),
    ]),
  )
  assert_eq(
    publisher.receive(SendCompleted(30, Sent)),
    Ok([EmitNotice(StreamSendFinished(30, Sent))]),
  )
  assert_eq(
    publisher.send_goaway(0UL),
    Ok([SendChunk(10, client_goaway_control(3UL, 0UL, b""))]),
  )
  assert_eq(publisher.state(), Draining)

  assert_eq(publisher.receive(InboundStreamStarted(31, Bidirectional)), Ok([]))
  let going_away = @protocol.RequestErrorMessage::{
    code: 6UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.receive(
      StreamData(
        31,
        @protocol.encode_subscribe({ request_id: 3UL, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([SendFin(31, @protocol.encode_request_error(going_away).unwrap())]),
  )
  assert_eq(
    publisher.receive(SendCompleted(31, Sent)),
    Ok([EmitNotice(StreamSendFinished(31, Sent))]),
  )

  let done = @protocol.PublishDoneMessage::{
    code: 3UL,
    stream_count: 0UL,
    reason: b"",
  }
  assert_eq(
    publisher.publish_done(StreamId(30), 3UL, b""),
    Ok([
      SendFin(30, @protocol.encode_publish_done(done).unwrap()),
      EmitNotice(PublishDone(1UL, done)),
    ]),
  )
  assert_eq(publisher.state(), Draining)
  assert_eq(
    publisher.cancel_request(publish_id),
    Ok([
      AbortSend(20, 1UL),
      StopReceiving(20, 1UL),
      EmitNotice(
        SubscriptionEnded(publish_id, 20, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(SendCompleted(30, Sent)),
    Ok([EmitNotice(StreamSendFinished(30, Sent))]),
  )
  assert_eq(publisher.state(), Draining)
  assert_eq(
    publisher.receive(SendCompleted(10, Sent)),
    Ok([
      EmitNotice(StreamSendFinished(10, Sent)),
      CloseConnection(0UL, b""),
      EmitNotice(SessionEnded(Local(0UL, b""))),
    ]),
  )
  assert_eq(publisher.state(), Closed)
}

///|
test "peer REQUEST_UPDATE after control GOAWAY is rejected and ends that subscription" {
  let publisher = client_session_ready()
  let track = client_track(b"video")
  let (publish_id, _) = publisher.publish_track(track, TrackAlias(1UL)).unwrap()
  assert_eq(
    publisher.receive(StreamOpened(Request(publish_id), 20)),
    Ok([
      SendChunk(
        20,
        @protocol.encode_publish({
          request_id: publish_id.to_uint64(),
          track,
          track_alias: 1UL,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ]),
  )
  ignore(publisher.receive(SendCompleted(20, Sent)))
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
          publish_id,
          { namespace_fields: [b"demo"], name: b"video", },
          1UL,
        ),
      ),
    ]),
  )
  assert_eq(publisher.receive(InboundStreamStarted(30, Bidirectional)), Ok([]))
  assert_eq(
    publisher.receive(
      StreamData(
        30,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscriptionRequested(1UL, 30, track))]),
  )
  ignore(publisher.accept_subscription(StreamId(30)).unwrap())
  ignore(publisher.receive(SendCompleted(30, Sent)))
  assert_eq(
    publisher.send_goaway(0UL),
    Ok([SendChunk(10, client_goaway_control(3UL, 0UL, b""))]),
  )
  let error = @protocol.RequestErrorMessage::{
    code: 6UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  let done = @protocol.PublishDoneMessage::{
    code: 8UL,
    stream_count: 0UL,
    reason: b"",
  }
  assert_eq(
    publisher.receive(
      StreamData(
        30,
        @protocol.encode_request_update({ request_id: 3UL, parameters: [], }).unwrap(),
      ),
    ),
    Ok([
      SendChunk(30, @protocol.encode_request_error(error).unwrap()),
      EmitNotice(SubscriptionUpdateRejected(1UL, 3UL, error)),
      SendFin(30, @protocol.encode_publish_done(done).unwrap()),
      EmitNotice(PublishDone(1UL, done)),
    ]),
  )
  assert_eq(
    publisher.cancel_request(publish_id),
    Ok([
      AbortSend(20, 1UL),
      StopReceiving(20, 1UL),
      EmitNotice(
        SubscriptionEnded(publish_id, 20, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(SendCompleted(30, Sent)),
    Ok([EmitNotice(StreamSendFinished(30, Sent))]),
  )
  assert_eq(publisher.state(), Draining)
  assert_eq(
    publisher.receive(SendCompleted(30, Sent)),
    Ok([EmitNotice(StreamSendFinished(30, Sent))]),
  )
  assert_eq(
    publisher.receive(SendCompleted(10, Sent)),
    Ok([
      EmitNotice(StreamSendFinished(10, Sent)),
      CloseConnection(0UL, b""),
      EmitNotice(SessionEnded(Local(0UL, b""))),
    ]),
  )
}

///|
test "received control GOAWAY rejects pending boundary requests and blocks new requests" {
  let subscriber = client_session_ready()
  let track = client_track(b"video")
  let (before, _) = subscriber.subscribe_track(track).unwrap()
  let (at_boundary, _) = subscriber
    .subscribe_track(client_track(b"audio"))
    .unwrap()
  assert_eq((before, at_boundary), (0UL, 2UL))

  let going_away = @protocol.RequestErrorMessage::{
    code: 6UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    subscriber.receive(
      StreamData(11, client_goaway_control(2UL, 0UL, b"moqt://next")),
    ),
    Ok([
      EmitNotice(
        ControlMessageReceived(
          GoAwayControl({
            uri: b"moqt://next",
            timeout_ms: 0UL,
            request_id: 2UL,
          }),
        ),
      ),
      EmitNotice(ControlGoAwayReceived(b"moqt://next", 0UL, 2UL)),
      EmitNotice(SubscribeRejected(at_boundary, going_away)),
    ]),
  )
  assert_eq(subscriber.state(), Draining)
  assert_eq(
    subscriber.subscribe_track(client_track(b"captions")),
    Err(SessionDraining),
  )
  assert_eq(
    subscriber.publish_track(client_track(b"camera"), TrackAlias(9UL)),
    Err(SessionDraining),
  )
  assert_eq(
    subscriber.receive(StreamOpened(Request(at_boundary), 32)),
    Ok([AbortSend(32, 4UL), StopReceiving(32, 4UL)]),
  )
  assert_eq(
    subscriber.cancel_request(before),
    Ok([CloseConnection(0UL, b""), EmitNotice(SessionEnded(Local(0UL, b"")))]),
  )
  assert_eq(subscriber.state(), Closed)
}

///|
test "GOAWAY request ID parity and duplicate control GOAWAY close the session" {
  let invalid = client_session_ready()
  assert_eq(
    invalid.receive(StreamData(11, client_goaway_control(1UL, 0UL, b""))),
    Ok([CloseConnection(4UL, b""), EmitNotice(SessionEnded(Local(4UL, b"")))]),
  )

  let duplicate = client_session_ready()
  let (_, _) = duplicate.subscribe_track(client_track(b"video")).unwrap()
  let goaway = client_goaway_control(2UL, 0UL, b"")
  assert_eq(
    duplicate.receive(StreamData(11, goaway)),
    Ok([
      EmitNotice(
        ControlMessageReceived(
          GoAwayControl({ uri: b"", timeout_ms: 0UL, request_id: 2UL, }),
        ),
      ),
      EmitNotice(ControlGoAwayReceived(b"", 0UL, 2UL)),
    ]),
  )
  assert_eq(
    duplicate.receive(StreamData(11, goaway)),
    Ok([CloseConnection(3UL, b""), EmitNotice(SessionEnded(Local(3UL, b"")))]),
  )
}

///|
test "request GOAWAY is scoped to its stream and resets after its deadline" {
  let subscriber = client_session_ready()
  let (request_id, _) = subscriber
    .subscribe_track(client_track(b"video"))
    .unwrap()
  assert_eq(
    subscriber.receive(StreamOpened(Request(request_id), 20)),
    Ok([
      SendChunk(
        20,
        @protocol.encode_subscribe({
          request_id: request_id.to_uint64(),
          track: client_track(b"video"),
          parameters: [],
        }).unwrap(),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(SendCompleted(20, Sent)),
    Ok([EmitNotice(StreamSendFinished(20, Sent))]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(
        20,
        @protocol.encode_subscribe_ok({
          track_alias: 1UL,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscribeEstablished(request_id, 1UL))]),
  )
  assert_eq(subscriber.receive(Clock(100UL)), Ok([]))
  let request_goaway = @protocol.GoAwayRequestMessage::{
    uri: b"moqt://next",
    timeout_ms: 10UL,
  }
  let request_goaway_wire = @protocol.encode_goaway_request(request_goaway).unwrap()
  assert_eq(
    subscriber.receive(StreamData(20, request_goaway_wire)),
    Ok([EmitNotice(RequestGoAwayReceived(request_id, 20, b"moqt://next", 10UL))]),
  )
  assert_eq(subscriber.state(), Active)
  assert_eq(subscriber.receive(Clock(109UL)), Ok([]))
  assert_eq(
    subscriber.receive(Clock(110UL)),
    Ok([
      AbortSend(20, 4UL),
      StopReceiving(20, 4UL),
      EmitNotice(
        SubscriptionEnded(request_id, 20, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(subscriber.state(), Active)
}

///|
test "request GOAWAY timeout zero does not reset and duplicate GOAWAY is a violation" {
  let subscriber = client_session_ready()
  let (request_id, _) = subscriber
    .subscribe_track(client_track(b"video"))
    .unwrap()
  assert_eq(
    subscriber.receive(StreamOpened(Request(request_id), 20)),
    Ok([
      SendChunk(
        20,
        @protocol.encode_subscribe({
          request_id: request_id.to_uint64(),
          track: client_track(b"video"),
          parameters: [],
        }).unwrap(),
      ),
    ]),
  )
  let goaway = @protocol.encode_goaway_request({ uri: b"", timeout_ms: 0UL, }).unwrap()
  assert_eq(
    subscriber.receive(StreamData(20, goaway)),
    Ok([EmitNotice(RequestGoAwayReceived(request_id, 20, b"", 0UL))]),
  )
  assert_eq(subscriber.receive(Clock(100000UL)), Ok([]))
  assert_eq(subscriber.state(), Active)
  assert_eq(
    subscriber.receive(StreamData(20, goaway)),
    Ok([CloseConnection(3UL, b""), EmitNotice(SessionEnded(Local(3UL, b"")))]),
  )
}

///|
test "drain closes after rejecting a local request that has not opened" {
  let subscriber = client_session_ready()
  let (request_id, _) = subscriber
    .subscribe_track(client_track(b"video"))
    .unwrap()
  assert_eq(
    subscriber.receive(StreamData(11, client_goaway_control(0UL, 0UL, b""))),
    Ok([
      EmitNotice(
        ControlMessageReceived(
          GoAwayControl({ uri: b"", timeout_ms: 0UL, request_id: 0UL, }),
        ),
      ),
      EmitNotice(ControlGoAwayReceived(b"", 0UL, 0UL)),
      EmitNotice(
        SubscribeRejected(request_id, {
          code: 6UL,
          retry_interval: 0UL,
          reason: b"",
          redirect: b"",
        }),
      ),
      CloseConnection(0UL, b""),
      EmitNotice(SessionEnded(Local(0UL, b""))),
    ]),
  )
  assert_eq(subscriber.state(), Closed)
  assert_eq(subscriber.receive(StreamOpened(Request(request_id), 20)), Ok([]))
}

///|
test "zero GOAWAY timeout does not expire a request and drain waits for sends" {
  let subscriber = client_session_ready()
  let (request_id, _) = subscriber
    .subscribe_track(client_track(b"video"))
    .unwrap()
  assert_eq(
    subscriber.send_goaway(0UL),
    Ok([SendChunk(10, client_goaway_control(1UL, 0UL, b""))]),
  )
  assert_eq(subscriber.receive(Clock(100000UL)), Ok([]))
  assert_eq(subscriber.state(), Draining)
  assert_eq(subscriber.cancel_request(request_id), Ok([]))
  assert_eq(subscriber.state(), Draining)
  assert_eq(
    subscriber.receive(SendCompleted(10, Sent)),
    Ok([
      EmitNotice(StreamSendFinished(10, Sent)),
      CloseConnection(0UL, b""),
      EmitNotice(SessionEnded(Local(0UL, b""))),
    ]),
  )
  assert_eq(subscriber.state(), Closed)
}

///|
test "sent control GOAWAY times out while requests remain" {
  let publisher = client_session_ready()
  let (_, _) = publisher
    .publish_track(client_track(b"video"), TrackAlias(1UL))
    .unwrap()
  assert_eq(
    publisher.send_goaway(10UL),
    Ok([SendChunk(10, client_goaway_control(1UL, 10UL, b""))]),
  )
  assert_eq(
    publisher.receive(Clock(10UL)),
    Ok([
      CloseConnection(0x10UL, b""),
      EmitNotice(SessionEnded(Local(0x10UL, b""))),
    ]),
  )
  assert_eq(publisher.state(), Closed)
}

///|
test "existing object delivery continues during session drain" {
  let subscriber = client_session_ready()
  let track = client_track(b"video")
  let (request_id, _) = subscriber.subscribe_track(track).unwrap()
  assert_eq(
    subscriber.receive(StreamOpened(Request(request_id), 20)),
    Ok([
      SendChunk(
        20,
        @protocol.encode_subscribe({
          request_id: request_id.to_uint64(),
          track,
          parameters: [],
        }).unwrap(),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(SendCompleted(20, Sent)),
    Ok([EmitNotice(StreamSendFinished(20, Sent))]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(
        20,
        @protocol.encode_subscribe_ok({
          track_alias: 1UL,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscribeEstablished(request_id, 1UL))]),
  )
  assert_eq(
    subscriber.receive(StreamData(11, client_goaway_control(2UL, 0UL, b""))),
    Ok([
      EmitNotice(
        ControlMessageReceived(
          GoAwayControl({ uri: b"", timeout_ms: 0UL, request_id: 2UL, }),
        ),
      ),
      EmitNotice(ControlGoAwayReceived(b"", 0UL, 2UL)),
    ]),
  )
  let header = client_subgroup_header(1UL, 9UL)
  let frame = client_object(0UL, b"payload")
  assert_eq(
    subscriber.receive(InboundStreamStarted(41, Unidirectional)),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(41, @protocol.encode_subgroup_stream(header, [frame]).unwrap()),
    ),
    Ok([
      EmitNotice(
        ObjectReceived(
          request_id,
          { namespace_fields: [b"demo"], name: b"video", },
          header,
          [frame],
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(InboundStreamFinished(41)),
    Ok([EmitNotice(GroupFinished(request_id, 9UL))]),
  )
  assert_eq(subscriber.state(), Draining)
}

///|
test "peer and transport close notifications are one-shot" {
  let peer_closed = client_session_ready()
  assert_eq(
    peer_closed.receive(PeerClosed(7UL)),
    Ok([EmitNotice(SessionEnded(Peer(7UL)))]),
  )
  assert_eq(peer_closed.receive(PeerClosed(7UL)), Ok([]))
  assert_eq(peer_closed.close(0UL, b""), [])
  assert_eq(
    peer_closed.subscribe_track(client_track(b"video")),
    Err(SessionClosed),
  )

  let transport_failed = client_session_ready()
  assert_eq(
    transport_failed.receive(TransportFailed(-1L)),
    Ok([EmitNotice(SessionEnded(Transport(-1L)))]),
  )

  let locally_closed = client_session_ready()
  assert_eq(locally_closed.close(9UL, b"done"), [
    CloseConnection(9UL, b"done"),
    EmitNotice(SessionEnded(Local(9UL, b"done"))),
  ])
  assert_eq(locally_closed.close(10UL, b"ignored"), [])
}

///|
test "aborting a request releases its pending sends for drain" {
  let subscriber = subscriber_started()
  assert_eq(
    subscriber.send_goaway(0UL),
    Ok([SendChunk(10, client_goaway_control(1UL, 0UL, b""))]),
  )
  ignore(subscriber.update_subscription(0UL, [forward_parameter(0)]).unwrap())
  assert_eq(
    subscriber.cancel_request(0UL),
    Ok([
      AbortSend(30, 1UL),
      StopReceiving(30, 1UL),
      EmitNotice(SubscriptionEnded(0, 30, subscriber_track())),
    ]),
  )
  assert_eq(subscriber.state(), Draining)
  assert_eq(
    subscriber.receive(SendCompleted(10, Sent)),
    Ok([
      EmitNotice(StreamSendFinished(10, Sent)),
      CloseConnection(0UL, b""),
      EmitNotice(SessionEnded(Local(0UL, b""))),
    ]),
  )
  assert_eq(subscriber.state(), Closed)
}
