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
  let (publish_id, _) = publisher.publish_track(track, 1).unwrap()
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Request(publish_id), 20)),
    Ok([
      @session.Send(
        20,
        @protocol.encode_publish({
          request_id: publish_id,
          track,
          track_alias: 1UL,
          parameters: [],
          properties: b"",
        }).unwrap(),
        false,
      ),
    ]),
  )
  assert_eq(
    publisher.receive(@session.SendComplete(20, false)),
    Ok([@session.Notify(@session.SendFinished(20, false))]),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(
        20,
        @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(
        @session.PublishAccepted(
          publish_id,
          { namespace_fields: [b"demo"], name: b"video", },
          1UL,
        ),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(@session.InboundStreamStarted(30, @session.Bidirectional)),
    Ok([]),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(
        30,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.SubscriptionRequested(1UL, 30, track))]),
  )
  assert_eq(
    publisher.accept_subscription(30),
    Ok([
      @session.Send(
        30,
        @protocol.encode_subscribe_ok({
          track_alias: 1UL,
          parameters: [],
          properties: b"",
        }).unwrap(),
        false,
      ),
      @session.Notify(@session.SubscriptionAccepted(1UL, 30, 1UL)),
    ]),
  )
  assert_eq(
    publisher.receive(@session.SendComplete(30, false)),
    Ok([@session.Notify(@session.SendFinished(30, false))]),
  )
  assert_eq(
    publisher.send_goaway(0UL),
    Ok([@session.Send(10, client_goaway_control(3UL, 0UL, b""), false)]),
  )
  assert_eq(publisher.state(), @session.Draining)

  assert_eq(
    publisher.receive(@session.InboundStreamStarted(31, @session.Bidirectional)),
    Ok([]),
  )
  let going_away : @protocol.RequestErrorMessage = {
    code: 6UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.receive(
      @session.StreamData(
        31,
        @protocol.encode_subscribe({ request_id: 3UL, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([
      @session.Send(
        31,
        @protocol.encode_request_error(going_away).unwrap(),
        true,
      ),
    ]),
  )
  assert_eq(
    publisher.receive(@session.SendComplete(31, false)),
    Ok([@session.Notify(@session.SendFinished(31, false))]),
  )

  let done : @protocol.PublishDoneMessage = {
    code: 3UL,
    stream_count: 0UL,
    reason: b"",
  }
  assert_eq(
    publisher.publish_done(30, 3UL, b""),
    Ok([
      @session.Send(30, @protocol.encode_publish_done(done).unwrap(), true),
      @session.Notify(@session.PublishDone(1UL, done)),
    ]),
  )
  assert_eq(publisher.state(), @session.Draining)
  assert_eq(
    publisher.cancel_request(publish_id),
    Ok([
      @session.AbortSend(20, 1UL),
      @session.StopReceiving(20, 1UL),
      @session.Notify(
        @session.SubscriptionEnded(publish_id, 20, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(@session.SendComplete(30, false)),
    Ok([@session.Notify(@session.SendFinished(30, false))]),
  )
  assert_eq(publisher.state(), @session.Draining)
  assert_eq(
    publisher.receive(@session.SendComplete(10, false)),
    Ok([
      @session.Notify(@session.SendFinished(10, false)),
      @session.CloseConnection(0UL, b""),
      @session.Notify(@session.SessionEnded(@session.Local(0UL, b""))),
    ]),
  )
  assert_eq(publisher.state(), @session.Closed)
}

///|
test "peer REQUEST_UPDATE after control GOAWAY is rejected and ends that subscription" {
  let publisher = client_session_ready()
  let track = client_track(b"video")
  let (publish_id, _) = publisher.publish_track(track, 1UL).unwrap()
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Request(publish_id), 20)),
    Ok([
      @session.Send(
        20,
        @protocol.encode_publish({
          request_id: publish_id,
          track,
          track_alias: 1UL,
          parameters: [],
          properties: b"",
        }).unwrap(),
        false,
      ),
    ]),
  )
  ignore(publisher.receive(@session.SendComplete(20, false)))
  assert_eq(
    publisher.receive(
      @session.StreamData(
        20,
        @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(
        @session.PublishAccepted(
          publish_id,
          { namespace_fields: [b"demo"], name: b"video", },
          1UL,
        ),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(@session.InboundStreamStarted(30, @session.Bidirectional)),
    Ok([]),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(
        30,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.SubscriptionRequested(1UL, 30, track))]),
  )
  ignore(publisher.accept_subscription(30).unwrap())
  ignore(publisher.receive(@session.SendComplete(30, false)))
  assert_eq(
    publisher.send_goaway(0UL),
    Ok([@session.Send(10, client_goaway_control(3UL, 0UL, b""), false)]),
  )
  let error : @protocol.RequestErrorMessage = {
    code: 6UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  let done : @protocol.PublishDoneMessage = {
    code: 8UL,
    stream_count: 0UL,
    reason: b"",
  }
  assert_eq(
    publisher.receive(
      @session.StreamData(
        30,
        @protocol.encode_request_update({ request_id: 3UL, parameters: [], }).unwrap(),
      ),
    ),
    Ok([
      @session.Send(30, @protocol.encode_request_error(error).unwrap(), false),
      @session.Notify(@session.SubscriptionUpdateRejected(1UL, 3UL, error)),
      @session.Send(30, @protocol.encode_publish_done(done).unwrap(), true),
      @session.Notify(@session.PublishDone(1UL, done)),
    ]),
  )
  assert_eq(
    publisher.cancel_request(publish_id),
    Ok([
      @session.AbortSend(20, 1UL),
      @session.StopReceiving(20, 1UL),
      @session.Notify(
        @session.SubscriptionEnded(publish_id, 20, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(@session.SendComplete(30, false)),
    Ok([@session.Notify(@session.SendFinished(30, false))]),
  )
  assert_eq(publisher.state(), @session.Draining)
  assert_eq(
    publisher.receive(@session.SendComplete(30, false)),
    Ok([@session.Notify(@session.SendFinished(30, false))]),
  )
  assert_eq(
    publisher.receive(@session.SendComplete(10, false)),
    Ok([
      @session.Notify(@session.SendFinished(10, false)),
      @session.CloseConnection(0UL, b""),
      @session.Notify(@session.SessionEnded(@session.Local(0UL, b""))),
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

  let going_away : @protocol.RequestErrorMessage = {
    code: 6UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    subscriber.receive(
      @session.StreamData(11, client_goaway_control(2UL, 0UL, b"moqt://next")),
    ),
    Ok([
      @session.Notify(
        @session.ControlMessageReceived(
          @protocol.GoAwayControl({
            uri: b"moqt://next",
            timeout_ms: 0UL,
            request_id: 2UL,
          }),
        ),
      ),
      @session.Notify(@session.ControlGoAwayReceived(b"moqt://next", 0UL, 2UL)),
      @session.Notify(@session.SubscribeRejected(at_boundary, going_away)),
    ]),
  )
  assert_eq(subscriber.state(), @session.Draining)
  assert_eq(
    subscriber.subscribe_track(client_track(b"captions")),
    Err(@session.SessionDraining),
  )
  assert_eq(
    subscriber.publish_track(client_track(b"camera"), 9UL),
    Err(@session.SessionDraining),
  )
  assert_eq(
    subscriber.receive(@session.StreamOpened(@session.Request(at_boundary), 32)),
    Ok([@session.AbortSend(32, 4UL), @session.StopReceiving(32, 4UL)]),
  )
  assert_eq(
    subscriber.cancel_request(before),
    Ok([
      @session.CloseConnection(0UL, b""),
      @session.Notify(@session.SessionEnded(@session.Local(0UL, b""))),
    ]),
  )
  assert_eq(subscriber.state(), @session.Closed)
}

///|
test "GOAWAY request ID parity and duplicate control GOAWAY close the session" {
  let invalid = client_session_ready()
  assert_eq(
    invalid.receive(
      @session.StreamData(11, client_goaway_control(1UL, 0UL, b"")),
    ),
    Ok([
      @session.CloseConnection(4UL, b""),
      @session.Notify(@session.SessionEnded(@session.Local(4UL, b""))),
    ]),
  )

  let duplicate = client_session_ready()
  let (_, _) = duplicate.subscribe_track(client_track(b"video")).unwrap()
  let goaway = client_goaway_control(2UL, 0UL, b"")
  assert_eq(
    duplicate.receive(@session.StreamData(11, goaway)),
    Ok([
      @session.Notify(
        @session.ControlMessageReceived(
          @protocol.GoAwayControl({
            uri: b"",
            timeout_ms: 0UL,
            request_id: 2UL,
          }),
        ),
      ),
      @session.Notify(@session.ControlGoAwayReceived(b"", 0UL, 2UL)),
    ]),
  )
  assert_eq(
    duplicate.receive(@session.StreamData(11, goaway)),
    Ok([
      @session.CloseConnection(3UL, b""),
      @session.Notify(@session.SessionEnded(@session.Local(3UL, b""))),
    ]),
  )
}

///|
test "request GOAWAY is scoped to its stream and resets after its deadline" {
  let subscriber = client_session_ready()
  let (request_id, _) = subscriber
    .subscribe_track(client_track(b"video"))
    .unwrap()
  assert_eq(
    subscriber.receive(@session.StreamOpened(@session.Request(request_id), 20)),
    Ok([
      @session.Send(
        20,
        @protocol.encode_subscribe({
          request_id,
          track: client_track(b"video"),
          parameters: [],
        }).unwrap(),
        false,
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.SendComplete(20, false)),
    Ok([@session.Notify(@session.SendFinished(20, false))]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        20,
        @protocol.encode_subscribe_ok({
          track_alias: 1UL,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.SubscribeEstablished(request_id, 1UL))]),
  )
  assert_eq(subscriber.receive(@session.Clock(100UL)), Ok([]))
  let request_goaway : @protocol.GoAwayRequestMessage = {
    uri: b"moqt://next",
    timeout_ms: 10UL,
  }
  let request_goaway_wire = @protocol.encode_goaway_request(request_goaway).unwrap()
  assert_eq(
    subscriber.receive(@session.StreamData(20, request_goaway_wire)),
    Ok([
      @session.Notify(
        @session.RequestGoAwayReceived(request_id, 20, b"moqt://next", 10UL),
      ),
    ]),
  )
  assert_eq(subscriber.state(), @session.Active)
  assert_eq(subscriber.receive(@session.Clock(109UL)), Ok([]))
  assert_eq(
    subscriber.receive(@session.Clock(110UL)),
    Ok([
      @session.AbortSend(20, 4UL),
      @session.StopReceiving(20, 4UL),
      @session.Notify(
        @session.SubscriptionEnded(request_id, 20, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(subscriber.state(), @session.Active)
}

///|
test "request GOAWAY timeout zero does not reset and duplicate GOAWAY is a violation" {
  let subscriber = client_session_ready()
  let (request_id, _) = subscriber
    .subscribe_track(client_track(b"video"))
    .unwrap()
  assert_eq(
    subscriber.receive(@session.StreamOpened(@session.Request(request_id), 20)),
    Ok([
      @session.Send(
        20,
        @protocol.encode_subscribe({
          request_id,
          track: client_track(b"video"),
          parameters: [],
        }).unwrap(),
        false,
      ),
    ]),
  )
  let goaway = @protocol.encode_goaway_request({ uri: b"", timeout_ms: 0UL, }).unwrap()
  assert_eq(
    subscriber.receive(@session.StreamData(20, goaway)),
    Ok([
      @session.Notify(@session.RequestGoAwayReceived(request_id, 20, b"", 0UL)),
    ]),
  )
  assert_eq(subscriber.receive(@session.Clock(100000UL)), Ok([]))
  assert_eq(subscriber.state(), @session.Active)
  assert_eq(
    subscriber.receive(@session.StreamData(20, goaway)),
    Ok([
      @session.CloseConnection(3UL, b""),
      @session.Notify(@session.SessionEnded(@session.Local(3UL, b""))),
    ]),
  )
}

///|
test "drain closes after rejecting a local request that has not opened" {
  let subscriber = client_session_ready()
  let (request_id, _) = subscriber
    .subscribe_track(client_track(b"video"))
    .unwrap()
  assert_eq(
    subscriber.receive(
      @session.StreamData(11, client_goaway_control(0UL, 0UL, b"")),
    ),
    Ok([
      @session.Notify(
        @session.ControlMessageReceived(
          @protocol.GoAwayControl({
            uri: b"",
            timeout_ms: 0UL,
            request_id: 0UL,
          }),
        ),
      ),
      @session.Notify(@session.ControlGoAwayReceived(b"", 0UL, 0UL)),
      @session.Notify(
        @session.SubscribeRejected(request_id, {
          code: 6UL,
          retry_interval: 0UL,
          reason: b"",
          redirect: b"",
        }),
      ),
      @session.CloseConnection(0UL, b""),
      @session.Notify(@session.SessionEnded(@session.Local(0UL, b""))),
    ]),
  )
  assert_eq(subscriber.state(), @session.Closed)
  assert_eq(
    subscriber.receive(@session.StreamOpened(@session.Request(request_id), 20)),
    Ok([]),
  )
}

///|
test "zero GOAWAY timeout does not expire a request and drain waits for sends" {
  let subscriber = client_session_ready()
  let (request_id, _) = subscriber
    .subscribe_track(client_track(b"video"))
    .unwrap()
  assert_eq(
    subscriber.send_goaway(0UL),
    Ok([@session.Send(10, client_goaway_control(1UL, 0UL, b""), false)]),
  )
  assert_eq(subscriber.receive(@session.Clock(100000UL)), Ok([]))
  assert_eq(subscriber.state(), @session.Draining)
  assert_eq(subscriber.cancel_request(request_id), Ok([]))
  assert_eq(subscriber.state(), @session.Draining)
  assert_eq(
    subscriber.receive(@session.SendComplete(10, false)),
    Ok([
      @session.Notify(@session.SendFinished(10, false)),
      @session.CloseConnection(0UL, b""),
      @session.Notify(@session.SessionEnded(@session.Local(0UL, b""))),
    ]),
  )
  assert_eq(subscriber.state(), @session.Closed)
}

///|
test "sent control GOAWAY times out while requests remain" {
  let publisher = client_session_ready()
  let (_, _) = publisher.publish_track(client_track(b"video"), 1UL).unwrap()
  assert_eq(
    publisher.send_goaway(10UL),
    Ok([@session.Send(10, client_goaway_control(1UL, 10UL, b""), false)]),
  )
  assert_eq(
    publisher.receive(@session.Clock(10UL)),
    Ok([
      @session.CloseConnection(0x10UL, b""),
      @session.Notify(@session.SessionEnded(@session.Local(0x10UL, b""))),
    ]),
  )
  assert_eq(publisher.state(), @session.Closed)
}

///|
test "existing object delivery continues during session drain" {
  let subscriber = client_session_ready()
  let track = client_track(b"video")
  let (request_id, _) = subscriber.subscribe_track(track).unwrap()
  assert_eq(
    subscriber.receive(@session.StreamOpened(@session.Request(request_id), 20)),
    Ok([
      @session.Send(
        20,
        @protocol.encode_subscribe({ request_id, track, parameters: [], }).unwrap(),
        false,
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.SendComplete(20, false)),
    Ok([@session.Notify(@session.SendFinished(20, false))]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        20,
        @protocol.encode_subscribe_ok({
          track_alias: 1UL,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.SubscribeEstablished(request_id, 1UL))]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(11, client_goaway_control(2UL, 0UL, b"")),
    ),
    Ok([
      @session.Notify(
        @session.ControlMessageReceived(
          @protocol.GoAwayControl({
            uri: b"",
            timeout_ms: 0UL,
            request_id: 2UL,
          }),
        ),
      ),
      @session.Notify(@session.ControlGoAwayReceived(b"", 0UL, 2UL)),
    ]),
  )
  let header = client_subgroup_header(1UL, 9UL)
  let frame = client_object(0UL, b"payload")
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(41, @session.Unidirectional),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        41,
        @protocol.encode_subgroup_stream(header, [frame]).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(
        @session.ObjectReceived(
          request_id,
          { namespace_fields: [b"demo"], name: b"video", },
          header,
          [frame],
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.StreamFinished(41)),
    Ok([@session.Notify(@session.GroupFinished(request_id, 9UL))]),
  )
  assert_eq(subscriber.state(), @session.Draining)
}

///|
test "peer and transport close notifications are one-shot" {
  let peer_closed = client_session_ready()
  assert_eq(
    peer_closed.receive(@session.PeerClosed(7UL)),
    Ok([@session.Notify(@session.SessionEnded(@session.Peer(7UL)))]),
  )
  assert_eq(peer_closed.receive(@session.PeerClosed(7UL)), Ok([]))
  assert_eq(peer_closed.close(0UL, b""), [])
  assert_eq(
    peer_closed.subscribe_track(client_track(b"video")),
    Err(@session.SessionClosed),
  )

  let transport_failed = client_session_ready()
  assert_eq(
    transport_failed.receive(@session.TransportFailed(-1L)),
    Ok([@session.Notify(@session.SessionEnded(@session.Transport(-1L)))]),
  )

  let locally_closed = client_session_ready()
  assert_eq(locally_closed.close(9UL, b"done"), [
    @session.CloseConnection(9UL, b"done"),
    @session.Notify(@session.SessionEnded(@session.Local(9UL, b"done"))),
  ])
  assert_eq(locally_closed.close(10UL, b"ignored"), [])
}

///|
test "aborting a request releases its pending sends for drain" {
  let subscriber = subscriber_started()
  assert_eq(
    subscriber.send_goaway(0UL),
    Ok([@session.Send(10, client_goaway_control(1UL, 0UL, b""), false)]),
  )
  ignore(subscriber.update_subscription(0UL, [forward_parameter(0)]).unwrap())
  assert_eq(
    subscriber.cancel_request(0UL),
    Ok([
      @session.AbortSend(30, 1UL),
      @session.StopReceiving(30, 1UL),
      @session.Notify(@session.SubscriptionEnded(0, 30, subscriber_track())),
    ]),
  )
  assert_eq(subscriber.state(), @session.Draining)
  assert_eq(
    subscriber.receive(@session.SendComplete(10, false)),
    Ok([
      @session.Notify(@session.SendFinished(10, false)),
      @session.CloseConnection(0UL, b""),
      @session.Notify(@session.SessionEnded(@session.Local(0UL, b""))),
    ]),
  )
  assert_eq(subscriber.state(), @session.Closed)
}
