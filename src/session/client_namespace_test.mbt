///|
fn namespace_request() -> @protocol.TrackNamespace {
  { fields: [b"example", b"room"], }
}

///|
test "local PUBLISH_NAMESPACE accepts and withdraws independently of tracks" {
  let publisher = client_session_ready()
  let (request_id, actions) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  assert_eq(request_id, RequestId(0UL))
  assert_eq(actions, [OpenStream(Bidirectional, Request(0UL))])
  assert_eq(
    publisher.receive(StreamOpened(Request(request_id), 30)),
    Ok([
      SendChunk(
        30,
        @protocol.encode_publish_namespace({
          request_id: request_id.to_uint64(),
          track_namespace: namespace_request(),
          parameters: [],
        }).unwrap(),
      ),
    ]),
  )
  let accepted = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    Namespace,
  ).unwrap()
  assert_eq(
    publisher.receive(StreamData(30, accepted)),
    Ok([EmitNotice(NamespacePublicationAccepted(request_id, 30))]),
  )
  assert_eq(
    publisher.cancel_request(request_id),
    Ok([
      AbortSend(30, 1UL),
      StopReceiving(30, 1UL),
      EmitNotice(NamespaceWithdrawn(request_id, 30)),
    ]),
  )
  assert_eq(publisher.receive(StreamData(30, accepted)), Ok([]))
}

///|
test "peer PUBLISH_NAMESPACE requires explicit application acceptance and can be revoked" {
  let subscriber = client_session_ready()
  let request = @protocol.NamespaceRequestMessage::{
    request_id: 1UL,
    track_namespace: namespace_request(),
    parameters: [],
  }
  assert_eq(subscriber.receive(InboundStreamStarted(40, Bidirectional)), Ok([]))
  assert_eq(
    subscriber.receive(
      StreamData(40, @protocol.encode_publish_namespace(request).unwrap()),
    ),
    Ok([
      EmitNotice(
        NamespacePublicationRequested(1UL, 40, namespace_request(), []),
      ),
    ]),
  )
  assert_eq(
    subscriber.accept_namespace_publication(StreamId(40)),
    Ok([
      SendChunk(
        40,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          Namespace,
        ).unwrap(),
      ),
      EmitNotice(NamespacePublicationAccepted(1UL, 40)),
    ]),
  )
  assert_eq(
    subscriber.cancel_request(1UL),
    Ok([
      AbortSend(40, 1UL),
      StopReceiving(40, 1UL),
      EmitNotice(NamespaceAcceptanceRevoked(1UL, 40)),
    ]),
  )
}

///|
test "peer PUBLISH_NAMESPACE can be rejected with REQUEST_ERROR and FIN" {
  let subscriber = client_session_ready()
  let request = @protocol.NamespaceRequestMessage::{
    request_id: 1UL,
    track_namespace: namespace_request(),
    parameters: [],
  }
  ignore(subscriber.receive(InboundStreamStarted(40, Bidirectional)))
  ignore(
    subscriber.receive(
      StreamData(40, @protocol.encode_publish_namespace(request).unwrap()),
    ),
  )
  let error = @protocol.RequestErrorMessage::{
    code: 3UL,
    retry_interval: 0UL,
    reason: b"denied",
    redirect: b"",
  }
  assert_eq(
    subscriber.reject_namespace_publication(StreamId(40), error),
    Ok([
      SendFin(40, @protocol.encode_request_error(error).unwrap()),
      EmitNotice(NamespacePublicationRejected(1UL, 40, error)),
    ]),
  )
}

///|
test "namespace request updates are surfaced before and after acceptance" {
  let subscriber = client_session_ready()
  let request = @protocol.NamespaceRequestMessage::{
    request_id: 1UL,
    track_namespace: namespace_request(),
    parameters: [],
  }
  ignore(subscriber.receive(InboundStreamStarted(40, Bidirectional)))
  ignore(
    subscriber.receive(
      StreamData(40, @protocol.encode_publish_namespace(request).unwrap()),
    ),
  )

  let first_update = @protocol.RequestUpdateMessage::{
    request_id: 3UL,
    parameters: [{ kind: 0x03UL, value: Data(b"token-a"), }],
  }
  assert_eq(
    subscriber.receive(
      StreamData(40, @protocol.encode_request_update(first_update).unwrap()),
    ),
    Ok([
      EmitNotice(
        NamespaceUpdateRequested(1UL, 3UL, 40, first_update.parameters),
      ),
    ]),
  )
  let queued_update = @protocol.RequestUpdateMessage::{
    request_id: 5UL,
    parameters: [{ kind: 0x03UL, value: Data(b"token-b"), }],
  }
  assert_eq(
    subscriber.receive(
      StreamData(40, @protocol.encode_request_update(queued_update).unwrap()),
    ),
    Ok([
      EmitNotice(
        NamespaceUpdateRequested(1UL, 5UL, 40, queued_update.parameters),
      ),
    ]),
  )
  let update_ok = @protocol.RequestOkMessage::{
    parameters: [],
    properties: b"",
  }
  assert_eq(
    subscriber.accept_namespace_update(40, 3UL, update_ok),
    Err(UnexpectedMessage),
  )
  assert_eq(
    subscriber.accept_namespace_publication(StreamId(40)),
    Ok([
      SendChunk(
        40,
        @protocol.encode_request_ok_for(update_ok, Namespace).unwrap(),
      ),
      EmitNotice(NamespacePublicationAccepted(1UL, 40)),
    ]),
  )
  assert_eq(
    subscriber.accept_namespace_update(40, 3UL, update_ok),
    Ok([
      SendChunk(
        40,
        @protocol.encode_request_ok_for(update_ok, RequestUpdate).unwrap(),
      ),
      EmitNotice(NamespaceUpdateAccepted(1UL, 3UL, update_ok)),
    ]),
  )
  assert_eq(
    subscriber.accept_namespace_update(40, 5UL, update_ok),
    Ok([
      SendChunk(
        40,
        @protocol.encode_request_ok_for(update_ok, RequestUpdate).unwrap(),
      ),
      EmitNotice(NamespaceUpdateAccepted(1UL, 5UL, update_ok)),
    ]),
  )

  let second_update = @protocol.RequestUpdateMessage::{
    request_id: 7UL,
    parameters: [
      { kind: 0x34UL, value: NamespacePrefix({ fields: [b"new"], }), },
    ],
  }
  assert_eq(
    subscriber.receive(
      StreamData(40, @protocol.encode_request_update(second_update).unwrap()),
    ),
    Ok([
      EmitNotice(
        NamespaceUpdateRequested(1UL, 7UL, 40, second_update.parameters),
      ),
    ]),
  )
  let error = @protocol.RequestErrorMessage::{
    code: 3UL,
    retry_interval: 0UL,
    reason: b"unsupported update",
    redirect: b"",
  }
  assert_eq(
    subscriber.accept_namespace_update(40, 7UL, update_ok),
    Err(UnexpectedMessage),
  )
  assert_eq(
    subscriber.reject_namespace_update(40, 7UL, error),
    Ok([
      SendFin(40, @protocol.encode_request_error(error).unwrap()),
      EmitNotice(NamespaceUpdateRejected(1UL, 7UL, error)),
      StopReceiving(40, 1UL),
      EmitNotice(NamespaceAcceptanceRevoked(1UL, 40)),
    ]),
  )
}

///|
test "cancel while opening a namespace request ignores the delayed open without a withdrawal notice" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  assert_eq(publisher.cancel_request(request_id), Ok([]))
  assert_eq(
    publisher.receive(StreamOpened(Request(request_id), 30)),
    Ok([AbortSend(30, 1UL), StopReceiving(30, 1UL)]),
  )
  assert_eq(publisher.receive(StreamData(30, b"late")), Ok([]))
}

///|
test "outgoing namespace update is matched to its REQUEST_OK response" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(publisher.receive(StreamOpened(Request(request_id), 30)))
  let namespace_ok = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    Namespace,
  ).unwrap()
  ignore(publisher.receive(StreamData(30, namespace_ok)))
  let token : Array[@protocol.MessageParameter] = [
    { kind: 0x03UL, value: Data(b"token-b"), },
  ]
  let (update_id, actions) = publisher
    .update_published_namespace(request_id, token)
    .unwrap()
  assert_eq(update_id, RequestId(2UL))
  assert_eq(actions, [
    SendChunk(
      30,
      @protocol.encode_request_update_for(
        { request_id: update_id.to_uint64(), parameters: token, },
        RequestStreamFor(Namespace),
      ).unwrap(),
    ),
  ])
  let update_ok = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    RequestUpdate,
  ).unwrap()
  assert_eq(
    publisher.receive(StreamData(30, update_ok)),
    Ok([
      EmitNotice(
        NamespaceUpdateAccepted(request_id, update_id, {
          parameters: [],
          properties: b"",
        }),
      ),
    ]),
  )
}

///|
test "invalid first response to PUBLISH_NAMESPACE closes the session" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(publisher.receive(StreamOpened(Request(request_id), 30)))
  let invalid = @protocol.encode_publish_done({
    code: 0UL,
    stream_count: 0UL,
    reason: b"",
  }).unwrap()
  assert_eq(
    publisher.receive(StreamData(30, invalid)),
    Ok([CloseConnection(3UL, b""), EmitNotice(SessionEnded(Local(3UL, b"")))]),
  )
  assert_eq(publisher.state(), Closed)
}

///|
test "duplicate initial PUBLISH_NAMESPACE response is a protocol violation" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(publisher.receive(StreamOpened(Request(request_id), 30)))
  let accepted = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    Namespace,
  ).unwrap()
  assert_eq(
    publisher.receive(StreamData(30, accepted)),
    Ok([EmitNotice(NamespacePublicationAccepted(request_id, 30))]),
  )
  assert_eq(
    publisher.receive(StreamData(30, accepted)),
    Ok([CloseConnection(3UL, b""), EmitNotice(SessionEnded(Local(3UL, b"")))]),
  )
}

///|
test "PUBLISH_NAMESPACE reset and cancellation crossed events notify only once" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(publisher.receive(StreamOpened(Request(request_id), 30)))
  let accepted = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    Namespace,
  ).unwrap()
  ignore(publisher.receive(StreamData(30, accepted)))
  assert_eq(
    publisher.receive(StreamReset(30, 7UL)),
    Ok([
      AbortSend(30, 7UL),
      EmitNotice(NamespaceAcceptanceRevoked(request_id, 30)),
    ]),
  )
  assert_eq(publisher.cancel_request(request_id), Ok([]))
  assert_eq(publisher.receive(SendStopped(30, 7UL)), Ok([]))
  assert_eq(publisher.receive(StreamReset(30, 7UL)), Ok([]))
  assert_eq(publisher.receive(StreamData(30, b"late")), Ok([]))
}

///|
test "FIN before the initial namespace response is a protocol violation" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(publisher.receive(StreamOpened(Request(request_id), 30)))
  assert_eq(
    publisher.receive(InboundStreamFinished(30)),
    Ok([CloseConnection(3UL, b""), EmitNotice(SessionEnded(Local(3UL, b"")))]),
  )
}

///|
test "withdrawing a namespace leaves established track subscriptions active" {
  let publisher = client_session_ready()
  let track = client_track(b"video")
  let (publish_id, _) = publisher.publish_track(track, 9UL).unwrap()
  ignore(publisher.receive(StreamOpened(Request(publish_id), 20)))
  let publish_ok = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    Publish,
  ).unwrap()
  ignore(publisher.receive(StreamData(20, publish_ok)))
  let peer_subscribe = @protocol.SubscribeMessage::{
    request_id: 1UL,
    track,
    parameters: [],
  }
  ignore(publisher.receive(InboundStreamStarted(40, Bidirectional)))
  ignore(
    publisher.receive(
      StreamData(40, @protocol.encode_subscribe(peer_subscribe).unwrap()),
    ),
  )
  ignore(publisher.accept_subscription(StreamId(40)))

  let (namespace_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(publisher.receive(StreamOpened(Request(namespace_id), 30)))
  ignore(
    publisher.receive(
      StreamData(
        30,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          Namespace,
        ).unwrap(),
      ),
    ),
  )
  assert_true(publisher.cancel_request(namespace_id) is Ok(_))

  assert_true(
    publisher.open_object_stream(StreamId(40), client_subgroup_header(9UL, 1UL))
    is Ok(_),
  )
}

///|
test "request GOAWAY timeout terminates a namespace request through its lifetime" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(publisher.receive(StreamOpened(Request(request_id), 30)))
  ignore(
    publisher.receive(
      StreamData(
        30,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          Namespace,
        ).unwrap(),
      ),
    ),
  )
  let goaway = @protocol.encode_goaway_request({ uri: b"", timeout_ms: 5UL, }).unwrap()
  assert_eq(
    publisher.receive(StreamData(30, goaway)),
    Ok([EmitNotice(RequestGoAwayReceived(request_id, 30, b"", 5UL))]),
  )
  assert_eq(
    publisher.receive(Clock(5UL)),
    Ok([
      AbortSend(30, 4UL),
      StopReceiving(30, 4UL),
      EmitNotice(NamespaceWithdrawn(request_id, 30)),
    ]),
  )
}

///|
test "PUBLISH_NAMESPACE must be the only initial request on its bidi" {
  let subscriber = client_session_ready()
  ignore(subscriber.receive(InboundStreamStarted(40, Bidirectional)))
  let first = @protocol.NamespaceRequestMessage::{
    request_id: 1UL,
    track_namespace: namespace_request(),
    parameters: [],
  }
  let second = @protocol.NamespaceRequestMessage::{
    request_id: 3UL,
    track_namespace: namespace_request(),
    parameters: [],
  }
  ignore(
    subscriber.receive(
      StreamData(40, @protocol.encode_publish_namespace(first).unwrap()),
    ),
  )
  assert_eq(
    subscriber.receive(
      StreamData(40, @protocol.encode_publish_namespace(second).unwrap()),
    ),
    Ok([
      AbortSend(40, 3UL),
      StopReceiving(40, 3UL),
      EmitNotice(NamespaceAcceptanceRevoked(1UL, 40)),
    ]),
  )
}

///|
test "GOAWAY cancels an opening namespace request before its delayed stream opens" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  let (accepted_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(publisher.receive(StreamOpened(Request(accepted_id), 32)))
  ignore(
    publisher.receive(
      StreamData(
        32,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          Namespace,
        ).unwrap(),
      ),
    ),
  )
  assert_eq(
    publisher.receive(StreamData(11, client_goaway_control(0UL, 0UL, b""))),
    Ok([
      EmitNotice(
        ControlMessageReceived(
          GoAwayControl({ uri: b"", timeout_ms: 0UL, request_id: 0UL, }),
        ),
      ),
      EmitNotice(ControlGoAwayReceived(b"", 0UL, 0UL)),
    ]),
  )
  assert_eq(
    publisher.receive(StreamOpened(Request(request_id), 30)),
    Ok([AbortSend(30, 4UL), StopReceiving(30, 4UL)]),
  )
}

///|
test "GOAWAY rejects a namespace REQUEST_UPDATE and closes its bidi" {
  let subscriber = client_session_ready()
  let request = @protocol.NamespaceRequestMessage::{
    request_id: 1UL,
    track_namespace: namespace_request(),
    parameters: [],
  }
  ignore(subscriber.receive(InboundStreamStarted(40, Bidirectional)))
  ignore(
    subscriber.receive(
      StreamData(40, @protocol.encode_publish_namespace(request).unwrap()),
    ),
  )
  ignore(subscriber.accept_namespace_publication(StreamId(40)))
  ignore(subscriber.send_goaway(0UL))
  let update = @protocol.RequestUpdateMessage::{
    request_id: 3UL,
    parameters: [{ kind: 0x03UL, value: Data(b"new-token"), }],
  }
  let error = @protocol.RequestErrorMessage::{
    code: 6UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    subscriber.receive(
      StreamData(40, @protocol.encode_request_update(update).unwrap()),
    ),
    Ok([
      SendFin(40, @protocol.encode_request_error(error).unwrap()),
      EmitNotice(NamespaceUpdateRejected(1UL, 3UL, error)),
      StopReceiving(40, 4UL),
      EmitNotice(NamespaceAcceptanceRevoked(1UL, 40)),
    ]),
  )
}

///|
test "GOAWAY cannot be the initial PUBLISH_NAMESPACE response" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(publisher.receive(StreamOpened(Request(request_id), 30)))
  let goaway = @protocol.encode_goaway_request({ uri: b"", timeout_ms: 0UL, }).unwrap()
  assert_eq(
    publisher.receive(StreamData(30, goaway)),
    Ok([CloseConnection(3UL, b""), EmitNotice(SessionEnded(Local(3UL, b"")))]),
  )
}

///|
test "FIN during a namespace REQUEST_UPDATE response is a protocol violation" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(publisher.receive(StreamOpened(Request(request_id), 30)))
  ignore(
    publisher.receive(
      StreamData(
        30,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          Namespace,
        ).unwrap(),
      ),
    ),
  )
  ignore(publisher.update_published_namespace(request_id, []).unwrap())
  let response = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    RequestUpdate,
  ).unwrap()
  assert_eq(publisher.receive(StreamData(30, response[:1].to_owned())), Ok([]))
  assert_eq(
    publisher.receive(InboundStreamFinished(30)),
    Ok([CloseConnection(3UL, b""), EmitNotice(SessionEnded(Local(3UL, b"")))]),
  )
}
