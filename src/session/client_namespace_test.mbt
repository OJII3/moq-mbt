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
  assert_eq(request_id, 0UL)
  assert_eq(actions, [
    @session.OpenStream(@session.Bidirectional, @session.Request(0UL)),
  ])
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Request(request_id), 30)),
    Ok([
      @session.Send(
        30,
        @protocol.encode_publish_namespace({
          request_id,
          track_namespace: namespace_request(),
          parameters: [],
        }).unwrap(),
        false,
      ),
    ]),
  )
  let accepted = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    @protocol.Namespace,
  ).unwrap()
  assert_eq(
    publisher.receive(@session.StreamData(30, accepted)),
    Ok([@session.Notify(@session.NamespacePublicationAccepted(request_id, 30))]),
  )
  assert_eq(
    publisher.cancel_request(request_id),
    Ok([
      @session.AbortSend(30, 1UL),
      @session.StopReceiving(30, 1UL),
      @session.Notify(@session.NamespaceWithdrawn(request_id, 30)),
    ]),
  )
  assert_eq(publisher.receive(@session.StreamData(30, accepted)), Ok([]))
}

///|
test "peer PUBLISH_NAMESPACE requires explicit application acceptance and can be revoked" {
  let subscriber = client_session_ready()
  let request : @protocol.NamespaceRequestMessage = {
    request_id: 1UL,
    track_namespace: namespace_request(),
    parameters: [],
  }
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(40, @session.Bidirectional),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        40,
        @protocol.encode_publish_namespace(request).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(
        @session.NamespacePublicationRequested(1UL, 40, namespace_request(), []),
      ),
    ]),
  )
  assert_eq(
    subscriber.accept_namespace_publication(40),
    Ok([
      @session.Send(
        40,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          @protocol.Namespace,
        ).unwrap(),
        false,
      ),
      @session.Notify(@session.NamespacePublicationAccepted(1UL, 40)),
    ]),
  )
  assert_eq(
    subscriber.cancel_request(1UL),
    Ok([
      @session.AbortSend(40, 1UL),
      @session.StopReceiving(40, 1UL),
      @session.Notify(@session.NamespaceAcceptanceRevoked(1UL, 40)),
    ]),
  )
}

///|
test "peer PUBLISH_NAMESPACE can be rejected with REQUEST_ERROR and FIN" {
  let subscriber = client_session_ready()
  let request : @protocol.NamespaceRequestMessage = {
    request_id: 1UL,
    track_namespace: namespace_request(),
    parameters: [],
  }
  ignore(
    subscriber.receive(
      @session.InboundStreamStarted(40, @session.Bidirectional),
    ),
  )
  ignore(
    subscriber.receive(
      @session.StreamData(
        40,
        @protocol.encode_publish_namespace(request).unwrap(),
      ),
    ),
  )
  let error : @protocol.RequestErrorMessage = {
    code: 3UL,
    retry_interval: 0UL,
    reason: b"denied",
    redirect: b"",
  }
  assert_eq(
    subscriber.reject_namespace_publication(40, error),
    Ok([
      @session.Send(40, @protocol.encode_request_error(error).unwrap(), true),
      @session.Notify(@session.NamespacePublicationRejected(1UL, 40, error)),
    ]),
  )
}

///|
test "namespace request updates are surfaced before and after acceptance" {
  let subscriber = client_session_ready()
  let request : @protocol.NamespaceRequestMessage = {
    request_id: 1UL,
    track_namespace: namespace_request(),
    parameters: [],
  }
  ignore(
    subscriber.receive(
      @session.InboundStreamStarted(40, @session.Bidirectional),
    ),
  )
  ignore(
    subscriber.receive(
      @session.StreamData(
        40,
        @protocol.encode_publish_namespace(request).unwrap(),
      ),
    ),
  )

  let first_update : @protocol.RequestUpdateMessage = {
    request_id: 3UL,
    parameters: [{ kind: 0x03UL, value: @protocol.Data(b"token-a"), }],
  }
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        40,
        @protocol.encode_request_update(first_update).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(
        @session.NamespaceUpdateRequested(1UL, 3UL, 40, first_update.parameters),
      ),
    ]),
  )
  let queued_update : @protocol.RequestUpdateMessage = {
    request_id: 5UL,
    parameters: [
      { kind: 0x03UL, value: @protocol.ParameterValue::Data(b"token-b"), },
    ],
  }
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        40,
        @protocol.encode_request_update(queued_update).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(
        @session.NamespaceUpdateRequested(
          1UL,
          5UL,
          40,
          queued_update.parameters,
        ),
      ),
    ]),
  )
  let update_ok : @protocol.RequestOkMessage = {
    parameters: [],
    properties: b"",
  }
  assert_eq(
    subscriber.accept_namespace_update(40, 3UL, update_ok),
    Ok([
      @session.Send(
        40,
        @protocol.encode_request_ok_for(update_ok, @protocol.RequestUpdate).unwrap(),
        false,
      ),
      @session.Notify(@session.NamespaceUpdateAccepted(1UL, 3UL, update_ok)),
    ]),
  )
  assert_eq(
    subscriber.accept_namespace_update(40, 5UL, update_ok),
    Ok([
      @session.Send(
        40,
        @protocol.encode_request_ok_for(update_ok, @protocol.RequestUpdate).unwrap(),
        false,
      ),
      @session.Notify(@session.NamespaceUpdateAccepted(1UL, 5UL, update_ok)),
    ]),
  )
  assert_eq(
    subscriber.accept_namespace_publication(40),
    Ok([
      @session.Send(
        40,
        @protocol.encode_request_ok_for(update_ok, @protocol.Namespace).unwrap(),
        false,
      ),
      @session.Notify(@session.NamespacePublicationAccepted(1UL, 40)),
    ]),
  )

  let second_update : @protocol.RequestUpdateMessage = {
    request_id: 7UL,
    parameters: [
      { kind: 0x34UL, value: @protocol.NamespacePrefix({ fields: [b"new"], }), },
    ],
  }
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        40,
        @protocol.encode_request_update(second_update).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(
        @session.NamespaceUpdateRequested(
          1UL,
          7UL,
          40,
          second_update.parameters,
        ),
      ),
    ]),
  )
  let error : @protocol.RequestErrorMessage = {
    code: 3UL,
    retry_interval: 0UL,
    reason: b"unsupported update",
    redirect: b"",
  }
  assert_eq(
    subscriber.accept_namespace_update(40, 7UL, update_ok),
    Err(@session.UnexpectedMessage),
  )
  assert_eq(
    subscriber.reject_namespace_update(40, 7UL, error),
    Ok([
      @session.Send(40, @protocol.encode_request_error(error).unwrap(), true),
      @session.Notify(@session.NamespaceUpdateRejected(1UL, 7UL, error)),
      @session.StopReceiving(40, 1UL),
      @session.Notify(@session.NamespaceAcceptanceRevoked(1UL, 40)),
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
    publisher.receive(@session.StreamOpened(@session.Request(request_id), 30)),
    Ok([@session.AbortSend(30, 1UL), @session.StopReceiving(30, 1UL)]),
  )
  assert_eq(publisher.receive(@session.StreamData(30, b"late")), Ok([]))
}

///|
test "outgoing namespace update is matched to its REQUEST_OK response" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(
    publisher.receive(@session.StreamOpened(@session.Request(request_id), 30)),
  )
  let namespace_ok = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    @protocol.Namespace,
  ).unwrap()
  ignore(publisher.receive(@session.StreamData(30, namespace_ok)))
  let token : Array[@protocol.MessageParameter] = [
    { kind: 0x03UL, value: @protocol.ParameterValue::Data(b"token-b"), },
  ]
  let (update_id, actions) = publisher
    .update_published_namespace(request_id, token)
    .unwrap()
  assert_eq(update_id, 2UL)
  assert_eq(actions, [
    @session.Send(
      30,
      @protocol.encode_request_update_for(
        { request_id: update_id, parameters: token, },
        @protocol.RequestStreamFor(@protocol.Namespace),
      ).unwrap(),
      false,
    ),
  ])
  let update_ok = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    @protocol.RequestUpdate,
  ).unwrap()
  assert_eq(
    publisher.receive(@session.StreamData(30, update_ok)),
    Ok([
      @session.Notify(
        @session.NamespaceUpdateAccepted(request_id, update_id, {
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
  ignore(
    publisher.receive(@session.StreamOpened(@session.Request(request_id), 30)),
  )
  let invalid = @protocol.encode_publish_done({
    code: 0UL,
    stream_count: 0UL,
    reason: b"",
  }).unwrap()
  assert_eq(
    publisher.receive(@session.StreamData(30, invalid)),
    Ok([
      @session.CloseConnection(3UL, b""),
      @session.Notify(@session.SessionEnded(@session.Local(3UL, b""))),
    ]),
  )
  assert_eq(publisher.state(), @session.Closed)
}

///|
test "duplicate initial PUBLISH_NAMESPACE response is a protocol violation" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(
    publisher.receive(@session.StreamOpened(@session.Request(request_id), 30)),
  )
  let accepted = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    @protocol.Namespace,
  ).unwrap()
  assert_eq(
    publisher.receive(@session.StreamData(30, accepted)),
    Ok([@session.Notify(@session.NamespacePublicationAccepted(request_id, 30))]),
  )
  assert_eq(
    publisher.receive(@session.StreamData(30, accepted)),
    Ok([
      @session.CloseConnection(3UL, b""),
      @session.Notify(@session.SessionEnded(@session.Local(3UL, b""))),
    ]),
  )
}

///|
test "PUBLISH_NAMESPACE reset and cancellation crossed events notify only once" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(
    publisher.receive(@session.StreamOpened(@session.Request(request_id), 30)),
  )
  let accepted = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    @protocol.Namespace,
  ).unwrap()
  ignore(publisher.receive(@session.StreamData(30, accepted)))
  assert_eq(
    publisher.receive(@session.StreamReset(30, 7UL)),
    Ok([
      @session.AbortSend(30, 7UL),
      @session.Notify(@session.NamespaceWithdrawn(request_id, 30)),
    ]),
  )
  assert_eq(publisher.cancel_request(request_id), Ok([]))
  assert_eq(publisher.receive(@session.SendStopped(30, 7UL)), Ok([]))
  assert_eq(publisher.receive(@session.StreamReset(30, 7UL)), Ok([]))
  assert_eq(publisher.receive(@session.StreamData(30, b"late")), Ok([]))
}

///|
test "FIN before the initial namespace response is a protocol violation" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(
    publisher.receive(@session.StreamOpened(@session.Request(request_id), 30)),
  )
  assert_eq(
    publisher.receive(@session.StreamFinished(30)),
    Ok([
      @session.CloseConnection(3UL, b""),
      @session.Notify(@session.SessionEnded(@session.Local(3UL, b""))),
    ]),
  )
}

///|
test "withdrawing a namespace leaves established track subscriptions active" {
  let publisher = client_session_ready()
  let track = client_track(b"video")
  let (publish_id, _) = publisher.publish_track(track, 9UL).unwrap()
  ignore(
    publisher.receive(@session.StreamOpened(@session.Request(publish_id), 20)),
  )
  let publish_ok = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    @protocol.Publish,
  ).unwrap()
  ignore(publisher.receive(@session.StreamData(20, publish_ok)))
  let peer_subscribe : @protocol.SubscribeMessage = {
    request_id: 1UL,
    track,
    parameters: [],
  }
  ignore(
    publisher.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        40,
        @protocol.encode_subscribe(peer_subscribe).unwrap(),
      ),
    ),
  )
  ignore(publisher.accept_subscription(40))

  let (namespace_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(
    publisher.receive(@session.StreamOpened(@session.Request(namespace_id), 30)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        30,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          @protocol.Namespace,
        ).unwrap(),
      ),
    ),
  )
  assert_true(publisher.cancel_request(namespace_id) is Ok(_))

  assert_true(
    publisher.open_object_stream(40, client_subgroup_header(9UL, 1UL)) is Ok(_),
  )
}

///|
test "request GOAWAY timeout terminates a namespace request through its lifetime" {
  let publisher = client_session_ready()
  let (request_id, _) = publisher
    .publish_namespace(namespace_request(), [])
    .unwrap()
  ignore(
    publisher.receive(@session.StreamOpened(@session.Request(request_id), 30)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        30,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          @protocol.Namespace,
        ).unwrap(),
      ),
    ),
  )
  let goaway = @protocol.encode_goaway_request({ uri: b"", timeout_ms: 5UL, }).unwrap()
  assert_eq(
    publisher.receive(@session.StreamData(30, goaway)),
    Ok([
      @session.Notify(@session.RequestGoAwayReceived(request_id, 30, b"", 5UL)),
    ]),
  )
  assert_eq(
    publisher.receive(@session.Clock(5UL)),
    Ok([
      @session.AbortSend(30, 4UL),
      @session.StopReceiving(30, 4UL),
      @session.Notify(@session.NamespaceWithdrawn(request_id, 30)),
    ]),
  )
}
