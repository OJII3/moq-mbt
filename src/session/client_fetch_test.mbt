///|
test "Standalone FETCH accepts data before its response and finishes after FIN" {
  let client = client_session_ready()
  let track = client_track(b"archive")
  let start : @protocol.FetchLocation = { group: 2UL, object: 1UL, }
  let end_location : @protocol.FetchLocation = { group: 3UL, object: 0UL, }
  let (request_id, actions) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  assert_eq(request_id, @session.RequestId(0UL))
  assert_eq(actions, [
    @session.OpenStream(@session.Bidirectional, @session.Request(0UL)),
  ])
  let fetch : @protocol.FetchMessage = {
    request_id: request_id.to_uint64(),
    fetch_type: @protocol.StandaloneFetch(track, start, end_location),
    parameters: [],
  }
  assert_eq(
    client.receive(@session.StreamOpened(@session.Request(request_id), 20)),
    Ok([@session.SendChunk(20, @protocol.encode_fetch(fetch).unwrap())]),
  )

  let object : @protocol.FetchObject = {
    group_id: 2UL,
    subgroup_id: Some(0UL),
    object_id: 1UL,
    publisher_priority: 0,
    datagram_forwarding: false,
    properties: b"",
    payload: b"historical",
  }
  let data = @protocol.encode_fetch_header({
      request_id: request_id.to_uint64(),
    }) +
    @protocol.FetchDataEncoder::new(@protocol.Ascending)
    .encode(@protocol.FetchDataObject(object))
    .unwrap()
  assert_eq(
    client.receive(@session.InboundStreamStarted(30, @session.Unidirectional)),
    Ok([]),
  )
  assert_eq(
    client.receive(@session.StreamData(30, data[:1].to_owned())),
    Ok([]),
  )
  assert_eq(
    client.receive(@session.StreamData(30, data[1:2].to_owned())),
    Ok([]),
  )
  assert_eq(
    client.receive(@session.StreamData(30, data[2:].to_owned())),
    Ok([@session.EmitNotice(@session.FetchObjectReceived(request_id, object))]),
  )
  assert_eq(client.receive(@session.InboundStreamFinished(30)), Ok([]))

  let response : @protocol.FetchOkMessage = {
    end_of_track: 0,
    end_location,
    parameters: [],
    track_properties: b"",
  }
  assert_eq(
    client.receive(
      @session.StreamData(20, @protocol.encode_fetch_ok(response).unwrap()),
    ),
    Ok([
      @session.EmitNotice(@session.FetchAccepted(request_id, 20)),
      @session.EmitNotice(
        @session.FetchResponseAccepted(request_id, 20, response),
      ),
      @session.EmitNotice(@session.FetchFinished(request_id)),
    ]),
  )
}

///|
test "peer Standalone FETCH is accepted and served on one data stream" {
  let server = client_session_ready()
  let track = client_track(b"archive")
  let start : @protocol.FetchLocation = { group: 1UL, object: 0UL, }
  let end_location : @protocol.FetchLocation = { group: 1UL, object: 3UL, }
  let fetch : @protocol.FetchMessage = {
    request_id: 1UL,
    fetch_type: @protocol.StandaloneFetch(track, start, end_location),
    parameters: [],
  }
  assert_eq(
    server.receive(@session.InboundStreamStarted(20, @session.Bidirectional)),
    Ok([]),
  )
  assert_eq(
    server.receive(
      @session.StreamData(20, @protocol.encode_fetch(fetch).unwrap()),
    ),
    Ok([
      @session.EmitNotice(
        @session.FetchRequested(1UL, 20, track, start, end_location, []),
      ),
    ]),
  )

  let response : @protocol.FetchOkMessage = {
    end_of_track: 0,
    end_location,
    parameters: [],
    track_properties: b"",
  }
  assert_eq(
    server.accept_fetch(1UL, response),
    Ok([
      @session.SendChunk(20, @protocol.encode_fetch_ok(response).unwrap()),
      @session.EmitNotice(@session.FetchAccepted(1UL, 20)),
      @session.OpenStream(@session.Unidirectional, @session.FetchData(1UL)),
    ]),
  )
  assert_eq(
    server.receive(@session.StreamOpened(@session.FetchData(1UL), 30)),
    Ok([
      @session.SendChunk(
        30,
        @protocol.encode_fetch_header({ request_id: 1UL, }),
      ),
    ]),
  )
  let object : @protocol.FetchObject = {
    group_id: 1UL,
    subgroup_id: Some(0UL),
    object_id: 0UL,
    publisher_priority: 0,
    datagram_forwarding: false,
    properties: b"",
    payload: b"object",
  }
  assert_eq(
    server.send_fetch_object(1UL, object),
    Ok([
      @session.SendChunk(
        30,
        @protocol.FetchDataEncoder::new(@protocol.Ascending)
        .encode(@protocol.FetchDataObject(object))
        .unwrap(),
      ),
    ]),
  )
  assert_eq(server.finish_fetch(1UL), Ok([@session.SendFin(30, b"")]))
}

///|
test "local Joining FETCH uses the referenced SUBSCRIBE request ID" {
  let publishing_endpoint = client_session_ready()
  ignore(
    publishing_endpoint.publish_track(client_track(b"published"), 9UL).unwrap(),
  )
  assert_eq(
    publishing_endpoint.fetch_joining_relative(
      subscription_request_id=0UL,
      groups_back=0UL,
      parameters=[],
    ),
    Err(@session.UnknownRequest),
  )

  let client = client_session_ready()
  let track = client_track(b"live")
  let (subscription_id, _) = client.subscribe_track(track).unwrap()
  assert_eq(subscription_id, @session.RequestId(0UL))
  ignore(client.receive(@session.StreamOpened(@session.Request(0UL), 20)))
  let subscribe_ok : @protocol.SubscribeOkMessage = {
    track_alias: 7UL,
    parameters: [{ kind: 0x09UL, value: @protocol.Location(12UL, 3UL), }],
    properties: b"",
  }
  assert_eq(
    client.receive(
      @session.StreamData(
        20,
        @protocol.encode_subscribe_ok(subscribe_ok).unwrap(),
      ),
    ),
    Ok([@session.EmitNotice(@session.SubscribeEstablished(0UL, 7UL))]),
  )
  let (fetch_id, actions) = client
    .fetch_joining_relative(subscription_request_id=0UL, groups_back=4UL, parameters=[])
    .unwrap()
  assert_eq(fetch_id, @session.RequestId(2UL))
  assert_eq(actions, [
    @session.OpenStream(@session.Bidirectional, @session.Request(2UL)),
  ])
  let fetch : @protocol.FetchMessage = {
    request_id: fetch_id.to_uint64(),
    fetch_type: @protocol.RelativeJoiningFetch(0UL, 4UL),
    parameters: [],
  }
  assert_eq(
    client.receive(@session.StreamOpened(@session.Request(fetch_id), 22)),
    Ok([@session.SendChunk(22, @protocol.encode_fetch(fetch).unwrap())]),
  )
  let (absolute_id, _) = client
    .fetch_joining_absolute(subscription_request_id=0UL, start_group=8UL, parameters=[])
    .unwrap()
  let absolute_fetch : @protocol.FetchMessage = {
    request_id: absolute_id.to_uint64(),
    fetch_type: @protocol.AbsoluteJoiningFetch(0UL, 8UL),
    parameters: [],
  }
  assert_eq(
    client.receive(@session.StreamOpened(@session.Request(absolute_id), 24)),
    Ok([@session.SendChunk(24, @protocol.encode_fetch(absolute_fetch).unwrap())]),
  )
  assert_eq(
    client.fetch_joining_absolute(
      subscription_request_id=0UL,
      start_group=13UL,
      parameters=[],
    ),
    Err(@session.InvalidFetchRange),
  )
}

///|
test "SUBSCRIBE sends its initial filter parameters" {
  let client = client_session_ready()
  let track = client_track(b"live")
  let parameters : Array[@protocol.MessageParameter] = [
    { kind: 0x21UL, value: @protocol.Filter(@protocol.LargestObject), },
  ]
  let (request_id, actions) = client
    .subscribe_track(track, parameters~)
    .unwrap()
  assert_eq(request_id, @session.RequestId(0UL))
  assert_eq(actions, [
    @session.OpenStream(@session.Bidirectional, @session.Request(0UL)),
  ])
  assert_eq(
    client.receive(@session.StreamOpened(@session.Request(0UL), 20)),
    Ok([
      @session.SendChunk(
        20,
        @protocol.encode_subscribe({
          request_id: request_id.to_uint64(),
          track,
          parameters,
        }).unwrap(),
      ),
    ]),
  )
}

///|
test "pending Joining FETCH resolves against the accepted subscription snapshot" {
  let publisher = client_session_ready()
  let track = client_track(b"live")
  ignore(publisher.publish_track(track, 7UL).unwrap())
  ignore(publisher.receive(@session.StreamOpened(@session.Request(0UL), 20)))
  ignore(publisher.receive(@session.StreamData(20, b"\x07\x00\x01\x00")))

  ignore(
    publisher.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
  )
  ignore(publisher.accept_subscription(@session.StreamId(40)).unwrap())
  let header : @protocol.SubgroupHeader = {
    track_alias: 7UL,
    group_id: 12UL,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: false,
  }
  ignore(publisher.open_object_stream(@session.StreamId(40), header).unwrap())
  ignore(publisher.receive(@session.StreamOpened(@session.Object(0UL), 30)))
  ignore(
    publisher
    .send_object(@session.StreamId(30), {
      object_id: 3UL,
      status: @protocol.Normal,
      properties: b"",
      payload: b"live-edge",
    })
    .unwrap(),
  )

  ignore(
    publisher.receive(@session.InboundStreamStarted(42, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        42,
        @protocol.encode_subscribe({ request_id: 3UL, track, parameters: [], }).unwrap(),
      ),
    ),
  )
  ignore(
    publisher.receive(@session.InboundStreamStarted(44, @session.Bidirectional)),
  )
  let fetch : @protocol.FetchMessage = {
    request_id: 5UL,
    fetch_type: @protocol.RelativeJoiningFetch(3UL, 4UL),
    parameters: [],
  }
  assert_eq(
    publisher.receive(
      @session.StreamData(44, @protocol.encode_fetch(fetch).unwrap()),
    ),
    Ok([]),
  )
  assert_eq(
    publisher.accept_subscription(@session.StreamId(42)),
    Ok([
      @session.SendChunk(
        42,
        @protocol.encode_subscribe_ok({
          track_alias: 7UL,
          parameters: [{ kind: 0x09UL, value: @protocol.Location(12UL, 3UL), }],
          properties: b"",
        }).unwrap(),
      ),
      @session.EmitNotice(@session.SubscriptionAccepted(3UL, 42, 7UL)),
      @session.EmitNotice(
        @session.FetchRequested(
          5UL,
          44,
          track,
          { group: 8UL, object: 0UL, },
          { group: 12UL, object: 4UL, },
          [],
        ),
      ),
    ]),
  )
  ignore(
    publisher.receive(@session.InboundStreamStarted(46, @session.Bidirectional)),
  )
  let absolute_fetch = @protocol.encode_fetch({
    request_id: 7UL,
    fetch_type: @protocol.AbsoluteJoiningFetch(3UL, 9UL),
    parameters: [],
  }).unwrap()
  assert_eq(
    publisher.receive(@session.StreamData(46, absolute_fetch)),
    Ok([
      @session.EmitNotice(
        @session.FetchRequested(
          7UL,
          46,
          track,
          { group: 9UL, object: 0UL, },
          { group: 12UL, object: 4UL, },
          [],
        ),
      ),
    ]),
  )
  ignore(
    publisher
    .send_object(@session.StreamId(30), {
      object_id: 4UL,
      status: @protocol.Normal,
      properties: b"",
      payload: b"first-live-object-after-join",
    })
    .unwrap(),
  )
  ignore(
    publisher.receive(@session.InboundStreamStarted(48, @session.Bidirectional)),
  )
  let relative_underflow = @protocol.encode_fetch({
    request_id: 9UL,
    fetch_type: @protocol.RelativeJoiningFetch(3UL, 13UL),
    parameters: [],
  }).unwrap()
  let invalid_range : @protocol.RequestErrorMessage = {
    code: 0x10UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.receive(@session.StreamData(48, relative_underflow)),
    Ok([
      @session.SendFin(
        48,
        @protocol.encode_request_error(invalid_range).unwrap(),
      ),
      @session.EmitNotice(@session.FetchRejected(9UL, 48, invalid_range)),
    ]),
  )

  ignore(
    publisher
    .send_object(@session.StreamId(30), {
      object_id: 0x3fffffffffffffffUL,
      status: @protocol.Normal,
      properties: b"",
      payload: b"edge",
    })
    .unwrap(),
  )
  ignore(
    publisher.receive(@session.InboundStreamStarted(50, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        50,
        @protocol.encode_subscribe({ request_id: 11UL, track, parameters: [], }).unwrap(),
      ),
    ),
  )
  ignore(publisher.accept_subscription(@session.StreamId(50)).unwrap())
  ignore(
    publisher.receive(@session.InboundStreamStarted(52, @session.Bidirectional)),
  )
  let max_object_joining_fetch = @protocol.encode_fetch({
    request_id: 13UL,
    fetch_type: @protocol.RelativeJoiningFetch(11UL, 0UL),
    parameters: [],
  }).unwrap()
  assert_eq(
    publisher.receive(@session.StreamData(52, max_object_joining_fetch)),
    Ok([
      @session.EmitNotice(
        @session.FetchRequested(
          13UL,
          52,
          track,
          { group: 12UL, object: 0UL, },
          { group: 12UL, object: 0x4000000000000000UL, },
          [],
        ),
      ),
    ]),
  )
}

///|
test "pending Joining FETCH times out and reports TIMEOUT" {
  let publisher = client_session_ready()
  let track = client_track(b"live")
  ignore(publisher.publish_track(track, 7UL).unwrap())
  ignore(publisher.receive(@session.StreamOpened(@session.Request(0UL), 20)))
  ignore(publisher.receive(@session.StreamData(20, b"\x07\x00\x01\x00")))
  ignore(
    publisher.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
  )
  ignore(
    publisher.receive(@session.InboundStreamStarted(42, @session.Bidirectional)),
  )
  let fetch : @protocol.FetchMessage = {
    request_id: 3UL,
    fetch_type: @protocol.RelativeJoiningFetch(1UL, 0UL),
    parameters: [],
  }
  assert_eq(
    publisher.receive(
      @session.StreamData(42, @protocol.encode_fetch(fetch).unwrap()),
    ),
    Ok([]),
  )
  let timeout : @protocol.RequestErrorMessage = {
    code: 2UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.receive(@session.Clock(30000UL)),
    Ok([
      @session.SendFin(42, @protocol.encode_request_error(timeout).unwrap()),
      @session.EmitNotice(@session.FetchRejected(3UL, 42, timeout)),
    ]),
  )
}

///|
test "Joining FETCH rejects unknown subscriptions and Forward State zero" {
  let publisher = client_session_ready()
  let track = client_track(b"live")
  ignore(publisher.publish_track(track, 7UL).unwrap())
  ignore(publisher.receive(@session.StreamOpened(@session.Request(0UL), 20)))
  ignore(publisher.receive(@session.StreamData(20, b"\x07\x00\x01\x00")))
  let fetch_for = fn(request_id, joining_id) {
    @protocol.encode_fetch({
      request_id,
      fetch_type: @protocol.AbsoluteJoiningFetch(joining_id, 0UL),
      parameters: [],
    }).unwrap()
  }
  let invalid_joining_id : @protocol.RequestErrorMessage = {
    code: 0x32UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  ignore(
    publisher.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
  )
  assert_eq(
    publisher.receive(@session.StreamData(40, fetch_for(1UL, 99UL))),
    Ok([
      @session.SendFin(
        40,
        @protocol.encode_request_error(invalid_joining_id).unwrap(),
      ),
      @session.EmitNotice(@session.FetchRejected(1UL, 40, invalid_joining_id)),
    ]),
  )

  ignore(
    publisher.receive(@session.InboundStreamStarted(42, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        42,
        @protocol.encode_subscribe({
          request_id: 3UL,
          track,
          parameters: [{ kind: 0x10UL, value: @protocol.Byte(0), }],
        }).unwrap(),
      ),
    ),
  )
  ignore(publisher.accept_subscription(@session.StreamId(42)).unwrap())
  ignore(
    publisher.receive(@session.InboundStreamStarted(44, @session.Bidirectional)),
  )
  let invalid_range : @protocol.RequestErrorMessage = {
    code: 0x10UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.receive(@session.StreamData(44, fetch_for(5UL, 3UL))),
    Ok([
      @session.SendFin(
        44,
        @protocol.encode_request_error(invalid_range).unwrap(),
      ),
      @session.EmitNotice(@session.FetchRejected(5UL, 44, invalid_range)),
    ]),
  )
}

///|
test "pending Joining FETCH is rejected when its subscription is refused" {
  let publisher = client_session_ready()
  let track = client_track(b"live")
  ignore(publisher.publish_track(track, 7UL).unwrap())
  ignore(publisher.receive(@session.StreamOpened(@session.Request(0UL), 20)))
  ignore(publisher.receive(@session.StreamData(20, b"\x07\x00\x01\x00")))
  ignore(
    publisher.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
  )
  ignore(
    publisher.receive(@session.InboundStreamStarted(42, @session.Bidirectional)),
  )
  let fetch : @protocol.FetchMessage = {
    request_id: 3UL,
    fetch_type: @protocol.RelativeJoiningFetch(1UL, 0UL),
    parameters: [],
  }
  assert_eq(
    publisher.receive(
      @session.StreamData(42, @protocol.encode_fetch(fetch).unwrap()),
    ),
    Ok([]),
  )
  let subscribe_error : @protocol.RequestErrorMessage = {
    code: 0x20UL,
    retry_interval: 0UL,
    reason: b"not interested",
    redirect: b"",
  }
  let joining_error : @protocol.RequestErrorMessage = {
    code: 0x32UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.reject_subscription(@session.StreamId(40), subscribe_error),
    Ok([
      @session.SendFin(
        40,
        @protocol.encode_request_error(subscribe_error).unwrap(),
      ),
      @session.EmitNotice(@session.SubscriptionRejected(1UL, 40)),
      @session.SendFin(
        42,
        @protocol.encode_request_error(joining_error).unwrap(),
      ),
      @session.EmitNotice(@session.FetchRejected(3UL, 42, joining_error)),
    ]),
  )
  ignore(
    publisher.receive(@session.InboundStreamStarted(44, @session.Bidirectional)),
  )
  let after_end : @protocol.FetchMessage = {
    request_id: 5UL,
    fetch_type: @protocol.RelativeJoiningFetch(1UL, 0UL),
    parameters: [],
  }
  assert_eq(
    publisher.receive(
      @session.StreamData(44, @protocol.encode_fetch(after_end).unwrap()),
    ),
    Ok([
      @session.SendFin(
        44,
        @protocol.encode_request_error(joining_error).unwrap(),
      ),
      @session.EmitNotice(@session.FetchRejected(5UL, 44, joining_error)),
    ]),
  )
}

///|
test "cancelling a pending subscription releases its Joining FETCH" {
  let publisher = client_session_ready()
  let track = client_track(b"live")
  ignore(publisher.publish_track(track, 7UL).unwrap())
  ignore(publisher.receive(@session.StreamOpened(@session.Request(0UL), 20)))
  ignore(publisher.receive(@session.StreamData(20, b"\x07\x00\x01\x00")))
  ignore(
    publisher.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
  )
  ignore(
    publisher.receive(@session.InboundStreamStarted(42, @session.Bidirectional)),
  )
  let fetch : @protocol.FetchMessage = {
    request_id: 3UL,
    fetch_type: @protocol.RelativeJoiningFetch(1UL, 0UL),
    parameters: [],
  }
  assert_eq(
    publisher.receive(
      @session.StreamData(42, @protocol.encode_fetch(fetch).unwrap()),
    ),
    Ok([]),
  )
  let invalid_joining_id : @protocol.RequestErrorMessage = {
    code: 0x32UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.cancel_request(1UL),
    Ok([
      @session.SendFin(
        42,
        @protocol.encode_request_error(invalid_joining_id).unwrap(),
      ),
      @session.EmitNotice(@session.FetchRejected(3UL, 42, invalid_joining_id)),
      @session.AbortSend(40, 1UL),
      @session.StopReceiving(40, 1UL),
      @session.EmitNotice(@session.SubscriptionEnded(1UL, 40, track)),
    ]),
  )
  assert_eq(publisher.receive(@session.Clock(30000UL)), Ok([]))
}

///|
test "PUBLISH request ID is a valid Joining subscription" {
  let publisher = client_session_ready()
  let track = client_track(b"live")
  ignore(publisher.publish_track(track, 7UL).unwrap())
  ignore(publisher.receive(@session.StreamOpened(@session.Request(0UL), 20)))
  assert_eq(
    publisher.receive(
      @session.StreamData(
        20,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          @protocol.RequestOkContext::Publish,
        ).unwrap(),
      ),
    ),
    Ok([@session.EmitNotice(@session.PublishAccepted(0UL, track, 7UL))]),
  )

  let header : @protocol.SubgroupHeader = {
    track_alias: 7UL,
    group_id: 4UL,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: false,
  }
  ignore(publisher.open_object_stream(@session.StreamId(20), header).unwrap())
  ignore(publisher.receive(@session.StreamOpened(@session.Object(0UL), 32)))
  ignore(
    publisher
    .send_object(@session.StreamId(32), {
      object_id: 0UL,
      status: @protocol.Normal,
      properties: b"",
      payload: b"new-live-group",
    })
    .unwrap(),
  )
  ignore(
    publisher.receive(@session.InboundStreamStarted(34, @session.Bidirectional)),
  )
  let joining_fetch = @protocol.encode_fetch({
    request_id: 1UL,
    fetch_type: @protocol.AbsoluteJoiningFetch(0UL, 2UL),
    parameters: [],
  }).unwrap()
  assert_eq(
    publisher.receive(@session.StreamData(34, joining_fetch)),
    Ok([
      @session.EmitNotice(
        @session.FetchRequested(
          1UL,
          34,
          track,
          { group: 2UL, object: 0UL, },
          { group: 4UL, object: 1UL, },
          [],
        ),
      ),
    ]),
  )
}

///|
test "empty successful FETCH requires an empty data stream and request errors fail once" {
  let client = client_session_ready()
  let track = client_track(b"archive")
  let start : @protocol.FetchLocation = { group: 0UL, object: 0UL, }
  let end_location : @protocol.FetchLocation = { group: 0UL, object: 1UL, }
  let (request_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  ignore(
    client.receive(@session.StreamOpened(@session.Request(request_id), 20)),
  )
  assert_eq(
    client.receive(@session.InboundStreamStarted(30, @session.Unidirectional)),
    Ok([]),
  )
  assert_eq(
    client.receive(
      @session.StreamData(
        30,
        @protocol.encode_fetch_header({ request_id: request_id.to_uint64(), }),
      ),
    ),
    Ok([]),
  )
  assert_eq(client.receive(@session.InboundStreamFinished(30)), Ok([]))
  let response : @protocol.FetchOkMessage = {
    end_of_track: 1,
    end_location,
    parameters: [],
    track_properties: b"",
  }
  assert_eq(
    client.receive(
      @session.StreamData(20, @protocol.encode_fetch_ok(response).unwrap()),
    ),
    Ok([
      @session.EmitNotice(@session.FetchAccepted(request_id, 20)),
      @session.EmitNotice(
        @session.FetchResponseAccepted(request_id, 20, response),
      ),
      @session.EmitNotice(@session.FetchEndOfTrack(request_id)),
      @session.EmitNotice(@session.FetchFinished(request_id)),
    ]),
  )

  let (failed_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  ignore(client.receive(@session.StreamOpened(@session.Request(failed_id), 21)))
  let error : @protocol.RequestErrorMessage = {
    code: 0x10UL,
    retry_interval: 0UL,
    reason: b"missing",
    redirect: b"",
  }
  assert_eq(
    client.receive(
      @session.StreamData(21, @protocol.encode_request_error(error).unwrap()),
    ),
    Ok([@session.EmitNotice(@session.FetchFailed(failed_id, error))]),
  )
  assert_eq(client.receive(@session.StreamData(21, b"late")), Ok([]))
}

///|
test "FETCH priority updates are acknowledged, and rejected updates reset data" {
  let server = client_session_ready()
  let track = client_track(b"archive")
  let start : @protocol.FetchLocation = { group: 0UL, object: 0UL, }
  let end_location : @protocol.FetchLocation = { group: 0UL, object: 4UL, }
  let fetch : @protocol.FetchMessage = {
    request_id: 1UL,
    fetch_type: @protocol.StandaloneFetch(track, start, end_location),
    parameters: [],
  }
  ignore(
    server.receive(@session.InboundStreamStarted(20, @session.Bidirectional)),
  )
  ignore(
    server.receive(
      @session.StreamData(20, @protocol.encode_fetch(fetch).unwrap()),
    ),
  )
  ignore(
    server
    .accept_fetch(1UL, {
      end_of_track: 0,
      end_location,
      parameters: [],
      track_properties: b"",
    })
    .unwrap(),
  )
  ignore(server.receive(@session.StreamOpened(@session.FetchData(1UL), 30)))

  let priority : @protocol.MessageParameter = {
    kind: 0x20UL,
    value: @protocol.Byte(2),
  }
  let update : @protocol.RequestUpdateMessage = {
    request_id: 3UL,
    parameters: [priority],
  }
  assert_eq(
    server.receive(
      @session.StreamData(
        20,
        @protocol.encode_request_update_for(
          update,
          @protocol.FetchRequestStream,
        ).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.FetchUpdateRequested(1UL, 3UL, 20, [priority]),
      ),
    ]),
  )
  assert_eq(
    server.accept_fetch_update(1UL, 3UL),
    Ok([
      @session.SendChunk(
        20,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          @protocol.RequestUpdate,
        ).unwrap(),
      ),
    ]),
  )

  let rejected_update : @protocol.RequestUpdateMessage = {
    request_id: 5UL,
    parameters: [priority],
  }
  assert_eq(
    server.receive(
      @session.StreamData(
        20,
        @protocol.encode_request_update_for(
          rejected_update,
          @protocol.FetchRequestStream,
        ).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.FetchUpdateRequested(1UL, 5UL, 20, [priority]),
      ),
    ]),
  )
  let error : @protocol.RequestErrorMessage = {
    code: 0x10UL,
    retry_interval: 0UL,
    reason: b"denied",
    redirect: b"",
  }
  assert_eq(
    server.reject_fetch_update(1UL, 5UL, error),
    Ok([
      @session.SendChunk(20, @protocol.encode_request_error(error).unwrap()),
      @session.AbortSend(30, error.code),
      @session.EmitNotice(@session.FetchFailed(1UL, error)),
    ]),
  )
}

///|
test "cancel before stream-open ignores a late FETCH data header and bytes" {
  let client = client_session_ready()
  let track = client_track(b"archive")
  let start : @protocol.FetchLocation = { group: 0UL, object: 0UL, }
  let end_location : @protocol.FetchLocation = { group: 1UL, object: 0UL, }
  let (request_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  assert_eq(
    client.cancel_fetch(request_id),
    Ok([@session.EmitNotice(@session.FetchCancelled(request_id, 1UL))]),
  )
  assert_eq(
    client.receive(@session.StreamOpened(@session.Request(request_id), 20)),
    Ok([@session.AbortSend(20, 1UL), @session.StopReceiving(20, 1UL)]),
  )
  assert_eq(
    client.receive(@session.InboundStreamStarted(30, @session.Unidirectional)),
    Ok([]),
  )
  assert_eq(
    client.receive(
      @session.StreamData(
        30,
        @protocol.encode_fetch_header({ request_id: request_id.to_uint64(), }) +
        b"late bytes",
      ),
    ),
    Ok([]),
  )
  assert_eq(client.receive(@session.InboundStreamFinished(30)), Ok([]))
  assert_eq(client.cancel_request(request_id), Ok([]))
}

///|
test "FETCH data reset and GOAWAY each terminate pending requests once" {
  let client = client_session_ready()
  let track = client_track(b"archive")
  let start : @protocol.FetchLocation = { group: 0UL, object: 0UL, }
  let end_location : @protocol.FetchLocation = { group: 1UL, object: 0UL, }
  let (reset_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  ignore(client.receive(@session.StreamOpened(@session.Request(reset_id), 20)))
  ignore(
    client.receive(@session.InboundStreamStarted(30, @session.Unidirectional)),
  )
  ignore(
    client.receive(
      @session.StreamData(
        30,
        @protocol.encode_fetch_header({ request_id: reset_id.to_uint64(), }),
      ),
    ),
  )
  assert_eq(
    client.receive(@session.StreamReset(30, 7UL)),
    Ok([@session.EmitNotice(@session.FetchReset(reset_id, 30, 7UL))]),
  )
  assert_eq(client.receive(@session.StreamReset(30, 7UL)), Ok([]))

  let (goaway_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  ignore(client.receive(@session.StreamOpened(@session.Request(goaway_id), 21)))
  assert_eq(
    client.receive(
      @session.StreamData(11, client_goaway_control(0UL, 0UL, b"")),
    ),
    Ok([
      @session.EmitNotice(
        @session.ControlMessageReceived(
          @protocol.GoAwayControl({
            uri: b"",
            timeout_ms: 0UL,
            request_id: 0UL,
          }),
        ),
      ),
      @session.EmitNotice(@session.ControlGoAwayReceived(b"", 0UL, 0UL)),
      @session.AbortSend(21, 4UL),
      @session.StopReceiving(21, 4UL),
      @session.EmitNotice(
        @session.FetchFailed(goaway_id, {
          code: 6UL,
          retry_interval: 0UL,
          reason: b"",
          redirect: b"",
        }),
      ),
    ]),
  )
}

///|
test "concurrent FETCH requests route independent data streams and local updates" {
  let client = client_session_ready()
  let track = client_track(b"archive")
  let start : @protocol.FetchLocation = { group: 0UL, object: 0UL, }
  let end_location : @protocol.FetchLocation = { group: 4UL, object: 0UL, }
  let (first_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  let (second_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  ignore(client.receive(@session.StreamOpened(@session.Request(first_id), 20)))
  ignore(client.receive(@session.StreamOpened(@session.Request(second_id), 22)))

  let first_object : @protocol.FetchObject = {
    group_id: 1UL,
    subgroup_id: Some(0UL),
    object_id: 0UL,
    publisher_priority: 1,
    datagram_forwarding: false,
    properties: b"",
    payload: b"first",
  }
  let second_object : @protocol.FetchObject = {
    group_id: 2UL,
    subgroup_id: Some(0UL),
    object_id: 0UL,
    publisher_priority: 1,
    datagram_forwarding: false,
    properties: b"",
    payload: b"second",
  }
  ignore(
    client.receive(@session.InboundStreamStarted(30, @session.Unidirectional)),
  )
  ignore(
    client.receive(@session.InboundStreamStarted(32, @session.Unidirectional)),
  )
  assert_eq(
    client.receive(
      @session.StreamData(
        30,
        @protocol.encode_fetch_header({ request_id: first_id.to_uint64(), }) +
        @protocol.FetchDataEncoder::new(@protocol.Ascending)
        .encode(@protocol.FetchDataObject(first_object))
        .unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(@session.FetchObjectReceived(first_id, first_object)),
    ]),
  )
  assert_eq(
    client.receive(
      @session.StreamData(
        32,
        @protocol.encode_fetch_header({ request_id: second_id.to_uint64(), }) +
        @protocol.FetchDataEncoder::new(@protocol.Ascending)
        .encode(@protocol.FetchDataObject(second_object))
        .unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.FetchObjectReceived(second_id, second_object),
      ),
    ]),
  )
  let response : @protocol.FetchOkMessage = {
    end_of_track: 0,
    end_location,
    parameters: [],
    track_properties: b"",
  }
  ignore(
    client.receive(
      @session.StreamData(20, @protocol.encode_fetch_ok(response).unwrap()),
    ),
  )
  ignore(
    client.receive(
      @session.StreamData(22, @protocol.encode_fetch_ok(response).unwrap()),
    ),
  )

  let priority : @protocol.MessageParameter = {
    kind: 0x20UL,
    value: @protocol.Byte(1),
  }
  let authorization : @protocol.MessageParameter = {
    kind: 0x03UL,
    value: @protocol.Data(b"\x03\x01\x00\x01x"),
  }
  assert_eq(
    client.update_fetch(first_id, [authorization, priority]),
    Ok(
      (
        4UL,
        [
          @session.SendChunk(
            20,
            @protocol.encode_request_update_for(
              { request_id: 4UL, parameters: [authorization, priority], },
              @protocol.FetchRequestStream,
            ).unwrap(),
          ),
        ],
      ),
    ),
  )
  let update_ok : @protocol.RequestOkMessage = {
    parameters: [],
    properties: b"",
  }
  assert_eq(
    client.receive(
      @session.StreamData(
        20,
        @protocol.encode_request_ok_for(update_ok, @protocol.FetchUpdate).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.FetchUpdateAccepted(first_id, 4UL, update_ok),
      ),
    ]),
  )
}

///|
test "peer FETCH can be rejected before opening its data stream" {
  let server = client_session_ready()
  let track = client_track(b"archive")
  let start : @protocol.FetchLocation = { group: 0UL, object: 0UL, }
  let end_location : @protocol.FetchLocation = { group: 0UL, object: 1UL, }
  let fetch : @protocol.FetchMessage = {
    request_id: 1UL,
    fetch_type: @protocol.StandaloneFetch(track, start, end_location),
    parameters: [],
  }
  ignore(
    server.receive(@session.InboundStreamStarted(20, @session.Bidirectional)),
  )
  ignore(
    server.receive(
      @session.StreamData(20, @protocol.encode_fetch(fetch).unwrap()),
    ),
  )
  let error : @protocol.RequestErrorMessage = {
    code: 0x10UL,
    retry_interval: 0UL,
    reason: b"unavailable",
    redirect: b"",
  }
  assert_eq(
    server.reject_fetch(1UL, error),
    Ok([
      @session.SendFin(20, @protocol.encode_request_error(error).unwrap()),
      @session.EmitNotice(@session.FetchRejected(1UL, 20, error)),
    ]),
  )
}
