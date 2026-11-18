///|
test "Standalone FETCH accepts data before its response and finishes after FIN" {
  let client = client_session_ready()
  let track = client_track(b"archive")
  let start = @protocol.FetchLocation::{ group: 2UL, object: 1UL, }
  let end_location = @protocol.FetchLocation::{ group: 3UL, object: 0UL, }
  let (request_id, actions) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  assert_eq(request_id, RequestId(0UL))
  assert_eq(actions, [OpenStream(Bidirectional, Request(0UL))])
  let fetch = @protocol.FetchMessage::{
    request_id: request_id.to_uint64(),
    fetch_type: StandaloneFetch(track, start, end_location),
    parameters: [],
  }
  assert_eq(
    client.receive(StreamOpened(Request(request_id), 20)),
    Ok([SendChunk(20, @protocol.encode_fetch(fetch).unwrap())]),
  )

  let object = @protocol.FetchObject::{
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
    @protocol.FetchDataEncoder::new(Ascending)
    .encode(FetchDataObject(object))
    .unwrap()
  assert_eq(client.receive(InboundStreamStarted(30, Unidirectional)), Ok([]))
  assert_eq(client.receive(StreamData(30, data[:1].to_owned())), Ok([]))
  assert_eq(client.receive(StreamData(30, data[1:2].to_owned())), Ok([]))
  assert_eq(
    client.receive(StreamData(30, data[2:].to_owned())),
    Ok([EmitNotice(FetchObjectReceived(request_id, object))]),
  )
  assert_eq(client.receive(InboundStreamFinished(30)), Ok([]))

  let response = @protocol.FetchOkMessage::{
    end_of_track: 0,
    end_location,
    parameters: [],
    track_properties: b"",
  }
  assert_eq(
    client.receive(StreamData(20, @protocol.encode_fetch_ok(response).unwrap())),
    Ok([
      EmitNotice(FetchAccepted(request_id, 20)),
      EmitNotice(FetchResponseAccepted(request_id, 20, response)),
      SendFin(20, b""),
      EmitNotice(FetchFinished(request_id)),
    ]),
  )
}

///|
test "peer Standalone FETCH is accepted and served on one data stream" {
  let server = client_session_ready()
  let track = client_track(b"archive")
  let start = @protocol.FetchLocation::{ group: 1UL, object: 0UL, }
  let end_location = @protocol.FetchLocation::{ group: 1UL, object: 3UL, }
  let fetch = @protocol.FetchMessage::{
    request_id: 1UL,
    fetch_type: StandaloneFetch(track, start, end_location),
    parameters: [],
  }
  assert_eq(server.receive(InboundStreamStarted(20, Bidirectional)), Ok([]))
  assert_eq(
    server.receive(StreamData(20, @protocol.encode_fetch(fetch).unwrap())),
    Ok([EmitNotice(FetchRequested(1UL, 20, track, start, end_location, []))]),
  )

  let response = @protocol.FetchOkMessage::{
    end_of_track: 0,
    end_location,
    parameters: [],
    track_properties: b"",
  }
  assert_eq(
    server.accept_fetch(1UL, response),
    Ok([
      SendChunk(20, @protocol.encode_fetch_ok(response).unwrap()),
      EmitNotice(FetchAccepted(1UL, 20)),
      OpenStream(Unidirectional, FetchData(1UL)),
    ]),
  )
  assert_eq(
    server.receive(StreamOpened(FetchData(1UL), 30)),
    Ok([SendChunk(30, @protocol.encode_fetch_header({ request_id: 1UL, }))]),
  )
  let object = @protocol.FetchObject::{
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
      SendChunk(
        30,
        @protocol.FetchDataEncoder::new(Ascending)
        .encode(FetchDataObject(object))
        .unwrap(),
      ),
    ]),
  )
  assert_eq(server.finish_fetch(1UL), Ok([SendFin(30, b"")]))
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
    Err(UnknownRequest),
  )

  let client = client_session_ready()
  let track = client_track(b"live")
  let (subscription_id, _) = client.subscribe_track(track).unwrap()
  assert_eq(subscription_id, RequestId(0UL))
  ignore(client.receive(StreamOpened(Request(0UL), 20)))
  let subscribe_ok = @protocol.SubscribeOkMessage::{
    track_alias: 7UL,
    parameters: [{ kind: 0x09UL, value: Location(12UL, 3UL), }],
    properties: b"",
  }
  assert_eq(
    client.receive(
      StreamData(20, @protocol.encode_subscribe_ok(subscribe_ok).unwrap()),
    ),
    Ok([EmitNotice(SubscribeEstablished(0UL, 7UL))]),
  )
  let (fetch_id, actions) = client
    .fetch_joining_relative(subscription_request_id=0UL, groups_back=4UL, parameters=[])
    .unwrap()
  assert_eq(fetch_id, RequestId(2UL))
  assert_eq(actions, [OpenStream(Bidirectional, Request(2UL))])
  let fetch = @protocol.FetchMessage::{
    request_id: fetch_id.to_uint64(),
    fetch_type: RelativeJoiningFetch(0UL, 4UL),
    parameters: [],
  }
  assert_eq(
    client.receive(StreamOpened(Request(fetch_id), 22)),
    Ok([SendChunk(22, @protocol.encode_fetch(fetch).unwrap())]),
  )
  let (absolute_id, _) = client
    .fetch_joining_absolute(subscription_request_id=0UL, start_group=8UL, parameters=[])
    .unwrap()
  let absolute_fetch = @protocol.FetchMessage::{
    request_id: absolute_id.to_uint64(),
    fetch_type: AbsoluteJoiningFetch(0UL, 8UL),
    parameters: [],
  }
  assert_eq(
    client.receive(StreamOpened(Request(absolute_id), 24)),
    Ok([SendChunk(24, @protocol.encode_fetch(absolute_fetch).unwrap())]),
  )
  assert_eq(
    client.fetch_joining_absolute(
      subscription_request_id=0UL,
      start_group=13UL,
      parameters=[],
    ),
    Err(InvalidFetchRange),
  )
}

///|
test "SUBSCRIBE sends its initial filter parameters" {
  let client = client_session_ready()
  let track = client_track(b"live")
  let parameters : Array[@protocol.MessageParameter] = [
    { kind: 0x21UL, value: Filter(LargestObject), },
  ]
  let (request_id, actions) = client
    .subscribe_track(track, parameters~)
    .unwrap()
  assert_eq(request_id, RequestId(0UL))
  assert_eq(actions, [OpenStream(Bidirectional, Request(0UL))])
  assert_eq(
    client.receive(StreamOpened(Request(0UL), 20)),
    Ok([
      SendChunk(
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
  ignore(publisher.receive(StreamOpened(Request(0UL), 20)))
  ignore(publisher.receive(StreamData(20, b"\x07\x00\x01\x00")))

  ignore(publisher.receive(InboundStreamStarted(40, Bidirectional)))
  ignore(
    publisher.receive(
      StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
  )
  ignore(publisher.accept_subscription(StreamId(40)).unwrap())
  let header = @protocol.SubgroupHeader::{
    track_alias: 7UL,
    group_id: 12UL,
    subgroup_id_mode: Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: false,
  }
  ignore(publisher.open_object_stream(StreamId(40), header).unwrap())
  ignore(publisher.receive(StreamOpened(Object(0UL), 30)))
  ignore(
    publisher
    .send_object(StreamId(30), {
      object_id: 3UL,
      status: Normal,
      properties: b"",
      payload: b"live-edge",
    })
    .unwrap(),
  )

  ignore(publisher.receive(InboundStreamStarted(42, Bidirectional)))
  ignore(
    publisher.receive(
      StreamData(
        42,
        @protocol.encode_subscribe({ request_id: 3UL, track, parameters: [], }).unwrap(),
      ),
    ),
  )
  ignore(publisher.receive(InboundStreamStarted(44, Bidirectional)))
  let fetch = @protocol.FetchMessage::{
    request_id: 5UL,
    fetch_type: RelativeJoiningFetch(3UL, 4UL),
    parameters: [],
  }
  assert_eq(
    publisher.receive(StreamData(44, @protocol.encode_fetch(fetch).unwrap())),
    Ok([]),
  )
  assert_eq(
    publisher.accept_subscription(StreamId(42)),
    Ok([
      SendChunk(
        42,
        @protocol.encode_subscribe_ok({
          track_alias: 7UL,
          parameters: [{ kind: 0x09UL, value: Location(12UL, 3UL), }],
          properties: b"",
        }).unwrap(),
      ),
      EmitNotice(SubscriptionAccepted(3UL, 42, 7UL)),
      EmitNotice(
        FetchRequested(
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
  ignore(publisher.receive(InboundStreamStarted(46, Bidirectional)))
  let absolute_fetch = @protocol.encode_fetch({
    request_id: 7UL,
    fetch_type: AbsoluteJoiningFetch(3UL, 9UL),
    parameters: [],
  }).unwrap()
  assert_eq(
    publisher.receive(StreamData(46, absolute_fetch)),
    Ok([
      EmitNotice(
        FetchRequested(
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
    .send_object(StreamId(30), {
      object_id: 4UL,
      status: Normal,
      properties: b"",
      payload: b"first-live-object-after-join",
    })
    .unwrap(),
  )
  ignore(publisher.receive(InboundStreamStarted(48, Bidirectional)))
  let relative_underflow = @protocol.encode_fetch({
    request_id: 9UL,
    fetch_type: RelativeJoiningFetch(3UL, 13UL),
    parameters: [],
  }).unwrap()
  let invalid_range = @protocol.RequestErrorMessage::{
    code: 0x10UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.receive(StreamData(48, relative_underflow)),
    Ok([
      SendFin(48, @protocol.encode_request_error(invalid_range).unwrap()),
      EmitNotice(FetchRejected(9UL, 48, invalid_range)),
    ]),
  )

  ignore(
    publisher
    .send_object(StreamId(30), {
      object_id: 0x3fffffffffffffffUL,
      status: Normal,
      properties: b"",
      payload: b"edge",
    })
    .unwrap(),
  )
  ignore(publisher.receive(InboundStreamStarted(50, Bidirectional)))
  ignore(
    publisher.receive(
      StreamData(
        50,
        @protocol.encode_subscribe({ request_id: 11UL, track, parameters: [], }).unwrap(),
      ),
    ),
  )
  ignore(publisher.accept_subscription(StreamId(50)).unwrap())
  ignore(publisher.receive(InboundStreamStarted(52, Bidirectional)))
  let max_object_joining_fetch = @protocol.encode_fetch({
    request_id: 13UL,
    fetch_type: RelativeJoiningFetch(11UL, 0UL),
    parameters: [],
  }).unwrap()
  assert_eq(
    publisher.receive(StreamData(52, max_object_joining_fetch)),
    Ok([
      EmitNotice(
        FetchRequested(
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
  ignore(publisher.receive(StreamOpened(Request(0UL), 20)))
  ignore(publisher.receive(StreamData(20, b"\x07\x00\x01\x00")))
  ignore(publisher.receive(InboundStreamStarted(40, Bidirectional)))
  ignore(
    publisher.receive(
      StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
  )
  ignore(publisher.receive(InboundStreamStarted(42, Bidirectional)))
  let fetch = @protocol.FetchMessage::{
    request_id: 3UL,
    fetch_type: RelativeJoiningFetch(1UL, 0UL),
    parameters: [],
  }
  assert_eq(
    publisher.receive(StreamData(42, @protocol.encode_fetch(fetch).unwrap())),
    Ok([]),
  )
  let timeout = @protocol.RequestErrorMessage::{
    code: 2UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.receive(Clock(30000UL)),
    Ok([
      SendFin(42, @protocol.encode_request_error(timeout).unwrap()),
      EmitNotice(FetchRejected(3UL, 42, timeout)),
    ]),
  )
}

///|
test "Joining FETCH rejects unknown subscriptions and Forward State zero" {
  let publisher = client_session_ready()
  let track = client_track(b"live")
  ignore(publisher.publish_track(track, 7UL).unwrap())
  ignore(publisher.receive(StreamOpened(Request(0UL), 20)))
  ignore(publisher.receive(StreamData(20, b"\x07\x00\x01\x00")))
  let fetch_for = fn(request_id, joining_id) {
    @protocol.encode_fetch({
      request_id,
      fetch_type: AbsoluteJoiningFetch(joining_id, 0UL),
      parameters: [],
    }).unwrap()
  }
  let invalid_joining_id = @protocol.RequestErrorMessage::{
    code: 0x32UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  ignore(publisher.receive(InboundStreamStarted(40, Bidirectional)))
  assert_eq(
    publisher.receive(StreamData(40, fetch_for(1UL, 99UL))),
    Ok([
      SendFin(40, @protocol.encode_request_error(invalid_joining_id).unwrap()),
      EmitNotice(FetchRejected(1UL, 40, invalid_joining_id)),
    ]),
  )

  ignore(publisher.receive(InboundStreamStarted(42, Bidirectional)))
  ignore(
    publisher.receive(
      StreamData(
        42,
        @protocol.encode_subscribe({
          request_id: 3UL,
          track,
          parameters: [{ kind: 0x10UL, value: Byte(0), }],
        }).unwrap(),
      ),
    ),
  )
  ignore(publisher.accept_subscription(StreamId(42)).unwrap())
  ignore(publisher.receive(InboundStreamStarted(44, Bidirectional)))
  let invalid_range = @protocol.RequestErrorMessage::{
    code: 0x10UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.receive(StreamData(44, fetch_for(5UL, 3UL))),
    Ok([
      SendFin(44, @protocol.encode_request_error(invalid_range).unwrap()),
      EmitNotice(FetchRejected(5UL, 44, invalid_range)),
    ]),
  )
}

///|
test "pending Joining FETCH is rejected when its subscription is refused" {
  let publisher = client_session_ready()
  let track = client_track(b"live")
  ignore(publisher.publish_track(track, 7UL).unwrap())
  ignore(publisher.receive(StreamOpened(Request(0UL), 20)))
  ignore(publisher.receive(StreamData(20, b"\x07\x00\x01\x00")))
  ignore(publisher.receive(InboundStreamStarted(40, Bidirectional)))
  ignore(
    publisher.receive(
      StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
  )
  ignore(publisher.receive(InboundStreamStarted(42, Bidirectional)))
  let fetch = @protocol.FetchMessage::{
    request_id: 3UL,
    fetch_type: RelativeJoiningFetch(1UL, 0UL),
    parameters: [],
  }
  assert_eq(
    publisher.receive(StreamData(42, @protocol.encode_fetch(fetch).unwrap())),
    Ok([]),
  )
  let subscribe_error = @protocol.RequestErrorMessage::{
    code: 0x20UL,
    retry_interval: 0UL,
    reason: b"not interested",
    redirect: b"",
  }
  let joining_error = @protocol.RequestErrorMessage::{
    code: 0x32UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.reject_subscription(StreamId(40), subscribe_error),
    Ok([
      SendFin(40, @protocol.encode_request_error(subscribe_error).unwrap()),
      EmitNotice(SubscriptionRejected(1UL, 40)),
      SendFin(42, @protocol.encode_request_error(joining_error).unwrap()),
      EmitNotice(FetchRejected(3UL, 42, joining_error)),
    ]),
  )
  ignore(publisher.receive(InboundStreamStarted(44, Bidirectional)))
  let after_end = @protocol.FetchMessage::{
    request_id: 5UL,
    fetch_type: RelativeJoiningFetch(1UL, 0UL),
    parameters: [],
  }
  assert_eq(
    publisher.receive(
      StreamData(44, @protocol.encode_fetch(after_end).unwrap()),
    ),
    Ok([
      SendFin(44, @protocol.encode_request_error(joining_error).unwrap()),
      EmitNotice(FetchRejected(5UL, 44, joining_error)),
    ]),
  )
}

///|
test "cancelling a pending subscription releases its Joining FETCH" {
  let publisher = client_session_ready()
  let track = client_track(b"live")
  ignore(publisher.publish_track(track, 7UL).unwrap())
  ignore(publisher.receive(StreamOpened(Request(0UL), 20)))
  ignore(publisher.receive(StreamData(20, b"\x07\x00\x01\x00")))
  ignore(publisher.receive(InboundStreamStarted(40, Bidirectional)))
  ignore(
    publisher.receive(
      StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
  )
  ignore(publisher.receive(InboundStreamStarted(42, Bidirectional)))
  let fetch = @protocol.FetchMessage::{
    request_id: 3UL,
    fetch_type: RelativeJoiningFetch(1UL, 0UL),
    parameters: [],
  }
  assert_eq(
    publisher.receive(StreamData(42, @protocol.encode_fetch(fetch).unwrap())),
    Ok([]),
  )
  let invalid_joining_id = @protocol.RequestErrorMessage::{
    code: 0x32UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.cancel_request(1UL),
    Ok([
      SendFin(42, @protocol.encode_request_error(invalid_joining_id).unwrap()),
      EmitNotice(FetchRejected(3UL, 42, invalid_joining_id)),
      AbortSend(40, 1UL),
      StopReceiving(40, 1UL),
      EmitNotice(SubscriptionEnded(1UL, 40, track)),
    ]),
  )
  assert_eq(publisher.receive(Clock(30000UL)), Ok([]))
}

///|
test "PUBLISH request ID is a valid Joining subscription" {
  let publisher = client_session_ready()
  let track = client_track(b"live")
  ignore(publisher.publish_track(track, 7UL).unwrap())
  ignore(publisher.receive(StreamOpened(Request(0UL), 20)))
  assert_eq(
    publisher.receive(
      StreamData(
        20,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          Publish,
        ).unwrap(),
      ),
    ),
    Ok([EmitNotice(PublishAccepted(0UL, track, 7UL))]),
  )

  let header = @protocol.SubgroupHeader::{
    track_alias: 7UL,
    group_id: 4UL,
    subgroup_id_mode: Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: false,
  }
  ignore(publisher.open_object_stream(StreamId(20), header).unwrap())
  ignore(publisher.receive(StreamOpened(Object(0UL), 32)))
  ignore(
    publisher
    .send_object(StreamId(32), {
      object_id: 0UL,
      status: Normal,
      properties: b"",
      payload: b"new-live-group",
    })
    .unwrap(),
  )
  ignore(publisher.receive(InboundStreamStarted(34, Bidirectional)))
  let joining_fetch = @protocol.encode_fetch({
    request_id: 1UL,
    fetch_type: AbsoluteJoiningFetch(0UL, 2UL),
    parameters: [],
  }).unwrap()
  assert_eq(
    publisher.receive(StreamData(34, joining_fetch)),
    Ok([
      EmitNotice(
        FetchRequested(
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
  let start = @protocol.FetchLocation::{ group: 0UL, object: 0UL, }
  let end_location = @protocol.FetchLocation::{ group: 0UL, object: 1UL, }
  let (request_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  ignore(client.receive(StreamOpened(Request(request_id), 20)))
  assert_eq(client.receive(InboundStreamStarted(30, Unidirectional)), Ok([]))
  assert_eq(
    client.receive(
      StreamData(
        30,
        @protocol.encode_fetch_header({ request_id: request_id.to_uint64(), }),
      ),
    ),
    Ok([]),
  )
  assert_eq(client.receive(InboundStreamFinished(30)), Ok([]))
  let response = @protocol.FetchOkMessage::{
    end_of_track: 1,
    end_location,
    parameters: [],
    track_properties: b"",
  }
  assert_eq(
    client.receive(StreamData(20, @protocol.encode_fetch_ok(response).unwrap())),
    Ok([
      EmitNotice(FetchAccepted(request_id, 20)),
      EmitNotice(FetchResponseAccepted(request_id, 20, response)),
      EmitNotice(FetchEndOfTrack(request_id)),
      SendFin(20, b""),
      EmitNotice(FetchFinished(request_id)),
    ]),
  )

  let (failed_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  ignore(client.receive(StreamOpened(Request(failed_id), 21)))
  let error = @protocol.RequestErrorMessage::{
    code: 0x10UL,
    retry_interval: 0UL,
    reason: b"missing",
    redirect: b"",
  }
  assert_eq(
    client.receive(
      StreamData(21, @protocol.encode_request_error(error).unwrap()),
    ),
    Ok([SendFin(21, b""), EmitNotice(FetchFailed(failed_id, error))]),
  )
  assert_eq(client.receive(StreamData(21, b"late")), Ok([]))
}

///|
test "FETCH priority updates are acknowledged, and rejected updates reset data" {
  let server = client_session_ready()
  let track = client_track(b"archive")
  let start = @protocol.FetchLocation::{ group: 0UL, object: 0UL, }
  let end_location = @protocol.FetchLocation::{ group: 0UL, object: 4UL, }
  let fetch = @protocol.FetchMessage::{
    request_id: 1UL,
    fetch_type: StandaloneFetch(track, start, end_location),
    parameters: [],
  }
  ignore(server.receive(InboundStreamStarted(20, Bidirectional)))
  ignore(server.receive(StreamData(20, @protocol.encode_fetch(fetch).unwrap())))
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
  ignore(server.receive(StreamOpened(FetchData(1UL), 30)))

  let priority = @protocol.MessageParameter::{ kind: 0x20UL, value: Byte(2), }
  let update = @protocol.RequestUpdateMessage::{
    request_id: 3UL,
    parameters: [priority],
  }
  assert_eq(
    server.receive(
      StreamData(
        20,
        @protocol.encode_request_update_for(update, FetchRequestStream).unwrap(),
      ),
    ),
    Ok([EmitNotice(FetchUpdateRequested(1UL, 3UL, 20, [priority]))]),
  )
  assert_eq(
    server.accept_fetch_update(1UL, 3UL),
    Ok([
      SendChunk(
        20,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          RequestUpdate,
        ).unwrap(),
      ),
    ]),
  )

  let rejected_update = @protocol.RequestUpdateMessage::{
    request_id: 5UL,
    parameters: [priority],
  }
  assert_eq(
    server.receive(
      StreamData(
        20,
        @protocol.encode_request_update_for(rejected_update, FetchRequestStream).unwrap(),
      ),
    ),
    Ok([EmitNotice(FetchUpdateRequested(1UL, 5UL, 20, [priority]))]),
  )
  let error = @protocol.RequestErrorMessage::{
    code: 0x10UL,
    retry_interval: 0UL,
    reason: b"denied",
    redirect: b"",
  }
  assert_eq(
    server.reject_fetch_update(1UL, 5UL, error),
    Ok([
      SendChunk(20, @protocol.encode_request_error(error).unwrap()),
      AbortSend(30, error.code),
      SendFin(20, b""),
      EmitNotice(FetchFailed(1UL, error)),
    ]),
  )
}

///|
test "cancel before stream-open ignores a late FETCH data header and bytes" {
  let client = client_session_ready()
  let track = client_track(b"archive")
  let start = @protocol.FetchLocation::{ group: 0UL, object: 0UL, }
  let end_location = @protocol.FetchLocation::{ group: 1UL, object: 0UL, }
  let (request_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  assert_eq(
    client.cancel_fetch(request_id),
    Ok([EmitNotice(FetchCancelled(request_id, 1UL))]),
  )
  assert_eq(
    client.receive(StreamOpened(Request(request_id), 20)),
    Ok([AbortSend(20, 1UL), StopReceiving(20, 1UL)]),
  )
  assert_eq(client.receive(InboundStreamStarted(30, Unidirectional)), Ok([]))
  assert_eq(
    client.receive(
      StreamData(
        30,
        @protocol.encode_fetch_header({ request_id: request_id.to_uint64(), }) +
        b"late bytes",
      ),
    ),
    Ok([]),
  )
  assert_eq(client.receive(InboundStreamFinished(30)), Ok([]))
  assert_eq(client.cancel_request(request_id), Ok([]))
}

///|
test "FETCH data reset and GOAWAY each terminate pending requests once" {
  let client = client_session_ready()
  let track = client_track(b"archive")
  let start = @protocol.FetchLocation::{ group: 0UL, object: 0UL, }
  let end_location = @protocol.FetchLocation::{ group: 1UL, object: 0UL, }
  let (reset_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  ignore(client.receive(StreamOpened(Request(reset_id), 20)))
  ignore(client.receive(InboundStreamStarted(30, Unidirectional)))
  ignore(
    client.receive(
      StreamData(
        30,
        @protocol.encode_fetch_header({ request_id: reset_id.to_uint64(), }),
      ),
    ),
  )
  assert_eq(
    client.receive(StreamReset(30, 7UL)),
    Ok([SendFin(20, b""), EmitNotice(FetchReset(reset_id, 30, 7UL))]),
  )
  assert_eq(client.receive(StreamReset(30, 7UL)), Ok([]))

  let (goaway_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  ignore(client.receive(StreamOpened(Request(goaway_id), 21)))
  assert_eq(
    client.receive(StreamData(11, client_goaway_control(0UL, 0UL, b""))),
    Ok([
      EmitNotice(
        ControlMessageReceived(
          GoAwayControl({ uri: b"", timeout_ms: 0UL, request_id: 0UL, }),
        ),
      ),
      EmitNotice(ControlGoAwayReceived(b"", 0UL, 0UL)),
      AbortSend(21, 4UL),
      StopReceiving(21, 4UL),
      EmitNotice(
        FetchFailed(goaway_id, {
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
  let start = @protocol.FetchLocation::{ group: 0UL, object: 0UL, }
  let end_location = @protocol.FetchLocation::{ group: 4UL, object: 0UL, }
  let (first_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  let (second_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  ignore(client.receive(StreamOpened(Request(first_id), 20)))
  ignore(client.receive(StreamOpened(Request(second_id), 22)))

  let first_object = @protocol.FetchObject::{
    group_id: 1UL,
    subgroup_id: Some(0UL),
    object_id: 0UL,
    publisher_priority: 1,
    datagram_forwarding: false,
    properties: b"",
    payload: b"first",
  }
  let second_object = @protocol.FetchObject::{
    group_id: 2UL,
    subgroup_id: Some(0UL),
    object_id: 0UL,
    publisher_priority: 1,
    datagram_forwarding: false,
    properties: b"",
    payload: b"second",
  }
  ignore(client.receive(InboundStreamStarted(30, Unidirectional)))
  ignore(client.receive(InboundStreamStarted(32, Unidirectional)))
  assert_eq(
    client.receive(
      StreamData(
        30,
        @protocol.encode_fetch_header({ request_id: first_id.to_uint64(), }) +
        @protocol.FetchDataEncoder::new(Ascending)
        .encode(FetchDataObject(first_object))
        .unwrap(),
      ),
    ),
    Ok([EmitNotice(FetchObjectReceived(first_id, first_object))]),
  )
  assert_eq(
    client.receive(
      StreamData(
        32,
        @protocol.encode_fetch_header({ request_id: second_id.to_uint64(), }) +
        @protocol.FetchDataEncoder::new(Ascending)
        .encode(FetchDataObject(second_object))
        .unwrap(),
      ),
    ),
    Ok([EmitNotice(FetchObjectReceived(second_id, second_object))]),
  )
  let response = @protocol.FetchOkMessage::{
    end_of_track: 0,
    end_location,
    parameters: [],
    track_properties: b"",
  }
  ignore(
    client.receive(StreamData(20, @protocol.encode_fetch_ok(response).unwrap())),
  )
  ignore(
    client.receive(StreamData(22, @protocol.encode_fetch_ok(response).unwrap())),
  )

  let priority = @protocol.MessageParameter::{ kind: 0x20UL, value: Byte(1), }
  let authorization = @protocol.MessageParameter::{
    kind: 0x03UL,
    value: Data(b"\x03\x01\x00\x01x"),
  }
  assert_eq(
    client.update_fetch(first_id, [authorization, priority]),
    Ok(
      (
        4UL,
        [
          SendChunk(
            20,
            @protocol.encode_request_update_for(
              { request_id: 4UL, parameters: [authorization, priority], },
              FetchRequestStream,
            ).unwrap(),
          ),
        ],
      ),
    ),
  )
  let update_ok = @protocol.RequestOkMessage::{
    parameters: [],
    properties: b"",
  }
  assert_eq(
    client.receive(
      StreamData(
        20,
        @protocol.encode_request_ok_for(update_ok, FetchUpdate).unwrap(),
      ),
    ),
    Ok([EmitNotice(FetchUpdateAccepted(first_id, 4UL, update_ok))]),
  )
}

///|
test "peer FETCH can be rejected before opening its data stream" {
  let server = client_session_ready()
  let track = client_track(b"archive")
  let start = @protocol.FetchLocation::{ group: 0UL, object: 0UL, }
  let end_location = @protocol.FetchLocation::{ group: 0UL, object: 1UL, }
  let fetch = @protocol.FetchMessage::{
    request_id: 1UL,
    fetch_type: StandaloneFetch(track, start, end_location),
    parameters: [],
  }
  ignore(server.receive(InboundStreamStarted(20, Bidirectional)))
  ignore(server.receive(StreamData(20, @protocol.encode_fetch(fetch).unwrap())))
  let error = @protocol.RequestErrorMessage::{
    code: 0x10UL,
    retry_interval: 0UL,
    reason: b"unavailable",
    redirect: b"",
  }
  assert_eq(
    server.reject_fetch(1UL, error),
    Ok([
      SendFin(20, @protocol.encode_request_error(error).unwrap()),
      EmitNotice(FetchRejected(1UL, 20, error)),
    ]),
  )
}

///|
test "completed, failed, and peer FETCH close request stream local send once" {
  let client = client_session_ready()
  let track = client_track(b"archive")
  let start = @protocol.FetchLocation::{ group: 0UL, object: 0UL, }
  let end_location = @protocol.FetchLocation::{ group: 1UL, object: 0UL, }

  // Local FETCH completes when data stream finishes after FetchOk.
  let (completed_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  ignore(client.receive(StreamOpened(Request(completed_id), 20)))
  let response = @protocol.FetchOkMessage::{
    end_of_track: 0,
    end_location,
    parameters: [],
    track_properties: b"",
  }
  assert_eq(
    client.receive(StreamData(20, @protocol.encode_fetch_ok(response).unwrap())),
    Ok([
      EmitNotice(FetchAccepted(completed_id, 20)),
      EmitNotice(FetchResponseAccepted(completed_id, 20, response)),
    ]),
  )
  ignore(client.receive(InboundStreamStarted(30, Unidirectional)))
  assert_eq(
    client.receive(
      StreamData(
        30,
        @protocol.encode_fetch_header({ request_id: completed_id.to_uint64(), }),
      ),
    ),
    Ok([]),
  )
  assert_eq(
    client.receive(InboundStreamFinished(30)),
    Ok([SendFin(20, b""), EmitNotice(FetchFinished(completed_id))]),
  )
  assert_eq(client.receive(InboundStreamFinished(20)), Ok([]))
  assert_eq(client.cancel_fetch(completed_id), Ok([]))
  assert_eq(
    client.receive(SendCompleted(20, Sent)),
    Ok([EmitNotice(StreamSendFinished(20, Sent))]),
  )

  // Local FETCH failed via RequestError sends SendFin once.
  let (failed_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  ignore(client.receive(StreamOpened(Request(failed_id), 22)))
  let error = @protocol.RequestErrorMessage::{
    code: 0x10UL,
    retry_interval: 0UL,
    reason: b"not found",
    redirect: b"",
  }
  assert_eq(
    client.receive(
      StreamData(22, @protocol.encode_request_error(error).unwrap()),
    ),
    Ok([SendFin(22, b""), EmitNotice(FetchFailed(failed_id, error))]),
  )
  assert_eq(client.receive(InboundStreamFinished(22)), Ok([]))
  assert_eq(client.cancel_fetch(failed_id), Ok([]))

  // Cancelled local FETCH suppresses late StreamSendFinished on request stream.
  let (cancelled_id, _) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  ignore(client.receive(StreamOpened(Request(cancelled_id), 24)))
  assert_eq(
    client.cancel_fetch(cancelled_id),
    Ok([
      AbortSend(24, 1UL),
      StopReceiving(24, 1UL),
      EmitNotice(FetchCancelled(cancelled_id, 1UL)),
    ]),
  )
  assert_eq(client.receive(SendCompleted(24, Sent)), Ok([]))

  // Outgoing peer FETCH sends SendFin on request stream when data stream finishes.
  let server = client_session_ready()
  let peer_fetch = @protocol.FetchMessage::{
    request_id: 1UL,
    fetch_type: StandaloneFetch(track, start, end_location),
    parameters: [],
  }
  ignore(server.receive(InboundStreamStarted(40, Bidirectional)))
  ignore(
    server.receive(StreamData(40, @protocol.encode_fetch(peer_fetch).unwrap())),
  )
  ignore(server.accept_fetch(1UL, response).unwrap())
  ignore(server.receive(StreamOpened(FetchData(1UL), 50)))
  assert_eq(
    server.receive(SendCompleted(50, Sent)),
    Ok([EmitNotice(StreamSendFinished(50, Sent))]),
  )
  assert_eq(server.finish_fetch(1UL), Ok([SendFin(50, b"")]))
  assert_eq(
    server.receive(SendCompleted(50, Sent)),
    Ok([EmitNotice(StreamSendFinished(50, Sent)), SendFin(40, b"")]),
  )
  assert_eq(
    server.receive(SendCompleted(40, Sent)),
    Ok([EmitNotice(StreamSendFinished(40, Sent))]),
  )
}
