///|
test "GROUP_ORDER Byte values are accepted for standalone and joining FETCH" {
  let ascending : Array[@protocol.MessageParameter] = [
    { kind: 0x22UL, value: @protocol.Byte(1), },
  ]
  let descending : Array[@protocol.MessageParameter] = [
    { kind: 0x22UL, value: @protocol.Byte(2), },
  ]
  let track = client_track(b"archive")
  let client = client_session_ready()
  assert_true(
    client.fetch_track(
      track,
      { group: 0UL, object: 0UL, },
      { group: 1UL, object: 0UL, },
      descending,
    )
    is Ok(_),
  )
  assert_true(
    client.fetch_track(
      track,
      { group: 0UL, object: 0UL, },
      { group: 1UL, object: 0UL, },
      ascending,
    )
    is Ok(_),
  )

  let joined = client_session_ready()
  ignore(joined.subscribe_track(track).unwrap())
  ignore(joined.receive(@session.StreamOpened(@session.Request(0UL), 20)))
  let subscribe_ok : @protocol.SubscribeOkMessage = {
    track_alias: 7UL,
    parameters: [{ kind: 0x09UL, value: @protocol.Location(12UL, 3UL), }],
    properties: b"",
  }
  ignore(
    joined.receive(
      @session.StreamData(
        20,
        @protocol.encode_subscribe_ok(subscribe_ok).unwrap(),
      ),
    ),
  )
  assert_true(joined.fetch_relative(0UL, 2UL, descending) is Ok(_))
  assert_true(joined.fetch_absolute(0UL, 8UL, descending) is Ok(_))
}

///|
test "peer FETCH GROUP_ORDER Byte selects descending object order" {
  let server = client_session_ready()
  let track = client_track(b"archive")
  let request_id = 1UL
  let start : @protocol.FetchLocation = { group: 1UL, object: 0UL, }
  let end : @protocol.FetchLocation = { group: 3UL, object: 0UL, }
  let fetch : @protocol.FetchMessage = {
    request_id,
    fetch_type: @protocol.StandaloneFetch(track, start, end),
    parameters: [{ kind: 0x22UL, value: @protocol.Byte(2), }],
  }
  ignore(
    server.receive(@session.InboundStreamStarted(20, @session.Bidirectional)),
  )
  assert_true(
    server.receive(
      @session.StreamData(20, @protocol.encode_fetch(fetch).unwrap()),
    )
    is Ok(_),
  )

  let client = client_session_ready()
  let (local_id, _) = client
    .fetch_track(track, start, end, [
      { kind: 0x22UL, value: @protocol.Byte(2), },
    ])
    .unwrap()
  ignore(client.receive(@session.StreamOpened(@session.Request(local_id), 22)))
  let response : @protocol.FetchOkMessage = {
    end_of_track: 0,
    end_location: end,
    parameters: [],
    track_properties: b"",
  }
  ignore(
    client.receive(
      @session.StreamData(22, @protocol.encode_fetch_ok(response).unwrap()),
    ),
  )
  let object : @protocol.FetchObject = {
    group_id: 2UL,
    subgroup_id: Some(0UL),
    object_id: 0UL,
    publisher_priority: 0,
    datagram_forwarding: false,
    properties: b"",
    payload: b"payload",
  }
  let peer_response : @protocol.FetchOkMessage = {
    end_of_track: 0,
    end_location: end,
    parameters: [],
    track_properties: b"",
  }
  assert_true(server.accept_fetch(request_id, peer_response) is Ok(_))
  assert_true(
    server.receive(@session.StreamOpened(@session.FetchData(request_id), 32))
    is Ok(_),
  )
  assert_true(server.send_fetch_object(request_id, object) is Ok(_))
  assert_true(
    server.send_fetch_object(request_id, { ..object, group_id: 1UL, }) is Ok(_),
  )

  let encoder = @protocol.FetchDataEncoder::new(@protocol.Descending)
  let data = @protocol.encode_fetch_header({ request_id: local_id, }) +
    encoder.encode(@protocol.FetchDataObject(object)).unwrap() +
    encoder
    .encode(@protocol.FetchDataObject({ ..object, group_id: 1UL, }))
    .unwrap()
  ignore(
    client.receive(@session.InboundStreamStarted(30, @session.Unidirectional)),
  )
  assert_eq(
    client.receive(@session.StreamData(30, data)),
    Ok([
      @session.Notify(@session.FetchObjectReceived(local_id, object)),
      @session.Notify(
        @session.FetchObjectReceived(local_id, { ..object, group_id: 1UL, }),
      ),
    ]),
  )
}

///|
test "FETCH data stream FIN prevents another stream for the same request" {
  let client = client_session_ready()
  let track = client_track(b"archive")
  let (request_id, _) = client
    .fetch_track(
      track,
      { group: 0UL, object: 0UL, },
      { group: 1UL, object: 0UL, },
      [],
    )
    .unwrap()
  ignore(
    client.receive(@session.StreamOpened(@session.Request(request_id), 20)),
  )
  ignore(
    client.receive(@session.InboundStreamStarted(30, @session.Unidirectional)),
  )
  let header = @protocol.encode_fetch_header({ request_id, })
  assert_eq(client.receive(@session.StreamData(30, header)), Ok([]))
  assert_eq(client.receive(@session.StreamFinished(30)), Ok([]))

  ignore(
    client.receive(@session.InboundStreamStarted(32, @session.Unidirectional)),
  )
  ignore(client.receive(@session.StreamData(32, header)))
  assert_eq(client.state(), @session.Closed)
}
