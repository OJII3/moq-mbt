///|
test "Standalone FETCH accepts data before its response and finishes after FIN" {
  let client = client_session_ready()
  let track = client_track(b"archive")
  let start : @protocol.FetchLocation = { group: 2UL, object: 1UL, }
  let end_location : @protocol.FetchLocation = { group: 3UL, object: 0UL, }
  let (request_id, actions) = client
    .fetch_track(track, start, end_location, [])
    .unwrap()
  assert_eq(request_id, 0UL)
  assert_eq(actions, [
    @session.OpenStream(@session.Bidirectional, @session.Request(0UL)),
  ])
  let fetch : @protocol.FetchMessage = {
    request_id,
    fetch_type: @protocol.StandaloneFetch(track, start, end_location),
    parameters: [],
  }
  assert_eq(
    client.receive(@session.StreamOpened(@session.Request(request_id), 20)),
    Ok([@session.Send(20, @protocol.encode_fetch(fetch).unwrap(), false)]),
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
  let data = @protocol.encode_fetch_header({ request_id, }) +
    @protocol.FetchDataEncoder::new(@protocol.Ascending)
    .encode(@protocol.FetchDataObject(object))
    .unwrap()
  assert_eq(
    client.receive(@session.InboundStreamStarted(30, @session.Unidirectional)),
    Ok([]),
  )
  assert_eq(
    client.receive(@session.StreamData(30, data)),
    Ok([@session.Notify(@session.FetchObjectReceived(request_id, object))]),
  )
  assert_eq(client.receive(@session.StreamFinished(30)), Ok([]))

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
      @session.Notify(@session.FetchAccepted(request_id, 20)),
      @session.Notify(@session.FetchFinished(request_id)),
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
      @session.Notify(
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
      @session.Send(20, @protocol.encode_fetch_ok(response).unwrap(), false),
      @session.Notify(@session.FetchAccepted(1UL, 20)),
      @session.OpenStream(@session.Unidirectional, @session.FetchData(1UL)),
    ]),
  )
  assert_eq(
    server.receive(@session.StreamOpened(@session.FetchData(1UL), 30)),
    Ok([
      @session.Send(
        30,
        @protocol.encode_fetch_header({ request_id: 1UL, }),
        false,
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
      @session.Send(
        30,
        @protocol.FetchDataEncoder::new(@protocol.Ascending)
        .encode(@protocol.FetchDataObject(object))
        .unwrap(),
        false,
      ),
    ]),
  )
  assert_eq(server.finish_fetch(1UL), Ok([@session.Send(30, b"", true)]))
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
      @session.StreamData(30, @protocol.encode_fetch_header({ request_id, })),
    ),
    Ok([]),
  )
  assert_eq(client.receive(@session.StreamFinished(30)), Ok([]))
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
      @session.Notify(@session.FetchAccepted(request_id, 20)),
      @session.Notify(@session.FetchEndOfTrack(request_id)),
      @session.Notify(@session.FetchFinished(request_id)),
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
    Ok([@session.Notify(@session.FetchFailed(failed_id, error))]),
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
      @session.Notify(@session.FetchUpdateRequested(1UL, 3UL, 20, [priority])),
    ]),
  )
  assert_eq(
    server.accept_fetch_update(1UL, 3UL),
    Ok([
      @session.Send(
        20,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          @protocol.RequestUpdate,
        ).unwrap(),
        false,
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
      @session.Notify(@session.FetchUpdateRequested(1UL, 5UL, 20, [priority])),
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
      @session.Send(20, @protocol.encode_request_error(error).unwrap(), false),
      @session.AbortSend(30, error.code),
      @session.Notify(@session.FetchFailed(1UL, error)),
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
    Ok([@session.Notify(@session.FetchCancelled(request_id, 1UL))]),
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
        @protocol.encode_fetch_header({ request_id, }) + b"late bytes",
      ),
    ),
    Ok([]),
  )
  assert_eq(client.receive(@session.StreamFinished(30)), Ok([]))
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
        @protocol.encode_fetch_header({ request_id: reset_id, }),
      ),
    ),
  )
  assert_eq(
    client.receive(@session.StreamReset(30, 7UL)),
    Ok([@session.Notify(@session.FetchReset(reset_id, 30, 7UL))]),
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
      @session.AbortSend(21, 4UL),
      @session.StopReceiving(21, 4UL),
      @session.Notify(
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
        @protocol.encode_fetch_header({ request_id: first_id, }) +
        @protocol.FetchDataEncoder::new(@protocol.Ascending)
        .encode(@protocol.FetchDataObject(first_object))
        .unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.FetchObjectReceived(first_id, first_object))]),
  )
  assert_eq(
    client.receive(
      @session.StreamData(
        32,
        @protocol.encode_fetch_header({ request_id: second_id, }) +
        @protocol.FetchDataEncoder::new(@protocol.Ascending)
        .encode(@protocol.FetchDataObject(second_object))
        .unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.FetchObjectReceived(second_id, second_object))]),
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
          @session.Send(
            20,
            @protocol.encode_request_update_for(
              { request_id: 4UL, parameters: [authorization, priority], },
              @protocol.FetchRequestStream,
            ).unwrap(),
            false,
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
    Ok([@session.Notify(@session.FetchUpdateAccepted(first_id, 4UL, update_ok))]),
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
      @session.Send(20, @protocol.encode_request_error(error).unwrap(), true),
      @session.Notify(@session.FetchRejected(1UL, 20, error)),
    ]),
  )
}
