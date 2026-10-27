///|
test "ClientSession buffers interleaved fragmented PUBLISH and SUBSCRIBE responses independently" {
  let publish_ok = @protocol.encode_request_ok({
    parameters: [],
    properties: b"",
  }).unwrap()
  let subscribe_ok = @protocol.encode_subscribe_ok({
    track_alias: 2,
    parameters: [],
    properties: b"",
  }).unwrap()
  for split = 1; split < publish_ok.length(); split = split + 1 {
    let session = client_session_ready()
    ignore(session.publish_track(client_track(b"video"), 1).unwrap())
    ignore(session.subscribe_track(client_track(b"audio")).unwrap())
    ignore(
      session.receive(@session.StreamOpened(@session.Request(0), 20)).unwrap(),
    )
    ignore(
      session.receive(@session.StreamOpened(@session.Request(2), 30)).unwrap(),
    )
    assert_eq(
      session.receive(@session.StreamData(20, publish_ok[:split].to_owned())),
      Ok([]),
    )
    assert_eq(
      session.receive(@session.StreamData(30, subscribe_ok[:split].to_owned())),
      Ok([]),
    )
    assert_eq(
      session.receive(@session.StreamData(30, subscribe_ok[split:].to_owned())),
      Ok([@session.Notify(@session.SubscribeEstablished(2, 2))]),
    )
    assert_eq(
      session.receive(@session.StreamData(20, publish_ok[split:].to_owned())),
      Ok([
        @session.Notify(
          @session.PublishAccepted(
            0,
            { namespace_fields: [b"demo"], name: b"video", },
            1UL,
          ),
        ),
      ]),
    )
  }
}

///|
test "ClientSession rejects a second initial response after either request is accepted" {
  let publish_ok = @protocol.encode_request_ok({
    parameters: [],
    properties: b"",
  }).unwrap()
  let subscribe_ok = @protocol.encode_subscribe_ok({
    track_alias: 2,
    parameters: [],
    properties: b"",
  }).unwrap()
  let rejected = @protocol.encode_request_error({
    code: 1,
    retry_interval: 0,
    reason: b"no",
    redirect: b"",
  }).unwrap()
  for response in [publish_ok, rejected] {
    let session = publisher_announced()
    assert_eq(
      session.receive(@session.StreamData(20, response)),
      Ok([
        @session.AbortSend(20, 3),
        @session.StopReceiving(20, 3),
        @session.Notify(
          @session.SubscriptionEnded(0, 20, {
            namespace_fields: [b"demo"],
            name: b"video",
          }),
        ),
      ]),
    )
  }
  for response in [subscribe_ok, rejected] {
    let session = subscriber_started()
    assert_eq(
      session.receive(@session.StreamData(30, response)),
      Ok([
        @session.AbortSend(30, 3),
        @session.StopReceiving(30, 3),
        @session.Notify(
          @session.SubscriptionEnded(0, 30, {
            namespace_fields: [b"demo"],
            name: b"video",
          }),
        ),
      ]),
    )
  }
}

///|
test "ClientSession routes fragmented request rejections by request kind" {
  let session = client_session_ready()
  ignore(session.publish_track(client_track(b"video"), 1).unwrap())
  ignore(session.subscribe_track(client_track(b"audio")).unwrap())
  ignore(
    session.receive(@session.StreamOpened(@session.Request(0), 20)).unwrap(),
  )
  ignore(
    session.receive(@session.StreamOpened(@session.Request(2), 30)).unwrap(),
  )
  let error : @protocol.RequestErrorMessage = {
    code: 1,
    retry_interval: 0,
    reason: b"no",
    redirect: b"",
  }
  let wire = @protocol.encode_request_error(error).unwrap()
  let last = wire.length() - 1
  assert_eq(
    session.receive(@session.StreamData(20, wire[:last].to_owned())),
    Ok([]),
  )
  assert_eq(
    session.receive(@session.StreamData(30, wire[:last].to_owned())),
    Ok([]),
  )
  assert_eq(
    session.receive(@session.StreamData(20, wire[last:].to_owned())),
    Ok([
      @session.Send(20, b"", true),
      @session.Notify(@session.PublishRejected(0, error)),
    ]),
  )
  assert_eq(
    session.receive(@session.StreamData(30, wire[last:].to_owned())),
    Ok([
      @session.Send(30, b"", true),
      @session.Notify(@session.SubscribeRejected(2, error)),
    ]),
  )
}
