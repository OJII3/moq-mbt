///|
test "single-track publish, subscribe, and one object" {
  let track : @protocol.TrackName = {
    namespace_fields: [b"demo"],
    name: b"video",
  }
  let publish_wire = b"\x1d\x00\x0f\x00\x01\x04demo\x05video\x01\x00"
  let publisher = @session.SingleTrackPublisher::new(track, 1)
  assert_eq(publisher.publish_request().unwrap(), publish_wire)

  let subscribe = @protocol.encode_subscribe({
    request_id: 1,
    track,
    parameters: [],
  }).unwrap()
  assert_eq(subscribe, b"\x03\x00\x0e\x01\x01\x04demo\x05video\x00")
  for split = 0; split < subscribe.length(); split = split + 1 {
    let publisher = @session.SingleTrackPublisher::new(track, 1)
    assert_eq(
      publisher.receive_subscribe(11, subscribe[:split].to_owned()),
      Ok(@session.Pending),
    )
    assert_eq(
      publisher.receive_subscribe(11, subscribe[split:].to_owned()),
      Ok(
        @session.SubscribeRequested(
          11,
          { request_id: 1, track, parameters: [], },
          b"",
        ),
      ),
    )
  }

  let publisher = @session.SingleTrackPublisher::new(track, 1)
  assert_eq(
    publisher.receive_subscribe(11, subscribe),
    Ok(
      @session.SubscribeRequested(
        11,
        { request_id: 1, track, parameters: [], },
        b"",
      ),
    ),
  )
  assert_eq(publisher.accept_subscribe(11).unwrap(), b"\x04\x00\x02\x01\x00")
  assert_eq(
    publisher.object_stream(0, 0, b"payload").unwrap(),
    @protocol.encode_one_object(1, 0, 0, b"payload"),
  )
  assert_true(publisher.object_stream(0, 1, b"second") is Err(_))
}

///|
test "publisher sends one object after PUBLISH is accepted" {
  let publisher = @session.SingleTrackPublisher::new(
    { namespace_fields: [b"demo"], name: b"video", },
    1,
  )
  publisher.publish_accepted()
  assert_eq(
    publisher.object_stream(0, 0, b"payload").unwrap(),
    @protocol.encode_one_object(1, 0, 0, b"payload"),
  )
  assert_true(publisher.object_stream(0, 1, b"second") is Err(_))
}

///|
test "single-track subscriber handles split responses and objects" {
  let track : @protocol.TrackName = {
    namespace_fields: [b"demo"],
    name: b"video",
  }
  let response = b"\x04\x00\x02\x01\x00"
  let wire = @protocol.encode_one_object(1, 4, 2, b"payload")
  for split = 0; split < response.length(); split = split + 1 {
    let subscriber = @session.SingleTrackSubscriber::new(track)
    assert_eq(
      subscriber.subscribe_request().unwrap(),
      b"\x03\x00\x0e\x00\x01\x04demo\x05video\x00",
    )
    assert_eq(
      subscriber.receive_response(8, response[:split].to_owned()),
      Ok(@session.Pending),
    )
    assert_eq(
      subscriber.receive_response(8, response[split:].to_owned()),
      Ok(@session.Established(1, b"")),
    )
  }

  let subscriber = @session.SingleTrackSubscriber::new(track)
  assert_eq(
    subscriber.receive_response(8, response),
    Ok(@session.Established(1, b"")),
  )
  for split = 0; split < wire.length(); split = split + 1 {
    let subscriber = @session.SingleTrackSubscriber::new(track)
    ignore(subscriber.receive_response(8, response))
    assert_eq(
      subscriber.receive_object(9, wire[:split].to_owned()),
      Ok(@session.Pending),
    )
    assert_eq(
      subscriber.receive_object(9, wire[split:].to_owned()),
      Ok(
        @session.Received(
          {
            track_alias: 1,
            group_id: 4,
            object_id: 2,
            end_of_group: true,
            payload: b"payload",
          },
          b"",
        ),
      ),
    )
  }
}

///|
test "subscriber accepts PUBLISH_DONE after SUBSCRIBE_OK" {
  let subscriber = @session.SingleTrackSubscriber::new({
    namespace_fields: [b"demo"],
    name: b"video",
  })
  assert_eq(
    subscriber.receive_response(8, b"\x04\x00\x02\x01\x00"),
    Ok(@session.Established(1, b"")),
  )
  assert_eq(
    subscriber.receive_response(8, b"\x0b\x00\x03\x03\x01\x00"),
    Ok(@session.Finished({ code: 3, stream_count: 1, reason: b"", }, b"")),
  )
}
