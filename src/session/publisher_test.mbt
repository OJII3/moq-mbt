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
    b"\x78\x01\x00\x00\x07payload",
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
    b"\x78\x01\x00\x00\x07payload",
  )
  assert_true(publisher.object_stream(0, 1, b"second") is Err(_))
}
