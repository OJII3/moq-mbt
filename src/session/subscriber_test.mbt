///|
test "single-track subscriber handles split responses and objects" {
  let track : @protocol.TrackName = {
    namespace_fields: [b"demo"],
    name: b"video",
  }
  let response = b"\x04\x00\x02\x01\x00"
  let object_header : @protocol.SubgroupHeader = {
    track_alias: 1,
    group_id: 4,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: true,
    first_object: true,
    has_properties: false,
  }
  let object_frame : @protocol.SubgroupFrame = {
    object_id: 2,
    status: @protocol.Normal,
    properties: b"",
    payload: b"payload",
  }
  let wire = @protocol.encode_subgroup_stream(object_header, [object_frame]).unwrap()
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
      Ok(@session.Received(object_header, object_frame, b"")),
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
