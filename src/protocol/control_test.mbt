///|
test "Draft 18 single-track request and response wire formats" {
  let track : @protocol.TrackName = {
    namespace_fields: [b"demo"],
    name: b"video",
  }
  let subscribe : @protocol.SubscribeMessage = {
    request_id: 0,
    track,
    parameters: [],
  }
  let subscribe_wire = b"\x03\x00\x0e\x00\x01\x04demo\x05video\x00"
  assert_eq(@protocol.encode_subscribe(subscribe).unwrap(), subscribe_wire)
  assert_eq(
    @protocol.decode_control(subscribe_wire),
    @protocol.Complete(@protocol.Subscribe(subscribe), subscribe_wire.length()),
  )

  let publish : @protocol.PublishMessage = {
    request_id: 0,
    track,
    track_alias: 1,
    parameters: [],
    properties: b"",
  }
  let publish_wire = b"\x1d\x00\x0f\x00\x01\x04demo\x05video\x01\x00"
  assert_eq(@protocol.encode_publish(publish).unwrap(), publish_wire)
  assert_eq(
    @protocol.decode_control(publish_wire),
    @protocol.Complete(@protocol.Publish(publish), publish_wire.length()),
  )

  let subscribe_ok : @protocol.SubscribeOkMessage = {
    track_alias: 1,
    parameters: [],
    properties: b"",
  }
  let subscribe_ok_wire = b"\x04\x00\x02\x01\x00"
  assert_eq(
    @protocol.encode_subscribe_ok(subscribe_ok).unwrap(),
    subscribe_ok_wire,
  )
  assert_eq(
    @protocol.decode_control(subscribe_ok_wire),
    @protocol.Complete(
      @protocol.SubscribeOk(subscribe_ok),
      subscribe_ok_wire.length(),
    ),
  )
  assert_eq(
    @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
    b"\x07\x00\x01\x00",
  )
}

///|
test "control message parsing handles every split and trailing frames" {
  let message : @protocol.SubscribeMessage = {
    request_id: 0,
    track: { namespace_fields: [b"demo"], name: b"video", },
    parameters: [
      { kind: 0x10, value: @protocol.Byte(1), },
      { kind: 0x21, value: @protocol.Data(b"\x03\x00\x00"), },
    ],
  }
  let frame = @protocol.encode_subscribe(message).unwrap()
  for split = 0; split < frame.length(); split = split + 1 {
    assert_eq(
      @protocol.decode_control(frame[:split].to_owned()),
      @protocol.Incomplete,
    )
  }
  assert_eq(
    @protocol.decode_control(frame + b"\x07\x00\x01\x00"),
    @protocol.Complete(@protocol.Subscribe(message), frame.length()),
  )
}

///|
test "one reliable subgroup object round-trips including empty payload" {
  let wire = b"\x78\x01\x00\x00\x05hello"
  let object : @protocol.SubgroupObject = {
    track_alias: 1,
    group_id: 0,
    object_id: 0,
    end_of_group: true,
    payload: b"hello",
  }
  assert_eq(@protocol.encode_one_object(1, 0, 0, b"hello"), wire)
  assert_eq(
    @protocol.decode_subgroup_object(wire),
    @protocol.Complete(object, wire.length()),
  )

  let empty = @protocol.encode_one_object(1, 0, 0, b"")
  assert_eq(empty, b"\x78\x01\x00\x00\x00\x00")
  let empty_object : @protocol.SubgroupObject = {
    track_alias: 1,
    group_id: 0,
    object_id: 0,
    end_of_group: true,
    payload: b"",
  }
  assert_eq(
    @protocol.decode_subgroup_object(empty),
    @protocol.Complete(empty_object, empty.length()),
  )
  assert_eq(
    @protocol.decode_subgroup_object(b"\x58\x01\x00\x80\x00\x05hello"),
    @protocol.Complete(object, 11),
  )
}

///|
test "one subgroup object parser handles every split" {
  let wire = @protocol.encode_one_object(7, 9, 3, b"payload")
  let object : @protocol.SubgroupObject = {
    track_alias: 7,
    group_id: 9,
    object_id: 3,
    end_of_group: true,
    payload: b"payload",
  }
  for split = 0; split < wire.length(); split = split + 1 {
    assert_eq(
      @protocol.decode_subgroup_object(wire[:split].to_owned()),
      @protocol.Incomplete,
    )
  }
  assert_eq(
    @protocol.decode_subgroup_object(wire),
    @protocol.Complete(object, wire.length()),
  )
}
