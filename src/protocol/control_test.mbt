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
