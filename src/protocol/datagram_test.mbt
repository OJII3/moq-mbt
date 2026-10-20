///|
fn object_datagram(
  object_id : UInt64,
  payload : Bytes,
) -> @protocol.ObjectDatagram {
  {
    track_alias: 1,
    group_id: 2,
    object_id,
    publisher_priority: None,
    end_of_group: false,
    properties: b"",
    status: @protocol.Normal,
    payload,
  }
}

///|
test "Draft 18 OBJECT_DATAGRAM round trips normal objects and optional fields" {
  let cases : Array[@protocol.ObjectDatagram] = [
    object_datagram(0, b"payload"),
    object_datagram(0, b""),
    {
      track_alias: 3,
      group_id: 7,
      object_id: 9,
      publisher_priority: Some(0x2a),
      end_of_group: true,
      properties: b"\x01\x00",
      status: @protocol.Normal,
      payload: b"data",
    },
    { ..object_datagram(5, b""), status: @protocol.EndOfGroup, },
    { ..object_datagram(5, b""), status: @protocol.EndOfTrack, },
  ]
  for value in cases {
    let wire = @protocol.encode_object_datagram(value).unwrap()
    assert_eq(
      @protocol.decode_object_datagram(wire),
      @protocol.Complete(value, wire.length()),
    )
  }
  assert_eq(
    @protocol.encode_object_datagram(object_datagram(0, b"payload")),
    Ok(b"\x0c\x01\x02payload"),
  )
}

///|
test "Draft 18 OBJECT_DATAGRAM rejects invalid types and fields" {
  for
    invalid in [
      b"\x10\x01\x02\x03payload", b"\x22\x01\x02\x03", b"\x0d\x01\x02\x00", b"\x25\x01\x02\x00\x01x\x03",
      b"\x0c\x01\x02",
    ] {
    assert_eq(
      @protocol.decode_object_datagram(invalid),
      @protocol.Invalid(@protocol.InvalidDatagram),
    )
  }
  assert_eq(
    @protocol.encode_object_datagram({
      ..object_datagram(1, b"payload"),
      status: @protocol.EndOfGroup,
    }),
    Err(@protocol.InvalidDatagram),
  )
}
