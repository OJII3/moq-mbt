///|
/// Cross-check a small, stable Draft 18 wire corpus against moq5's independent
/// C tests: test_d18_setup_options.c and test_d18_datagram.c, plus its
/// FETCH_HEADER value 42 case in test_control_d18.c.
test "moq5 Draft 18 reference wire vectors" {
  let setup : @protocol.Setup = {
    options: [{ kind: 4, value: @protocol.Integer(100), }],
  }
  let setup_wire = b"\xaf\x00\x00\x02\x04\x64"
  assert_eq(@protocol.encode_setup(setup), Ok(setup_wire))
  assert_eq(
    @protocol.decode_setup(setup_wire),
    @protocol.Complete(setup, setup_wire.length()),
  )

  let fetch_header : @protocol.FetchHeader = { request_id: 42, }
  let fetch_wire = b"\x05\x2a"
  assert_eq(@protocol.encode_fetch_header(fetch_header), fetch_wire)
  assert_eq(
    @protocol.decode_fetch_header(fetch_wire),
    @protocol.Complete(fetch_header, fetch_wire.length()),
  )

  let datagram : @protocol.ObjectDatagram = {
    track_alias: 7,
    group_id: 21,
    object_id: 2,
    publisher_priority: Some(128),
    end_of_group: false,
    properties: b"",
    status: @protocol.Normal,
    payload: b"\x52\x53",
  }
  let datagram_wire = b"\x00\x07\x15\x02\x80\x52\x53"
  assert_eq(@protocol.encode_object_datagram(datagram), Ok(datagram_wire))
  assert_eq(
    @protocol.decode_datagram(datagram_wire),
    @protocol.Complete(@protocol.Object(datagram), datagram_wire.length()),
  )
}
