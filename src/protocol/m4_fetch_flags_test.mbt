///|
test "FETCH datagram objects ignore subgroup mode bits" {
  let expected : @protocol.FetchDataFrame = @protocol.FetchDataObject({
    group_id: 1UL,
    subgroup_id: None,
    object_id: 0UL,
    publisher_priority: 0,
    datagram_forwarding: true,
    properties: b"",
    payload: b"",
  })
  for wire in [b"\x5d\x01\x00\x00\x00", b"\x5e\x01\x00\x00\x00"] {
    assert_eq(
      @protocol.FetchDataDecoder::new(@protocol.Ascending).decode_next(wire),
      @protocol.Complete(expected, wire.length()),
    )
  }
}
