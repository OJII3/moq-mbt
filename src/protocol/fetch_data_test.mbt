///|
test "FETCH_HEADER encodes and decodes the request ID" {
  let header : @protocol.FetchHeader = { request_id: 42, }
  let wire = b"\x05\x2a"
  assert_eq(@protocol.encode_fetch_header(header), wire)
  assert_eq(
    @protocol.decode_fetch_header(wire),
    @protocol.Complete(header, wire.length()),
  )
  assert_eq(@protocol.decode_fetch_header(b"\x05"), @protocol.Incomplete)
}

///|
test "FETCH objects round-trip and retain fetch-specific delta state" {
  let first : @protocol.FetchObject = {
    group_id: 10,
    subgroup_id: Some(3),
    object_id: 7,
    publisher_priority: 0x20,
    datagram_forwarding: false,
    properties: b"\xaa",
    payload: b"first",
  }
  let second : @protocol.FetchObject = {
    group_id: 10,
    subgroup_id: Some(3),
    object_id: 8,
    publisher_priority: 0x20,
    datagram_forwarding: false,
    properties: b"",
    payload: b"",
  }
  let encoder = @protocol.FetchDataEncoder::new(@protocol.Ascending)
  let first_wire = encoder.encode(@protocol.FetchDataObject(first)).unwrap()
  let second_wire = encoder.encode(@protocol.FetchDataObject(second)).unwrap()
  let decoder = @protocol.FetchDataDecoder::new(@protocol.Ascending)
  assert_eq(
    decoder.decode_next(first_wire),
    @protocol.Complete(@protocol.FetchDataObject(first), first_wire.length()),
  )
  assert_eq(
    decoder.decode_next(second_wire),
    @protocol.Complete(@protocol.FetchDataObject(second), second_wire.length()),
  )
  assert_eq(decoder.finish(b""), Ok(()))
}

///|
test "FETCH object decoder consumes consecutive frames from one chunk" {
  let objects : Array[@protocol.FetchObject] = [
    {
      group_id: 10,
      subgroup_id: Some(2),
      object_id: 4,
      publisher_priority: 1,
      datagram_forwarding: false,
      properties: b"",
      payload: b"a",
    },
    {
      group_id: 12,
      subgroup_id: Some(3),
      object_id: 0,
      publisher_priority: 1,
      datagram_forwarding: false,
      properties: b"",
      payload: b"b",
    },
  ]
  let encoder = @protocol.FetchDataEncoder::new(@protocol.Ascending)
  let wire = encoder.encode(@protocol.FetchDataObject(objects[0])).unwrap() +
    encoder.encode(@protocol.FetchDataObject(objects[1])).unwrap()
  let decoder = @protocol.FetchDataDecoder::new(@protocol.Ascending)
  match decoder.decode_next(wire) {
    @protocol.Complete(value, consumed) => {
      assert_eq(value, @protocol.FetchDataObject(objects[0]))
      match decoder.decode_next(wire[consumed:].to_owned()) {
        @protocol.Complete(next, final_consumed) => {
          assert_eq(next, @protocol.FetchDataObject(objects[1]))
          assert_eq(consumed + final_consumed, wire.length())
        }
        _ => fail("expected second FETCH object")
      }
    }
    _ => fail("expected first FETCH object")
  }
}

///|
test "FETCH object decoder handles byte splits without advancing on incomplete input" {
  let object : @protocol.FetchObject = {
    group_id: 1,
    subgroup_id: Some(0),
    object_id: 2,
    publisher_priority: 7,
    datagram_forwarding: false,
    properties: b"",
    payload: b"payload",
  }
  let wire = @protocol.FetchDataEncoder::new(@protocol.Ascending)
    .encode(@protocol.FetchDataObject(object))
    .unwrap()
  for split = 0; split < wire.length(); split = split + 1 {
    let decoder = @protocol.FetchDataDecoder::new(@protocol.Ascending)
    assert_eq(
      decoder.decode_next(wire[:split].to_owned()),
      @protocol.Incomplete,
    )
    assert_eq(
      decoder.decode_next(wire),
      @protocol.Complete(@protocol.FetchDataObject(object), wire.length()),
    )
  }
}

///|
test "FETCH object decoder preserves prior state across an incomplete next frame" {
  let first : @protocol.FetchObject = {
    group_id: 1,
    subgroup_id: Some(0),
    object_id: 2,
    publisher_priority: 7,
    datagram_forwarding: false,
    properties: b"",
    payload: b"a",
  }
  let next : @protocol.FetchObject = {
    group_id: 1,
    subgroup_id: Some(0),
    object_id: 3,
    publisher_priority: 7,
    datagram_forwarding: false,
    properties: b"",
    payload: b"payload",
  }
  let encoder = @protocol.FetchDataEncoder::new(@protocol.Ascending)
  let first_wire = encoder.encode(@protocol.FetchDataObject(first)).unwrap()
  let next_wire = encoder.encode(@protocol.FetchDataObject(next)).unwrap()
  let decoder = @protocol.FetchDataDecoder::new(@protocol.Ascending)
  assert_true(decoder.decode_next(first_wire) is @protocol.Complete(_, _))
  assert_eq(decoder.decode_next(next_wire[:2].to_owned()), @protocol.Incomplete)
  assert_eq(
    decoder.decode_next(next_wire),
    @protocol.Complete(@protocol.FetchDataObject(next), next_wire.length()),
  )
}

///|
test "FETCH range end is distinct and updates group and object references" {
  let marker : @protocol.FetchRangeEnd = {
    group_id: 5,
    object_id: 9,
    kind: @protocol.EndOfUnknownRange,
  }
  let encoder = @protocol.FetchDataEncoder::new(@protocol.Ascending)
  let wire = encoder.encode(@protocol.FetchDataRangeEnd(marker)).unwrap()
  assert_eq(wire, b"\x81\x0c\x05\x09")
  let decoder = @protocol.FetchDataDecoder::new(@protocol.Ascending)
  assert_eq(
    decoder.decode_next(wire),
    @protocol.Complete(@protocol.FetchDataRangeEnd(marker), wire.length()),
  )
  assert_eq(decoder.finish(b"\x05"), Err(@protocol.InvalidFetchObject))
}

///|
test "FETCH range marker fields are absolute and retain actual object state" {
  let object : @protocol.FetchObject = {
    group_id: 5,
    subgroup_id: Some(2),
    object_id: 8,
    publisher_priority: 9,
    datagram_forwarding: false,
    properties: b"",
    payload: b"x",
  }
  let range : @protocol.FetchRangeEnd = {
    group_id: 5,
    object_id: 10,
    kind: @protocol.EndOfNonExistentRange,
  }
  let after : @protocol.FetchObject = {
    group_id: 5,
    subgroup_id: Some(2),
    object_id: 11,
    publisher_priority: 9,
    datagram_forwarding: false,
    properties: b"",
    payload: b"y",
  }
  let encoder = @protocol.FetchDataEncoder::new(@protocol.Ascending)
  let object_wire = encoder.encode(@protocol.FetchDataObject(object)).unwrap()
  let range_wire = encoder.encode(@protocol.FetchDataRangeEnd(range)).unwrap()
  let after_wire = encoder.encode(@protocol.FetchDataObject(after)).unwrap()
  let decoder = @protocol.FetchDataDecoder::new(@protocol.Ascending)
  assert_true(decoder.decode_next(object_wire) is @protocol.Complete(_, _))
  assert_eq(
    decoder.decode_next(range_wire),
    @protocol.Complete(@protocol.FetchDataRangeEnd(range), range_wire.length()),
  )
  assert_eq(
    decoder.decode_next(after_wire),
    @protocol.Complete(@protocol.FetchDataObject(after), after_wire.length()),
  )
}

///|
test "FETCH datagram bit ignores subgroup mode bits" {
  let object : @protocol.FetchObject = {
    group_id: 3,
    subgroup_id: None,
    object_id: 2,
    publisher_priority: 4,
    datagram_forwarding: true,
    properties: b"",
    payload: b"d",
  }
  let wire = @protocol.FetchDataEncoder::new(@protocol.Ascending)
    .encode(@protocol.FetchDataObject(object))
    .unwrap()
  let altered = Bytes::from_array([wire[0] | 0x03, ..wire[1:]])
  assert_eq(
    @protocol.FetchDataDecoder::new(@protocol.Ascending).decode_next(altered),
    @protocol.Complete(@protocol.FetchDataObject(object), wire.length()),
  )
}

///|
test "FETCH descending group order and integer overflow are validated" {
  let first : @protocol.FetchObject = {
    group_id: 9,
    subgroup_id: Some(0),
    object_id: 4,
    publisher_priority: 3,
    datagram_forwarding: false,
    properties: b"",
    payload: b"a",
  }
  let next : @protocol.FetchObject = {
    group_id: 7,
    subgroup_id: Some(0),
    object_id: 0,
    publisher_priority: 3,
    datagram_forwarding: false,
    properties: b"",
    payload: b"b",
  }
  let encoder = @protocol.FetchDataEncoder::new(@protocol.Descending)
  let first_wire = encoder.encode(@protocol.FetchDataObject(first)).unwrap()
  let next_wire = encoder.encode(@protocol.FetchDataObject(next)).unwrap()
  let decoder = @protocol.FetchDataDecoder::new(@protocol.Descending)
  assert_true(decoder.decode_next(first_wire) is @protocol.Complete(_, _))
  assert_eq(
    decoder.decode_next(next_wire),
    @protocol.Complete(@protocol.FetchDataObject(next), next_wire.length()),
  )

  let max = 0xffffffffffffffffUL
  let initial = b"\x1c" +
    @protocol.encode_vi64(max) +
    @protocol.encode_vi64(max) +
    b"\x03\x00"
  let at_max = @protocol.FetchDataDecoder::new(@protocol.Ascending)
  assert_true(at_max.decode_next(initial) is @protocol.Complete(_, _))
  assert_eq(
    at_max.decode_next(b"\x00"),
    @protocol.Invalid(@protocol.InvalidFetchObject),
  )
}

///|
test "FETCH rejects undefined flags and leading marker state references" {
  assert_true(
    @protocol.FetchDataDecoder::new(@protocol.Ascending).decode_next(
      b"\x81\x00",
    )
    is @protocol.Invalid(@protocol.InvalidFetchObject),
  )
  let decoder = @protocol.FetchDataDecoder::new(@protocol.Ascending)
  assert_true(
    decoder.decode_next(b"\x81\x0c\x01\x02") is @protocol.Complete(_, _),
  )
  assert_eq(
    decoder.decode_next(b"\x01\x00"),
    @protocol.Invalid(@protocol.InvalidFetchObject),
  )
}
