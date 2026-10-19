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

///|
test "subgroup header encodes all optional fields and id modes" {
  let header : @protocol.SubgroupHeader = {
    track_alias: 1,
    group_id: 2,
    subgroup_id_mode: @protocol.Explicit(9),
    publisher_priority: Some(0x2a),
    end_of_group: true,
    first_object: false,
    has_properties: true,
  }
  let wire = b"\x1d\x01\x02\x09\x2a"
  assert_eq(@protocol.encode_subgroup_header(header), wire)
  assert_eq(
    @protocol.decode_subgroup_header(wire),
    @protocol.Complete(header, wire.length()),
  )

  let inherited : @protocol.SubgroupHeader = {
    track_alias: 1,
    group_id: 2,
    subgroup_id_mode: @protocol.FirstObject,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: false,
  }
  let inherited_wire = b"\x72\x01\x02"
  assert_eq(@protocol.encode_subgroup_header(inherited), inherited_wire)
  assert_eq(
    @protocol.decode_subgroup_header(inherited_wire),
    @protocol.Complete(inherited, inherited_wire.length()),
  )
}

///|
test "subgroup stream round-trips multiple objects, deltas, properties, and statuses" {
  let header : @protocol.SubgroupHeader = {
    track_alias: 7,
    group_id: 9,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: true,
  }
  let frames : Array[@protocol.SubgroupFrame] = [
    {
      object_id: 2,
      status: @protocol.Normal,
      properties: b"\x01\xaa",
      payload: b"one",
    },
    { object_id: 3, status: @protocol.Normal, properties: b"", payload: b"", },
    {
      object_id: 7,
      status: @protocol.EndOfGroup,
      properties: b"",
      payload: b"",
    },
    {
      object_id: 8,
      status: @protocol.EndOfTrack,
      properties: b"",
      payload: b"",
    },
  ]
  let wire = @protocol.encode_subgroup_stream(header, frames).unwrap()
  let (decoded_header, header_length) = match
    @protocol.decode_subgroup_header(wire) {
    @protocol.Complete(value, consumed) => (value, consumed)
    _ => fail("expected a complete subgroup header")
  }
  assert_eq(decoded_header, header)
  let decoder = @protocol.SubgroupDecoder::new(decoded_header)
  assert_eq(decoder.subgroup_id(), Some(0))
  let mut offset = header_length
  for frame in frames {
    match decoder.decode_next(wire[offset:].to_owned()) {
      @protocol.Complete(decoded, consumed) => {
        assert_eq(decoded, frame)
        offset += consumed
      }
      _ => fail("expected a complete subgroup object")
    }
  }
  assert_eq(offset, wire.length())
}

///|
test "first-object subgroup id resolves from the first object id" {
  let header : @protocol.SubgroupHeader = {
    track_alias: 1,
    group_id: 3,
    subgroup_id_mode: @protocol.FirstObject,
    publisher_priority: None,
    end_of_group: false,
    first_object: false,
    has_properties: false,
  }
  let frame : @protocol.SubgroupFrame = {
    object_id: 12,
    status: @protocol.Normal,
    properties: b"",
    payload: b"payload",
  }
  let wire = @protocol.encode_subgroup_stream(header, [frame]).unwrap()
  let (decoded_header, header_length) = match
    @protocol.decode_subgroup_header(wire) {
    @protocol.Complete(value, consumed) => (value, consumed)
    _ => fail("expected a complete subgroup header")
  }
  let decoder = @protocol.SubgroupDecoder::new(decoded_header)
  assert_eq(decoder.subgroup_id(), None)
  assert_true(
    decoder.decode_next(wire[header_length:].to_owned())
    is @protocol.Complete(_, _),
  )
  assert_eq(decoder.subgroup_id(), Some(12))
}

///|
test "subgroup stream decoder handles every byte split" {
  let header : @protocol.SubgroupHeader = {
    track_alias: 7,
    group_id: 9,
    subgroup_id_mode: @protocol.Explicit(4),
    publisher_priority: Some(0x80),
    end_of_group: true,
    first_object: true,
    has_properties: true,
  }
  let frame : @protocol.SubgroupFrame = {
    object_id: 2,
    status: @protocol.Normal,
    properties: b"\x01\xaa",
    payload: b"payload",
  }
  let wire = @protocol.encode_subgroup_stream(header, [frame]).unwrap()
  for split = 0; split < wire.length(); split = split + 1 {
    let partial = wire[:split].to_owned()
    match @protocol.decode_subgroup_header(partial) {
      @protocol.Incomplete => ()
      @protocol.Complete(partial_header, header_length) =>
        assert_eq(
          @protocol.SubgroupDecoder::new(partial_header).decode_next(
            partial[header_length:].to_owned(),
          ),
          @protocol.Incomplete,
        )
      @protocol.Invalid(_) => fail("invalid subgroup header")
    }
  }
  let (decoded_header, header_length) = match
    @protocol.decode_subgroup_header(wire) {
    @protocol.Complete(value, consumed) => (value, consumed)
    _ => fail("expected a complete subgroup header")
  }
  assert_eq(
    @protocol.SubgroupDecoder::new(decoded_header).decode_next(
      wire[header_length:].to_owned(),
    ),
    @protocol.Complete(frame, wire.length() - header_length),
  )
}

///|
test "draft 18 rejects undefined subgroup object statuses" {
  assert_true(
    @protocol.SubgroupDecoder::new({
      track_alias: 1,
      group_id: 0,
      subgroup_id_mode: @protocol.Zero,
      publisher_priority: None,
      end_of_group: false,
      first_object: true,
      has_properties: false,
    }).decode_next(b"\x00\x00\x01")
    is @protocol.Invalid(_),
  )
  assert_true(
    @protocol.SubgroupDecoder::new({
      track_alias: 1,
      group_id: 0,
      subgroup_id_mode: @protocol.Zero,
      publisher_priority: None,
      end_of_group: false,
      first_object: true,
      has_properties: false,
    }).decode_next(b"\x00\x00\x02")
    is @protocol.Invalid(_),
  )
}
