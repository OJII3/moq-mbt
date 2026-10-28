///|
test "streaming object header matches complete frame encoding" {
  let header : @protocol.SubgroupHeader = {
    track_alias: 1,
    group_id: 0,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: true,
  }
  for payload in [b"", b"hello"] {
    let frame : @protocol.SubgroupFrame = {
      object_id: 3,
      status: @protocol.Normal,
      properties: b"p",
      payload,
    }
    let prefix = @protocol.encode_subgroup_object_header(
      header,
      Some(1UL),
      3UL,
      b"p",
      payload.length().to_uint64(),
    ).unwrap()
    assert_eq(
      prefix + payload,
      @protocol.encode_subgroup_frame(header, Some(1UL), frame).unwrap(),
    )
  }
}

///|
test "one reliable subgroup frame round-trips including empty payload" {
  let header : @protocol.SubgroupHeader = {
    track_alias: 1,
    group_id: 0,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: true,
    first_object: true,
    has_properties: false,
  }
  let frame : @protocol.SubgroupFrame = {
    object_id: 0,
    status: @protocol.Normal,
    properties: b"",
    payload: b"hello",
  }
  let wire = b"\x78\x01\x00\x00\x05hello"
  assert_eq(@protocol.encode_subgroup_stream(header, [frame]), Ok(wire))
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

  let empty_frame : @protocol.SubgroupFrame = {
    object_id: 0,
    status: @protocol.Normal,
    properties: b"",
    payload: b"",
  }
  let empty = @protocol.encode_subgroup_stream(header, [empty_frame]).unwrap()
  assert_eq(empty, b"\x78\x01\x00\x00\x00\x00")
  let (empty_header, empty_header_length) = match
    @protocol.decode_subgroup_header(empty) {
    @protocol.Complete(value, consumed) => (value, consumed)
    _ => fail("expected a complete subgroup header")
  }
  assert_eq(
    @protocol.SubgroupDecoder::new(empty_header).decode_next(
      empty[empty_header_length:].to_owned(),
    ),
    @protocol.Complete(empty_frame, empty.length() - empty_header_length),
  )
}

///|
test "one subgroup frame parser handles every split" {
  let header : @protocol.SubgroupHeader = {
    track_alias: 7,
    group_id: 9,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: true,
    first_object: true,
    has_properties: false,
  }
  let frame : @protocol.SubgroupFrame = {
    object_id: 3,
    status: @protocol.Normal,
    properties: b"",
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
test "subgroup frame encoder accepts the previous object ID" {
  let header : @protocol.SubgroupHeader = {
    track_alias: 1,
    group_id: 2,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: true,
  }
  let frame : @protocol.SubgroupFrame = {
    object_id: 7,
    status: @protocol.Normal,
    properties: b"\xaa",
    payload: b"abc",
  }
  assert_eq(
    @protocol.encode_subgroup_frame(header, Some(2), frame),
    Ok(b"\x04\x01\xaa\x03abc"),
  )
  assert_eq(
    @protocol.encode_subgroup_stream(header, [
      { object_id: 2, status: @protocol.Normal, properties: b"", payload: b"", },
      frame,
    ]),
    Ok(
      @protocol.encode_subgroup_header(header) +
      b"\x02\x00\x00\x00\x04\x01\xaa\x03abc",
    ),
  )
  assert_eq(
    @protocol.encode_subgroup_frame(header, Some(7), frame),
    Err(@protocol.InvalidSubgroup),
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
