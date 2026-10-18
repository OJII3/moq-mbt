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
