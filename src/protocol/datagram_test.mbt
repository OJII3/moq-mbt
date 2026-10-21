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
test "Draft 18 OBJECT_DATAGRAM matches golden bytes and round trips" {
  let with_fields : @protocol.ObjectDatagram = {
    track_alias: 7,
    group_id: 100,
    object_id: 9,
    publisher_priority: Some(200),
    end_of_group: true,
    properties: b"\x02\x2a",
    status: @protocol.Normal,
    payload: b"\x01\x02\x03\x04\x05",
  }
  let with_fields_wire = b"\x03\x07\x64\x09\xc8\x02\x02\x2a\x01\x02\x03\x04\x05"
  assert_eq(@protocol.encode_object_datagram(with_fields), Ok(with_fields_wire))
  assert_eq(
    @protocol.decode_object_datagram(with_fields_wire),
    @protocol.Complete(with_fields, with_fields_wire.length()),
  )

  let empty : @protocol.ObjectDatagram = { ..object_datagram(0, b""), }
  let empty_wire = b"\x2c\x01\x02\x00"
  assert_eq(@protocol.encode_object_datagram(empty), Ok(empty_wire))
  assert_eq(
    @protocol.decode_object_datagram(empty_wire),
    @protocol.Complete(empty, empty_wire.length()),
  )
  assert_eq(
    @protocol.encode_object_datagram({ ..empty, status: @protocol.EndOfGroup, }),
    Ok(b"\x2c\x01\x02\x03"),
  )
  assert_eq(
    @protocol.encode_object_datagram({ ..empty, status: @protocol.EndOfTrack, }),
    Ok(b"\x2c\x01\x02\x04"),
  )
}

///|
test "Draft 18 OBJECT_DATAGRAM supports every valid type-bit combination" {
  for has_properties in [false, true] {
    for end_of_group in [false, true] {
      for zero_object_id in [false, true] {
        for default_priority in [false, true] {
          let value : @protocol.ObjectDatagram = {
            track_alias: 3,
            group_id: 5,
            object_id: if zero_object_id {
              0UL
            } else {
              7UL
            },
            publisher_priority: if default_priority {
              None
            } else {
              Some(0x80)
            },
            end_of_group,
            properties: if has_properties {
              b"\x02\x2a"
            } else {
              b""
            },
            status: @protocol.Normal,
            payload: b"payload",
          }
          let mut expected_type = 0UL
          if has_properties {
            expected_type = expected_type | 0x01UL
          }
          if end_of_group {
            expected_type = expected_type | 0x02UL
          }
          if zero_object_id {
            expected_type = expected_type | 0x04UL
          }
          if default_priority {
            expected_type = expected_type | 0x08UL
          }
          let wire = @protocol.encode_object_datagram(value).unwrap()
          assert_eq(
            @protocol.decode_vi64(wire),
            @protocol.Complete(expected_type, 1),
          )
          assert_eq(
            @protocol.decode_object_datagram(wire),
            @protocol.Complete(value, wire.length()),
          )
        }
      }
    }
  }

  for status in [@protocol.Normal, @protocol.EndOfGroup, @protocol.EndOfTrack] {
    for has_properties in [false, true] {
      if status == @protocol.Normal || !has_properties {
        for zero_object_id in [false, true] {
          for default_priority in [false, true] {
            let value : @protocol.ObjectDatagram = {
              track_alias: 3,
              group_id: 5,
              object_id: if zero_object_id {
                0UL
              } else {
                7UL
              },
              publisher_priority: if default_priority {
                None
              } else {
                Some(0x80)
              },
              end_of_group: false,
              properties: if has_properties {
                b"\x02\x2a"
              } else {
                b""
              },
              status,
              payload: b"",
            }
            let mut expected_type = 0x20UL
            if has_properties {
              expected_type = expected_type | 0x01UL
            }
            if zero_object_id {
              expected_type = expected_type | 0x04UL
            }
            if default_priority {
              expected_type = expected_type | 0x08UL
            }
            let wire = @protocol.encode_object_datagram(value).unwrap()
            assert_eq(
              @protocol.decode_vi64(wire),
              @protocol.Complete(expected_type, 1),
            )
            assert_eq(
              @protocol.decode_object_datagram(wire),
              @protocol.Complete(value, wire.length()),
            )
          }
        }
      }
    }
  }
}

///|
test "Draft 18 OBJECT_DATAGRAM covers vi64 and properties length boundaries" {
  for
    value in [
      0UL, 127UL, 128UL, 16383UL, 16384UL, 2097151UL, 2097152UL, 268435455UL, 268435456UL,
      0xffffffffffffffffUL,
    ] {
    let datagram : @protocol.ObjectDatagram = {
      track_alias: value,
      group_id: value,
      object_id: value,
      publisher_priority: Some(0xff),
      end_of_group: false,
      properties: b"",
      status: @protocol.Normal,
      payload: b"x",
    }
    let wire = @protocol.encode_object_datagram(datagram).unwrap()
    assert_eq(
      @protocol.decode_object_datagram(wire),
      @protocol.Complete(datagram, wire.length()),
    )
  }

  for length in [127, 128] {
    let datagram : @protocol.ObjectDatagram = {
      ..object_datagram(3, b"x"),
      properties: Bytes::from_array(Array::make(length, 0xaa)),
    }
    let wire = @protocol.encode_object_datagram(datagram).unwrap()
    assert_eq(
      @protocol.decode_object_datagram(wire),
      @protocol.Complete(datagram, wire.length()),
    )
  }
}

///|
test "Draft 18 padding datagrams are distinguished and consumed as one datagram" {
  let padding_datagrams = [
    b"\xf0\x13\x2b\x3e\x29", b"\xf0\x13\x2b\x3e\x29\x00\x00", b"\xf0\x13\x2b\x3e\x29\xff\x80\x01",
  ]
  for padding in padding_datagrams {
    assert_eq(
      @protocol.decode_datagram(padding),
      @protocol.Complete(@protocol.Padding, padding.length()),
    )
    assert_eq(
      @protocol.decode_object_datagram(padding),
      @protocol.Invalid(@protocol.InvalidDatagram),
    )
  }
  assert_eq(
    @protocol.decode_datagram(b"\x30"),
    @protocol.Invalid(@protocol.InvalidDatagram),
  )
}

///|
test "Draft 18 OBJECT_DATAGRAM rejects malformed fields and incomplete headers" {
  for
    invalid in [
      b"\x10\x01\x02\x03payload", b"\x22\x01\x02\x03", b"\x30\x01\x02\x03", b"\x0d\x01\x02\x00x",
      b"\x2d\x01\x02\x01\xaa\x03", b"\x2c\x01\x02\x05", b"\x2c\x01\x02\x03x", b"\x0c\x01\x02",
      b"\x0d\x01\x02\x01",
    ] {
    assert_eq(
      @protocol.decode_datagram(invalid),
      @protocol.Invalid(@protocol.InvalidDatagram),
    )
  }

  for
    incomplete in [
      b"", b"\x00", b"\x00\x01", b"\x00\x01\x02", b"\x00\x01\x02\x03", b"\x2c\x01\x02",
      b"\x0d\x01\x02", b"\xf0\x13",
    ] {
    assert_eq(@protocol.decode_datagram(incomplete), @protocol.Incomplete)
  }

  let invalid_end_of_group : @protocol.ObjectDatagram = {
    ..object_datagram(0, b""),
    end_of_group: true,
  }
  assert_eq(
    @protocol.encode_object_datagram(invalid_end_of_group),
    Err(@protocol.InvalidDatagram),
  )
  assert_eq(
    @protocol.encode_object_datagram({
      ..object_datagram(1, b"payload"),
      status: @protocol.EndOfGroup,
    }),
    Err(@protocol.InvalidDatagram),
  )
}
