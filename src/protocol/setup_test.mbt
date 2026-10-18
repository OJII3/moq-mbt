///|
/// moq5 tests/unit/test_d18_setup_options.c pins this vi64 KVP boundary.
test "moq5 cache and URI golden bytes" {
  let cache : @protocol.Setup = {
    options: [{ kind: 4, value: Integer(100), }],
  }
  assert_eq(@protocol.encode_setup(cache), Ok(b"\xaf\x00\x00\x02\x04\x64"))
  let setup : @protocol.Setup = {
    options: [
      { kind: 1, value: Data(b"/moq?interop=1"), },
      { kind: 5, value: Data(b"127.0.0.1:31000"), },
    ],
  }
  let golden = b"\xaf\x00\x00\x21\x01\x0e/moq?interop=1\x04\x0f127.0.0.1:31000"
  assert_eq(@protocol.encode_setup(setup), Ok(golden))
  assert_eq(
    @protocol.decode_setup(golden),
    @protocol.Complete(setup, golden.length()),
  )
  for length = 0; length < golden.length(); length = length + 1 {
    assert_eq(
      @protocol.decode_setup(golden[:length].to_owned()),
      @protocol.Incomplete,
    )
  }
  assert_eq(
    @protocol.decode_setup(golden + b"\xaf\x00\x00\x00"),
    @protocol.Complete(setup, golden.length()),
  )
  assert_eq(
    @protocol.decode_setup(b"\xc0\x2f\x00\x00\x00"),
    @protocol.Complete({ options: [], }, 5),
  )
}

///|
test "SETUP unknown options and repeatable tokens retain all values" {
  let setup : @protocol.Setup = {
    options: [
      { kind: 3, value: Data(b"\x03\x07t"), },
      { kind: 3, value: Data(b"\x03\x07u"), },
      { kind: 6, value: Integer(5), },
      { kind: 9, value: Data(b"zzz"), },
      { kind: 9, value: Data(b"q"), },
    ],
  }
  let golden = b"\xaf\x00\x00\x14\x03\x03\x03\x07t\x00\x03\x03\x07u\x03\x05\x03\x03zzz\x00\x01q"
  assert_eq(@protocol.encode_setup(setup), Ok(golden))
  assert_eq(
    @protocol.decode_setup(golden),
    @protocol.Complete(setup, golden.length()),
  )
}

///|
test "SETUP malformed payload differs from incomplete frame" {
  assert_eq(@protocol.decode_setup(b"\xaf\x00\x00\x01"), @protocol.Incomplete)
  let malformed : Array[Bytes] = [
    b"\xaf\x00\x00\x01\x80", // Truncated type delta.
     b"\xaf\x00\x00\x01\x04", // Missing integer value.
     b"\xaf\x00\x00\x02\x01\x02", // Missing byte value.
     b"\xaf\x00\x00\x04\x01\xc1\x00\x00", // Length > 65535.
  ]
  for bytes in malformed {
    assert_eq(
      @protocol.decode_setup(bytes),
      @protocol.Invalid(@protocol.MalformedOptions),
    )
  }
  assert_eq(
    @protocol.decode_setup(b"\x01"),
    @protocol.Invalid(@protocol.UnexpectedMessageType(1)),
  )
  assert_eq(
    @protocol.decode_setup(b"\xaf\x00\x00\x04\x04\x00\x00\x00"),
    @protocol.Invalid(@protocol.DuplicateOption(4)),
  )
  let overflow = b"\xaf\x00\x00\x0d\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x01\x00\x00"
  assert_eq(
    @protocol.decode_setup(overflow),
    @protocol.Invalid(@protocol.OptionTypeOverflow),
  )
}

///|
test "SETUP encoder rejects invalid parity order duplicates and size" {
  assert_eq(
    @protocol.encode_setup({ options: [{ kind: 1, value: Integer(0), }], }),
    Err(@protocol.InvalidOptionValue(1)),
  )
  assert_eq(
    @protocol.encode_setup({ options: [{ kind: 4, value: Data(b""), }], }),
    Err(@protocol.InvalidOptionValue(4)),
  )
  assert_eq(
    @protocol.encode_setup({
      options: [{ kind: 4, value: Integer(0), }, { kind: 1, value: Data(b""), }],
    }),
    Err(@protocol.OptionsOutOfOrder),
  )
  for kind in [1UL, 4UL, 5UL, 7UL] {
    let value = if (kind & 1) == 0 {
      @protocol.Integer(0)
    } else {
      @protocol.Data(b"")
    }
    assert_eq(
      @protocol.encode_setup({ options: [{ kind, value, }, { kind, value, }], }),
      Err(@protocol.DuplicateOption(kind)),
    )
  }
  assert_eq(
    @protocol.encode_setup({
      options: [{ kind: 9, value: Data(Bytes::make(65536, 0)), }],
    }),
    Err(@protocol.MessageTooLarge),
  )
  // The option value can fit its own limit while exceeding the message limit.
  assert_eq(
    @protocol.encode_setup({
      options: [{ kind: 9, value: Data(Bytes::make(65535, 0)), }],
    }),
    Err(@protocol.MessageTooLarge),
  )
}
