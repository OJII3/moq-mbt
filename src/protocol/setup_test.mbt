///|
/// Draft 18 §1.4.1 Table 2 (including a nonminimal encoding).
test "vi64 specification examples" {
  let examples : Array[(Bytes, UInt64)] = [
    (b"\x25", 37UL),
    (b"\x80\x25", 37UL),
    (b"\xbb\xbd", 15293UL),
    (b"\xed\x7f\x3e\x7d", 226442877UL),
    (b"\xfa\xa1\xa0\xe4\x03\xd8", 2893212287960UL),
    (b"\xfc\x89\x98\xab\xc6\x6b\xc0", 151288809941952UL),
    (b"\xfe\xfa\x31\x8f\xa8\xe3\xca\x11", 70423237261249041UL),
    (b"\xff\xff\xff\xff\xff\xff\xff\xff\xff", 0xffffffffffffffffUL),
  ]
  for (bytes, value) in examples {
    assert_eq(
      @protocol.decode_vi64(bytes),
      @protocol.Complete(value, bytes.length()),
    )
    for length = 0; length < bytes.length(); length = length + 1 {
      assert_eq(
        @protocol.decode_vi64(bytes[:length].to_owned()),
        @protocol.Incomplete,
      )
    }
  }
}

///|
/// Each width transition has a pinned minimal representation.
test "vi64 width boundaries and full UInt64 range" {
  let encodings : Array[(UInt64, Bytes)] = [
    (0UL, b"\x00"),
    (127UL, b"\x7f"),
    (128UL, b"\x80\x80"),
    (16383UL, b"\xbf\xff"),
    (16384UL, b"\xc0\x40\x00"),
    (2097151UL, b"\xdf\xff\xff"),
    (2097152UL, b"\xe0\x20\x00\x00"),
    (268435455UL, b"\xef\xff\xff\xff"),
    (268435456UL, b"\xf0\x10\x00\x00\x00"),
    (34359738367UL, b"\xf7\xff\xff\xff\xff"),
    (34359738368UL, b"\xf8\x08\x00\x00\x00\x00"),
    (4398046511103UL, b"\xfb\xff\xff\xff\xff\xff"),
    (4398046511104UL, b"\xfc\x04\x00\x00\x00\x00\x00"),
    (562949953421311UL, b"\xfd\xff\xff\xff\xff\xff\xff"),
    (562949953421312UL, b"\xfe\x02\x00\x00\x00\x00\x00\x00"),
    (72057594037927935UL, b"\xfe\xff\xff\xff\xff\xff\xff\xff"),
    (72057594037927936UL, b"\xff\x01\x00\x00\x00\x00\x00\x00\x00"),
    (0xffffffffffffffffUL, b"\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
  ]
  for (value, bytes) in encodings {
    assert_eq(@protocol.encode_vi64(value), bytes)
    assert_eq(
      @protocol.decode_vi64(bytes),
      @protocol.Complete(value, bytes.length()),
    )
  }
  assert_eq(
    @protocol.decode_vi64(b"\xff\x00\x00\x00\x00\x00\x00\x00\x25"),
    @protocol.Complete(37UL, 9),
  )
}

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
