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
