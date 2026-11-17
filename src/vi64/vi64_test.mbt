///|
test "encoded_width byte boundaries" {
  assert_eq(@vi64.encoded_width(0UL), 1)
  assert_eq(@vi64.encoded_width(127UL), 1)
  assert_eq(@vi64.encoded_width(128UL), 2)
  assert_eq(@vi64.encoded_width(16383UL), 2)
  assert_eq(@vi64.encoded_width(16384UL), 3)
}

///|
test "encoded_width wider boundaries" {
  assert_eq(@vi64.encoded_width(2097151UL), 3)
  assert_eq(@vi64.encoded_width(2097152UL), 4)
  assert_eq(@vi64.encoded_width(72057594037927935UL), 8)
  assert_eq(@vi64.encoded_width(72057594037927936UL), 9)
  assert_eq(@vi64.encoded_width(18446744073709551615UL), 9)
}

///|
test "decoded_width first byte boundaries" {
  assert_eq(@vi64.decoded_width(0x00), 1)
  assert_eq(@vi64.decoded_width(0x7f), 1)
  assert_eq(@vi64.decoded_width(0x80), 2)
  assert_eq(@vi64.decoded_width(0xbf), 2)
  assert_eq(@vi64.decoded_width(0xc0), 3)
  assert_eq(@vi64.decoded_width(0xfe), 8)
  assert_eq(@vi64.decoded_width(0xff), 9)
}
