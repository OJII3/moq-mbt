///|
#proof_pure
fn pow128(n : Int32) -> UInt64 {
  if n <= 0 {
    1UL
  } else if n == 1 {
    128UL
  } else if n == 2 {
    16384UL
  } else if n == 3 {
    2097152UL
  } else if n == 4 {
    268435456UL
  } else if n == 5 {
    34359738368UL
  } else if n == 6 {
    4398046511104UL
  } else if n == 7 {
    562949953421312UL
  } else if n == 8 {
    72057594037927936UL
  } else {
    9223372036854775808UL
  }
}

///|
/// Number of bytes the Draft 18 vi64 width loop assigns to `value`.
pub fn encoded_width(
  value : UInt64,
) -> Int32 where {
  proof_ensure: result => encoded_width_post(value, result),
} {
  let mut result = 1
  for width = 1; width < 9; {
    if value >= pow128(width) {
      result = width + 1
    }
    continue width + 1
  } nobreak {
    result
  } where {
    proof_invariant: encoded_width_loop_inv(value, width, result),
  }
}

///|
/// Number of bytes the Draft 18 vi64 decoder consumes for a first byte.
pub fn decoded_width(
  first : Int32,
) -> Int32 where {
  proof_require: 0 <= first && first <= 255,
  proof_ensure: result => decoded_width_post(first, result),
} {
  if first < 128 {
    1
  } else if first < 192 {
    2
  } else if first < 224 {
    3
  } else if first < 240 {
    4
  } else if first < 248 {
    5
  } else if first < 252 {
    6
  } else if first < 254 {
    7
  } else if first < 255 {
    8
  } else {
    9
  }
}
