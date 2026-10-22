///|
fn fuzz_next(state : UInt64) -> UInt64 {
  let a = state ^ (state << 13)
  let b = a ^ (a >> 7)
  b ^ (b << 17)
}

///|
fn exercise_wire_input(input : Bytes) -> Bool {
  let mut valid = true
  let vi64 = @protocol.decode_vi64(input)
  match vi64 {
    @protocol.Complete(value, consumed) => {
      valid = valid && consumed > 0 && consumed <= input.length()
      let canonical = @protocol.encode_vi64(value)
      valid = valid &&
        @protocol.decode_vi64(canonical) ==
        @protocol.Complete(value, canonical.length())
    }
    @protocol.Incomplete | @protocol.Invalid(_) => ()
  }

  let setup = @protocol.decode_setup(input)
  match setup {
    @protocol.Complete(_, consumed) =>
      valid = valid && consumed > 0 && consumed <= input.length()
    @protocol.Incomplete | @protocol.Invalid(_) => ()
  }

  let control = @protocol.decode_control(input)
  match control {
    @protocol.Complete(_, consumed) =>
      valid = valid && consumed > 0 && consumed <= input.length()
    @protocol.Incomplete | @protocol.Invalid(_) => ()
  }

  let subgroup = @protocol.decode_subgroup_header(input)
  match subgroup {
    @protocol.Complete(_, consumed) =>
      valid = valid && consumed > 0 && consumed <= input.length()
    @protocol.Incomplete | @protocol.Invalid(_) => ()
  }

  let fetch_header = @protocol.decode_fetch_header(input)
  match fetch_header {
    @protocol.Complete(_, consumed) =>
      valid = valid && consumed > 0 && consumed <= input.length()
    @protocol.Incomplete | @protocol.Invalid(_) => ()
  }

  ignore(@protocol.decode_datagram(input))
  valid
}

///|
/// Deterministic malformed-input smoke corpus for the public wire decoders.
/// The fixed seed makes any discovered failure straightforward to reproduce.
test "wire decoders tolerate deterministic arbitrary byte inputs" {
  let mut state = 0x6d6f712d6d6274UL
  for _ in 0..<1024 {
    state = fuzz_next(state)
    let length = (state % 97UL).to_int()
    let bytes : Array[Byte] = []
    for _ in 0..<length {
      state = fuzz_next(state)
      bytes.push((state & 0xffUL).to_byte())
    }
    assert_true(exercise_wire_input(Bytes::from_array(bytes)))
  }

  let reference_inputs = [
    b"\xaf\x00\x00\x02\x04\x64", b"\x03\x00\x0e\x00\x01\x04demo\x05video\x00", b"\x05\x2a",
    b"\x00\x07\x15\x02\x80\x52\x53",
  ]
  for original in reference_inputs {
    for index = 0; index < original.length(); index = index + 1 {
      for mask in [0x01, 0x80] {
        let mutated : Array[Byte] = []
        for i = 0; i < original.length(); i = i + 1 {
          mutated.push(
            if i == index {
              (original[i].to_int() ^ mask).to_byte()
            } else {
              original[i]
            },
          )
        }
        assert_true(exercise_wire_input(Bytes::from_array(mutated)))
      }
    }
  }
}
