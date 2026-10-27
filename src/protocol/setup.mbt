///|
pub(all) enum OptionValue {
  Integer(UInt64)
  Data(Bytes)
} derive(Debug, Eq)

///|
/// Options retain unknown types and duplicate unknown values.
pub(all) struct SetupOption {
  kind : UInt64
  value : OptionValue
} derive(Debug, Eq)

///|
pub(all) struct Setup {
  options : Array[SetupOption]
} derive(Debug, Eq)

///|
/// Return a copy without the Native-only PATH (1) and AUTHORITY (5) options.
/// WebTransport supplies these values through the connection URL instead.
pub fn Setup::for_webtransport(self : Setup) -> Setup {
  let options : Array[SetupOption] = []
  for option in self.options {
    if option.kind != 1UL && option.kind != 5UL {
      options.push(option)
    }
  }
  { options, }
}

///|
fn unique_option(kind : UInt64) -> Bool {
  kind == 1 || kind == 4 || kind == 5 || kind == 7
}

///|
/// The caller supplies options in ascending type order for delta encoding.
/// AUTHORIZATION TOKEN (type 3) and unknown options may be repeated.
pub fn encode_setup(setup : Setup) -> Result[Bytes, CodecError] {
  let payload : Array[Byte] = []
  let mut previous = 0UL
  let mut first = true
  for option in setup.options {
    if option.kind < previous {
      return Err(OptionsOutOfOrder)
    }
    if !first && option.kind == previous && unique_option(option.kind) {
      return Err(DuplicateOption(option.kind))
    }
    let delta = encode_vi64(option.kind - previous)
    let value = match option.value {
      Integer(value) => {
        if (option.kind & 1) != 0 {
          return Err(InvalidOptionValue(option.kind))
        }
        encode_vi64(value)
      }
      Data(value) => {
        if (option.kind & 1) == 0 {
          return Err(InvalidOptionValue(option.kind))
        }
        if value.length() > 65535 {
          return Err(MessageTooLarge)
        }
        encode_vi64(value.length().to_uint64()) + value
      }
    }
    if payload.length() + delta.length() + value.length() > 65535 {
      return Err(MessageTooLarge)
    }
    for byte in delta {
      payload.push(byte)
    }
    for byte in value {
      payload.push(byte)
    }
    previous = option.kind
    first = false
  }
  let length = payload.length()
  Ok(
    b"\xaf\x00" +
    Bytes::from_array([(length >> 8).to_byte(), length.to_byte()]) +
    Bytes::from_array(payload),
  )
}

///|
/// Parse one SETUP frame, leaving any following frames unconsumed.
/// A complete frame with truncated option fields is malformed, not incomplete.
/// Option semantics (URI, token authorization) are handled by the session.
pub fn decode_setup(input : Bytes) -> DecodeResult[Setup] {
  let (kind, header) = match read_vi64(input, 0, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  if kind != 0x2f00 {
    return Invalid(UnexpectedMessageType(kind))
  }
  if input.length() - header < 2 {
    return Incomplete
  }
  let length = (input[header].to_int() << 8) | input[header + 1].to_int()
  let end = header + 2 + length
  if input.length() < end {
    return Incomplete
  }
  let options : Array[SetupOption] = []
  let mut offset = header + 2
  let mut previous = 0UL
  while offset < end {
    let (delta, next) = match read_vi64(input, offset, end) {
      Some(pair) => pair
      None => return Invalid(MalformedOptions)
    }
    if delta > 0xffffffffffffffffUL - previous {
      return Invalid(OptionTypeOverflow)
    }
    let kind = previous + delta
    if !options.is_empty() && delta == 0 && unique_option(kind) {
      return Invalid(DuplicateOption(kind))
    }
    let (number, next) = match read_vi64(input, next, end) {
      Some(pair) => pair
      None => return Invalid(MalformedOptions)
    }
    let value = if (kind & 1) == 0 {
      offset = next
      Integer(number)
    } else {
      if number > 65535 || number > (end - next).to_uint64() {
        return Invalid(MalformedOptions)
      }
      offset = next + number.to_int()
      Data(input[next:offset].to_owned())
    }
    options.push({ kind, value, })
    previous = kind
  }
  Complete({ options, }, end)
}

///|
pub extend OptionValue with Debug::{to_repr}

///|
pub extend OptionValue with Eq::{equal, not_equal}

///|
pub extend SetupOption with Debug::{to_repr}

///|
pub extend SetupOption with Eq::{equal, not_equal}

///|
pub extend Setup with Debug::{to_repr}

///|
pub extend Setup with Eq::{equal, not_equal}
