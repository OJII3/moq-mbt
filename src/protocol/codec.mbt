///|
/// Early Draft 18 codecs, using vi64 rather than QUIC's 62-bit varint.
pub(all) enum CodecError {
  UnexpectedMessageType(UInt64)
  MalformedOptions
  OptionTypeOverflow
  DuplicateOption(UInt64)
  InvalidOptionValue(UInt64)
  OptionsOutOfOrder
  MessageTooLarge
  MalformedControlMessage
  UnsupportedControlMessage(UInt64)
  UnsupportedParameter(UInt64)
  InvalidParameterScope(UInt64)
  InvalidParameterValue(UInt64)
  InvalidRequestOkProperties
  InvalidFetchRange
  InvalidEndOfTrack
  InvalidTrackProperties
  InvalidGoAwayUri
  InvalidTrackName
  InvalidSubgroup
  InvalidFetchObject
  InvalidDatagram
} derive(Debug, Eq)

///|
/// Complete includes the number of consumed input bytes.
pub(all) enum DecodeResult[T] {
  Complete(T, Int32)
  Incomplete
  Invalid(CodecError)
} derive(Debug, Eq)

///|
pub fn encode_vi64(value : UInt64) -> Bytes {
  let mut width = 1
  while width < 9 && value >> (7 * width) != 0 {
    width += 1
  }
  let bytes : Array[Byte] = Array::make(width, 0)
  for i = width - 1; i > 0; i = i - 1 {
    bytes[i] = (value >> (8 * (width - 1 - i))).to_byte()
  }
  bytes[0] = if width == 9 {
    0xff
  } else {
    let prefix = (0xff << (9 - width)) & 0xff
    (prefix | (value >> (8 * (width - 1))).to_int()).to_byte()
  }
  Bytes::from_array(bytes)
}

///|
fn append_vi64s(values : Array[Byte], value : UInt64) -> Unit {
  for byte in encode_vi64(value) {
    values.push(byte)
  }
}

///|
fn read_vi64(input : Bytes, offset : Int32, end : Int32) -> (UInt64, Int32)? {
  if offset >= end {
    return None
  }
  let first = input[offset].to_int()
  let mut leading = 0
  while leading < 8 && (first & (0x80 >> leading)) != 0 {
    leading += 1
  }
  let width = leading + 1
  if end - offset < width {
    return None
  }
  let mut value = (first & (0xff >> width)).to_uint64()
  for i = 1; i < width; i = i + 1 {
    value = (value << 8) | input[offset + i].to_uint64()
  }
  Some((value, offset + width))
}

///|
pub fn decode_vi64(input : Bytes) -> DecodeResult[UInt64] {
  match read_vi64(input, 0, input.length()) {
    Some((value, end)) => Complete(value, end)
    None => Incomplete
  }
}

///|
fn read_varint_at(
  input : Bytes,
  offset : Int32,
  end : Int32,
) -> (UInt64, Int32)? {
  read_vi64(input, offset, end)
}

///|
fn read_bytes_at(input : Bytes, offset : Int32, end : Int32) -> (Bytes, Int32)? {
  let (length, next) = match read_varint_at(input, offset, end) {
    Some(pair) => pair
    None => return None
  }
  if length > (end - next).to_uint64() {
    return None
  }
  let value_end = next + length.to_int()
  Some((input[next:value_end].to_owned(), value_end))
}

///|
pub extend CodecError with Debug::{to_repr}

///|
pub extend CodecError with Eq::{equal, not_equal}

///|
pub extend DecodeResult with Debug::{to_repr}

///|
pub extend DecodeResult with Eq::{equal, not_equal}
