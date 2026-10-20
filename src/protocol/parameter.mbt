///|
/// One full track name. Namespace fields and track names are opaque bytes.
pub(all) struct TrackName {
  namespace_fields : Array[Bytes]
  name : Bytes
} derive(Debug, Eq)

///|
pub(all) enum ParameterValue {
  VarInt(UInt64)
  Byte(Byte)
  Data(Bytes)
  Location(UInt64, UInt64)
  Filter(SubscriptionFilter)
} derive(Debug, Eq)

///|
pub(all) enum SubscriptionFilter {
  NextGroupStart
  LargestObject
  AbsoluteStart(UInt64, UInt64)
  AbsoluteRange(UInt64, UInt64, UInt64)
} derive(Debug, Eq)

///|
pub(all) struct MessageParameter {
  kind : UInt64
  value : ParameterValue
} derive(Debug, Eq)

///|
fn encode_track_name(track : TrackName) -> Result[Bytes, CodecError] {
  if track.namespace_fields.length() > 32 {
    return Err(InvalidTrackName)
  }
  let encoded : Array[Byte] = []
  append_vi64s(encoded, track.namespace_fields.length().to_uint64())
  let mut total = track.name.length()
  for field in track.namespace_fields {
    if field.is_empty() {
      return Err(InvalidTrackName)
    }
    total += field.length()
    if total > 4096 {
      return Err(InvalidTrackName)
    }
    append_vi64s(encoded, field.length().to_uint64())
    for byte in field {
      encoded.push(byte)
    }
  }
  if total > 4096 {
    return Err(InvalidTrackName)
  }
  append_vi64s(encoded, track.name.length().to_uint64())
  for byte in track.name {
    encoded.push(byte)
  }
  Ok(Bytes::from_array(encoded))
}

///|
fn encode_parameters(
  parameters : Array[MessageParameter],
) -> Result[Bytes, CodecError] {
  let encoded : Array[Byte] = []
  append_vi64s(encoded, parameters.length().to_uint64())
  let mut previous = 0UL
  let mut first = true
  for parameter in parameters {
    if parameter.kind < previous ||
      (!first && parameter.kind == previous && parameter.kind != 3) {
      return Err(OptionsOutOfOrder)
    }
    append_vi64s(encoded, parameter.kind - previous)
    match (parameter.kind, parameter.value) {
      (0x10, Byte(value)) => {
        if value > 1 {
          return Err(InvalidParameterValue(parameter.kind))
        }
        encoded.push(value)
      }
      (0x20, Byte(value)) => encoded.push(value)
      (0x22, Byte(value)) => {
        if value < 1 || value > 2 {
          return Err(InvalidParameterValue(parameter.kind))
        }
        encoded.push(value)
      }
      (0x09, Location(group, object)) => {
        append_vi64s(encoded, group)
        append_vi64s(encoded, object)
      }
      (0x02 | 0x04 | 0x06 | 0x08 | 0x32, VarInt(value)) =>
        append_vi64s(encoded, value)
      (0x03, Data(value)) => {
        append_vi64s(encoded, value.length().to_uint64())
        for byte in value {
          encoded.push(byte)
        }
      }
      (0x21, Filter(filter)) => {
        let body = match encode_subscription_filter(filter) {
          Ok(value) => value
          Err(error) => return Err(error)
        }
        append_vi64s(encoded, body.length().to_uint64())
        for byte in body {
          encoded.push(byte)
        }
      }
      _ => return Err(UnsupportedParameter(parameter.kind))
    }
    previous = parameter.kind
    first = false
  }
  Ok(Bytes::from_array(encoded))
}

///|
fn encode_subscription_filter(
  filter : SubscriptionFilter,
) -> Result[Bytes, CodecError] {
  let encoded : Array[Byte] = []
  match filter {
    NextGroupStart => append_vi64s(encoded, 1UL)
    LargestObject => append_vi64s(encoded, 2UL)
    AbsoluteStart(group, object) => {
      append_vi64s(encoded, 3UL)
      append_vi64s(encoded, group)
      append_vi64s(encoded, object)
    }
    AbsoluteRange(group, object, end_group) => {
      if end_group < group {
        return Err(InvalidParameterValue(0x21))
      }
      append_vi64s(encoded, 4UL)
      append_vi64s(encoded, group)
      append_vi64s(encoded, object)
      append_vi64s(encoded, end_group - group)
    }
  }
  Ok(Bytes::from_array(encoded))
}

///|
fn decode_subscription_filter(input : Bytes) -> SubscriptionFilter? {
  let end = input.length()
  let (kind, offset) = match read_varint_at(input, 0, end) {
    Some(pair) => pair
    None => return None
  }
  let filter = match kind {
    1 => {
      if offset != end {
        return None
      }
      NextGroupStart
    }
    2 => {
      if offset != end {
        return None
      }
      LargestObject
    }
    3 => {
      let (group, offset) = match read_varint_at(input, offset, end) {
        Some(pair) => pair
        None => return None
      }
      let (object, offset) = match read_varint_at(input, offset, end) {
        Some(pair) => pair
        None => return None
      }
      if offset != end {
        return None
      }
      AbsoluteStart(group, object)
    }
    4 => {
      let (group, offset) = match read_varint_at(input, offset, end) {
        Some(pair) => pair
        None => return None
      }
      let (object, offset) = match read_varint_at(input, offset, end) {
        Some(pair) => pair
        None => return None
      }
      let (delta, offset) = match read_varint_at(input, offset, end) {
        Some(pair) => pair
        None => return None
      }
      if offset != end || delta > 0xffffffffffffffffUL - group {
        return None
      }
      AbsoluteRange(group, object, group + delta)
    }
    _ => return None
  }
  Some(filter)
}

///|
fn read_track_at(
  input : Bytes,
  offset : Int32,
  end : Int32,
) -> (TrackName, Int32)? {
  let (field_count, first_next) = match read_varint_at(input, offset, end) {
    Some(pair) => pair
    None => return None
  }
  if field_count > 32UL {
    return None
  }
  let namespace_fields : Array[Bytes] = []
  let mut total = 0
  let mut next = first_next
  for _ in 0..<field_count.to_int() {
    let (field, field_end) = match read_bytes_at(input, next, end) {
      Some(pair) => pair
      None => return None
    }
    if field.is_empty() {
      return None
    }
    total += field.length()
    namespace_fields.push(field)
    next = field_end
  }
  let (name, name_end) = match read_bytes_at(input, next, end) {
    Some(pair) => pair
    None => return None
  }
  total += name.length()
  if total > 4096 {
    return None
  }
  Some(({ namespace_fields, name, }, name_end))
}

///|
fn read_parameters_at(
  input : Bytes,
  offset : Int32,
  end : Int32,
) -> (Array[MessageParameter], Int32)? {
  let (count, first_next) = match read_varint_at(input, offset, end) {
    Some(pair) => pair
    None => return None
  }
  if count > (end - first_next).to_uint64() {
    return None
  }
  let parameters : Array[MessageParameter] = []
  let mut previous = 0UL
  let mut next = first_next
  for i in 0..<count.to_int() {
    let (delta, after_delta) = match read_varint_at(input, next, end) {
      Some(pair) => pair
      None => return None
    }
    if delta > 0xffffffffffffffffUL - previous {
      return None
    }
    let kind = previous + delta
    if i > 0 && delta == 0 && kind != 3 {
      return None
    }
    let value = match kind {
      0x10 | 0x20 | 0x22 => {
        if after_delta >= end {
          return None
        }
        let byte = input[after_delta]
        if (kind == 0x10 && byte > 1) ||
          (kind == 0x22 && (byte < 1 || byte > 2)) {
          return None
        }
        next = after_delta + 1
        Byte(byte)
      }
      0x09 => {
        let (group, after_group) = match
          read_varint_at(input, after_delta, end) {
          Some(pair) => pair
          None => return None
        }
        let (object, after_object) = match
          read_varint_at(input, after_group, end) {
          Some(pair) => pair
          None => return None
        }
        next = after_object
        Location(group, object)
      }
      0x02 | 0x04 | 0x06 | 0x08 | 0x32 => {
        let (value, after_value) = match
          read_varint_at(input, after_delta, end) {
          Some(pair) => pair
          None => return None
        }
        next = after_value
        VarInt(value)
      }
      0x03 => {
        let (value, after_value) = match
          read_bytes_at(input, after_delta, end) {
          Some(pair) => pair
          None => return None
        }
        next = after_value
        Data(value)
      }
      0x21 => {
        let (bytes, after_value) = match
          read_bytes_at(input, after_delta, end) {
          Some(pair) => pair
          None => return None
        }
        let filter = match decode_subscription_filter(bytes) {
          Some(value) => value
          None => return None
        }
        next = after_value
        Filter(filter)
      }
      _ => return None
    }
    parameters.push({ kind, value, })
    previous = kind
  }
  Some((parameters, next))
}

///|
fn read_reason_at(
  input : Bytes,
  offset : Int32,
  end : Int32,
) -> (Bytes, Int32)? {
  let (reason, next) = match read_bytes_at(input, offset, end) {
    Some(pair) => pair
    None => return None
  }
  if reason.length() > 1024 {
    return None
  }
  Some((reason, next))
}

///|
pub extend TrackName with Debug::{to_repr}

///|
pub extend TrackName with Eq::{equal, not_equal}

///|
pub extend ParameterValue with Debug::{to_repr}

///|
pub extend ParameterValue with Eq::{equal, not_equal}

///|
pub extend SubscriptionFilter with Debug::{to_repr}

///|
pub extend SubscriptionFilter with Eq::{equal, not_equal}

///|
pub extend MessageParameter with Debug::{to_repr}

///|
pub extend MessageParameter with Eq::{equal, not_equal}
