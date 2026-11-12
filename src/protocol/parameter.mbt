///|
/// One full track name. Namespace fields and track names are opaque bytes.
#derive(Debug, Eq)
pub(all) struct TrackName {
  namespace_fields : Array[Bytes]
  name : Bytes
}

///|
/// Build a track name with a single namespace field, for the common
/// `namespace/track` shape.
pub fn TrackName::from_utf8_field(
  namespace_field : String,
  name : String,
) -> TrackName {
  {
    namespace_fields: [@utf8.encode(namespace_field)],
    name: @utf8.encode(name),
  }
}

///|
/// Build a track name from UTF-8 strings when the namespace has several fields.
pub fn TrackName::from_utf8_fields(
  namespace_fields : Array[String],
  name : String,
) -> TrackName {
  {
    namespace_fields: namespace_fields.map(fn(field) { @utf8.encode(field) }),
    name: @utf8.encode(name),
  }
}

///|
/// Render the track name as `namespace/field/.../track` for logs and display.
/// This is not a canonical identifier: distinct namespace field splits can
/// render to the same string. Byte fields that are not valid UTF-8 are decoded
/// lossily; use the struct fields when exact bytes matter.
pub fn TrackName::to_display_string(self : TrackName) -> String {
  let parts = self.namespace_fields.map(fn(field) { @utf8.decode_lossy(field) })
  parts.push(@utf8.decode_lossy(self.name))
  parts.join("/")
}

///|
/// A namespace is an opaque tuple of non-empty byte fields.
#derive(Debug, Eq)
pub(all) struct TrackNamespace {
  fields : Array[Bytes]
}

///|
#derive(Debug, Eq)
pub(all) enum ParameterValue {
  VarInt(UInt64)
  Byte(Byte)
  Data(Bytes)
  Location(UInt64, UInt64)
  Filter(SubscriptionFilter)
  NamespacePrefix(TrackNamespace)
}

///|
#derive(Debug, Eq)
pub(all) enum SubscriptionFilter {
  NextGroupStart
  LargestObject
  AbsoluteStart(UInt64, UInt64)
  AbsoluteRange(UInt64, UInt64, UInt64)
}

///|
#derive(Debug, Eq)
pub(all) struct MessageParameter {
  kind : UInt64
  value : ParameterValue
}

///|
let forward_parameter_kind : UInt64 = 0x10

///|
/// Whether a subscription asks the publisher to forward Objects. The FORWARD
/// parameter defaults to true when it is absent.
pub fn forward_requested(parameters : Array[MessageParameter]) -> Bool {
  for parameter in parameters {
    if parameter.kind == forward_parameter_kind {
      match parameter.value {
        Byte(value) => return value != 0
        _ => return false
      }
    }
  }
  true
}

///|
/// Build the FORWARD parameter used by SUBSCRIBE and REQUEST_UPDATE.
pub fn forward_parameter(enabled : Bool) -> MessageParameter {
  { kind: forward_parameter_kind, value: Byte(if enabled { 1 } else { 0 }), }
}

///|
/// Build the RENDEZVOUS_TIMEOUT parameter used by SUBSCRIBE. The value is the
/// time in milliseconds the subscriber is willing to wait for a publisher.
pub fn rendezvous_timeout_parameter(milliseconds : UInt64) -> MessageParameter {
  { kind: 0x04, value: VarInt(milliseconds), }
}

///|
fn encode_track_name(track : TrackName) -> Result[Bytes, CodecError] {
  if track.namespace_fields.length() > 32 {
    return Err(InvalidTrackName)
  }
  let capacity = 9 +
    track.namespace_fields.fold(
      fn(acc, field) { acc + 9 + field.length() },
      init=track.name.length(),
    )
  let encoded : Array[Byte] = Array::Array(capacity~)
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
      (0x02 | 0x04 | 0x06 | 0x08 | 0x0a | 0x32, VarInt(value)) =>
        append_vi64s(encoded, value)
      (0x03, Data(value)) => {
        append_vi64s(encoded, value.length().to_uint64())
        for byte in value {
          encoded.push(byte)
        }
      }
      (0x34, NamespacePrefix(value)) => {
        let body = match encode_track_namespace(value) {
          Ok(bytes) => bytes
          Err(error) => return Err(error)
        }
        append_vi64s(encoded, body.length().to_uint64())
        for byte in body {
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
fn encode_track_namespace(
  track_namespace : TrackNamespace,
) -> Result[Bytes, CodecError] {
  if track_namespace.fields.length() > 32 {
    return Err(InvalidTrackName)
  }
  let capacity = 9 +
    track_namespace.fields.fold(
      fn(acc, field) { acc + 9 + field.length() },
      init=0,
    )
  let encoded : Array[Byte] = Array::Array(capacity~)
  append_vi64s(encoded, track_namespace.fields.length().to_uint64())
  let mut total = 0
  for field in track_namespace.fields {
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
  Ok(Bytes::from_array(encoded))
}

///|
/// Join a namespace prefix and suffix while enforcing full namespace limits.
pub fn join_track_namespace(
  prefix : TrackNamespace,
  suffix : TrackNamespace,
) -> Result[TrackNamespace, CodecError] {
  if prefix.fields.length() + suffix.fields.length() > 32 {
    return Err(InvalidTrackName)
  }
  let fields = prefix.fields.copy()
  for field in suffix.fields {
    fields.push(field)
  }
  match encode_track_namespace({ fields, }) {
    Ok(_) => Ok({ fields, })
    Err(error) => Err(error)
  }
}

///|
fn encode_subscription_filter(
  filter : SubscriptionFilter,
) -> Result[Bytes, CodecError] {
  let encoded : Array[Byte] = Array::Array(capacity=40)
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
  guard read_varint_at(input, 0, end) is Some((kind, offset)) else {
    return None
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
      guard read_varint_at(input, offset, end) is Some((group, offset)) else {
        return None
      }
      guard read_varint_at(input, offset, end) is Some((object, offset)) else {
        return None
      }
      if offset != end {
        return None
      }
      AbsoluteStart(group, object)
    }
    4 => {
      guard read_varint_at(input, offset, end) is Some((group, offset)) else {
        return None
      }
      guard read_varint_at(input, offset, end) is Some((object, offset)) else {
        return None
      }
      guard read_varint_at(input, offset, end) is Some((delta, offset)) else {
        return None
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
fn read_namespace_fields_at(
  input : Bytes,
  offset : Int32,
  end : Int32,
  count : UInt64,
) -> (Array[Bytes], Int32, Int32)? {
  let fields : Array[Bytes] = []
  let mut total = 0
  let mut next = offset
  for _ in 0..<count.to_int() {
    guard read_bytes_at(input, next, end) is Some((field, field_end)) else {
      return None
    }
    if field.is_empty() {
      return None
    }
    total += field.length()
    if total > 4096 {
      return None
    }
    fields.push(field)
    next = field_end
  }
  Some((fields, total, next))
}

///|
fn read_track_at(
  input : Bytes,
  offset : Int32,
  end : Int32,
) -> (TrackName, Int32)? {
  guard read_varint_at(input, offset, end) is Some((field_count, first_next)) else {
    return None
  }
  if field_count > 32UL {
    return None
  }
  guard read_namespace_fields_at(input, first_next, end, field_count)
    is Some((namespace_fields, total, next)) else {
    return None
  }
  guard read_bytes_at(input, next, end) is Some((name, name_end)) else {
    return None
  }
  if total + name.length() > 4096 {
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
  guard read_varint_at(input, offset, end) is Some((count, first_next)) else {
    return None
  }
  if count > (end - first_next).to_uint64() {
    return None
  }
  let parameters : Array[MessageParameter] = []
  let mut previous = 0UL
  let mut next = first_next
  for i in 0..<count.to_int() {
    guard read_varint_at(input, next, end) is Some((delta, after_delta)) else {
      return None
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
        guard read_varint_at(input, after_delta, end)
          is Some((group, after_group)) else {
          return None
        }
        guard read_varint_at(input, after_group, end)
          is Some((object, after_object)) else {
          return None
        }
        next = after_object
        Location(group, object)
      }
      0x02 | 0x04 | 0x06 | 0x08 | 0x0a | 0x32 => {
        guard read_varint_at(input, after_delta, end)
          is Some((value, after_value)) else {
          return None
        }
        next = after_value
        VarInt(value)
      }
      0x03 => {
        guard read_bytes_at(input, after_delta, end)
          is Some((value, after_value)) else {
          return None
        }
        next = after_value
        Data(value)
      }
      0x34 => {
        guard read_bytes_at(input, after_delta, end)
          is Some((value, after_value)) else {
          return None
        }
        guard read_track_namespace_at(value, 0, value.length())
          is Some((track_namespace, namespace_end)) else {
          return None
        }
        if namespace_end != value.length() {
          return None
        }
        next = after_value
        NamespacePrefix(track_namespace)
      }
      0x21 => {
        guard read_bytes_at(input, after_delta, end)
          is Some((bytes, after_value)) else {
          return None
        }
        guard decode_subscription_filter(bytes) is Some(filter) else {
          return None
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
fn read_track_namespace_at(
  input : Bytes,
  offset : Int32,
  end : Int32,
) -> (TrackNamespace, Int32)? {
  guard read_varint_at(input, offset, end) is Some((count, first_next)) else {
    return None
  }
  if count > 32UL {
    return None
  }
  guard read_namespace_fields_at(input, first_next, end, count)
    is Some((fields, _, next)) else {
    return None
  }
  Some(({ fields, }, next))
}

///|
fn read_reason_at(
  input : Bytes,
  offset : Int32,
  end : Int32,
) -> (Bytes, Int32)? {
  guard read_bytes_at(input, offset, end) is Some((reason, next)) else {
    return None
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
pub extend TrackNamespace with Debug::{to_repr}

///|
pub extend TrackNamespace with Eq::{equal, not_equal}

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
