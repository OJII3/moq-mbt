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
} derive(Debug, Eq)

///|
pub(all) struct MessageParameter {
  kind : UInt64
  value : ParameterValue
} derive(Debug, Eq)

///|
pub(all) struct SubscribeMessage {
  request_id : UInt64
  track : TrackName
  parameters : Array[MessageParameter]
} derive(Debug, Eq)

///|
pub(all) struct PublishMessage {
  request_id : UInt64
  track : TrackName
  track_alias : UInt64
  parameters : Array[MessageParameter]
  properties : Bytes
} derive(Debug, Eq)

///|
pub(all) struct SubscribeOkMessage {
  track_alias : UInt64
  parameters : Array[MessageParameter]
  properties : Bytes
} derive(Debug, Eq)

///|
pub(all) struct RequestOkMessage {
  parameters : Array[MessageParameter]
  properties : Bytes
} derive(Debug, Eq)

///|
pub(all) struct RequestErrorMessage {
  code : UInt64
  retry_interval : UInt64
  reason : Bytes
  redirect : Bytes
} derive(Debug, Eq)

///|
pub(all) struct PublishDoneMessage {
  code : UInt64
  stream_count : UInt64
  reason : Bytes
} derive(Debug, Eq)

///|
pub(all) enum ControlMessage {
  Subscribe(SubscribeMessage)
  Publish(PublishMessage)
  SubscribeOk(SubscribeOkMessage)
  RequestOk(RequestOkMessage)
  RequestError(RequestErrorMessage)
  PublishDone(PublishDoneMessage)
} derive(Debug, Eq)

///|
fn append_vi64s(values : Array[Byte], value : UInt64) -> Unit {
  for byte in encode_vi64(value) {
    values.push(byte)
  }
}

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
      (0x10 | 0x20 | 0x22, Byte(value)) => encoded.push(value)
      (0x09, Location(group, object)) => {
        append_vi64s(encoded, group)
        append_vi64s(encoded, object)
      }
      (0x02 | 0x04 | 0x06 | 0x08 | 0x32, VarInt(value)) =>
        append_vi64s(encoded, value)
      (0x03 | 0x21, Data(value)) => {
        append_vi64s(encoded, value.length().to_uint64())
        for byte in value {
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
fn control_frame(kind : UInt64, payload : Bytes) -> Result[Bytes, CodecError] {
  if payload.length() > 65535 {
    return Err(MessageTooLarge)
  }
  let length = payload.length()
  Ok(
    encode_vi64(kind) +
    Bytes::from_array([(length >> 8).to_byte(), length.to_byte()]) +
    payload,
  )
}

///|
/// Encode a Draft 18 SUBSCRIBE request (type 0x3).
pub fn encode_subscribe(
  message : SubscribeMessage,
) -> Result[Bytes, CodecError] {
  let track = match encode_track_name(message.track) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  let parameters = match encode_parameters(message.parameters) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  control_frame(3, encode_vi64(message.request_id) + track + parameters)
}

///|
/// Encode a Draft 18 PUBLISH request (type 0x1d).
pub fn encode_publish(message : PublishMessage) -> Result[Bytes, CodecError] {
  let track = match encode_track_name(message.track) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  let parameters = match encode_parameters(message.parameters) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  control_frame(
    0x1d,
    encode_vi64(message.request_id) +
    track +
    encode_vi64(message.track_alias) +
    parameters +
    message.properties,
  )
}

///|
/// Encode SUBSCRIBE_OK (type 0x4), the first response to SUBSCRIBE.
pub fn encode_subscribe_ok(
  message : SubscribeOkMessage,
) -> Result[Bytes, CodecError] {
  let parameters = match encode_parameters(message.parameters) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  control_frame(
    4,
    encode_vi64(message.track_alias) + parameters + message.properties,
  )
}

///|
/// Encode REQUEST_OK (type 0x7), including the PUBLISH_OK response alias.
pub fn encode_request_ok(
  message : RequestOkMessage,
) -> Result[Bytes, CodecError] {
  let parameters = match encode_parameters(message.parameters) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  control_frame(7, parameters + message.properties)
}

///|
/// Encode REQUEST_ERROR (type 0x5). Redirect is an optional raw structure.
pub fn encode_request_error(
  message : RequestErrorMessage,
) -> Result[Bytes, CodecError] {
  let payload = encode_vi64(message.code) +
    encode_vi64(message.retry_interval) +
    encode_vi64(message.reason.length().to_uint64()) +
    message.reason +
    message.redirect
  control_frame(5, payload)
}

///|
/// Encode PUBLISH_DONE (type 0xb).
pub fn encode_publish_done(
  message : PublishDoneMessage,
) -> Result[Bytes, CodecError] {
  let payload = encode_vi64(message.code) +
    encode_vi64(message.stream_count) +
    encode_vi64(message.reason.length().to_uint64()) +
    message.reason
  control_frame(0x0b, payload)
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
        next = after_delta + 1
        Byte(input[after_delta])
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
      0x03 | 0x21 => {
        let (value, after_value) = match
          read_bytes_at(input, after_delta, end) {
          Some(pair) => pair
          None => return None
        }
        next = after_value
        Data(value)
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
/// Decode one complete Draft 18 control message. Bytes after the returned
/// consumed count belong to the next control message on the same stream.
pub fn decode_control(input : Bytes) -> DecodeResult[ControlMessage] {
  let (kind, header) = match read_vi64(input, 0, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  if input.length() - header < 2 {
    return Incomplete
  }
  let length = (input[header].to_int() << 8) | input[header + 1].to_int()
  let start = header + 2
  let end = start + length
  if input.length() < end {
    return Incomplete
  }
  let invalid = Invalid(MalformedControlMessage)
  match kind {
    3 => {
      let (request_id, offset) = match read_varint_at(input, start, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (track, offset) = match read_track_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (parameters, offset) = match read_parameters_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      if offset != end {
        return invalid
      }
      Complete(Subscribe({ request_id, track, parameters, }), end)
    }
    0x1d => {
      let (request_id, offset) = match read_varint_at(input, start, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (track, offset) = match read_track_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (track_alias, offset) = match read_varint_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (parameters, offset) = match read_parameters_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      Complete(
        Publish({
          request_id,
          track,
          track_alias,
          parameters,
          properties: input[offset:end].to_owned(),
        }),
        end,
      )
    }
    4 => {
      let (track_alias, offset) = match read_varint_at(input, start, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (parameters, offset) = match read_parameters_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      Complete(
        SubscribeOk({
          track_alias,
          parameters,
          properties: input[offset:end].to_owned(),
        }),
        end,
      )
    }
    7 => {
      let (parameters, offset) = match read_parameters_at(input, start, end) {
        Some(pair) => pair
        None => return invalid
      }
      Complete(
        RequestOk({ parameters, properties: input[offset:end].to_owned(), }),
        end,
      )
    }
    5 => {
      let (code, offset) = match read_varint_at(input, start, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (retry_interval, offset) = match read_varint_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (reason, offset) = match read_reason_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      Complete(
        RequestError({
          code,
          retry_interval,
          reason,
          redirect: input[offset:end].to_owned(),
        }),
        end,
      )
    }
    0x0b => {
      let (code, offset) = match read_varint_at(input, start, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (stream_count, offset) = match read_varint_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (reason, offset) = match read_reason_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      if offset != end {
        return invalid
      }
      Complete(PublishDone({ code, stream_count, reason, }), end)
    }
    _ => Invalid(UnsupportedControlMessage(kind))
  }
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
pub extend MessageParameter with Debug::{to_repr}

///|
pub extend MessageParameter with Eq::{equal, not_equal}

///|
pub extend SubscribeMessage with Debug::{to_repr}

///|
pub extend SubscribeMessage with Eq::{equal, not_equal}

///|
pub extend PublishMessage with Debug::{to_repr}

///|
pub extend PublishMessage with Eq::{equal, not_equal}

///|
pub extend SubscribeOkMessage with Debug::{to_repr}

///|
pub extend SubscribeOkMessage with Eq::{equal, not_equal}

///|
pub extend RequestOkMessage with Debug::{to_repr}

///|
pub extend RequestOkMessage with Eq::{equal, not_equal}

///|
pub extend RequestErrorMessage with Debug::{to_repr}

///|
pub extend RequestErrorMessage with Eq::{equal, not_equal}

///|
pub extend PublishDoneMessage with Debug::{to_repr}

///|
pub extend PublishDoneMessage with Eq::{equal, not_equal}

///|
pub extend ControlMessage with Debug::{to_repr}

///|
pub extend ControlMessage with Eq::{equal, not_equal}
