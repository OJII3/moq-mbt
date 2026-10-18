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
