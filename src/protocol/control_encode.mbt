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
  match validate_parameters_scope(message.parameters, Subscribe) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
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
  match validate_parameters_scope(message.parameters, Publish) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
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
  match validate_parameters_scope(message.parameters, SubscribeOk) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
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
  encode_request_ok_for(message, Publish)
}

///|
/// Encode REQUEST_OK for the request type it answers. REQUEST_UPDATE_OK uses
/// this same wire message and permits only EXPIRES and LARGEST_OBJECT.
pub fn encode_request_ok_for(
  message : RequestOkMessage,
  context : RequestOkContext,
) -> Result[Bytes, CodecError] {
  match validate_request_ok(message, context) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let parameters = match encode_parameters(message.parameters) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  control_frame(7, parameters + message.properties)
}

///|
/// Encode a REQUEST_UPDATE. Its Request ID is newly consumed by this update;
/// the request being updated is identified by the bidirectional stream.
pub fn encode_request_update(
  message : RequestUpdateMessage,
) -> Result[Bytes, CodecError] {
  encode_request_update_for(message, RequestStream)
}

///|
/// Encode a REQUEST_UPDATE using the request type's permitted parameter scope.
pub fn encode_request_update_for(
  message : RequestUpdateMessage,
  context : ControlStreamContext,
) -> Result[Bytes, CodecError] {
  let scope : ParameterScope = if context == FetchRequestStream {
    ParameterScope::FetchUpdate
  } else {
    ParameterScope::RequestUpdate
  }
  match validate_parameters_scope(message.parameters, scope) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let parameters = match encode_parameters(message.parameters) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  control_frame(2, encode_vi64(message.request_id) + parameters)
}

///|
/// Encode a Draft 18 FETCH request (type 0x16).
pub fn encode_fetch(message : FetchMessage) -> Result[Bytes, CodecError] {
  match validate_parameters_scope(message.parameters, Fetch) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let mut payload = encode_vi64(message.request_id)
  match message.fetch_type {
    StandaloneFetch(track, start, end) => {
      if start.group > end.group ||
        (
          start.group == end.group &&
          end.object != 0UL &&
          start.object >= end.object
        ) {
        return Err(InvalidFetchRange)
      }
      let track_bytes = match encode_track_name(track) {
        Ok(bytes) => bytes
        Err(error) => return Err(error)
      }
      payload = payload +
        encode_vi64(1UL) +
        track_bytes +
        encode_vi64(start.group) +
        encode_vi64(start.object) +
        encode_vi64(end.group) +
        encode_vi64(end.object)
    }
    RelativeJoiningFetch(joining_request_id, joining_start) =>
      payload = payload +
        encode_vi64(2UL) +
        encode_vi64(joining_request_id) +
        encode_vi64(joining_start)
    AbsoluteJoiningFetch(joining_request_id, joining_start) =>
      payload = payload +
        encode_vi64(3UL) +
        encode_vi64(joining_request_id) +
        encode_vi64(joining_start)
  }
  let parameters = match encode_parameters(message.parameters) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  control_frame(0x16, payload + parameters)
}

///|
/// Encode FETCH_OK, the first successful response to a FETCH request.
pub fn encode_fetch_ok(message : FetchOkMessage) -> Result[Bytes, CodecError] {
  if message.end_of_track > 1 {
    return Err(InvalidEndOfTrack)
  }
  match validate_parameters_scope(message.parameters, FetchOk) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if !valid_object_properties(message.track_properties) {
    return Err(InvalidTrackProperties)
  }
  let parameters = match encode_parameters(message.parameters) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  control_frame(
    0x18,
    Bytes::from_array([message.end_of_track]) +
    encode_vi64(message.end_location.group) +
    encode_vi64(message.end_location.object) +
    parameters +
    message.track_properties,
  )
}

///|
/// Encode a GOAWAY sent on the control stream. This form includes a Request ID.
pub fn encode_goaway_control(
  message : GoAwayControlMessage,
) -> Result[Bytes, CodecError] {
  encode_goaway_payload(
    message.uri,
    message.timeout_ms,
    Some(message.request_id),
  )
}

///|
/// Encode a GOAWAY sent on a request stream. This form has no Request ID.
pub fn encode_goaway_request(
  message : GoAwayRequestMessage,
) -> Result[Bytes, CodecError] {
  encode_goaway_payload(message.uri, message.timeout_ms, None)
}

///|
fn encode_goaway_payload(
  uri : Bytes,
  timeout_ms : UInt64,
  request_id : UInt64?,
) -> Result[Bytes, CodecError] {
  if uri.length() > 8192 {
    return Err(InvalidGoAwayUri)
  }
  let mut payload = encode_vi64(uri.length().to_uint64()) +
    uri +
    encode_vi64(timeout_ms)
  match request_id {
    Some(value) => payload = payload + encode_vi64(value)
    None => ()
  }
  control_frame(0x10, payload)
}

///|
pub fn encode_publish_namespace(
  message : NamespaceRequestMessage,
) -> Result[Bytes, CodecError] {
  encode_namespace_request(6, message)
}

///|
pub fn encode_subscribe_namespace(
  message : NamespaceRequestMessage,
) -> Result[Bytes, CodecError] {
  encode_namespace_request(0x50, message)
}

///|
fn encode_namespace_request(
  kind : UInt64,
  message : NamespaceRequestMessage,
) -> Result[Bytes, CodecError] {
  match validate_parameters_scope(message.parameters, NamespaceRequest) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let ns_bytes = match encode_track_namespace(message.track_namespace) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  let parameters = match encode_parameters(message.parameters) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  control_frame(kind, encode_vi64(message.request_id) + ns_bytes + parameters)
}

///|
pub fn encode_namespace(
  message : NamespaceSuffixMessage,
) -> Result[Bytes, CodecError] {
  encode_namespace_suffix(8, message)
}

///|
pub fn encode_namespace_done(
  message : NamespaceSuffixMessage,
) -> Result[Bytes, CodecError] {
  encode_namespace_suffix(0xe, message)
}

///|
fn encode_namespace_suffix(
  kind : UInt64,
  message : NamespaceSuffixMessage,
) -> Result[Bytes, CodecError] {
  let ns_bytes = match encode_track_namespace(message.track_namespace) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  control_frame(kind, ns_bytes)
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
