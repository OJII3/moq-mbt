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
pub(all) struct RequestUpdateMessage {
  request_id : UInt64
  parameters : Array[MessageParameter]
} derive(Debug, Eq)

///|
pub(all) struct FetchLocation {
  group : UInt64
  object : UInt64
} derive(Debug, Eq)

///|
/// End Location stores the wire value: Object is exclusive (last Object + 1),
/// and zero requests the entire Group.
pub(all) enum FetchType {
  StandaloneFetch(TrackName, FetchLocation, FetchLocation)
  RelativeJoiningFetch(UInt64, UInt64)
  AbsoluteJoiningFetch(UInt64, UInt64)
} derive(Debug, Eq)

///|
pub(all) struct FetchMessage {
  request_id : UInt64
  fetch_type : FetchType
  parameters : Array[MessageParameter]
} derive(Debug, Eq)

///|
pub(all) struct FetchOkMessage {
  end_of_track : Byte
  end_location : FetchLocation
  parameters : Array[MessageParameter]
  track_properties : Bytes
} derive(Debug, Eq)

///|
pub(all) struct GoAwayControlMessage {
  uri : Bytes
  timeout_ms : UInt64
  request_id : UInt64
} derive(Debug, Eq)

///|
pub(all) struct GoAwayRequestMessage {
  uri : Bytes
  timeout_ms : UInt64
} derive(Debug, Eq)

///|
pub(all) enum RequestOkContext {
  Publish
  RequestUpdate
  TrackStatus
  Namespace
} derive(Debug, Eq)

///|
pub(all) enum ControlStreamContext {
  AnyStream
  ControlStream
  RequestStream
  RequestStreamFor(RequestOkContext)
  FetchRequestStream
} derive(Debug, Eq)

///|
priv enum ParameterScope {
  Subscribe
  Publish
  SubscribeOk
  RequestUpdate
  Fetch
  FetchUpdate
  FetchOk
  NamespaceRequest
  RequestOk(RequestOkContext)
}

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
pub(all) struct NamespaceRequestMessage {
  request_id : UInt64
  track_namespace : TrackNamespace
  parameters : Array[MessageParameter]
} derive(Debug, Eq)

///|
pub(all) struct NamespaceSuffixMessage {
  track_namespace : TrackNamespace
} derive(Debug, Eq)

///|
pub(all) enum ControlMessage {
  Subscribe(SubscribeMessage)
  Publish(PublishMessage)
  SubscribeOk(SubscribeOkMessage)
  RequestOk(RequestOkMessage)
  RequestUpdate(RequestUpdateMessage)
  Fetch(FetchMessage)
  FetchOk(FetchOkMessage)
  GoAwayControl(GoAwayControlMessage)
  GoAwayRequest(GoAwayRequestMessage)
  RequestError(RequestErrorMessage)
  PublishDone(PublishDoneMessage)
  PublishNamespace(NamespaceRequestMessage)
  SubscribeNamespace(NamespaceRequestMessage)
  Namespace(NamespaceSuffixMessage)
  NamespaceDone(NamespaceSuffixMessage)
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
  match validate_request_update_parameters(message.parameters) {
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
/// Encode a REQUEST_UPDATE using the scope of the request it updates.
pub fn encode_request_update_for(
  message : RequestUpdateMessage,
  context : ControlStreamContext,
) -> Result[Bytes, CodecError] {
  let scope = if context == FetchRequestStream {
    FetchUpdate
  } else {
    RequestUpdate
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

///|
/// Decode one complete Draft 18 control message. Bytes after the returned
/// consumed count belong to the next control message on the same stream.
pub fn decode_control(input : Bytes) -> DecodeResult[ControlMessage] {
  decode_control_with_context(input, AnyStream)
}

///|
/// Decode one control message using its stream and response context. Use this
/// entry point for GOAWAY and REQUEST_OK so their context-specific fields are
/// checked instead of inferred from the payload.
pub fn decode_control_with_context(
  input : Bytes,
  context : ControlStreamContext,
) -> DecodeResult[ControlMessage] {
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
      match validate_parameters_scope(parameters, Subscribe) {
        Ok(_) => ()
        Err(_) => return invalid
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
      match validate_parameters_scope(parameters, Publish) {
        Ok(_) => ()
        Err(_) => return invalid
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
      match validate_parameters_scope(parameters, SubscribeOk) {
        Ok(_) => ()
        Err(_) => return invalid
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
      let message : RequestOkMessage = {
        parameters,
        properties: input[offset:end].to_owned(),
      }
      let response_context = match context {
        RequestStreamFor(value) => Some(value)
        FetchRequestStream => return invalid
        ControlStream | RequestStream => return invalid
        AnyStream => None
      }
      match response_context {
        Some(value) =>
          match validate_request_ok(message, value) {
            Ok(_) => ()
            Err(_) => return invalid
          }
        None => ()
      }
      Complete(RequestOk(message), end)
    }
    2 => {
      if context == ControlStream {
        return invalid
      }
      let (request_id, offset) = match read_varint_at(input, start, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (parameters, offset) = match read_parameters_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      let update_context = match context {
        FetchRequestStream => FetchUpdate
        _ => RequestUpdate
      }
      match validate_parameters_scope(parameters, update_context) {
        Ok(_) => ()
        Err(_) => return invalid
      }
      if offset != end {
        return invalid
      }
      Complete(RequestUpdate({ request_id, parameters, }), end)
    }
    0x16 => {
      if context == ControlStream {
        return invalid
      }
      let (request_id, offset) = match read_varint_at(input, start, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (fetch_kind, offset) = match read_varint_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      let fetch_type = match fetch_kind {
        1 => {
          let (track, offset) = match read_track_at(input, offset, end) {
            Some(pair) => pair
            None => return invalid
          }
          let (start_group, offset) = match read_varint_at(input, offset, end) {
            Some(pair) => pair
            None => return invalid
          }
          let (start_object, offset) = match
            read_varint_at(input, offset, end) {
            Some(pair) => pair
            None => return invalid
          }
          let (end_group, offset) = match read_varint_at(input, offset, end) {
            Some(pair) => pair
            None => return invalid
          }
          let (end_object, offset) = match read_varint_at(input, offset, end) {
            Some(pair) => pair
            None => return invalid
          }
          if start_group > end_group ||
            (
              start_group == end_group &&
              end_object != 0UL &&
              start_object >= end_object
            ) {
            return invalid
          }
          (
            StandaloneFetch(
              track,
              { group: start_group, object: start_object, },
              { group: end_group, object: end_object, },
            ),
            offset,
          )
        }
        2 | 3 => {
          let (joining_request_id, offset) = match
            read_varint_at(input, offset, end) {
            Some(pair) => pair
            None => return invalid
          }
          let (joining_start, offset) = match
            read_varint_at(input, offset, end) {
            Some(pair) => pair
            None => return invalid
          }
          let value = if fetch_kind == 2 {
            RelativeJoiningFetch(joining_request_id, joining_start)
          } else {
            AbsoluteJoiningFetch(joining_request_id, joining_start)
          }
          (value, offset)
        }
        _ => return invalid
      }
      let (parameters, offset) = match
        read_parameters_at(input, fetch_type.1, end) {
        Some(pair) => pair
        None => return invalid
      }
      match validate_parameters_scope(parameters, Fetch) {
        Ok(_) => ()
        Err(_) => return invalid
      }
      if offset != end {
        return invalid
      }
      Complete(
        Fetch({ request_id, fetch_type: fetch_type.0, parameters, }),
        end,
      )
    }
    0x18 => {
      match context {
        ControlStream | RequestStream => return invalid
        FetchRequestStream => ()
        RequestStreamFor(_) => return invalid
        AnyStream => ()
      }
      if start >= end || input[start] > 1 {
        return invalid
      }
      let (end_group, offset) = match read_varint_at(input, start + 1, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (end_object, offset) = match read_varint_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (parameters, offset) = match read_parameters_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      match validate_parameters_scope(parameters, FetchOk) {
        Ok(_) => ()
        Err(_) => return invalid
      }
      let track_properties = input[offset:end].to_owned()
      if !valid_object_properties(track_properties) {
        return invalid
      }
      Complete(
        FetchOk({
          end_of_track: input[start],
          end_location: { group: end_group, object: end_object, },
          parameters,
          track_properties,
        }),
        end,
      )
    }
    0x10 => {
      let (uri, offset) = match read_bytes_at(input, start, end) {
        Some(pair) => pair
        None => return invalid
      }
      if uri.length() > 8192 {
        return invalid
      }
      let (timeout_ms, offset) = match read_varint_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      match context {
        ControlStream => {
          let (request_id, offset) = match read_varint_at(input, offset, end) {
            Some(pair) => pair
            None => return invalid
          }
          if offset != end {
            return invalid
          }
          Complete(GoAwayControl({ uri, timeout_ms, request_id, }), end)
        }
        RequestStream | RequestStreamFor(_) | FetchRequestStream => {
          if offset != end {
            return invalid
          }
          Complete(GoAwayRequest({ uri, timeout_ms, }), end)
        }
        AnyStream =>
          if offset == end {
            Complete(GoAwayRequest({ uri, timeout_ms, }), end)
          } else {
            let (request_id, offset) = match
              read_varint_at(input, offset, end) {
              Some(pair) => pair
              None => return invalid
            }
            if offset != end {
              return invalid
            }
            Complete(GoAwayControl({ uri, timeout_ms, request_id, }), end)
          }
      }
    }
    5 => {
      if context == ControlStream {
        return invalid
      }
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
    6 | 0x50 => {
      if context == ControlStream {
        return invalid
      }
      let (request_id, offset) = match read_varint_at(input, start, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (track_namespace, offset) = match
        read_track_namespace_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (parameters, offset) = match read_parameters_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
      }
      match validate_parameters_scope(parameters, NamespaceRequest) {
        Ok(_) => ()
        Err(_) => return invalid
      }
      if offset != end {
        return invalid
      }
      let message = { request_id, track_namespace, parameters, }
      if kind == 6 {
        Complete(PublishNamespace(message), end)
      } else {
        Complete(SubscribeNamespace(message), end)
      }
    }
    8 | 0xe => {
      if context == ControlStream {
        return invalid
      }
      let (track_namespace, offset) = match
        read_track_namespace_at(input, start, end) {
        Some(pair) => pair
        None => return invalid
      }
      if offset != end {
        return invalid
      }
      if kind == 8 {
        Complete(Namespace({ track_namespace, }), end)
      } else {
        Complete(NamespaceDone({ track_namespace, }), end)
      }
    }
    _ => Invalid(UnsupportedControlMessage(kind))
  }
}

///|
fn validate_parameters_scope(
  parameters : Array[MessageParameter],
  context : ParameterScope,
) -> Result[Unit, CodecError] {
  for parameter in parameters {
    if !parameter_allowed(parameter.kind, context) {
      return Err(InvalidParameterScope(parameter.kind))
    }
  }
  Ok(())
}

///|
fn validate_request_update_parameters(
  parameters : Array[MessageParameter],
) -> Result[Unit, CodecError] {
  validate_parameters_scope(parameters, RequestUpdate)
}

///|
fn parameter_allowed(kind : UInt64, context : ParameterScope) -> Bool {
  match context {
    Subscribe =>
      kind == 0x02 ||
      kind == 0x03 ||
      kind == 0x04 ||
      kind == 0x06 ||
      kind == 0x10 ||
      kind == 0x20 ||
      kind == 0x21 ||
      kind == 0x22 ||
      kind == 0x32
    Publish => kind == 0x03 || kind == 0x08 || kind == 0x09 || kind == 0x10
    SubscribeOk => kind == 0x08 || kind == 0x09
    RequestUpdate =>
      kind == 0x02 ||
      kind == 0x03 ||
      kind == 0x06 ||
      kind == 0x10 ||
      kind == 0x20 ||
      kind == 0x21 ||
      kind == 0x32 ||
      kind == 0x34
    Fetch => kind == 0x03 || kind == 0x0a || kind == 0x20 || kind == 0x22
    FetchUpdate => kind == 0x20
    FetchOk => false
    NamespaceRequest => kind == 0x03
    RequestOk(context) =>
      match context {
        Publish =>
          kind == 0x02 ||
          kind == 0x06 ||
          kind == 0x08 ||
          kind == 0x10 ||
          kind == 0x20 ||
          kind == 0x21 ||
          kind == 0x22 ||
          kind == 0x32
        RequestUpdate => kind == 0x08 || kind == 0x09
        TrackStatus => kind == 0x09
        Namespace => false
      }
  }
}

///|
fn validate_request_ok(
  message : RequestOkMessage,
  context : RequestOkContext,
) -> Result[Unit, CodecError] {
  match validate_parameters_scope(message.parameters, RequestOk(context)) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if context != TrackStatus && !message.properties.is_empty() {
    return Err(InvalidRequestOkProperties)
  }
  Ok(())
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
pub extend RequestUpdateMessage with Debug::{to_repr}

///|
pub extend RequestUpdateMessage with Eq::{equal, not_equal}

///|
pub extend FetchLocation with Debug::{to_repr}

///|
pub extend FetchLocation with Eq::{equal, not_equal}

///|
pub extend FetchType with Debug::{to_repr}

///|
pub extend FetchType with Eq::{equal, not_equal}

///|
pub extend FetchMessage with Debug::{to_repr}

///|
pub extend FetchMessage with Eq::{equal, not_equal}

///|
pub extend FetchOkMessage with Debug::{to_repr}

///|
pub extend FetchOkMessage with Eq::{equal, not_equal}

///|
pub extend GoAwayControlMessage with Debug::{to_repr}

///|
pub extend GoAwayControlMessage with Eq::{equal, not_equal}

///|
pub extend GoAwayRequestMessage with Debug::{to_repr}

///|
pub extend GoAwayRequestMessage with Eq::{equal, not_equal}

///|
pub extend RequestOkContext with Debug::{to_repr}

///|
pub extend RequestOkContext with Eq::{equal, not_equal}

///|
pub extend ControlStreamContext with Debug::{to_repr}

///|
pub extend ControlStreamContext with Eq::{equal, not_equal}

///|
pub extend RequestErrorMessage with Debug::{to_repr}

///|
pub extend RequestErrorMessage with Eq::{equal, not_equal}

///|
pub extend PublishDoneMessage with Debug::{to_repr}

///|
pub extend PublishDoneMessage with Eq::{equal, not_equal}

///|
pub extend NamespaceRequestMessage with Debug::{to_repr}

///|
pub extend NamespaceRequestMessage with Eq::{equal, not_equal}

///|
pub extend NamespaceSuffixMessage with Debug::{to_repr}

///|
pub extend NamespaceSuffixMessage with Eq::{equal, not_equal}

///|
pub extend ControlMessage with Debug::{to_repr}

///|
pub extend ControlMessage with Eq::{equal, not_equal}
