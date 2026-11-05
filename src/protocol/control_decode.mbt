///|
/// Decode one complete Draft 18 control message. Bytes after the returned
/// consumed count belong to the next control message on the same stream.
///
/// This entry point assumes `AnyStream`. Use `decode_control_with_context`
/// when the stream position is known: `REQUEST_OK` parameter scope and
/// `GOAWAY` layout differ between the control stream and request streams.
pub fn decode_control(input : Bytes) -> DecodeResult[ControlMessage] {
  decode_control_with_context(input, AnyStream)
}

///|
/// Decode one control message using its stream and response context. Use this
/// entry point for GOAWAY and REQUEST_OK so their context-specific fields are
/// checked instead of inferred from the payload.
///
/// Context is required when the same message type has a different shape
/// depending on where it appears. `RequestStreamFor(context)` tells the decoder
/// which request type a REQUEST_OK answers; `FetchRequestStream` selects the
/// FETCH update parameter scope.
pub fn decode_control_with_context(
  input : Bytes,
  context : ControlStreamContext,
) -> DecodeResult[ControlMessage] {
  guard read_vi64(input, 0, input.length()) is Some((kind, header)) else {
    return Incomplete
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
      guard read_varint_at(input, start, end) is Some((request_id, offset)) else {
        return invalid
      }
      guard read_track_at(input, offset, end) is Some((track, offset)) else {
        return invalid
      }
      guard read_parameters_at(input, offset, end) is Some((parameters, offset)) else {
        return invalid
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
      guard read_varint_at(input, start, end) is Some((request_id, offset)) else {
        return invalid
      }
      guard read_track_at(input, offset, end) is Some((track, offset)) else {
        return invalid
      }
      guard read_varint_at(input, offset, end) is Some((track_alias, offset)) else {
        return invalid
      }
      guard read_parameters_at(input, offset, end) is Some((parameters, offset)) else {
        return invalid
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
      guard read_varint_at(input, start, end) is Some((track_alias, offset)) else {
        return invalid
      }
      guard read_parameters_at(input, offset, end) is Some((parameters, offset)) else {
        return invalid
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
      guard read_parameters_at(input, start, end) is Some((parameters, offset)) else {
        return invalid
      }
      let message : RequestOkMessage = {
        parameters,
        properties: input[offset:end].to_owned(),
      }
      let response_context = match context {
        RequestStreamFor(value) => Some(value)
        FetchRequestStream => Some(RequestOkContext::FetchUpdate)
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
      guard read_varint_at(input, start, end) is Some((request_id, offset)) else {
        return invalid
      }
      guard read_parameters_at(input, offset, end) is Some((parameters, offset)) else {
        return invalid
      }
      let update_context = match context {
        FetchRequestStream => ParameterScope::FetchUpdate
        _ => ParameterScope::RequestUpdate
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
      guard read_varint_at(input, start, end) is Some((request_id, offset)) else {
        return invalid
      }
      guard read_varint_at(input, offset, end) is Some((fetch_kind, offset)) else {
        return invalid
      }
      let fetch_type = match fetch_kind {
        1 => {
          guard read_track_at(input, offset, end) is Some((track, offset)) else {
            return invalid
          }
          guard read_varint_at(input, offset, end)
            is Some((start_group, offset)) else {
            return invalid
          }
          guard read_varint_at(input, offset, end)
            is Some((start_object, offset)) else {
            return invalid
          }
          guard read_varint_at(input, offset, end) is Some((end_group, offset)) else {
            return invalid
          }
          guard read_varint_at(input, offset, end) is Some((end_object, offset)) else {
            return invalid
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
          guard read_varint_at(input, offset, end)
            is Some((joining_request_id, offset)) else {
            return invalid
          }
          guard read_varint_at(input, offset, end)
            is Some((joining_start, offset)) else {
            return invalid
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
      guard read_parameters_at(input, fetch_type.1, end)
        is Some((parameters, offset)) else {
        return invalid
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
      guard read_varint_at(input, start + 1, end) is Some((end_group, offset)) else {
        return invalid
      }
      guard read_varint_at(input, offset, end) is Some((end_object, offset)) else {
        return invalid
      }
      guard read_parameters_at(input, offset, end) is Some((parameters, offset)) else {
        return invalid
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
      guard read_bytes_at(input, start, end) is Some((uri, offset)) else {
        return invalid
      }
      if uri.length() > 8192 {
        return invalid
      }
      guard read_varint_at(input, offset, end) is Some((timeout_ms, offset)) else {
        return invalid
      }
      match context {
        ControlStream => {
          guard read_varint_at(input, offset, end) is Some((request_id, offset)) else {
            return invalid
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
            guard read_varint_at(input, offset, end)
              is Some((request_id, offset)) else {
              return invalid
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
      guard read_varint_at(input, start, end) is Some((code, offset)) else {
        return invalid
      }
      guard read_varint_at(input, offset, end) is Some((retry_interval, offset)) else {
        return invalid
      }
      guard read_reason_at(input, offset, end) is Some((reason, offset)) else {
        return invalid
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
      guard read_varint_at(input, start, end) is Some((code, offset)) else {
        return invalid
      }
      guard read_varint_at(input, offset, end) is Some((stream_count, offset)) else {
        return invalid
      }
      guard read_reason_at(input, offset, end) is Some((reason, offset)) else {
        return invalid
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
      guard read_varint_at(input, start, end) is Some((request_id, offset)) else {
        return invalid
      }
      guard read_track_namespace_at(input, offset, end)
        is Some((track_namespace, offset)) else {
        return invalid
      }
      guard read_parameters_at(input, offset, end) is Some((parameters, offset)) else {
        return invalid
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
      guard read_track_namespace_at(input, start, end)
        is Some((track_namespace, offset)) else {
        return invalid
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
