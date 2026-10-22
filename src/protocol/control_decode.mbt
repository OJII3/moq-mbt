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
      let (request_id, offset) = match read_varint_at(input, start, end) {
        Some(pair) => pair
        None => return invalid
      }
      let (parameters, offset) = match read_parameters_at(input, offset, end) {
        Some(pair) => pair
        None => return invalid
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
