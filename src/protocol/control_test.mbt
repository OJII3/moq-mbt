///|
test "Draft 18 single-track request and response wire formats" {
  let track = @protocol.TrackName::{
    namespace_fields: [b"demo"],
    name: b"video",
  }
  let subscribe = @protocol.SubscribeMessage::{
    request_id: 0,
    track,
    parameters: [],
  }
  let subscribe_wire = b"\x03\x00\x0e\x00\x01\x04demo\x05video\x00"
  assert_eq(@protocol.encode_subscribe(subscribe).unwrap(), subscribe_wire)
  assert_eq(
    @protocol.decode_control(subscribe_wire),
    Complete(Subscribe(subscribe), subscribe_wire.length()),
  )

  let publish = @protocol.PublishMessage::{
    request_id: 0,
    track,
    track_alias: 1,
    parameters: [],
    properties: b"",
  }
  let publish_wire = b"\x1d\x00\x0f\x00\x01\x04demo\x05video\x01\x00"
  assert_eq(@protocol.encode_publish(publish).unwrap(), publish_wire)
  assert_eq(
    @protocol.decode_control(publish_wire),
    Complete(Publish(publish), publish_wire.length()),
  )

  let subscribe_ok = @protocol.SubscribeOkMessage::{
    track_alias: 1,
    parameters: [],
    properties: b"",
  }
  let subscribe_ok_wire = b"\x04\x00\x02\x01\x00"
  assert_eq(
    @protocol.encode_subscribe_ok(subscribe_ok).unwrap(),
    subscribe_ok_wire,
  )
  assert_eq(
    @protocol.decode_control(subscribe_ok_wire),
    Complete(SubscribeOk(subscribe_ok), subscribe_ok_wire.length()),
  )
  assert_eq(
    @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
    b"\x07\x00\x01\x00",
  )
}

///|
test "Draft 18 FETCH variants and FETCH_OK wire formats" {
  let track = @protocol.TrackName::{ namespace_fields: [b"a"], name: b"x", }
  let standalone = @protocol.FetchMessage::{
    request_id: 0,
    fetch_type: StandaloneFetch(track, { group: 1, object: 2, }, {
      group: 3,
      object: 0,
    }),
    parameters: [],
  }
  let standalone_wire = b"\x16\x00\x0c\x00\x01\x01\x01a\x01x\x01\x02\x03\x00\x00"
  assert_eq(@protocol.encode_fetch(standalone), Ok(standalone_wire))
  assert_eq(
    @protocol.decode_control(standalone_wire),
    Complete(Fetch(standalone), standalone_wire.length()),
  )

  let moq5_standalone = @protocol.FetchMessage::{
    request_id: 4,
    fetch_type: StandaloneFetch(
      { namespace_fields: [b"live"], name: b"video", },
      { group: 2, object: 0, },
      { group: 5, object: 0, },
    ),
    parameters: [],
  }
  let moq5_standalone_wire = b"\x16\x00\x13\x04\x01\x01\x04live\x05video\x02\x00\x05\x00\x00"
  assert_eq(@protocol.encode_fetch(moq5_standalone), Ok(moq5_standalone_wire))
  assert_eq(
    @protocol.decode_control(moq5_standalone_wire),
    Complete(Fetch(moq5_standalone), moq5_standalone_wire.length()),
  )

  let relative = @protocol.FetchMessage::{
    request_id: 4,
    fetch_type: RelativeJoiningFetch(2, 7),
    parameters: [{ kind: 0x20, value: Byte(5), }],
  }
  let relative_wire = b"\x16\x00\x07\x04\x02\x02\x07\x01\x20\x05"
  assert_eq(@protocol.encode_fetch(relative), Ok(relative_wire))
  assert_eq(
    @protocol.decode_control(relative_wire),
    Complete(Fetch(relative), relative_wire.length()),
  )

  let absolute = @protocol.FetchMessage::{
    request_id: 5,
    fetch_type: AbsoluteJoiningFetch(2, 7),
    parameters: [],
  }
  let absolute_wire = b"\x16\x00\x05\x05\x03\x02\x07\x00"
  assert_eq(@protocol.encode_fetch(absolute), Ok(absolute_wire))
  assert_eq(
    @protocol.decode_control(absolute_wire),
    Complete(Fetch(absolute), absolute_wire.length()),
  )

  let fetch_ok = @protocol.FetchOkMessage::{
    end_of_track: 1,
    end_location: { group: 3, object: 4, },
    parameters: [],
    track_properties: b"\x02\x2a",
  }
  let fetch_ok_wire = b"\x18\x00\x06\x01\x03\x04\x00\x02\x2a"
  assert_eq(@protocol.encode_fetch_ok(fetch_ok), Ok(fetch_ok_wire))
  assert_eq(
    @protocol.decode_control_with_context(fetch_ok_wire, FetchRequestStream),
    Complete(FetchOk(fetch_ok), fetch_ok_wire.length()),
  )

  let moq5_fetch_ok = @protocol.FetchOkMessage::{
    end_of_track: 1,
    end_location: { group: 9, object: 3, },
    parameters: [],
    track_properties: b"",
  }
  let moq5_fetch_ok_wire = b"\x18\x00\x04\x01\x09\x03\x00"
  assert_eq(@protocol.encode_fetch_ok(moq5_fetch_ok), Ok(moq5_fetch_ok_wire))
  assert_eq(
    @protocol.decode_control_with_context(
      moq5_fetch_ok_wire,
      FetchRequestStream,
    ),
    Complete(FetchOk(moq5_fetch_ok), moq5_fetch_ok_wire.length()),
  )
  let fetch_error = @protocol.RequestErrorMessage::{
    code: 3,
    retry_interval: 0,
    reason: b"",
    redirect: b"",
  }
  let fetch_error_wire = @protocol.encode_request_error(fetch_error).unwrap()
  assert_eq(
    @protocol.decode_control_with_context(fetch_error_wire, FetchRequestStream),
    Complete(RequestError(fetch_error), fetch_error_wire.length()),
  )
  assert_eq(
    @protocol.decode_control_with_context(fetch_error_wire, ControlStream),
    Invalid(MalformedControlMessage),
  )

  for wire in [standalone_wire, relative_wire, absolute_wire, fetch_ok_wire] {
    for split = 0; split < wire.length(); split = split + 1 {
      assert_eq(@protocol.decode_control(wire[:split].to_owned()), Incomplete)
    }
  }
}

///|
test "FETCH validates type, range, parameter scope, and Track Properties" {
  assert_eq(
    @protocol.encode_fetch({
      request_id: 1,
      fetch_type: StandaloneFetch(
        { namespace_fields: [], name: b"t", },
        { group: 2, object: 4, },
        { group: 2, object: 4, },
      ),
      parameters: [],
    }),
    Err(InvalidFetchRange),
  )
  let fill_timeout = @protocol.FetchMessage::{
    request_id: 8,
    fetch_type: RelativeJoiningFetch(4, 0xffffffffffffffffUL),
    parameters: [
      { kind: 0x03, value: Data(b"auth"), },
      { kind: 0x0a, value: VarInt(0xffffffffffffffffUL), },
      { kind: 0x20, value: Byte(255), },
      { kind: 0x22, value: Byte(2), },
    ],
  }
  let fill_timeout_wire = @protocol.encode_fetch(fill_timeout).unwrap()
  assert_eq(
    @protocol.decode_control(fill_timeout_wire),
    Complete(Fetch(fill_timeout), fill_timeout_wire.length()),
  )
  let max_locations = @protocol.FetchMessage::{
    request_id: 0xffffffffffffffffUL,
    fetch_type: StandaloneFetch(
      { namespace_fields: [], name: b"t", },
      { group: 0xffffffffffffffffUL, object: 0xfffffffffffffffeUL, },
      { group: 0xffffffffffffffffUL, object: 0xffffffffffffffffUL, },
    ),
    parameters: [],
  }
  let max_locations_wire = @protocol.encode_fetch(max_locations).unwrap()
  assert_eq(
    @protocol.decode_control(max_locations_wire),
    Complete(Fetch(max_locations), max_locations_wire.length()),
  )
  let unknown_fetch_type = b"\x16\x00\x03\x00\x04\x00"
  assert_eq(
    @protocol.decode_control(unknown_fetch_type),
    Invalid(MalformedControlMessage),
  )
  assert_eq(
    @protocol.encode_fetch({
      request_id: 1,
      fetch_type: AbsoluteJoiningFetch(2, 7),
      parameters: [{ kind: 0x02, value: VarInt(1), }],
    }),
    Err(InvalidParameterScope(0x02)),
  )
  assert_eq(
    @protocol.encode_request_update_for(
      { request_id: 2, parameters: [{ kind: 0x22, value: Byte(1), }], },
      FetchRequestStream,
    ),
    Err(InvalidParameterScope(0x22)),
  )
  let fetch_update = @protocol.encode_request_update_for(
    { request_id: 2, parameters: [{ kind: 0x20, value: Byte(1), }], },
    FetchRequestStream,
  ).unwrap()
  assert_eq(
    @protocol.decode_control_with_context(fetch_update, FetchRequestStream),
    Complete(
      RequestUpdate({
        request_id: 2,
        parameters: [{ kind: 0x20, value: Byte(1), }],
      }),
      fetch_update.length(),
    ),
  )
  let invalid_fetch_update = b"\x02\x00\x04\x02\x01\x22\x01"
  assert_eq(
    @protocol.decode_control_with_context(
      invalid_fetch_update,
      FetchRequestStream,
    ),
    Invalid(MalformedControlMessage),
  )
  assert_eq(
    @protocol.encode_fetch_ok({
      end_of_track: 2,
      end_location: { group: 1, object: 0, },
      parameters: [],
      track_properties: b"",
    }),
    Err(InvalidEndOfTrack),
  )
  assert_eq(
    @protocol.encode_fetch_ok({
      end_of_track: 0,
      end_location: { group: 1, object: 0, },
      parameters: [],
      track_properties: b"\x01",
    }),
    Err(InvalidTrackProperties),
  )
  let invalid_fetch_ok = b"\x18\x00\x07\x00\x01\x00\x01\x09\x00\x00"
  assert_eq(
    @protocol.decode_control(invalid_fetch_ok),
    Invalid(MalformedControlMessage),
  )
  let invalid_end_of_track = b"\x18\x00\x04\x02\x01\x00\x00"
  assert_eq(
    @protocol.decode_control(invalid_end_of_track),
    Invalid(MalformedControlMessage),
  )
}

///|
test "Draft 18 namespace message golden bytes and incremental decoding" {
  let request = @protocol.NamespaceRequestMessage::{
    request_id: 1,
    track_namespace: { fields: [b"a", b"b"], },
    parameters: [],
  }
  let publish_wire = b"\x06\x00\x07\x01\x02\x01a\x01b\x00"
  assert_eq(@protocol.encode_publish_namespace(request), Ok(publish_wire))
  assert_eq(
    @protocol.decode_control(publish_wire),
    Complete(PublishNamespace(request), publish_wire.length()),
  )
  let subscribe_wire = b"\x50\x00\x07\x01\x02\x01a\x01b\x00"
  assert_eq(@protocol.encode_subscribe_namespace(request), Ok(subscribe_wire))
  assert_eq(
    @protocol.decode_control(subscribe_wire),
    Complete(SubscribeNamespace(request), subscribe_wire.length()),
  )
  for wire in [publish_wire, subscribe_wire] {
    for split = 0; split < wire.length(); split = split + 1 {
      assert_eq(@protocol.decode_control(wire[:split].to_owned()), Incomplete)
    }
  }
  let suffix = @protocol.NamespaceSuffixMessage::{
    track_namespace: { fields: [b"a"], },
  }
  let namespace_wire = b"\x08\x00\x03\x01\x01a"
  let done_wire = b"\x0e\x00\x03\x01\x01a"
  assert_eq(@protocol.encode_namespace(suffix), Ok(namespace_wire))
  assert_eq(@protocol.encode_namespace_done(suffix), Ok(done_wire))
  assert_eq(
    @protocol.decode_control(namespace_wire),
    Complete(Namespace(suffix), namespace_wire.length()),
  )
  assert_eq(
    @protocol.decode_control(done_wire),
    Complete(NamespaceDone(suffix), done_wire.length()),
  )
  for wire in [namespace_wire, done_wire] {
    for split = 0; split < wire.length(); split = split + 1 {
      assert_eq(@protocol.decode_control(wire[:split].to_owned()), Incomplete)
    }
  }
}

///|
test "namespace tuple and prefix parameter validation" {
  let invalid = @protocol.NamespaceRequestMessage::{
    request_id: 0,
    track_namespace: { fields: [b""], },
    parameters: [],
  }
  assert_true(
    @protocol.encode_subscribe_namespace(invalid) is Err(InvalidTrackName),
  )
  let prefix = @protocol.RequestUpdateMessage::{
    request_id: 2,
    parameters: [{ kind: 0x34, value: NamespacePrefix({ fields: [b"a"], }), }],
  }
  assert_eq(
    @protocol.encode_request_update(prefix),
    Ok(b"\x02\x00\x07\x02\x01\x34\x03\x01\x01a"),
  )
  assert_true(
    @protocol.encode_subscribe_namespace({
      request_id: 1,
      track_namespace: { fields: [], },
      parameters: [{ kind: 0x02, value: VarInt(1), }],
    })
    is Err(InvalidParameterScope(0x02)),
  )
  assert_true(
    @protocol.encode_request_update({
      request_id: 2,
      parameters: [{ kind: 0x34, value: NamespacePrefix({ fields: [b""], }), }],
    })
    is Err(InvalidTrackName),
  )
  assert_eq(
    @protocol.encode_namespace({ track_namespace: { fields: [], }, }),
    Ok(b"\x08\x00\x01\x00"),
  )
  assert_true(
    @protocol.encode_namespace({
      track_namespace: { fields: Array::make(32, b"x"), },
    })
    is Ok(_),
  )
  assert_true(
    @protocol.encode_namespace({
      track_namespace: { fields: Array::make(33, b"x"), },
    })
    is Err(InvalidTrackName),
  )
  assert_true(
    @protocol.join_track_namespace({ fields: Array::make(32, b"x"), }, {
      fields: [b"y"],
    })
    is Err(InvalidTrackName),
  )
  assert_true(
    @protocol.join_track_namespace(
      { fields: [Bytes::from_array(Array::make(4096, 0x78))], },
      { fields: [b"y"], },
    )
    is Err(InvalidTrackName),
  )
  assert_eq(
    @protocol.decode_control(b"\x08\x00\x03\x01\x00x"),
    Invalid(MalformedControlMessage),
  )
}

///|
test "control message parsing handles every split and trailing frames" {
  let message = @protocol.SubscribeMessage::{
    request_id: 0,
    track: { namespace_fields: [b"demo"], name: b"video", },
    parameters: [
      { kind: 0x10, value: Byte(1), },
      { kind: 0x21, value: Filter(AbsoluteStart(0, 0)), },
    ],
  }
  let frame = @protocol.encode_subscribe(message).unwrap()
  for split = 0; split < frame.length(); split = split + 1 {
    assert_eq(@protocol.decode_control(frame[:split].to_owned()), Incomplete)
  }
  assert_eq(
    @protocol.decode_control(frame + b"\x07\x00\x01\x00"),
    Complete(Subscribe(message), frame.length()),
  )
}

///|
test "Draft 18 REQUEST_UPDATE uses a fresh request ID and typed parameters" {
  let priority = @protocol.RequestUpdateMessage::{
    request_id: 4,
    parameters: [{ kind: 0x20, value: Byte(5), }],
  }
  let priority_wire = b"\x02\x00\x04\x04\x01\x20\x05"
  assert_eq(@protocol.encode_request_update(priority), Ok(priority_wire))
  assert_eq(
    @protocol.decode_control(priority_wire),
    Complete(RequestUpdate(priority), priority_wire.length()),
  )
  assert_eq(
    @protocol.decode_control_with_context(priority_wire, RequestStream),
    Complete(RequestUpdate(priority), priority_wire.length()),
  )
  assert_eq(
    @protocol.decode_control_with_context(priority_wire, ControlStream),
    Invalid(MalformedControlMessage),
  )

  let range = @protocol.RequestUpdateMessage::{
    request_id: 2,
    parameters: [{ kind: 0x21, value: Filter(AbsoluteRange(3, 7, 10)), }],
  }
  let range_wire = b"\x02\x00\x08\x02\x01\x21\x04\x04\x03\x07\x07"
  assert_eq(@protocol.encode_request_update(range), Ok(range_wire))
  assert_eq(
    @protocol.decode_control(range_wire),
    Complete(RequestUpdate(range), range_wire.length()),
  )

  let timeouts = @protocol.RequestUpdateMessage::{
    request_id: 2,
    parameters: [
      { kind: 0x02, value: VarInt(3000), },
      { kind: 0x06, value: VarInt(4000), },
    ],
  }
  assert_eq(
    @protocol.encode_request_update(timeouts),
    Ok(b"\x02\x00\x08\x02\x02\x02\x8b\xb8\x04\x8f\xa0"),
  )

  let empty = @protocol.RequestUpdateMessage::{ request_id: 2, parameters: [], }
  assert_eq(@protocol.encode_request_update(empty), Ok(b"\x02\x00\x02\x02\x00"))
}

///|
test "REQUEST_UPDATE validates parameter scope, order, duplicates and values" {
  assert_eq(
    @protocol.encode_request_update({
      request_id: 4,
      parameters: [{ kind: 0x22, value: Byte(1), }],
    }),
    Err(InvalidParameterScope(0x22)),
  )
  assert_eq(
    @protocol.encode_request_update({
      request_id: 4,
      parameters: [{ kind: 0x10, value: Byte(2), }],
    }),
    Err(InvalidParameterValue(0x10)),
  )
  assert_eq(
    @protocol.encode_request_update({
      request_id: 4,
      parameters: [
        { kind: 0x20, value: Byte(5), },
        { kind: 0x10, value: Byte(1), },
      ],
    }),
    Err(OptionsOutOfOrder),
  )
  let repeated_tokens = @protocol.RequestUpdateMessage::{
    request_id: 2,
    parameters: [
      { kind: 0x03, value: Data(b"a"), },
      { kind: 0x03, value: Data(b"b"), },
    ],
  }
  let repeated_tokens_wire = b"\x02\x00\x08\x02\x02\x03\x01a\x00\x01b"
  assert_eq(
    @protocol.encode_request_update(repeated_tokens),
    Ok(repeated_tokens_wire),
  )
  assert_eq(
    @protocol.decode_control(repeated_tokens_wire),
    Complete(RequestUpdate(repeated_tokens), repeated_tokens_wire.length()),
  )
  assert_eq(
    @protocol.encode_request_update({
      request_id: 4,
      parameters: [
        { kind: 0x10, value: Byte(1), },
        { kind: 0x10, value: Byte(0), },
      ],
    }),
    Err(OptionsOutOfOrder),
  )
  assert_eq(
    @protocol.encode_request_update({
      request_id: 4,
      parameters: [{ kind: 0x21, value: Filter(AbsoluteRange(5, 0, 4)), }],
    }),
    Err(InvalidParameterValue(0x21)),
  )

  let invalid : Array[Bytes] = [
    b"\x02\x00\x04\x04\x01\x11\x00", // Unknown parameter.
     b"\x02\x00\x04\x02\x01\x22\x01", // GROUP_ORDER is not updatable.
     b"\x02\x00\x04\x04\x01\x10\x02", // FORWARD is 0 or 1.
     b"\x02\x00\x06\x02\x02\x10\x01\x00\x00", // Duplicate FORWARD.
     b"\x02\x00\x05\x02\x01\x21\x01\x05", // Unknown filter type.
  ]
  for wire in invalid {
    assert_eq(@protocol.decode_control(wire), Invalid(MalformedControlMessage))
  }
}

///|
test "subscription filters validate shape and uint64 range" {
  let max = 0xffffffffffffffffUL
  let filters : Array[@protocol.SubscriptionFilter] = [
    NextGroupStart,
    LargestObject,
    AbsoluteRange(10, 0, 10),
    AbsoluteRange(max, max, max),
  ]
  for filter in filters {
    let message = @protocol.RequestUpdateMessage::{
      request_id: 2,
      parameters: [{ kind: 0x21, value: Filter(filter), }],
    }
    let wire = @protocol.encode_request_update(message).unwrap()
    assert_eq(
      @protocol.decode_control(wire),
      Complete(RequestUpdate(message), wire.length()),
    )
  }

  let overflow_range = b"\x02\x00\x10\x02\x01\x21\x0c\x04" +
    @protocol.encode_vi64(max) +
    b"\x00\x01"
  let malformed : Array[Bytes] = [
    b"\x02\x00\x04\x02\x01\x21\x00", // Empty filter body.
     b"\x02\x00\x06\x02\x01\x21\x02\x01\x00", // Extra field.
     b"\x02\x00\x06\x02\x01\x21\x02\x03\x00", // Missing object.
     b"\x02\x00\x07\x02\x01\x21\x03\x04\x00\x00", // Missing range delta.
     b"\x02\x00\x05\x02\x01\x21\x02\x03", // Truncated filter body.
     overflow_range, // End group addition overflows UInt64.
  ]
  for wire in malformed {
    assert_eq(@protocol.decode_control(wire), Invalid(MalformedControlMessage))
  }
}

///|
test "GROUP_ORDER rejects values outside 1 and 2" {
  let values : Array[Byte] = [0, 3]
  for value in values {
    assert_eq(
      @protocol.encode_subscribe({
        request_id: 0,
        track: { namespace_fields: [], name: b"v", },
        parameters: [{ kind: 0x22, value: Byte(value), }],
      }),
      Err(InvalidParameterValue(0x22)),
    )
    let wire = b"\x03\x00\x07\x00\x00\x01v\x01\x22" + Bytes::from_array([value])
    assert_eq(@protocol.decode_control(wire), Invalid(MalformedControlMessage))
  }
}

///|
test "REQUEST_UPDATE handles all splits and leaves a following message" {
  let update = @protocol.encode_request_update({
    request_id: 4,
    parameters: [
      { kind: 0x10, value: Byte(1), },
      { kind: 0x20, value: Byte(200), },
    ],
  }).unwrap()
  let goaway = @protocol.encode_goaway_request({ uri: b"", timeout_ms: 0, }).unwrap()
  for split = 0; split < update.length(); split = split + 1 {
    assert_eq(@protocol.decode_control(update[:split].to_owned()), Incomplete)
  }
  assert_eq(
    @protocol.decode_control(update + goaway),
    Complete(
      RequestUpdate({
        request_id: 4,
        parameters: [
          { kind: 0x10, value: Byte(1), },
          { kind: 0x20, value: Byte(200), },
        ],
      }),
      update.length(),
    ),
  )
}

///|
test "Draft 18 GOAWAY distinguishes control and request stream forms" {
  let control = @protocol.GoAwayControlMessage::{
    uri: b"https://m",
    timeout_ms: 5000,
    request_id: 7,
  }
  let control_wire = b"\x10\x00\x0d\x09https://m\x93\x88\x07"
  assert_eq(@protocol.encode_goaway_control(control), Ok(control_wire))
  for split = 0; split < control_wire.length(); split = split + 1 {
    assert_eq(
      @protocol.decode_control_with_context(
        control_wire[:split].to_owned(),
        ControlStream,
      ),
      Incomplete,
    )
  }
  assert_eq(
    @protocol.decode_control_with_context(control_wire, ControlStream),
    Complete(GoAwayControl(control), control_wire.length()),
  )

  let request = @protocol.GoAwayRequestMessage::{ uri: b"", timeout_ms: 0, }
  let request_wire = b"\x10\x00\x02\x00\x00"
  assert_eq(@protocol.encode_goaway_request(request), Ok(request_wire))
  for split = 0; split < request_wire.length(); split = split + 1 {
    assert_eq(
      @protocol.decode_control_with_context(
        request_wire[:split].to_owned(),
        RequestStream,
      ),
      Incomplete,
    )
  }
  assert_eq(
    @protocol.decode_control_with_context(request_wire, RequestStream),
    Complete(GoAwayRequest(request), request_wire.length()),
  )

  assert_eq(
    @protocol.decode_control_with_context(control_wire, RequestStream),
    Invalid(MalformedControlMessage),
  )
  assert_eq(
    @protocol.decode_control_with_context(request_wire, ControlStream),
    Invalid(MalformedControlMessage),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      control_wire + control_wire,
      ControlStream,
    ),
    Complete(GoAwayControl(control), control_wire.length()),
  )
  assert_eq(
    @protocol.decode_control_with_context(control_wire, ControlStream),
    Complete(GoAwayControl(control), control_wire.length()),
  )
  let request_tail = @protocol.encode_request_error({
    code: 3,
    retry_interval: 0,
    reason: b"",
    redirect: b"",
  }).unwrap()
  assert_eq(
    @protocol.decode_control_with_context(
      request_wire + request_tail,
      RequestStream,
    ),
    Complete(GoAwayRequest(request), request_wire.length()),
  )
  assert_eq(
    @protocol.decode_control_with_context(request_tail, RequestStream),
    Complete(
      RequestError({ code: 3, retry_interval: 0, reason: b"", redirect: b"", }),
      request_tail.length(),
    ),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      b"\x10\x00\x0e\x09https://m\x93\x88\x07\xff",
      ControlStream,
    ),
    Invalid(MalformedControlMessage),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      b"\x10\x00\x03\x00\x00\x01",
      RequestStream,
    ),
    Invalid(MalformedControlMessage),
  )
}

///|
test "GOAWAY validates URI length boundaries and required fields" {
  let uri = Bytes::make(8192, 0x61)
  let valid = @protocol.GoAwayControlMessage::{
    uri,
    timeout_ms: 0,
    request_id: 0,
  }
  let wire = @protocol.encode_goaway_control(valid).unwrap()
  assert_eq(
    @protocol.decode_control_with_context(wire, ControlStream),
    Complete(GoAwayControl(valid), wire.length()),
  )
  assert_eq(
    @protocol.encode_goaway_control({
      uri: Bytes::make(8193, 0x61),
      timeout_ms: 0,
      request_id: 0,
    }),
    Err(InvalidGoAwayUri),
  )
  let too_big_uri_frame = b"\x10\x20\x05\xa0\x01" +
    Bytes::make(8193, 0x61) +
    b"\x00\x00"
  assert_eq(
    @protocol.decode_control_with_context(too_big_uri_frame, ControlStream),
    Invalid(MalformedControlMessage),
  )
  for
    malformed in [
      b"\x10\x00\x01\x02", // URI length exceeds the payload.
       b"\x10\x00\x01\x00", // Missing timeout.
       b"\x10\x00\x02\x00\x00", // Missing control-stream Request ID.
    ] {
    assert_eq(
      @protocol.decode_control_with_context(malformed, ControlStream),
      Invalid(MalformedControlMessage),
    )
  }
}

///|
test "REQUEST_OK is validated according to response context" {
  let update_ok = @protocol.RequestOkMessage::{
    parameters: [
      { kind: 0x08, value: VarInt(60000), },
      { kind: 0x09, value: Location(100, 5), },
    ],
    properties: b"",
  }
  let wire = b"\x07\x00\x08\x02\x08\xc0\xea\x60\x01\x64\x05"
  assert_eq(@protocol.encode_request_ok_for(update_ok, RequestUpdate), Ok(wire))
  assert_eq(
    @protocol.decode_control_with_context(wire, RequestStreamFor(RequestUpdate)),
    Complete(RequestOk(update_ok), wire.length()),
  )
  assert_eq(
    @protocol.encode_request_ok_for(
      { parameters: [], properties: b"track properties", },
      RequestUpdate,
    ),
    Err(InvalidRequestOkProperties),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      b"\x07\x00\x03\x00\x06\x00",
      RequestStreamFor(RequestUpdate),
    ),
    Invalid(MalformedControlMessage),
  )
  assert_eq(
    @protocol.encode_request_ok_for(
      { parameters: [], properties: b"publish properties", },
      Publish,
    ),
    Err(InvalidRequestOkProperties),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      b"\x07\x00\x02\x00x",
      RequestStreamFor(Publish),
    ),
    Invalid(MalformedControlMessage),
  )
  assert_eq(
    @protocol.encode_request_ok_for(
      { parameters: [], properties: b"track properties", },
      TrackStatus,
    ),
    Ok(b"\x07\x00\x11\x00track properties"),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      b"\x07\x00\x11\x00track properties",
      RequestStreamFor(TrackStatus),
    ),
    Complete(
      RequestOk({ parameters: [], properties: b"track properties", }),
      20,
    ),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      b"\x07\x00\x11\x00track properties",
      ControlStream,
    ),
    Invalid(MalformedControlMessage),
  )
  assert_eq(
    @protocol.encode_request_ok_for(
      { parameters: [{ kind: 0x10, value: Byte(1), }], properties: b"", },
      RequestUpdate,
    ),
    Err(InvalidParameterScope(0x10)),
  )
}
