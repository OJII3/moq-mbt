///|
test "Draft 18 single-track request and response wire formats" {
  let track : @protocol.TrackName = {
    namespace_fields: [b"demo"],
    name: b"video",
  }
  let subscribe : @protocol.SubscribeMessage = {
    request_id: 0,
    track,
    parameters: [],
  }
  let subscribe_wire = b"\x03\x00\x0e\x00\x01\x04demo\x05video\x00"
  assert_eq(@protocol.encode_subscribe(subscribe).unwrap(), subscribe_wire)
  assert_eq(
    @protocol.decode_control(subscribe_wire),
    @protocol.Complete(@protocol.Subscribe(subscribe), subscribe_wire.length()),
  )

  let publish : @protocol.PublishMessage = {
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
    @protocol.Complete(@protocol.Publish(publish), publish_wire.length()),
  )

  let subscribe_ok : @protocol.SubscribeOkMessage = {
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
    @protocol.Complete(
      @protocol.SubscribeOk(subscribe_ok),
      subscribe_ok_wire.length(),
    ),
  )
  assert_eq(
    @protocol.encode_request_ok({ parameters: [], properties: b"", }).unwrap(),
    b"\x07\x00\x01\x00",
  )
}

///|
test "Draft 18 namespace message golden bytes and incremental decoding" {
  let request : @protocol.NamespaceRequestMessage = {
    request_id: 1,
    track_namespace: { fields: [b"a", b"b"], },
    parameters: [],
  }
  let publish_wire = b"\x06\x00\x07\x01\x02\x01a\x01b\x00"
  assert_eq(@protocol.encode_publish_namespace(request), Ok(publish_wire))
  assert_eq(
    @protocol.decode_control(publish_wire),
    @protocol.Complete(
      @protocol.PublishNamespace(request),
      publish_wire.length(),
    ),
  )
  let subscribe_wire = b"\x50\x00\x07\x01\x02\x01a\x01b\x00"
  assert_eq(@protocol.encode_subscribe_namespace(request), Ok(subscribe_wire))
  assert_eq(
    @protocol.decode_control(subscribe_wire),
    @protocol.Complete(
      @protocol.SubscribeNamespace(request),
      subscribe_wire.length(),
    ),
  )
  for wire in [publish_wire, subscribe_wire] {
    for split = 0; split < wire.length(); split = split + 1 {
      assert_eq(
        @protocol.decode_control(wire[:split].to_owned()),
        @protocol.Incomplete,
      )
    }
  }
  let suffix : @protocol.NamespaceSuffixMessage = {
    track_namespace: { fields: [b"a"], },
  }
  let namespace_wire = b"\x08\x00\x03\x01\x01a"
  let done_wire = b"\x0e\x00\x03\x01\x01a"
  assert_eq(@protocol.encode_namespace(suffix), Ok(namespace_wire))
  assert_eq(@protocol.encode_namespace_done(suffix), Ok(done_wire))
  assert_eq(
    @protocol.decode_control(namespace_wire),
    @protocol.Complete(@protocol.Namespace(suffix), namespace_wire.length()),
  )
  assert_eq(
    @protocol.decode_control(done_wire),
    @protocol.Complete(@protocol.NamespaceDone(suffix), done_wire.length()),
  )
}

///|
test "namespace tuple and prefix parameter validation" {
  let invalid : @protocol.NamespaceRequestMessage = {
    request_id: 0,
    track_namespace: { fields: [b""], },
    parameters: [],
  }
  assert_true(
    @protocol.encode_subscribe_namespace(invalid)
    is Err(@protocol.InvalidTrackName),
  )
  let prefix : @protocol.RequestUpdateMessage = {
    request_id: 2,
    parameters: [
      { kind: 0x34, value: @protocol.NamespacePrefix({ fields: [b"a"], }), },
    ],
  }
  assert_eq(
    @protocol.encode_request_update(prefix),
    Ok(b"\x02\x00\x07\x02\x01\x34\x03\x01\x01a"),
  )
  assert_true(
    @protocol.encode_subscribe_namespace({
      request_id: 1,
      track_namespace: { fields: [], },
      parameters: [{ kind: 0x02, value: @protocol.VarInt(1), }],
    })
    is Err(@protocol.InvalidParameterScope(0x02)),
  )
  assert_true(
    @protocol.encode_request_update({
      request_id: 2,
      parameters: [
        { kind: 0x34, value: @protocol.NamespacePrefix({ fields: [b""], }), },
      ],
    })
    is Err(@protocol.InvalidTrackName),
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
    is Err(@protocol.InvalidTrackName),
  )
  assert_true(
    @protocol.join_track_namespace({ fields: Array::make(32, b"x"), }, {
      fields: [b"y"],
    })
    is Err(@protocol.InvalidTrackName),
  )
  assert_eq(
    @protocol.decode_control(b"\x08\x00\x03\x01\x00x"),
    @protocol.Invalid(@protocol.MalformedControlMessage),
  )
}

///|
test "control message parsing handles every split and trailing frames" {
  let message : @protocol.SubscribeMessage = {
    request_id: 0,
    track: { namespace_fields: [b"demo"], name: b"video", },
    parameters: [
      { kind: 0x10, value: @protocol.Byte(1), },
      { kind: 0x21, value: @protocol.Filter(@protocol.AbsoluteStart(0, 0)), },
    ],
  }
  let frame = @protocol.encode_subscribe(message).unwrap()
  for split = 0; split < frame.length(); split = split + 1 {
    assert_eq(
      @protocol.decode_control(frame[:split].to_owned()),
      @protocol.Incomplete,
    )
  }
  assert_eq(
    @protocol.decode_control(frame + b"\x07\x00\x01\x00"),
    @protocol.Complete(@protocol.Subscribe(message), frame.length()),
  )
}

///|
test "Draft 18 REQUEST_UPDATE uses a fresh request ID and typed parameters" {
  let priority : @protocol.RequestUpdateMessage = {
    request_id: 4,
    parameters: [{ kind: 0x20, value: @protocol.Byte(5), }],
  }
  let priority_wire = b"\x02\x00\x04\x04\x01\x20\x05"
  assert_eq(@protocol.encode_request_update(priority), Ok(priority_wire))
  assert_eq(
    @protocol.decode_control(priority_wire),
    @protocol.Complete(
      @protocol.RequestUpdate(priority),
      priority_wire.length(),
    ),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      priority_wire,
      @protocol.RequestStream,
    ),
    @protocol.Complete(
      @protocol.RequestUpdate(priority),
      priority_wire.length(),
    ),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      priority_wire,
      @protocol.ControlStream,
    ),
    @protocol.Invalid(@protocol.MalformedControlMessage),
  )

  let range : @protocol.RequestUpdateMessage = {
    request_id: 2,
    parameters: [
      {
        kind: 0x21,
        value: @protocol.Filter(@protocol.AbsoluteRange(3, 7, 10)),
      },
    ],
  }
  let range_wire = b"\x02\x00\x08\x02\x01\x21\x04\x04\x03\x07\x07"
  assert_eq(@protocol.encode_request_update(range), Ok(range_wire))
  assert_eq(
    @protocol.decode_control(range_wire),
    @protocol.Complete(@protocol.RequestUpdate(range), range_wire.length()),
  )

  let timeouts : @protocol.RequestUpdateMessage = {
    request_id: 2,
    parameters: [
      { kind: 0x02, value: @protocol.VarInt(3000), },
      { kind: 0x06, value: @protocol.VarInt(4000), },
    ],
  }
  assert_eq(
    @protocol.encode_request_update(timeouts),
    Ok(b"\x02\x00\x08\x02\x02\x02\x8b\xb8\x04\x8f\xa0"),
  )

  let empty : @protocol.RequestUpdateMessage = {
    request_id: 2,
    parameters: [],
  }
  assert_eq(@protocol.encode_request_update(empty), Ok(b"\x02\x00\x02\x02\x00"))
}

///|
test "REQUEST_UPDATE validates parameter scope, order, duplicates and values" {
  assert_eq(
    @protocol.encode_request_update({
      request_id: 4,
      parameters: [{ kind: 0x22, value: @protocol.Byte(1), }],
    }),
    Err(@protocol.InvalidParameterScope(0x22)),
  )
  assert_eq(
    @protocol.encode_request_update({
      request_id: 4,
      parameters: [{ kind: 0x10, value: @protocol.Byte(2), }],
    }),
    Err(@protocol.InvalidParameterValue(0x10)),
  )
  assert_eq(
    @protocol.encode_request_update({
      request_id: 4,
      parameters: [
        { kind: 0x20, value: @protocol.Byte(5), },
        { kind: 0x10, value: @protocol.Byte(1), },
      ],
    }),
    Err(@protocol.OptionsOutOfOrder),
  )
  let repeated_tokens : @protocol.RequestUpdateMessage = {
    request_id: 2,
    parameters: [
      { kind: 0x03, value: @protocol.Data(b"a"), },
      { kind: 0x03, value: @protocol.Data(b"b"), },
    ],
  }
  let repeated_tokens_wire = b"\x02\x00\x08\x02\x02\x03\x01a\x00\x01b"
  assert_eq(
    @protocol.encode_request_update(repeated_tokens),
    Ok(repeated_tokens_wire),
  )
  assert_eq(
    @protocol.decode_control(repeated_tokens_wire),
    @protocol.Complete(
      @protocol.RequestUpdate(repeated_tokens),
      repeated_tokens_wire.length(),
    ),
  )
  assert_eq(
    @protocol.encode_request_update({
      request_id: 4,
      parameters: [
        { kind: 0x10, value: @protocol.Byte(1), },
        { kind: 0x10, value: @protocol.Byte(0), },
      ],
    }),
    Err(@protocol.OptionsOutOfOrder),
  )
  assert_eq(
    @protocol.encode_request_update({
      request_id: 4,
      parameters: [
        {
          kind: 0x21,
          value: @protocol.Filter(@protocol.AbsoluteRange(5, 0, 4)),
        },
      ],
    }),
    Err(@protocol.InvalidParameterValue(0x21)),
  )

  let invalid : Array[Bytes] = [
    b"\x02\x00\x04\x04\x01\x11\x00", // Unknown parameter.
     b"\x02\x00\x04\x02\x01\x22\x01", // GROUP_ORDER is not updatable.
     b"\x02\x00\x04\x04\x01\x10\x02", // FORWARD is 0 or 1.
     b"\x02\x00\x06\x02\x02\x10\x01\x00\x00", // Duplicate FORWARD.
     b"\x02\x00\x05\x02\x01\x21\x01\x05", // Unknown filter type.
  ]
  for wire in invalid {
    assert_eq(
      @protocol.decode_control(wire),
      @protocol.Invalid(@protocol.MalformedControlMessage),
    )
  }
}

///|
test "subscription filters validate shape and uint64 range" {
  let max = 0xffffffffffffffffUL
  let filters : Array[@protocol.SubscriptionFilter] = [
    @protocol.SubscriptionFilter::NextGroupStart,
    @protocol.SubscriptionFilter::LargestObject,
    @protocol.SubscriptionFilter::AbsoluteRange(10, 0, 10),
    @protocol.SubscriptionFilter::AbsoluteRange(max, max, max),
  ]
  for filter in filters {
    let message : @protocol.RequestUpdateMessage = {
      request_id: 2,
      parameters: [{ kind: 0x21, value: @protocol.Filter(filter), }],
    }
    let wire = @protocol.encode_request_update(message).unwrap()
    assert_eq(
      @protocol.decode_control(wire),
      @protocol.Complete(@protocol.RequestUpdate(message), wire.length()),
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
    assert_eq(
      @protocol.decode_control(wire),
      @protocol.Invalid(@protocol.MalformedControlMessage),
    )
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
        parameters: [{ kind: 0x22, value: @protocol.Byte(value), }],
      }),
      Err(@protocol.InvalidParameterValue(0x22)),
    )
    let wire = b"\x03\x00\x07\x00\x00\x01v\x01\x22" + Bytes::from_array([value])
    assert_eq(
      @protocol.decode_control(wire),
      @protocol.Invalid(@protocol.MalformedControlMessage),
    )
  }
}

///|
test "REQUEST_UPDATE handles all splits and leaves a following message" {
  let update = @protocol.encode_request_update({
    request_id: 4,
    parameters: [
      { kind: 0x10, value: @protocol.Byte(1), },
      { kind: 0x20, value: @protocol.Byte(200), },
    ],
  }).unwrap()
  let goaway = @protocol.encode_goaway_request({ uri: b"", timeout_ms: 0, }).unwrap()
  for split = 0; split < update.length(); split = split + 1 {
    assert_eq(
      @protocol.decode_control(update[:split].to_owned()),
      @protocol.Incomplete,
    )
  }
  assert_eq(
    @protocol.decode_control(update + goaway),
    @protocol.Complete(
      @protocol.RequestUpdate({
        request_id: 4,
        parameters: [
          { kind: 0x10, value: @protocol.Byte(1), },
          { kind: 0x20, value: @protocol.Byte(200), },
        ],
      }),
      update.length(),
    ),
  )
}

///|
test "Draft 18 GOAWAY distinguishes control and request stream forms" {
  let control : @protocol.GoAwayControlMessage = {
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
        @protocol.ControlStream,
      ),
      @protocol.Incomplete,
    )
  }
  assert_eq(
    @protocol.decode_control_with_context(control_wire, @protocol.ControlStream),
    @protocol.Complete(@protocol.GoAwayControl(control), control_wire.length()),
  )

  let request : @protocol.GoAwayRequestMessage = { uri: b"", timeout_ms: 0, }
  let request_wire = b"\x10\x00\x02\x00\x00"
  assert_eq(@protocol.encode_goaway_request(request), Ok(request_wire))
  for split = 0; split < request_wire.length(); split = split + 1 {
    assert_eq(
      @protocol.decode_control_with_context(
        request_wire[:split].to_owned(),
        @protocol.RequestStream,
      ),
      @protocol.Incomplete,
    )
  }
  assert_eq(
    @protocol.decode_control_with_context(request_wire, @protocol.RequestStream),
    @protocol.Complete(@protocol.GoAwayRequest(request), request_wire.length()),
  )

  assert_eq(
    @protocol.decode_control_with_context(control_wire, @protocol.RequestStream),
    @protocol.Invalid(@protocol.MalformedControlMessage),
  )
  assert_eq(
    @protocol.decode_control_with_context(request_wire, @protocol.ControlStream),
    @protocol.Invalid(@protocol.MalformedControlMessage),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      control_wire + control_wire,
      @protocol.ControlStream,
    ),
    @protocol.Complete(@protocol.GoAwayControl(control), control_wire.length()),
  )
  assert_eq(
    @protocol.decode_control_with_context(control_wire, @protocol.ControlStream),
    @protocol.Complete(@protocol.GoAwayControl(control), control_wire.length()),
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
      @protocol.RequestStream,
    ),
    @protocol.Complete(@protocol.GoAwayRequest(request), request_wire.length()),
  )
  assert_eq(
    @protocol.decode_control_with_context(request_tail, @protocol.RequestStream),
    @protocol.Complete(
      @protocol.RequestError({
        code: 3,
        retry_interval: 0,
        reason: b"",
        redirect: b"",
      }),
      request_tail.length(),
    ),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      b"\x10\x00\x0e\x09https://m\x93\x88\x07\xff",
      @protocol.ControlStream,
    ),
    @protocol.Invalid(@protocol.MalformedControlMessage),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      b"\x10\x00\x03\x00\x00\x01",
      @protocol.RequestStream,
    ),
    @protocol.Invalid(@protocol.MalformedControlMessage),
  )
}

///|
test "GOAWAY validates URI length boundaries and required fields" {
  let uri = Bytes::make(8192, 0x61)
  let valid : @protocol.GoAwayControlMessage = {
    uri,
    timeout_ms: 0,
    request_id: 0,
  }
  let wire = @protocol.encode_goaway_control(valid).unwrap()
  assert_eq(
    @protocol.decode_control_with_context(wire, @protocol.ControlStream),
    @protocol.Complete(@protocol.GoAwayControl(valid), wire.length()),
  )
  assert_eq(
    @protocol.encode_goaway_control({
      uri: Bytes::make(8193, 0x61),
      timeout_ms: 0,
      request_id: 0,
    }),
    Err(@protocol.InvalidGoAwayUri),
  )
  let too_big_uri_frame = b"\x10\x20\x05\xa0\x01" +
    Bytes::make(8193, 0x61) +
    b"\x00\x00"
  assert_eq(
    @protocol.decode_control_with_context(
      too_big_uri_frame,
      @protocol.ControlStream,
    ),
    @protocol.Invalid(@protocol.MalformedControlMessage),
  )
  for
    malformed in [
      b"\x10\x00\x01\x02", // URI length exceeds the payload.
       b"\x10\x00\x01\x00", // Missing timeout.
       b"\x10\x00\x02\x00\x00", // Missing control-stream Request ID.
    ] {
    assert_eq(
      @protocol.decode_control_with_context(malformed, @protocol.ControlStream),
      @protocol.Invalid(@protocol.MalformedControlMessage),
    )
  }
}

///|
test "REQUEST_OK is validated according to response context" {
  let update_ok : @protocol.RequestOkMessage = {
    parameters: [
      { kind: 0x08, value: @protocol.VarInt(60000), },
      { kind: 0x09, value: @protocol.Location(100, 5), },
    ],
    properties: b"",
  }
  let wire = b"\x07\x00\x08\x02\x08\xc0\xea\x60\x01\x64\x05"
  assert_eq(
    @protocol.encode_request_ok_for(
      update_ok,
      @protocol.RequestOkContext::RequestUpdate,
    ),
    Ok(wire),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      wire,
      @protocol.RequestStreamFor(@protocol.RequestOkContext::RequestUpdate),
    ),
    @protocol.Complete(@protocol.RequestOk(update_ok), wire.length()),
  )
  assert_eq(
    @protocol.encode_request_ok_for(
      { parameters: [], properties: b"track properties", },
      @protocol.RequestOkContext::RequestUpdate,
    ),
    Err(@protocol.InvalidRequestOkProperties),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      b"\x07\x00\x03\x00\x06\x00",
      @protocol.RequestStreamFor(@protocol.RequestOkContext::RequestUpdate),
    ),
    @protocol.Invalid(@protocol.MalformedControlMessage),
  )
  assert_eq(
    @protocol.encode_request_ok_for(
      { parameters: [], properties: b"publish properties", },
      @protocol.RequestOkContext::Publish,
    ),
    Err(@protocol.InvalidRequestOkProperties),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      b"\x07\x00\x02\x00x",
      @protocol.RequestStreamFor(@protocol.RequestOkContext::Publish),
    ),
    @protocol.Invalid(@protocol.MalformedControlMessage),
  )
  assert_eq(
    @protocol.encode_request_ok_for(
      { parameters: [], properties: b"track properties", },
      @protocol.RequestOkContext::TrackStatus,
    ),
    Ok(b"\x07\x00\x11\x00track properties"),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      b"\x07\x00\x11\x00track properties",
      @protocol.RequestStreamFor(@protocol.RequestOkContext::TrackStatus),
    ),
    @protocol.Complete(
      @protocol.RequestOk({ parameters: [], properties: b"track properties", }),
      20,
    ),
  )
  assert_eq(
    @protocol.decode_control_with_context(
      b"\x07\x00\x11\x00track properties",
      @protocol.ControlStream,
    ),
    @protocol.Invalid(@protocol.MalformedControlMessage),
  )
  assert_eq(
    @protocol.encode_request_ok_for(
      {
        parameters: [{ kind: 0x10, value: @protocol.Byte(1), }],
        properties: b"",
      },
      @protocol.RequestOkContext::RequestUpdate,
    ),
    Err(@protocol.InvalidParameterScope(0x10)),
  )
}
