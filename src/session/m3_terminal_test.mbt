///|
test "SUBSCRIBE PUBLISH_DONE closes its request direction once while objects drain" {
  let subscriber = subscriber_started()
  let done = @protocol.encode_publish_done({
    code: 0UL,
    stream_count: 1UL,
    reason: b"",
  }).unwrap()
  assert_eq(
    subscriber.receive(@session.StreamData(30, done)),
    Ok([@session.SendFin(30, b"")]),
  )
  assert_eq(subscriber.receive(@session.InboundStreamFinished(30)), Ok([]))
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(40, @session.Unidirectional),
    ),
    Ok([]),
  )
  let header = subscriber_header(0UL, true)
  assert_eq(
    subscriber.receive(
      @session.StreamData(40, @protocol.encode_subgroup_header(header)),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        40,
        @protocol.encode_subgroup_frame(
          header,
          None,
          subscriber_frame(0UL, b"late"),
        ).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.ObjectReceived(
          0UL,
          { namespace_fields: [b"demo"], name: b"video", },
          header,
          [subscriber_frame(0UL, b"late")],
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.InboundStreamFinished(40)),
    Ok([
      @session.EmitNotice(@session.GroupFinished(0UL, 0UL)),
      @session.EmitNotice(
        @session.PublishDone(0UL, { code: 0UL, stream_count: 1UL, reason: b"", }),
      ),
    ]),
  )
}

///|
test "PUBLISH_DONE received before malformed Object FIN reports once after stream retirement" {
  let subscriber = subscriber_started()
  let done : @protocol.PublishDoneMessage = {
    code: 0UL,
    stream_count: 1UL,
    reason: b"",
  }
  assert_eq(
    subscriber.receive(
      @session.StreamData(30, @protocol.encode_publish_done(done).unwrap()),
    ),
    Ok([@session.SendFin(30, b"")]),
  )
  assert_eq(subscriber.receive(@session.InboundStreamFinished(30)), Ok([]))
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(40, @session.Unidirectional),
    ),
    Ok([]),
  )
  let header = subscriber_header(1UL, false)
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        40,
        @protocol.encode_subgroup_header(header) + b"\x00\x02x",
      ),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(@session.InboundStreamFinished(40)),
    Ok([
      @session.EmitNotice(@session.PublishDone(0UL, done)),
      @session.StopReceiving(40, 3UL),
    ]),
  )
  assert_eq(subscriber.receive(@session.StreamReset(40, 1UL)), Ok([]))
}

///|
test "initial REQUEST_ERROR closes the request direction once" {
  let subscriber = subscriber_pending()
  let error : @protocol.RequestErrorMessage = {
    code: 1UL,
    retry_interval: 0UL,
    reason: b"rejected",
    redirect: b"",
  }
  assert_eq(
    subscriber.receive(
      @session.StreamData(30, @protocol.encode_request_error(error).unwrap()),
    ),
    Ok([
      @session.SendFin(30, b""),
      @session.EmitNotice(@session.SubscribeRejected(0UL, error)),
    ]),
  )
  assert_eq(subscriber.receive(@session.InboundStreamFinished(30)), Ok([]))
  assert_eq(subscriber.cancel_request(0UL), Ok([]))
}

///|
test "NEW_GROUP_REQUEST requires DYNAMIC_GROUPS in SUBSCRIBE_OK properties" {
  let cases : Array[(Bytes, Bool)] = [
    (b"", false),
    (b"\x30\x00", false),
    (b"\x30\x01", true),
    (b"\x2f\x01x\x01\x01", true),
  ]
  let new_group : Array[@protocol.MessageParameter] = [
    { kind: 0x32UL, value: @protocol.VarInt(1UL), },
  ]
  for (properties, supported) in cases {
    let subscriber = subscriber_pending()
    let response = @protocol.encode_subscribe_ok({
      track_alias: 1UL,
      parameters: [],
      properties,
    }).unwrap()
    ignore(subscriber.receive(@session.StreamData(30, response)).unwrap())
    match subscriber.update_subscription(0UL, new_group) {
      Ok((_, [@session.SendChunk(30, _)])) if supported => ()
      Err(@session.Encoding(@protocol.InvalidParameterValue(0x32UL))) if !supported =>
        ()
      _ => fail("NEW_GROUP_REQUEST did not follow DYNAMIC_GROUPS support")
    }
  }

  for properties in [b"\x30\x02", b"\x30"] {
    let subscriber = subscriber_pending()
    let response = @protocol.encode_subscribe_ok({
      track_alias: 1UL,
      parameters: [],
      properties,
    }).unwrap()
    assert_eq(
      subscriber.receive(@session.StreamData(30, response)),
      Ok([
        @session.CloseConnection(3UL, b""),
        @session.EmitNotice(@session.SessionEnded(@session.Local(3UL, b""))),
      ]),
    )
  }
}

///|
test "SUBSCRIBE update FIN still accepts SUBSCRIBE_OK, late Objects, and PUBLISH_DONE" {
  let subscriber = subscriber_pending()
  assert_eq(
    subscriber.finish_subscription_updates(0UL),
    Ok([@session.SendFin(30, b"")]),
  )
  assert_eq(subscriber.finish_subscription_updates(0UL), Ok([]))
  let subscribe_ok = @protocol.encode_subscribe_ok({
    track_alias: 1UL,
    parameters: [],
    properties: b"",
  }).unwrap()
  assert_eq(
    subscriber.receive(@session.StreamData(30, subscribe_ok)),
    Ok([@session.EmitNotice(@session.SubscribeEstablished(0UL, 1UL))]),
  )
  assert_eq(
    subscriber.update_subscription(0UL, []),
    Err(@session.SubscriptionNotAccepted),
  )
  let done : @protocol.PublishDoneMessage = {
    code: 0UL,
    stream_count: 1UL,
    reason: b"",
  }
  assert_eq(
    subscriber.receive(
      @session.StreamData(30, @protocol.encode_publish_done(done).unwrap()),
    ),
    Ok([]),
  )
  assert_eq(subscriber.receive(@session.InboundStreamFinished(30)), Ok([]))
  let header = subscriber_header(0UL, true)
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(40, @session.Unidirectional),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(40, @protocol.encode_subgroup_header(header)),
    ),
    Ok([]),
  )
  let frame = subscriber_frame(0UL, b"late")
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        40,
        @protocol.encode_subgroup_frame(header, None, frame).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.ObjectReceived(
          0UL,
          { namespace_fields: [b"demo"], name: b"video", },
          header,
          [frame],
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(@session.InboundStreamFinished(40)),
    Ok([
      @session.EmitNotice(@session.GroupFinished(0UL, 0UL)),
      @session.EmitNotice(@session.PublishDone(0UL, done)),
    ]),
  )
}
