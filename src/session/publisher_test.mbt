///|
fn publisher_track() -> @protocol.TrackName {
  { namespace_fields: [b"demo"], name: b"video", }
}

///|
fn publisher_header(
  group_id : UInt64,
  end_of_group : Bool,
) -> @protocol.SubgroupHeader {
  {
    track_alias: 1,
    group_id,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group,
    first_object: true,
    has_properties: false,
  }
}

///|
fn publisher_frame(
  object_id : UInt64,
  payload : Bytes,
) -> @protocol.SubgroupFrame {
  { object_id, status: @protocol.Normal, properties: b"", payload, }
}

///|
fn publisher_started() -> @session.SingleTrackPublisher raise {
  let publisher = @session.SingleTrackPublisher::new(publisher_track(), 1)
  assert_eq(
    publisher.receive_subscribe(
      11,
      @protocol.encode_subscribe({
        request_id: 1,
        track: publisher_track(),
        parameters: [],
      }).unwrap(),
    ),
    Ok(
      @session.SubscribeRequested(
        11,
        { request_id: 1, track: publisher_track(), parameters: [], },
        b"",
      ),
    ),
  )
  assert_eq(publisher.accept_subscribe(11).unwrap(), b"\x04\x00\x02\x01\x00")
  publisher
}

///|
test "publisher encodes one header and increasing object deltas" {
  let publish_wire = b"\x1d\x00\x0f\x00\x01\x04demo\x05video\x01\x00"
  let publisher = @session.SingleTrackPublisher::new(publisher_track(), 1)
  assert_eq(publisher.publish_request().unwrap(), publish_wire)
  let subscribe = @protocol.encode_subscribe({
    request_id: 1,
    track: publisher_track(),
    parameters: [],
  }).unwrap()
  assert_eq(subscribe, b"\x03\x00\x0e\x01\x01\x04demo\x05video\x00")
  for split = 0; split < subscribe.length(); split = split + 1 {
    let publisher = @session.SingleTrackPublisher::new(publisher_track(), 1)
    assert_eq(
      publisher.receive_subscribe(11, subscribe[:split].to_owned()),
      Ok(@session.Pending),
    )
    assert_eq(
      publisher.receive_subscribe(11, subscribe[split:].to_owned()),
      Ok(
        @session.SubscribeRequested(
          11,
          { request_id: 1, track: publisher_track(), parameters: [], },
          b"",
        ),
      ),
    )
  }

  let publisher = publisher_started()
  let header = publisher_header(0, true)
  assert_eq(
    publisher.open_object_stream(11, 20, header),
    Ok(@protocol.encode_subgroup_header(header)),
  )
  assert_eq(
    publisher.object_stream(20, publisher_frame(0, b"payload")),
    Ok(b"\x00\x07payload"),
  )
  assert_eq(
    publisher.object_stream(20, publisher_frame(2, b"second")),
    Ok(b"\x01\x06second"),
  )
  assert_eq(
    publisher.object_stream(20, publisher_frame(2, b"duplicate")),
    Err(@session.ObjectIdNotIncreasing),
  )
  assert_eq(publisher.finish_object_stream(20), Ok(()))
  let done = publisher.publish_done(11, 3, b"").unwrap()
  assert_eq(
    @protocol.decode_control(done),
    @protocol.Complete(
      @protocol.PublishDone({ code: 3, stream_count: 1, reason: b"", }),
      done.length(),
    ),
  )
}

///|
test "publisher counts empty and parallel streams per subscription" {
  let publisher = @session.SingleTrackPublisher::new(publisher_track(), 1)
  assert_eq(publisher.publish_accepted(50), Ok(()))
  assert_eq(publisher.publish_accepted(51), Ok(()))
  let header_a = publisher_header(1, false)
  let header_b = publisher_header(2, true)
  ignore(publisher.open_object_stream(50, 60, header_a).unwrap())
  ignore(publisher.open_object_stream(50, 61, header_b).unwrap())
  ignore(publisher.open_object_stream(51, 62, header_a).unwrap())
  assert_eq(
    publisher.object_stream(60, publisher_frame(3, b"a")),
    Ok(b"\x03\x01a"),
  )
  assert_eq(
    publisher.object_stream(60, publisher_frame(4, b"b")),
    Ok(b"\x00\x01b"),
  )
  assert_eq(
    publisher.publish_done(50, 3, b""),
    Err(@session.ObjectStreamsStillOpen),
  )
  assert_eq(publisher.finish_object_stream(60), Ok(()))
  assert_eq(publisher.reset_object_stream(61), Ok(()))
  assert_eq(publisher.finish_object_stream(62), Ok(()))
  let done = publisher.publish_done(50, 3, b"").unwrap()
  assert_eq(
    @protocol.decode_control(done),
    @protocol.Complete(
      @protocol.PublishDone({ code: 3, stream_count: 2, reason: b"", }),
      done.length(),
    ),
  )
  let empty_done = publisher.publish_done(51, 3, b"").unwrap()
  assert_eq(
    @protocol.decode_control(empty_done),
    @protocol.Complete(
      @protocol.PublishDone({ code: 3, stream_count: 1, reason: b"", }),
      empty_done.length(),
    ),
  )

  let no_stream_publisher = @session.SingleTrackPublisher::new(
    publisher_track(),
    1,
  )
  assert_eq(no_stream_publisher.publish_accepted(52), Ok(()))
  let no_stream_done = no_stream_publisher.publish_done(52, 3, b"").unwrap()
  assert_eq(
    @protocol.decode_control(no_stream_done),
    @protocol.Complete(
      @protocol.PublishDone({ code: 3, stream_count: 0, reason: b"", }),
      no_stream_done.length(),
    ),
  )
}

///|
test "publisher validates stream alias and allows only one publish done" {
  let publisher = @session.SingleTrackPublisher::new(publisher_track(), 1)
  assert_eq(publisher.publish_accepted(70), Ok(()))
  let invalid_header : @protocol.SubgroupHeader = {
    track_alias: 2,
    group_id: 0,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: false,
  }
  assert_eq(
    publisher.open_object_stream(70, 71, invalid_header),
    Err(@session.AliasMismatch),
  )
  assert_eq(publisher.publish_done(70, 3, b""), Ok(b"\x0b\x00\x03\x03\x00\x00"))
  assert_eq(publisher.publish_done(70, 3, b""), Err(@session.UnexpectedMessage))
}

///|
test "publisher keeps EndOfGroup status separate from header End of Group" {
  let publisher = publisher_started()
  let header = publisher_header(1, false)
  let header_bytes = publisher.open_object_stream(11, 80, header).unwrap()
  let frame : @protocol.SubgroupFrame = {
    object_id: 4,
    status: @protocol.EndOfGroup,
    properties: b"",
    payload: b"",
  }
  let wire = header_bytes + publisher.object_stream(80, frame).unwrap()
  let subscriber = @session.SingleTrackSubscriber::new(publisher_track())
  ignore(subscriber.receive_response(8, b"\x04\x00\x02\x01\x00"))
  assert_eq(
    subscriber.receive_object(80, wire),
    Ok(@session.Received(header, [frame])),
  )
  assert_eq(publisher.finish_object_stream(80), Ok(()))
  assert_eq(subscriber.finish_object_stream(80), Ok(false))
}
