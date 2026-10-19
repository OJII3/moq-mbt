///|
fn subscriber_track() -> @protocol.TrackName {
  { namespace_fields: [b"demo"], name: b"video", }
}

///|
fn subscriber_header(
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
fn subscriber_frame(
  object_id : UInt64,
  payload : Bytes,
) -> @protocol.SubgroupFrame {
  { object_id, status: @protocol.Normal, properties: b"", payload, }
}

///|
fn collect_frames(
  result : Result[@session.ObjectProgress, @session.TrackSessionError],
) -> Array[@protocol.SubgroupFrame] {
  match result {
    Ok(@session.Received(_, frames)) => frames
    Ok(@session.Pending) | Err(_) => []
  }
}

///|
test "subscriber decodes multiple objects from one stream chunk" {
  let frames = [subscriber_frame(2, b"first"), subscriber_frame(5, b"second")]
  let wire = @protocol.encode_subgroup_stream(
    subscriber_header(4, true),
    frames,
  ).unwrap()
  let subscriber = @session.SingleTrackSubscriber::new(subscriber_track())
  assert_eq(
    subscriber.receive_response(8, b"\x04\x00\x02\x01\x00"),
    Ok(@session.Established(1, b"")),
  )
  assert_eq(
    subscriber.receive_object(9, wire),
    Ok(@session.Received(subscriber_header(4, true), frames)),
  )
}

///|
test "subscriber handles every split across a header and multiple frames" {
  let frames = [subscriber_frame(0, b"one"), subscriber_frame(1, b"two")]
  let wire = @protocol.encode_subgroup_stream(
    subscriber_header(4, false),
    frames,
  ).unwrap()
  for split = 0; split < wire.length(); split = split + 1 {
    let subscriber = @session.SingleTrackSubscriber::new(subscriber_track())
    ignore(subscriber.receive_response(8, b"\x04\x00\x02\x01\x00"))
    let decoded : Array[@protocol.SubgroupFrame] = []
    for
      frame in collect_frames(
        subscriber.receive_object(9, wire[:split].to_owned()),
      ) {
      decoded.push(frame)
    }
    for
      frame in collect_frames(
        subscriber.receive_object(9, wire[split:].to_owned()),
      ) {
      decoded.push(frame)
    }
    assert_eq(decoded, frames)
  }
}

///|
test "subscriber isolates interleaved subgroup streams" {
  let subscriber = @session.SingleTrackSubscriber::new(subscriber_track())
  ignore(subscriber.receive_response(8, b"\x04\x00\x02\x01\x00"))
  let header_a = subscriber_header(10, false)
  let header_b = subscriber_header(11, true)
  let wire_a = @protocol.encode_subgroup_stream(header_a, [
    subscriber_frame(1, b"a1"),
    subscriber_frame(3, b"a2"),
  ]).unwrap()
  let wire_b = @protocol.encode_subgroup_stream(header_b, [
    subscriber_frame(7, b"b1"),
  ]).unwrap()
  let header_length_a = match @protocol.decode_subgroup_header(wire_a) {
    @protocol.Complete(_, length) => length
    _ => fail("expected header a")
  }
  let header_length_b = match @protocol.decode_subgroup_header(wire_b) {
    @protocol.Complete(_, length) => length
    _ => fail("expected header b")
  }
  let split_a = header_length_a + 4
  let split_b = header_length_b + 1
  assert_eq(
    subscriber.receive_object(21, wire_a[:split_a].to_owned()),
    Ok(@session.Received(header_a, [subscriber_frame(1, b"a1")])),
  )
  assert_eq(
    subscriber.receive_object(23, wire_b[:split_b].to_owned()),
    Ok(@session.Pending),
  )
  assert_eq(
    subscriber.receive_object(21, wire_a[split_a:].to_owned()),
    Ok(@session.Received(header_a, [subscriber_frame(3, b"a2")])),
  )
  assert_eq(
    subscriber.receive_object(23, wire_b[split_b:].to_owned()),
    Ok(@session.Received(header_b, [subscriber_frame(7, b"b1")])),
  )
}

///|
test "subscriber rejects alias mismatches and invalid object status" {
  let subscriber = @session.SingleTrackSubscriber::new(subscriber_track())
  ignore(subscriber.receive_response(8, b"\x04\x00\x02\x01\x00"))
  let bad_alias : @protocol.SubgroupHeader = {
    track_alias: 2,
    group_id: 1,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: false,
  }
  let alias_wire = @protocol.encode_subgroup_header(bad_alias) + b"\x00\x01x"
  assert_eq(
    subscriber.receive_object(30, alias_wire),
    Err(@session.AliasMismatch),
  )

  let invalid_status = @protocol.encode_subgroup_header(
      subscriber_header(1, false),
    ) +
    b"\x00\x00\x01"
  assert_eq(
    subscriber.receive_object(31, invalid_status),
    Err(@session.InvalidControlMessage),
  )
}

///|
test "subscriber distinguishes FIN, reset, and incomplete frames" {
  let subscriber = @session.SingleTrackSubscriber::new(subscriber_track())
  ignore(subscriber.receive_response(8, b"\x04\x00\x02\x01\x00"))
  let header = subscriber_header(1, true)
  let header_bytes = @protocol.encode_subgroup_header(header)
  assert_eq(subscriber.receive_object(40, header_bytes), Ok(@session.Pending))
  assert_eq(subscriber.finish_object_stream(40), Ok(true))

  let reset_header = header_bytes + b"\x00\x01x"
  assert_eq(
    subscriber.receive_object(41, reset_header),
    Ok(@session.Received(header, [subscriber_frame(0, b"x")])),
  )
  subscriber.reset_object_stream(41)
  assert_eq(subscriber.finish_object_stream(41), Ok(false))

  assert_eq(
    subscriber.receive_object(42, header_bytes[:1].to_owned()),
    Ok(@session.Pending),
  )
  assert_eq(
    subscriber.finish_object_stream(42),
    Err(@session.IncompleteObjectStream),
  )

  assert_eq(
    subscriber.receive_object(43, header_bytes + b"\x00\x02x"),
    Ok(@session.Pending),
  )
  assert_eq(
    subscriber.finish_object_stream(43),
    Err(@session.IncompleteObjectStream),
  )
}

///|
test "EndOfGroup object status does not imply header End of Group" {
  let subscriber = @session.SingleTrackSubscriber::new(subscriber_track())
  ignore(subscriber.receive_response(8, b"\x04\x00\x02\x01\x00"))
  let header = subscriber_header(1, false)
  let frame : @protocol.SubgroupFrame = {
    object_id: 0,
    status: @protocol.EndOfGroup,
    properties: b"",
    payload: b"",
  }
  let wire = @protocol.encode_subgroup_stream(header, [frame]).unwrap()
  assert_eq(
    subscriber.receive_object(44, wire),
    Ok(@session.Received(header, [frame])),
  )
  assert_eq(subscriber.finish_object_stream(44), Ok(false))
}

///|
test "subscriber accepts PUBLISH_DONE after SUBSCRIBE_OK" {
  let subscriber = @session.SingleTrackSubscriber::new(subscriber_track())
  assert_eq(
    subscriber.receive_response(8, b"\x04\x00\x02\x01\x00"),
    Ok(@session.Established(1, b"")),
  )
  assert_eq(
    subscriber.receive_response(8, b"\x0b\x00\x03\x03\x01\x00"),
    Ok(@session.Finished({ code: 3, stream_count: 1, reason: b"", }, b"")),
  )
}
