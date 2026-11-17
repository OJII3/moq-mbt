///|
fn delivery_publisher() -> @session.ClientSession raise {
  let publisher = publisher_announced()
  ignore(publisher.receive(Clock(0UL)).unwrap())
  ignore(publisher.receive(InboundStreamStarted(40, Bidirectional)).unwrap())
  ignore(
    publisher
    .receive(
      StreamData(
        40,
        @protocol.encode_subscribe({
          request_id: 1UL,
          track: publisher_track(),
          parameters: [
            { kind: 0x02UL, value: VarInt(10UL), },
            { kind: 0x06UL, value: VarInt(20UL), },
          ],
        }).unwrap(),
      ),
    )
    .unwrap(),
  )
  ignore(publisher.accept_subscription(StreamId(40)).unwrap())
  publisher
}

///|
test "object delivery deadline starts after stream opens" {
  let publisher = delivery_publisher()
  ignore(publisher.receive(Clock(100UL)).unwrap())
  let (group, token) = writer_group_open(publisher, 0UL)
  assert_eq(group.send(b"payload"), Err(ObjectStreamNotReady))
  ignore(publisher.receive(Clock(108UL)).unwrap())
  ignore(publisher.receive(StreamOpened(Object(token), 50)).unwrap())
  let actions = group.send(b"payload").unwrap()
  assert_true(actions[0] is SendObjectChunk(50, _, 118UL))
}

///|
test "subgroup timeout waits for committed data after local FIN completion" {
  let publisher = delivery_publisher()
  ignore(publisher.receive(Clock(100UL)).unwrap())
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(publisher.receive(StreamOpened(Object(token), 50)).unwrap())
  ignore(group.send(b"payload").unwrap())
  ignore(group.finish().unwrap())
  for _ in 0..<3 {
    ignore(publisher.receive(SendCompleted(50, Sent)).unwrap())
  }
  assert_eq(publisher.receive(Clock(119UL)), Ok([]))
  let expired = publisher.receive(Clock(120UL)).unwrap()
  assert_true(expired.contains(AbortSend(50, 2UL)))
  assert_eq(publisher.receive(StreamDataCommitted(50)), Ok([]))
}

///|
test "committed subgroup cancels deadline and finish permits safe abort" {
  let publisher = delivery_publisher()
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(publisher.receive(StreamOpened(Object(token), 50)).unwrap())
  ignore(group.finish().unwrap())
  let aborted = group.abort(7UL).unwrap()
  assert_true(aborted.contains(AbortSend(50, 7UL)))
  assert_eq(group.abort(7UL), Ok([]))
  assert_eq(publisher.receive(StreamDataCommitted(50)), Ok([]))
  assert_eq(publisher.receive(Clock(50UL)), Ok([]))
}

///|
test "commit before deadline makes later abort harmless" {
  let publisher = delivery_publisher()
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(publisher.receive(StreamOpened(Object(token), 50)).unwrap())
  ignore(group.finish().unwrap())
  ignore(publisher.receive(StreamDataCommitted(50)).unwrap())
  assert_eq(group.abort(7UL), Ok([]))
  assert_eq(publisher.receive(Clock(50UL)), Ok([]))
}

///|
test "pending FIN deadline expires before stream opens" {
  let publisher = delivery_publisher()
  ignore(publisher.receive(Clock(100UL)).unwrap())
  let (group, token) = writer_group_open(publisher, 0UL)
  assert_eq(group.send(b"queued"), Err(ObjectStreamNotReady))
  ignore(group.finish().unwrap())
  ignore(publisher.receive(Clock(120UL)).unwrap())
  assert_eq(
    publisher.receive(StreamOpened(Object(token), 50)),
    Ok([AbortSend(50, 2UL)]),
  )
}

///|
test "datagram uses smaller nonzero timeout and clock never rewinds" {
  let publisher = delivery_publisher()
  ignore(publisher.receive(Clock(100UL)).unwrap())
  ignore(publisher.receive(Clock(90UL)).unwrap())
  let (_, actions) = publisher
    .send_object_datagram(StreamId(40), update_datagram(1UL, 0UL, b"payload"))
    .unwrap()
  assert_true(actions[0] is SendObjectDatagram(_, _, 110UL))
}

///|
test "default transport capability refuses outgoing subgroups" {
  let publisher = delivery_publisher()
  publisher.set_stream_delivery_capability(
    committed=false,
    abort_after_fin=false,
  )
  assert_true(
    publisher.open_object_group(StreamId(40), 0UL)
    is Err(DeliveryTimeoutUnsupported),
  )
  publisher.set_stream_delivery_capability(
    committed=true,
    abort_after_fin=false,
  )
  assert_true(
    publisher.open_object_group(StreamId(40), 0UL)
    is Err(DeliveryTimeoutUnsupported),
  )
}

///|
test "local FIN completion without timeout still permits abort" {
  let publisher = publisher_started()
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(publisher.receive(StreamOpened(Object(token), 50)).unwrap())
  ignore(group.finish().unwrap())
  for _ in 0..<2 {
    ignore(publisher.receive(SendCompleted(50, Sent)).unwrap())
  }
  assert_true(group.abort(9UL).unwrap().contains(AbortSend(50, 9UL)))
}

///|
test "abort after FIN explicitly rejects transports without support" {
  let publisher = publisher_started()
  publisher.set_stream_delivery_capability(
    committed=true,
    abort_after_fin=false,
  )
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(publisher.receive(StreamOpened(Object(token), 50)).unwrap())
  ignore(group.finish().unwrap())
  assert_eq(group.abort(9UL), Err(DeliveryTimeoutUnsupported))
  ignore(publisher.receive(StreamDataCommitted(50)).unwrap())
  assert_eq(group.abort(9UL), Ok([]))
}

///|
fn delivery_properties_publisher(
  properties : Bytes,
  object_timeout : UInt64,
  subgroup_timeout : UInt64,
) -> @session.ClientSession raise {
  let publisher = client_session_ready()
  ignore(publisher.publish_track(publisher_track(), 1UL, properties~).unwrap())
  let opened = publisher.receive(StreamOpened(Request(0UL), 20L)).unwrap()
  assert_eq(opened, [
    SendChunk(
      20L,
      @protocol.encode_publish({
        request_id: 0UL,
        track: publisher_track(),
        track_alias: 1UL,
        parameters: [],
        properties,
      }).unwrap(),
    ),
  ])
  ignore(publisher.receive(StreamData(20L, b"\x07\x00\x01\x00")).unwrap())
  ignore(publisher.receive(Clock(100UL)).unwrap())
  ignore(publisher.receive(InboundStreamStarted(40L, Bidirectional)).unwrap())
  ignore(
    publisher
    .receive(
      StreamData(
        40L,
        @protocol.encode_subscribe({
          request_id: 1UL,
          track: publisher_track(),
          parameters: [
            { kind: 0x02UL, value: VarInt(object_timeout), },
            { kind: 0x06UL, value: VarInt(subgroup_timeout), },
          ],
        }).unwrap(),
      ),
    )
    .unwrap(),
  )
  let accepted = publisher.accept_subscription(40L).unwrap()
  assert_eq(
    accepted[0],
    SendChunk(
      40L,
      @protocol.encode_subscribe_ok({
        track_alias: 1UL,
        parameters: [],
        properties,
      }).unwrap(),
    ),
  )
  publisher
}

///|
test "publisher Track Properties are sent and cap subscriber delivery timeouts" {
  // Property keys are delta encoded: OBJECT=2, SUBGROUP=2+4.
  let publisher = delivery_properties_publisher(b"\x02\x05\x04\x07", 10UL, 20UL)
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(publisher.receive(StreamOpened(Object(token), 50L)).unwrap())
  assert_true(
    group.send(b"payload").unwrap()[0] is SendObjectChunk(50L, _, 105UL),
  )
  ignore(group.finish().unwrap())
  assert_eq(publisher.receive(Clock(106UL)), Ok([]))
  assert_true(
    publisher.receive(Clock(107UL)).unwrap().contains(AbortSend(50L, 2UL)),
  )
}

///|
test "zero timeout on either endpoint preserves the other endpoint timeout" {
  let publisher = delivery_properties_publisher(b"\x02\x05\x04\x00", 0UL, 20UL)
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(publisher.receive(StreamOpened(Object(token), 50L)).unwrap())
  assert_true(
    group.send(b"payload").unwrap()[0] is SendObjectChunk(50L, _, 105UL),
  )
  ignore(group.finish().unwrap())
  assert_eq(publisher.receive(Clock(119UL)), Ok([]))
  assert_true(
    publisher.receive(Clock(120UL)).unwrap().contains(AbortSend(50L, 2UL)),
  )
}

///|
test "accepted REQUEST_UPDATE recomputes subscriber timeout under publisher cap" {
  let publisher = delivery_properties_publisher(b"\x02\x08\x04\x1e", 10UL, 20UL)
  let update = @protocol.encode_request_update({
    request_id: 3UL,
    parameters: [
      { kind: 0x02UL, value: VarInt(3UL), },
      { kind: 0x06UL, value: VarInt(50UL), },
    ],
  }).unwrap()
  ignore(publisher.receive(StreamData(40L, update)).unwrap())
  ignore(publisher.accept_subscription_update(40L, 3UL, update_ok()).unwrap())
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(publisher.receive(StreamOpened(Object(token), 50L)).unwrap())
  assert_true(
    group.send(b"payload").unwrap()[0] is SendObjectChunk(50L, _, 103UL),
  )
  ignore(group.finish().unwrap())
  assert_eq(publisher.receive(Clock(129UL)), Ok([]))
  assert_true(
    publisher.receive(Clock(130UL)).unwrap().contains(AbortSend(50L, 2UL)),
  )
}

///|
test "subscriber timeout wins when smaller and both zero leave writes unbounded" {
  let publisher = delivery_properties_publisher(b"\x02\x1e\x04\x28", 10UL, 20UL)
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(publisher.receive(StreamOpened(Object(token), 50L)).unwrap())
  assert_true(
    group.send(b"payload").unwrap()[0] is SendObjectChunk(50L, _, 110UL),
  )
  let publisher = delivery_properties_publisher(b"\x02\x00\x04\x00", 0UL, 0UL)
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(publisher.receive(StreamOpened(Object(token), 50L)).unwrap())
  assert_true(group.send(b"payload").unwrap()[0] is SendChunk(50L, _))
}

///|
test "invalid publisher Track Properties do not allocate a PUBLISH request" {
  let publisher = client_session_ready()
  assert_eq(
    publisher.publish_track(publisher_track(), 1UL, properties=b"\x02"),
    Err(UnexpectedMessage),
  )
  assert_eq(publisher.publish_track(publisher_track(), 1UL).unwrap().0, 0UL)
}

///|
test "finishing open group rejects timeout enabled after stream creation" {
  let publisher = publisher_started()
  ignore(publisher.receive(Clock(100UL)).unwrap())
  publisher.set_stream_delivery_capability(
    committed=true,
    abort_after_fin=false,
  )
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(publisher.receive(StreamOpened(Object(token), 50L)).unwrap())
  ignore(
    publisher
    .receive(
      StreamData(
        40L,
        request_update_bytes(3UL, [{ kind: 0x06UL, value: VarInt(20UL), }]),
      ),
    )
    .unwrap(),
  )
  ignore(publisher.accept_subscription_update(40L, 3UL, update_ok()).unwrap())
  assert_eq(group.finish(), Err(DeliveryTimeoutUnsupported))
  // Failure must not mark the writer finished or emit FIN.
  ignore(
    publisher
    .receive(
      StreamData(
        40L,
        request_update_bytes(5UL, [{ kind: 0x06UL, value: VarInt(0UL), }]),
      ),
    )
    .unwrap(),
  )
  ignore(publisher.accept_subscription_update(40L, 5UL, update_ok()).unwrap())
  assert_eq(group.finish(), Ok([SendFin(50L, b"")]))
}

///|
test "finishing pending group rejects timeout enabled after open request" {
  let publisher = publisher_started()
  ignore(publisher.receive(Clock(100UL)).unwrap())
  publisher.set_stream_delivery_capability(
    committed=true,
    abort_after_fin=false,
  )
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(
    publisher
    .receive(
      StreamData(
        40L,
        request_update_bytes(3UL, [{ kind: 0x06UL, value: VarInt(20UL), }]),
      ),
    )
    .unwrap(),
  )
  ignore(publisher.accept_subscription_update(40L, 3UL, update_ok()).unwrap())
  assert_eq(group.finish(), Err(DeliveryTimeoutUnsupported))
  let actions = publisher.receive(StreamOpened(Object(token), 50L)).unwrap()
  assert_eq(actions.length(), 1)
  assert_true(actions[0] is SendChunk(50L, _))
}
