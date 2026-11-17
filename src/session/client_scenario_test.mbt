///|
test "GOAWAY, pending update, cancellation, and late object events stay ordered" {
  let subscriber = subscriber_started()
  let forward_off = [forward_parameter(0)]
  let (update_id, actions) = subscriber
    .update_subscription(RequestId(0), forward_off)
    .unwrap()
  assert_eq(update_id, RequestId(2UL))
  assert_eq(actions, [
    SendChunk(30, request_update_bytes(update_id, forward_off)),
  ])

  assert_eq(
    subscriber.receive(
      StreamData(11, client_goaway_control(2UL, 100UL, b"moqt://next")),
    ),
    Ok([
      EmitNotice(
        ControlMessageReceived(
          GoAwayControl({
            uri: b"moqt://next",
            timeout_ms: 100UL,
            request_id: 2UL,
          }),
        ),
      ),
      EmitNotice(ControlGoAwayReceived(b"moqt://next", 100UL, 2UL)),
    ]),
  )
  assert_eq(subscriber.state(), Draining)

  let datagram = update_datagram(1, 7, b"in-flight")
  assert_eq(
    subscriber.receive(
      DatagramReceived(@protocol.encode_object_datagram(datagram).unwrap()),
    ),
    Ok([
      EmitNotice(
        ObjectDatagramReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          datagram,
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(
        30,
        @protocol.encode_request_ok_for(update_ok(), RequestUpdate).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscriptionUpdateAccepted(0, 2, update_ok()))]),
  )
  assert_eq(
    subscriber.receive(
      DatagramReceived(@protocol.encode_object_datagram(datagram).unwrap()),
    ),
    Ok([]),
  )

  assert_eq(
    subscriber.send_goaway(0UL),
    Ok([SendChunk(10, client_goaway_control(1UL, 0UL, b""))]),
  )
  assert_eq(
    subscriber.cancel_request(0UL),
    Ok([
      AbortSend(30, 1UL),
      StopReceiving(30, 1UL),
      EmitNotice(
        SubscriptionEnded(0, 30, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      DatagramReceived(@protocol.encode_object_datagram(datagram).unwrap()),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(InboundStreamStarted(40, Unidirectional)),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(
        40,
        @protocol.encode_subgroup_stream(subscriber_header(1, true), [
          subscriber_frame(0, b"late"),
        ]).unwrap(),
      ),
    ),
    Ok([StopReceiving(40, 1UL)]),
  )
  assert_eq(subscriber.receive(InboundStreamFinished(40)), Ok([]))
  assert_eq(subscriber.cancel_request(0UL), Ok([]))
}

///|
test "Datagram send completion is separate from peer Object delivery" {
  let publisher = publisher_started()
  let object = update_datagram(1, 7, b"peer-sample")
  assert_eq(
    publisher.send_object_datagram(StreamId(20), object),
    Ok(
      (
        0UL,
        [
          SendDatagram(0UL, @protocol.encode_object_datagram(object).unwrap()),
          EmitNotice(DatagramQueued(0UL)),
        ],
      ),
    ),
  )
  assert_eq(publisher.receive(DatagramSendComplete(0UL)), Ok([]))

  let peer = subscriber_started()
  assert_eq(
    peer.receive(
      DatagramReceived(@protocol.encode_object_datagram(object).unwrap()),
    ),
    Ok([
      EmitNotice(
        ObjectDatagramReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          object,
        ),
      ),
    ]),
  )
}
