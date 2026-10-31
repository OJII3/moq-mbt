///|
test "GOAWAY, pending update, cancellation, and late object events stay ordered" {
  let subscriber = subscriber_started()
  let forward_off = [forward_parameter(0)]
  let (update_id, actions) = subscriber
    .update_subscription(@session.RequestId(0), forward_off)
    .unwrap()
  assert_eq(update_id, @session.RequestId(2UL))
  assert_eq(actions, [
    @session.SendChunk(30, request_update_bytes(update_id, forward_off)),
  ])

  assert_eq(
    subscriber.receive(
      @session.StreamData(11, client_goaway_control(2UL, 100UL, b"moqt://next")),
    ),
    Ok([
      @session.EmitNotice(
        @session.ControlMessageReceived(
          @protocol.GoAwayControl({
            uri: b"moqt://next",
            timeout_ms: 100UL,
            request_id: 2UL,
          }),
        ),
      ),
      @session.EmitNotice(
        @session.ControlGoAwayReceived(b"moqt://next", 100UL, 2UL),
      ),
    ]),
  )
  assert_eq(subscriber.state(), @session.Draining)

  let datagram = update_datagram(1, 7, b"in-flight")
  assert_eq(
    subscriber.receive(
      @session.DatagramReceived(
        @protocol.encode_object_datagram(datagram).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.ObjectDatagramReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          datagram,
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        30,
        @protocol.encode_request_ok_for(
          update_ok(),
          @protocol.RequestOkContext::RequestUpdate,
        ).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.SubscriptionUpdateAccepted(0, 2, update_ok()),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.DatagramReceived(
        @protocol.encode_object_datagram(datagram).unwrap(),
      ),
    ),
    Ok([]),
  )

  assert_eq(
    subscriber.send_goaway(0UL),
    Ok([@session.SendChunk(10, client_goaway_control(1UL, 0UL, b""))]),
  )
  assert_eq(
    subscriber.cancel_request(0UL),
    Ok([
      @session.AbortSend(30, 1UL),
      @session.StopReceiving(30, 1UL),
      @session.EmitNotice(
        @session.SubscriptionEnded(0, 30, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.DatagramReceived(
        @protocol.encode_object_datagram(datagram).unwrap(),
      ),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(40, @session.Unidirectional),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        40,
        @protocol.encode_subgroup_stream(subscriber_header(1, true), [
          subscriber_frame(0, b"late"),
        ]).unwrap(),
      ),
    ),
    Ok([@session.StopReceiving(40, 1UL)]),
  )
  assert_eq(subscriber.receive(@session.InboundStreamFinished(40)), Ok([]))
  assert_eq(subscriber.cancel_request(0UL), Ok([]))
}

///|
test "Datagram send completion is separate from peer Object delivery" {
  let publisher = publisher_started()
  let object = update_datagram(1, 7, b"peer-sample")
  assert_eq(
    publisher.send_object_datagram(@session.StreamId(20), object),
    Ok(
      (
        0UL,
        [
          @session.SendDatagram(
            0UL,
            @protocol.encode_object_datagram(object).unwrap(),
          ),
          @session.EmitNotice(@session.DatagramQueued(0UL)),
        ],
      ),
    ),
  )
  assert_eq(publisher.receive(@session.DatagramSendComplete(0UL)), Ok([]))

  let peer = subscriber_started()
  assert_eq(
    peer.receive(
      @session.DatagramReceived(
        @protocol.encode_object_datagram(object).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.ObjectDatagramReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          object,
        ),
      ),
    ]),
  )
}
