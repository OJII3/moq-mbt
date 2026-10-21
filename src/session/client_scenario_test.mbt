///|
test "GOAWAY, pending update, cancellation, and late object events stay ordered" {
  let subscriber = subscriber_started()
  let forward_off = [forward_parameter(0)]
  let (update_id, actions) = subscriber
    .update_subscription(0, forward_off)
    .unwrap()
  assert_eq(update_id, 2UL)
  assert_eq(actions, [
    @session.Send(30, request_update_bytes(update_id, forward_off), false),
  ])

  assert_eq(
    subscriber.receive(
      @session.StreamData(11, client_goaway_control(2UL, 100UL, b"moqt://next")),
    ),
    Ok([
      @session.Notify(
        @session.ControlMessageReceived(
          @protocol.GoAwayControl({
            uri: b"moqt://next",
            timeout_ms: 100UL,
            request_id: 2UL,
          }),
        ),
      ),
      @session.Notify(
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
    Ok([@session.Notify(@session.ObjectDatagramReceived(0, datagram))]),
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
    Ok([@session.Notify(@session.SubscriptionUpdateAccepted(0, 2, update_ok()))]),
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
    subscriber.cancel_request(0UL),
    Ok([
      @session.AbortSend(30, 1UL),
      @session.StopReceiving(30, 1UL),
      @session.Notify(@session.SubscriptionEnded(0, 30)),
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
  assert_eq(subscriber.receive(@session.StreamFinished(40)), Ok([]))
  assert_eq(subscriber.cancel_request(0UL), Ok([]))
}
