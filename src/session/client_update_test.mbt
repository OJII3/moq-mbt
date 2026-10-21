///|
fn request_update_bytes(
  request_id : UInt64,
  parameters : Array[@protocol.MessageParameter],
) -> Bytes {
  @protocol.encode_request_update({ request_id, parameters, }).unwrap()
}

///|
fn forward_parameter(value : Byte) -> @protocol.MessageParameter {
  { kind: 0x10, value: @protocol.Byte(value), }
}

///|
fn subscriber_priority_parameter(value : Byte) -> @protocol.MessageParameter {
  { kind: 0x20, value: @protocol.Byte(value), }
}

///|
fn update_ok() -> @protocol.RequestOkMessage {
  { parameters: [], properties: b"", }
}

///|
fn update_datagram(
  track_alias : UInt64,
  object_id : UInt64,
  payload : Bytes,
) -> @protocol.ObjectDatagram {
  {
    track_alias,
    group_id: 1,
    object_id,
    publisher_priority: None,
    end_of_group: false,
    properties: b"",
    status: @protocol.Normal,
    payload,
  }
}

///|
test "subscriber keeps its request stream open and matches one update response" {
  let subscriber = subscriber_started()
  let parameters = [forward_parameter(0)]
  let (update_id, actions) = subscriber
    .update_subscription(0, parameters)
    .unwrap()
  assert_eq(update_id, 2UL)
  assert_eq(actions, [
    @session.Send(30, request_update_bytes(update_id, parameters), false),
  ])
  assert_eq(
    subscriber.update_subscription(0, [subscriber_priority_parameter(4)]),
    Err(@session.RequestUpdatePending),
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
    subscriber
    .update_subscription(0, [subscriber_priority_parameter(4)])
    .unwrap().0,
    4UL,
  )
}

///|
test "datagrams and subgroup objects use the accepted subscription FORWARD state" {
  let subscriber = subscriber_started()
  let forward_off = [forward_parameter(0)]
  let forward_on = [forward_parameter(1)]
  let datagram = update_datagram(1, 2, b"payload")
  let datagram_bytes = @protocol.encode_object_datagram(datagram).unwrap()
  assert_eq(
    subscriber.receive(@session.DatagramReceived(datagram_bytes)),
    Ok([@session.Notify(@session.ObjectDatagramReceived(0, datagram))]),
  )
  let subgroup : @protocol.SubgroupHeader = {
    track_alias: 1,
    group_id: 1,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: false,
  }
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(9, @session.Unidirectional),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        9,
        @protocol.encode_subgroup_stream(subgroup, [
          {
            object_id: 1,
            status: @protocol.Normal,
            properties: b"",
            payload: b"stream",
          },
        ]).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(
        @session.ObjectReceived(0, subgroup, [
          {
            object_id: 1,
            status: @protocol.Normal,
            properties: b"",
            payload: b"stream",
          },
        ]),
      ),
    ]),
  )

  let (disable_id, _) = subscriber.update_subscription(0, forward_off).unwrap()
  assert_eq(disable_id, 2UL)
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
    subscriber.receive(@session.DatagramReceived(datagram_bytes)),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.InboundStreamStarted(13, @session.Unidirectional),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        13,
        @protocol.encode_subgroup_stream(subgroup, [
          {
            object_id: 2,
            status: @protocol.Normal,
            properties: b"",
            payload: b"dropped",
          },
        ]).unwrap(),
      ),
    ),
    Ok([]),
  )

  let (enable_id, _) = subscriber.update_subscription(0, forward_on).unwrap()
  assert_eq(enable_id, 4UL)
  ignore(
    subscriber
    .receive(
      @session.StreamData(
        30,
        @protocol.encode_request_ok_for(
          update_ok(),
          @protocol.RequestOkContext::RequestUpdate,
        ).unwrap(),
      ),
    )
    .unwrap(),
  )
  assert_eq(
    subscriber.receive(@session.DatagramReceived(datagram_bytes)),
    Ok([@session.Notify(@session.ObjectDatagramReceived(0, datagram))]),
  )
}

///|
test "publisher datagram send obeys the same FORWARD update and alias checks" {
  let publisher = publisher_started()
  let datagram = update_datagram(1, 2, b"payload")
  assert_eq(
    publisher.send_object_datagram(20, datagram),
    Ok([
      @session.SendDatagram(@protocol.encode_object_datagram(datagram).unwrap()),
    ]),
  )
  let forward_off = [forward_parameter(0)]
  assert_eq(
    publisher.send_object_datagram(40, datagram),
    Ok([
      @session.SendDatagram(@protocol.encode_object_datagram(datagram).unwrap()),
    ]),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(40, request_update_bytes(3, forward_off)),
    ),
    Ok([
      @session.Notify(
        @session.SubscriptionUpdateRequested(1, 3, 40, forward_off, forward_off),
      ),
    ]),
  )
  ignore(publisher.accept_subscription_update(40, 3, update_ok()).unwrap())
  assert_eq(
    publisher.send_object_datagram(40, datagram),
    Err(@session.SubscriptionNotAccepted),
  )
  let forward_on = [forward_parameter(1)]
  assert_eq(
    publisher.receive(
      @session.StreamData(40, request_update_bytes(5, forward_on)),
    ),
    Ok([
      @session.Notify(
        @session.SubscriptionUpdateRequested(1, 5, 40, forward_on, forward_on),
      ),
    ]),
  )
  ignore(publisher.accept_subscription_update(40, 5, update_ok()).unwrap())
  assert_eq(
    publisher.send_object_datagram(40, datagram),
    Ok([
      @session.SendDatagram(@protocol.encode_object_datagram(datagram).unwrap()),
    ]),
  )
  assert_eq(
    publisher.send_object_datagram(40, update_datagram(99, 2, b"payload")),
    Err(@session.Track(@session.AliasMismatch)),
  )
}

///|
test "unknown, malformed, and post-cancel datagrams do not reach the application" {
  let subscriber = subscriber_started()
  assert_eq(
    subscriber.receive(
      @session.DatagramReceived(b"\xf0\x13\x2b\x3e\x29\xff\x80\x01"),
    ),
    Ok([]),
  )
  // These two datagrams form a valid wire only if their bytes are combined.
  assert_eq(
    subscriber.receive(@session.DatagramReceived(b"\x0c\x01")),
    Err(@session.UnexpectedMessage),
  )
  assert_eq(
    subscriber.receive(@session.DatagramReceived(b"\x02\x04")),
    Err(@session.UnexpectedMessage),
  )
  assert_eq(
    subscriber.receive(
      @session.DatagramReceived(
        @protocol.encode_object_datagram(update_datagram(99, 1, b"unknown")).unwrap(),
      ),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(@session.DatagramReceived(b"invalid")),
    Err(@session.UnexpectedMessage),
  )
  assert_eq(
    subscriber.cancel_request(0),
    Ok([
      @session.AbortSend(30, 1),
      @session.StopReceiving(30, 1),
      @session.Notify(@session.SubscriptionEnded(0, 30)),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.DatagramReceived(
        @protocol.encode_object_datagram(update_datagram(1, 1, b"late")).unwrap(),
      ),
    ),
    Ok([]),
  )
}

///|
test "publisher accepts incremental queued updates and preserves omitted parameters" {
  let publisher = publisher_started()
  let token = match
    publisher.open_object_stream(40, publisher_header(1, false)).unwrap() {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("expected object stream")
  }
  ignore(
    publisher
    .receive(@session.StreamOpened(@session.Object(token), 50))
    .unwrap(),
  )
  let forward_off = [forward_parameter(0)]
  let priority = [subscriber_priority_parameter(7)]
  let update_a = request_update_bytes(3, forward_off)
  let update_b = request_update_bytes(5, priority)
  assert_eq(
    publisher.receive(@session.StreamData(40, update_a + update_b)),
    Ok([
      @session.Notify(
        @session.SubscriptionUpdateRequested(1, 3, 40, forward_off, forward_off),
      ),
      @session.Notify(
        @session.SubscriptionUpdateRequested(1, 5, 40, priority, [
          forward_parameter(0),
          subscriber_priority_parameter(7),
        ]),
      ),
    ]),
  )
  assert_eq(
    publisher.publish_done(40, 3, b""),
    Err(@session.RequestUpdatePending),
  )
  assert_eq(
    publisher.accept_subscription_update(40, 5, update_ok()),
    Err(@session.RequestUpdatePending),
  )
  assert_eq(
    publisher.accept_subscription_update(40, 3, update_ok()),
    Ok([
      @session.Send(
        40,
        @protocol.encode_request_ok_for(
          update_ok(),
          @protocol.RequestOkContext::RequestUpdate,
        ).unwrap(),
        false,
      ),
      @session.AbortSend(50, 0),
      @session.Notify(@session.SubscriptionUpdateApplied(1, 3, 40, forward_off)),
    ]),
  )
  assert_eq(
    publisher.receive(@session.SendComplete(50, true)),
    Ok([@session.Notify(@session.SendFinished(50, true))]),
  )
  assert_eq(
    publisher.open_object_stream(40, publisher_header(1, false)),
    Err(@session.SubscriptionNotAccepted),
  )
  assert_eq(
    publisher.accept_subscription_update(40, 5, update_ok()),
    Ok([
      @session.Send(
        40,
        @protocol.encode_request_ok_for(
          update_ok(),
          @protocol.RequestOkContext::RequestUpdate,
        ).unwrap(),
        false,
      ),
      @session.Notify(
        @session.SubscriptionUpdateApplied(1, 5, 40, [
          forward_parameter(0),
          subscriber_priority_parameter(7),
        ]),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(@session.StreamData(40, update_a)),
    Err(@session.DuplicateRequestId),
  )
  let forward_on = [forward_parameter(1)]
  assert_eq(
    publisher.receive(
      @session.StreamData(40, request_update_bytes(7, forward_on)),
    ),
    Ok([
      @session.Notify(
        @session.SubscriptionUpdateRequested(1, 7, 40, forward_on, [
          forward_on[0],
          subscriber_priority_parameter(7),
        ]),
      ),
    ]),
  )
  assert_eq(
    publisher.accept_subscription_update(40, 7, update_ok()),
    Ok([
      @session.Send(
        40,
        @protocol.encode_request_ok_for(
          update_ok(),
          @protocol.RequestOkContext::RequestUpdate,
        ).unwrap(),
        false,
      ),
      @session.Notify(
        @session.SubscriptionUpdateApplied(1, 7, 40, [
          forward_parameter(1),
          subscriber_priority_parameter(7),
        ]),
      ),
    ]),
  )
  assert_eq(
    publisher.open_object_stream(40, publisher_header(1, false)),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Object(1))]),
  )
}

///|
test "subscriber parses every split of a peer update on the established stream" {
  let wire = request_update_bytes(3, [forward_parameter(0)])
  for split = 0; split < wire.length(); split = split + 1 {
    let publisher = publisher_started()
    let first = publisher
      .receive(@session.StreamData(40, wire[:split].to_owned()))
      .unwrap()
    let second = publisher
      .receive(@session.StreamData(40, wire[split:].to_owned()))
      .unwrap()
    assert_eq(first + second, [
      @session.Notify(
        @session.SubscriptionUpdateRequested(1, 3, 40, [forward_parameter(0)], [
          forward_parameter(0),
        ]),
      ),
    ])
  }
}

///|
test "rejected update terminates its subscription after active object streams stop" {
  let publisher = publisher_started()
  let token = match
    publisher.open_object_stream(40, publisher_header(1, false)).unwrap() {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("expected object stream")
  }
  ignore(
    publisher
    .receive(@session.StreamOpened(@session.Object(token), 50))
    .unwrap(),
  )
  let parameters : Array[@protocol.MessageParameter] = []
  ignore(
    publisher
    .receive(@session.StreamData(40, request_update_bytes(3, parameters)))
    .unwrap(),
  )
  let error : @protocol.RequestErrorMessage = {
    code: 1,
    retry_interval: 0,
    reason: b"unsupported",
    redirect: b"",
  }
  assert_eq(
    publisher.reject_subscription_update(40, 3, error),
    Ok([
      @session.Send(40, @protocol.encode_request_error(error).unwrap(), false),
      @session.AbortSend(50, 0),
    ]),
  )
  let done : @protocol.PublishDoneMessage = {
    code: 8,
    stream_count: 1,
    reason: b"",
  }
  assert_eq(
    publisher.receive(@session.SendComplete(50, true)),
    Ok([
      @session.Notify(@session.SendFinished(50, true)),
      @session.Send(40, @protocol.encode_publish_done(done).unwrap(), true),
      @session.Notify(@session.PublishDone(1, done)),
    ]),
  )
  assert_eq(
    publisher.receive(@session.StreamData(40, request_update_bytes(5, []))),
    Err(@session.SubscriptionNotAccepted),
  )
}

///|
test "cancellation supersedes a pending UPDATE_FAILED PUBLISH_DONE without aborting twice" {
  let publisher = publisher_started()
  let token = match
    publisher.open_object_stream(40, publisher_header(1, false)).unwrap() {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("expected object stream")
  }
  ignore(
    publisher
    .receive(@session.StreamOpened(@session.Object(token), 50))
    .unwrap(),
  )
  ignore(
    publisher
    .receive(@session.StreamData(40, request_update_bytes(3, [])))
    .unwrap(),
  )
  let error : @protocol.RequestErrorMessage = {
    code: 1,
    retry_interval: 0,
    reason: b"unsupported",
    redirect: b"",
  }
  assert_eq(
    publisher.reject_subscription_update(40, 3, error),
    Ok([
      @session.Send(40, @protocol.encode_request_error(error).unwrap(), false),
      @session.AbortSend(50, 0),
    ]),
  )
  assert_eq(
    publisher.cancel_request(1UL),
    Ok([
      @session.AbortSend(40, 1UL),
      @session.StopReceiving(40, 1UL),
      @session.Notify(@session.SubscriptionEnded(1UL, 40)),
    ]),
  )
  assert_eq(
    publisher.receive(@session.SendComplete(50, true)),
    Ok([@session.Notify(@session.SendFinished(50, true))]),
  )
}

///|
test "cancellation can supersede a pending UPDATE_FAILED PUBLISH_DONE on local PUBLISH" {
  let publisher = publisher_announced()
  let token = match
    publisher.open_object_stream(20, publisher_header(1, false)).unwrap() {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("expected object stream")
  }
  ignore(
    publisher
    .receive(@session.StreamOpened(@session.Object(token), 50))
    .unwrap(),
  )
  assert_eq(
    publisher.receive(@session.StreamData(20, request_update_bytes(3, []))),
    Ok([@session.Notify(@session.SubscriptionUpdateRequested(0, 3, 20, [], []))]),
  )
  let error : @protocol.RequestErrorMessage = {
    code: 1,
    retry_interval: 0,
    reason: b"unsupported",
    redirect: b"",
  }
  assert_eq(
    publisher.reject_subscription_update(20, 3, error),
    Ok([
      @session.Send(20, @protocol.encode_request_error(error).unwrap(), false),
      @session.AbortSend(50, 0),
    ]),
  )
  assert_eq(
    publisher.cancel_request(0UL),
    Ok([
      @session.AbortSend(20, 1UL),
      @session.StopReceiving(20, 1UL),
      @session.Notify(@session.SubscriptionEnded(0UL, 20)),
    ]),
  )
  assert_eq(publisher.cancel_request(0UL), Ok([]))
  assert_eq(
    publisher.receive(@session.SendComplete(50, true)),
    Ok([@session.Notify(@session.SendFinished(50, true))]),
  )
}

///|
test "failed local update requires UPDATE_FAILED PUBLISH_DONE" {
  let subscriber = subscriber_started()
  let (update_id, _) = subscriber
    .update_subscription(0, [forward_parameter(0)])
    .unwrap()
  let error : @protocol.RequestErrorMessage = {
    code: 1,
    retry_interval: 0,
    reason: b"unsupported",
    redirect: b"",
  }
  let done : @protocol.PublishDoneMessage = {
    code: 8,
    stream_count: 0,
    reason: b"",
  }
  let wire = @protocol.encode_request_error(error).unwrap() +
    @protocol.encode_publish_done(done).unwrap()
  assert_eq(
    subscriber.receive(@session.StreamData(30, wire)),
    Ok([
      @session.Notify(@session.SubscriptionUpdateRejected(0, update_id, error)),
      @session.Notify(@session.PublishDone(0, done)),
    ]),
  )
  assert_eq(
    subscriber.update_subscription(0, []),
    Err(@session.SubscriptionNotAccepted),
  )
}

///|
test "request FIN closes the update direction without ending the local PUBLISH subscription" {
  let publisher = publisher_announced()
  let forward_off = [forward_parameter(0)]
  assert_eq(
    publisher.receive(
      @session.StreamData(20, request_update_bytes(1, forward_off)),
    ),
    Ok([
      @session.Notify(
        @session.SubscriptionUpdateRequested(0, 1, 20, forward_off, forward_off),
      ),
    ]),
  )
  assert_eq(
    publisher.accept_subscription_update(20, 1, update_ok()),
    Ok([
      @session.Send(
        20,
        @protocol.encode_request_ok_for(
          update_ok(),
          @protocol.RequestOkContext::RequestUpdate,
        ).unwrap(),
        false,
      ),
      @session.Notify(@session.SubscriptionUpdateApplied(0, 1, 20, forward_off)),
    ]),
  )
  let inbound = publisher_started()
  assert_eq(inbound.receive(@session.StreamFinished(40)), Ok([]))
  assert_eq(
    inbound.open_object_stream(40, publisher_header(1, false)),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Object(0))]),
  )
  assert_eq(
    inbound.receive(
      @session.StreamData(40, request_update_bytes(3, forward_off)),
    ),
    Ok([]),
  )
}
