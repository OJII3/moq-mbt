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
test "local PUBLISH receives peer updates and cancellation prevents late updates" {
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
  assert_eq(
    inbound.receive(@session.StreamFinished(40)),
    Ok([
      @session.AbortSend(40, 0),
      @session.Notify(@session.SubscriptionEnded(1, 40)),
    ]),
  )
  assert_eq(
    inbound.receive(
      @session.StreamData(40, request_update_bytes(3, forward_off)),
    ),
    Err(@session.SubscriptionNotAccepted),
  )
}
