///|
fn m3_absolute_range(
  start_group : UInt64,
  start_object : UInt64,
  end_group : UInt64,
) -> @protocol.MessageParameter {
  {
    kind: 0x21,
    value: @protocol.Filter(
      @protocol.AbsoluteRange(start_group, start_object, end_group),
    ),
  }
}

///|
test "M3 accepted absolute range constrains publisher Object sends" {
  let publisher = publisher_started()
  let filter = [m3_absolute_range(10UL, 2UL, 12UL)]
  ignore(
    publisher
    .receive(@session.StreamData(40, request_update_bytes(3UL, filter)))
    .unwrap(),
  )
  ignore(publisher.accept_subscription_update(40, 3UL, update_ok()).unwrap())
  ignore(publisher_open(publisher, 40, 50, publisher_header(10UL, false)))
  assert_eq(
    publisher.send_object(
      @session.StreamId(50),
      publisher_frame(0UL, b"outside"),
    ),
    Err(@session.SubscriptionNotAccepted),
  )
  assert_true(
    publisher.send_object(
      @session.StreamId(50),
      publisher_frame(2UL, b"inside"),
    )
    is Ok(_),
  )
}

///|
test "M3 accepted absolute range constrains publisher Object Datagrams" {
  let publisher = publisher_started()
  let filter = [m3_absolute_range(10UL, 0UL, 12UL)]
  ignore(
    publisher
    .receive(@session.StreamData(40, request_update_bytes(3UL, filter)))
    .unwrap(),
  )
  ignore(publisher.accept_subscription_update(40, 3UL, update_ok()).unwrap())
  assert_eq(
    publisher.send_object_datagram(
      @session.StreamId(40),
      update_datagram(1UL, 0UL, b"outside"),
    ),
    Err(@session.SubscriptionNotAccepted),
  )
  let beyond_range : @protocol.ObjectDatagram = {
    track_alias: 1UL,
    group_id: 13UL,
    object_id: 0UL,
    publisher_priority: None,
    end_of_group: false,
    properties: b"",
    status: @protocol.Normal,
    payload: b"outside",
  }
  assert_eq(
    publisher.send_object_datagram(@session.StreamId(40), beyond_range),
    Err(@session.SubscriptionNotAccepted),
  )
  let inside : @protocol.ObjectDatagram = {
    track_alias: 1UL,
    group_id: 12UL,
    object_id: 0UL,
    publisher_priority: None,
    end_of_group: false,
    properties: b"",
    status: @protocol.Normal,
    payload: b"inside",
  }
  assert_true(
    publisher.send_object_datagram(@session.StreamId(40), inside) is Ok(_),
  )
}

///|
test "M3 publish done waits for an Object stream opening callback" {
  let publisher = publisher_started()
  ignore(
    publisher
    .open_object_stream(@session.StreamId(40), publisher_header(1UL, false))
    .unwrap(),
  )
  assert_eq(
    publisher.publish_done(@session.StreamId(40), 2UL, b""),
    Err(@session.Track(@session.ObjectStreamsStillOpen)),
  )
}

///|
test "M3 relative filter snapshots only when its filter parameter is updated" {
  let publisher = publisher_started()
  ignore(
    publisher
    .send_object_datagram(
      @session.StreamId(40),
      update_datagram(1UL, 5UL, b"seed"),
    )
    .unwrap(),
  )
  let largest_object : Array[@protocol.MessageParameter] = [
    { kind: 0x21UL, value: @protocol.Filter(@protocol.LargestObject), },
  ]
  ignore(
    publisher
    .receive(@session.StreamData(40, request_update_bytes(3UL, largest_object)))
    .unwrap(),
  )
  ignore(publisher.accept_subscription_update(40, 3UL, update_ok()).unwrap())
  assert_eq(
    publisher.send_object_datagram(
      @session.StreamId(40),
      update_datagram(1UL, 5UL, b"old"),
    ),
    Err(@session.SubscriptionNotAccepted),
  )
  ignore(
    publisher
    .send_object_datagram(
      @session.StreamId(40),
      update_datagram(1UL, 8UL, b"new largest"),
    )
    .unwrap(),
  )
  let priority = [subscriber_priority_parameter(4)]
  ignore(
    publisher
    .receive(@session.StreamData(40, request_update_bytes(5UL, priority)))
    .unwrap(),
  )
  ignore(publisher.accept_subscription_update(40, 5UL, update_ok()).unwrap())
  assert_true(
    publisher.send_object_datagram(
      40,
      update_datagram(1UL, 7UL, b"kept window"),
    )
    is Ok(_),
  )
  ignore(
    publisher
    .receive(@session.StreamData(40, request_update_bytes(7UL, largest_object)))
    .unwrap(),
  )
  ignore(publisher.accept_subscription_update(40, 7UL, update_ok()).unwrap())
  assert_eq(
    publisher.send_object_datagram(
      40,
      update_datagram(1UL, 7UL, b"refreshed window"),
    ),
    Err(@session.SubscriptionNotAccepted),
  )
  assert_true(
    publisher.send_object_datagram(
      40,
      update_datagram(1UL, 9UL, b"after snapshot"),
    )
    is Ok(_),
  )
}
