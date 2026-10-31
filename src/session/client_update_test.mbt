///|
fn request_update_bytes(
  request_id : @session.RequestId,
  parameters : Array[@protocol.MessageParameter],
) -> Bytes {
  @protocol.encode_request_update({
    request_id: request_id.to_uint64(),
    parameters,
  }).unwrap()
}

///|
fn forward_parameter(value : Byte) -> @protocol.MessageParameter {
  @protocol.forward_parameter(value != 0)
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
    .update_subscription(@session.RequestId(0), parameters)
    .unwrap()
  assert_eq(update_id, @session.RequestId(2UL))
  assert_eq(actions, [
    @session.SendChunk(30, request_update_bytes(update_id, parameters)),
  ])
  assert_eq(
    subscriber.update_subscription(@session.RequestId(0), [
      subscriber_priority_parameter(4),
    ]),
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
    Ok([
      @session.EmitNotice(
        @session.SubscriptionUpdateAccepted(0, 2, update_ok()),
      ),
    ]),
  )
  assert_eq(
    subscriber
    .update_subscription(@session.RequestId(0), [
      subscriber_priority_parameter(4),
    ])
    .unwrap().0,
    4UL,
  )
}

///|
test "Joining FETCH can follow a pending Forward State 0 to 1 update" {
  let subscriber = subscriber_started()
  let (off_id, _) = subscriber
    .update_subscription(0UL, [forward_parameter(0)])
    .unwrap()
  assert_eq(off_id, @session.RequestId(2UL))
  ignore(
    subscriber.receive(
      @session.StreamData(
        30,
        @protocol.encode_request_ok_for(
          update_ok(),
          @protocol.RequestOkContext::RequestUpdate,
        ).unwrap(),
      ),
    ),
  )

  let (on_id, _) = subscriber
    .update_subscription(0UL, [forward_parameter(1)])
    .unwrap()
  assert_eq(on_id, @session.RequestId(4UL))
  let (fetch_id, actions) = subscriber
    .fetch_joining_relative(subscription_request_id=0UL, groups_back=2UL, parameters=[])
    .unwrap()
  assert_eq(fetch_id, @session.RequestId(6UL))
  assert_eq(actions, [
    @session.OpenStream(@session.Bidirectional, @session.Request(6UL)),
  ])
  let fetch : @protocol.FetchMessage = {
    request_id: fetch_id.to_uint64(),
    fetch_type: @protocol.RelativeJoiningFetch(0UL, 2UL),
    parameters: [],
  }
  assert_eq(
    subscriber.receive(@session.StreamOpened(@session.Request(fetch_id), 32)),
    Ok([@session.SendChunk(32, @protocol.encode_fetch(fetch).unwrap())]),
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
      @session.EmitNotice(
        @session.ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          subgroup,
          [
            {
              object_id: 1,
              status: @protocol.Normal,
              properties: b"",
              payload: b"stream",
            },
          ],
        ),
      ),
    ]),
  )

  let (disable_id, _) = subscriber
    .update_subscription(@session.RequestId(0), forward_off)
    .unwrap()
  assert_eq(disable_id, @session.RequestId(2UL))
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

  let (enable_id, _) = subscriber
    .update_subscription(@session.RequestId(0), forward_on)
    .unwrap()
  assert_eq(enable_id, @session.RequestId(4UL))
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
}

///|
test "publisher datagram send obeys the same FORWARD update and alias checks" {
  let publisher = publisher_started()
  let datagram = update_datagram(1, 2, b"payload")
  assert_eq(
    publisher.send_object_datagram(@session.StreamId(20), datagram),
    Ok(
      (
        0UL,
        [
          @session.SendDatagram(
            0UL,
            @protocol.encode_object_datagram(datagram).unwrap(),
          ),
          @session.EmitNotice(@session.DatagramQueued(0UL)),
        ],
      ),
    ),
  )
  let forward_off = [forward_parameter(0)]
  assert_eq(
    publisher.send_object_datagram(@session.StreamId(40), datagram),
    Ok(
      (
        1UL,
        [
          @session.SendDatagram(
            1UL,
            @protocol.encode_object_datagram(datagram).unwrap(),
          ),
          @session.EmitNotice(@session.DatagramQueued(1UL)),
        ],
      ),
    ),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(40, request_update_bytes(3, forward_off)),
    ),
    Ok([
      @session.EmitNotice(
        @session.SubscriptionUpdateRequested(1, 3, 40, forward_off, forward_off),
      ),
    ]),
  )
  ignore(
    publisher
    .accept_subscription_update(
      @session.StreamId(40),
      @session.RequestId(3),
      update_ok(),
    )
    .unwrap(),
  )
  assert_eq(
    publisher.send_object_datagram(@session.StreamId(40), datagram),
    Err(@session.SubscriptionNotAccepted),
  )
  let forward_on = [forward_parameter(1)]
  assert_eq(
    publisher.receive(
      @session.StreamData(40, request_update_bytes(5, forward_on)),
    ),
    Ok([
      @session.EmitNotice(
        @session.SubscriptionUpdateRequested(1, 5, 40, forward_on, forward_on),
      ),
    ]),
  )
  ignore(
    publisher
    .accept_subscription_update(
      @session.StreamId(40),
      @session.RequestId(5),
      update_ok(),
    )
    .unwrap(),
  )
  assert_eq(
    publisher.send_object_datagram(@session.StreamId(40), datagram),
    Ok(
      (
        2UL,
        [
          @session.SendDatagram(
            2UL,
            @protocol.encode_object_datagram(datagram).unwrap(),
          ),
          @session.EmitNotice(@session.DatagramQueued(2UL)),
        ],
      ),
    ),
  )
  assert_eq(
    publisher.send_object_datagram(
      @session.StreamId(40),
      update_datagram(99, 2, b"payload"),
    ),
    Err(@session.Track(@session.AliasMismatch)),
  )
}

///|
test "Joining FETCH waits for REQUEST_UPDATE Forward transition and keeps its range" {
  let publisher = publisher_announced()
  let track = publisher_track()
  let first_subscribe = @protocol.encode_subscribe({
    request_id: 1UL,
    track,
    parameters: [],
  }).unwrap()
  ignore(
    publisher.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
  )
  ignore(publisher.receive(@session.StreamData(40, first_subscribe)))
  ignore(publisher.accept_subscription(@session.StreamId(40)).unwrap())
  let header : @protocol.SubgroupHeader = {
    track_alias: 1UL,
    group_id: 10UL,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: false,
  }
  ignore(publisher.open_object_stream(@session.StreamId(40), header).unwrap())
  ignore(publisher.receive(@session.StreamOpened(@session.Object(0UL), 30)))
  ignore(
    publisher
    .send_object(@session.StreamId(30), publisher_frame(3UL, b"object"))
    .unwrap(),
  )

  let second_subscribe = @protocol.encode_subscribe({
    request_id: 3UL,
    track,
    parameters: [forward_parameter(0)],
  }).unwrap()
  ignore(
    publisher.receive(@session.InboundStreamStarted(42, @session.Bidirectional)),
  )
  ignore(publisher.receive(@session.StreamData(42, second_subscribe)))
  ignore(publisher.accept_subscription(@session.StreamId(42)).unwrap())
  let update = @protocol.encode_request_update({
    request_id: 5UL,
    parameters: [forward_parameter(1)],
  }).unwrap()
  assert_eq(
    publisher.receive(@session.StreamData(42, update)),
    Ok([
      @session.EmitNotice(
        @session.SubscriptionUpdateRequested(
          3UL,
          5UL,
          42,
          [forward_parameter(1)],
          [forward_parameter(1)],
        ),
      ),
    ]),
  )
  ignore(
    publisher.receive(@session.InboundStreamStarted(44, @session.Bidirectional)),
  )
  let joining_fetch = @protocol.encode_fetch({
    request_id: 9UL,
    fetch_type: @protocol.RelativeJoiningFetch(3UL, 2UL),
    parameters: [],
  }).unwrap()
  ignore(
    publisher.receive(@session.InboundStreamStarted(46, @session.Bidirectional)),
  )
  let update_id_as_join = @protocol.encode_fetch({
    request_id: 7UL,
    fetch_type: @protocol.RelativeJoiningFetch(5UL, 0UL),
    parameters: [],
  }).unwrap()
  let invalid_joining_id : @protocol.RequestErrorMessage = {
    code: 0x32UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.receive(@session.StreamData(46, update_id_as_join)),
    Ok([
      @session.SendFin(
        46,
        @protocol.encode_request_error(invalid_joining_id).unwrap(),
      ),
      @session.EmitNotice(@session.FetchRejected(7UL, 46, invalid_joining_id)),
    ]),
  )
  assert_eq(publisher.receive(@session.StreamData(44, joining_fetch)), Ok([]))
  let largest : @protocol.MessageParameter = {
    kind: 0x09UL,
    value: @protocol.Location(10UL, 3UL),
  }
  let response : @protocol.RequestOkMessage = {
    parameters: [],
    properties: b"",
  }
  assert_eq(
    publisher.accept_subscription_update(42, 5UL, response),
    Ok([
      @session.SendChunk(
        42,
        @protocol.encode_request_ok_for(
          { parameters: [largest], properties: b"", },
          @protocol.RequestOkContext::RequestUpdate,
        ).unwrap(),
      ),
      @session.EmitNotice(
        @session.SubscriptionUpdateApplied(3UL, 5UL, 42, [forward_parameter(1)]),
      ),
      @session.EmitNotice(
        @session.FetchRequested(
          9UL,
          44,
          track,
          { group: 8UL, object: 0UL, },
          { group: 10UL, object: 4UL, },
          [],
        ),
      ),
    ]),
  )
  assert_eq(
    publisher.accept_fetch(9UL, {
      end_of_track: 0,
      end_location: { group: 10UL, object: 4UL, },
      parameters: [],
      track_properties: b"",
    }),
    Ok([
      @session.SendChunk(
        44,
        @protocol.encode_fetch_ok({
          end_of_track: 0,
          end_location: { group: 10UL, object: 4UL, },
          parameters: [],
          track_properties: b"",
        }).unwrap(),
      ),
      @session.EmitNotice(@session.FetchAccepted(9UL, 44)),
      @session.OpenStream(@session.Unidirectional, @session.FetchData(9UL)),
    ]),
  )
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.FetchData(9UL), 48)),
    Ok([
      @session.SendChunk(
        48,
        @protocol.encode_fetch_header({ request_id: 9UL, }),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(
        42,
        request_update_bytes(11UL, [forward_parameter(0)]),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.SubscriptionUpdateRequested(
          3UL,
          11UL,
          42,
          [forward_parameter(0)],
          [forward_parameter(0)],
        ),
      ),
    ]),
  )
  assert_eq(
    publisher.accept_subscription_update(42, 11UL, update_ok()),
    Ok([
      @session.SendChunk(
        42,
        @protocol.encode_request_ok_for(
          update_ok(),
          @protocol.RequestOkContext::RequestUpdate,
        ).unwrap(),
      ),
      @session.EmitNotice(
        @session.SubscriptionUpdateApplied(3UL, 11UL, 42, [forward_parameter(0)]),
      ),
    ]),
  )
  ignore(
    publisher.receive(@session.InboundStreamStarted(50, @session.Bidirectional)),
  )
  let after_forward_off = @protocol.encode_fetch({
    request_id: 13UL,
    fetch_type: @protocol.RelativeJoiningFetch(3UL, 0UL),
    parameters: [],
  }).unwrap()
  let invalid_range : @protocol.RequestErrorMessage = {
    code: 0x10UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.receive(@session.StreamData(50, after_forward_off)),
    Ok([
      @session.SendFin(
        50,
        @protocol.encode_request_error(invalid_range).unwrap(),
      ),
      @session.EmitNotice(@session.FetchRejected(13UL, 50, invalid_range)),
    ]),
  )
  assert_eq(
    publisher.send_fetch_object(9UL, {
      group_id: 8UL,
      subgroup_id: Some(0UL),
      object_id: 0UL,
      publisher_priority: 0,
      datagram_forwarding: false,
      properties: b"",
      payload: b"historical",
    }),
    Ok([
      @session.SendChunk(
        48,
        @protocol.FetchDataEncoder::new(@protocol.Ascending)
        .encode(
          @protocol.FetchDataObject({
            group_id: 8UL,
            subgroup_id: Some(0UL),
            object_id: 0UL,
            publisher_priority: 0,
            datagram_forwarding: false,
            properties: b"",
            payload: b"historical",
          }),
        )
        .unwrap(),
      ),
    ]),
  )
}

///|
test "unknown, malformed, and post-cancel datagrams do not reach the application" {
  let subscriber = subscriber_started()
  assert_eq(
    subscriber.receive(
      @session.DatagramReceived(b"\xf0\x13\x2b\x3e\x29\x00\x00"),
    ),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(@session.DatagramReceived(b"\xf0\x13\x2b\x3e\x29\x01")),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      @session.DatagramReceived(
        @protocol.encode_object_datagram(update_datagram(99, 1, b"unknown")).unwrap(),
      ),
    ),
    Ok([]),
  )
  let canceled = subscriber_started()
  assert_eq(
    canceled.cancel_request(@session.RequestId(0)),
    Ok([
      @session.AbortSend(30, 1),
      @session.StopReceiving(30, 1),
      @session.EmitNotice(
        @session.SubscriptionEnded(0, 30, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    canceled.receive(
      @session.DatagramReceived(
        @protocol.encode_object_datagram(update_datagram(1, 1, b"late")).unwrap(),
      ),
    ),
    Ok([]),
  )
  let malformed = subscriber_started()
  assert_eq(
    malformed.receive(@session.DatagramReceived(b"\x0c\x01")),
    Ok([
      @session.CloseConnection(3, b""),
      @session.EmitNotice(@session.SessionEnded(@session.Local(3, b""))),
    ]),
  )
  assert_eq(malformed.receive(@session.DatagramReceived(b"\x02\x04")), Ok([]))
  assert_eq(malformed.receive(@session.DatagramReceived(b"invalid")), Ok([]))
  assert_eq(malformed.state(), @session.Closed)
  assert_eq(
    malformed.cancel_request(@session.RequestId(0)),
    Err(@session.SessionClosed),
  )
}

///|
test "publisher accepts incremental queued updates and preserves omitted parameters" {
  let publisher = publisher_started()
  let token = match
    publisher
    .open_object_stream(@session.StreamId(40), publisher_header(1, false))
    .unwrap() {
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
      @session.EmitNotice(
        @session.SubscriptionUpdateRequested(1, 3, 40, forward_off, forward_off),
      ),
      @session.EmitNotice(
        @session.SubscriptionUpdateRequested(1, 5, 40, priority, [
          forward_parameter(0),
          subscriber_priority_parameter(7),
        ]),
      ),
    ]),
  )
  assert_eq(
    publisher.publish_done(@session.StreamId(40), 3, b""),
    Err(@session.RequestUpdatePending),
  )
  assert_eq(
    publisher.accept_subscription_update(
      @session.StreamId(40),
      @session.RequestId(5),
      update_ok(),
    ),
    Err(@session.RequestUpdatePending),
  )
  assert_eq(
    publisher.accept_subscription_update(
      @session.StreamId(40),
      @session.RequestId(3),
      update_ok(),
    ),
    Ok([
      @session.SendChunk(
        40,
        @protocol.encode_request_ok_for(
          update_ok(),
          @protocol.RequestOkContext::RequestUpdate,
        ).unwrap(),
      ),
      @session.AbortSend(50, 0),
      @session.EmitNotice(
        @session.SubscriptionUpdateApplied(1, 3, 40, forward_off),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(@session.SendCompleted(50, @session.Reset)),
    Ok([@session.EmitNotice(@session.StreamSendFinished(50, @session.Reset))]),
  )
  assert_eq(
    publisher.open_object_stream(
      @session.StreamId(40),
      publisher_header(1, false),
    ),
    Err(@session.SubscriptionNotAccepted),
  )
  assert_eq(
    publisher.accept_subscription_update(
      @session.StreamId(40),
      @session.RequestId(5),
      update_ok(),
    ),
    Ok([
      @session.SendChunk(
        40,
        @protocol.encode_request_ok_for(
          update_ok(),
          @protocol.RequestOkContext::RequestUpdate,
        ).unwrap(),
      ),
      @session.EmitNotice(
        @session.SubscriptionUpdateApplied(1, 5, 40, [
          forward_parameter(0),
          subscriber_priority_parameter(7),
        ]),
      ),
    ]),
  )
  let forward_on = [forward_parameter(1)]
  assert_eq(
    publisher.receive(
      @session.StreamData(40, request_update_bytes(7, forward_on)),
    ),
    Ok([
      @session.EmitNotice(
        @session.SubscriptionUpdateRequested(1, 7, 40, forward_on, [
          forward_on[0],
          subscriber_priority_parameter(7),
        ]),
      ),
    ]),
  )
  assert_eq(
    publisher.accept_subscription_update(
      @session.StreamId(40),
      @session.RequestId(7),
      update_ok(),
    ),
    Ok([
      @session.SendChunk(
        40,
        @protocol.encode_request_ok_for(
          update_ok(),
          @protocol.RequestOkContext::RequestUpdate,
        ).unwrap(),
      ),
      @session.EmitNotice(
        @session.SubscriptionUpdateApplied(1, 7, 40, [
          forward_parameter(1),
          subscriber_priority_parameter(7),
        ]),
      ),
    ]),
  )
  assert_eq(
    publisher.open_object_stream(
      @session.StreamId(40),
      publisher_header(1, false),
    ),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Object(1))]),
  )
}

///|
test "duplicate peer update request ID closes the session once" {
  let publisher = publisher_started()
  let update = request_update_bytes(3, [forward_parameter(0)])
  assert_eq(
    publisher.receive(@session.StreamData(40, update)),
    Ok([
      @session.EmitNotice(
        @session.SubscriptionUpdateRequested(1, 3, 40, [forward_parameter(0)], [
          forward_parameter(0),
        ]),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(@session.StreamData(40, update)),
    Ok([
      @session.CloseConnection(4, b""),
      @session.EmitNotice(@session.SessionEnded(@session.Local(4, b""))),
    ]),
  )
  assert_eq(publisher.receive(@session.StreamData(40, update)), Ok([]))
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
      @session.EmitNotice(
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
    publisher
    .open_object_stream(@session.StreamId(40), publisher_header(1, false))
    .unwrap() {
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
    publisher.reject_subscription_update(
      @session.StreamId(40),
      @session.RequestId(3),
      error,
    ),
    Ok([
      @session.SendChunk(40, @protocol.encode_request_error(error).unwrap()),
      @session.AbortSend(50, 0),
    ]),
  )
  let done : @protocol.PublishDoneMessage = {
    code: 8,
    stream_count: 1,
    reason: b"",
  }
  assert_eq(
    publisher.receive(@session.SendCompleted(50, @session.Reset)),
    Ok([
      @session.EmitNotice(@session.StreamSendFinished(50, @session.Reset)),
      @session.SendFin(40, @protocol.encode_publish_done(done).unwrap()),
      @session.EmitNotice(@session.PublishDone(1, done)),
    ]),
  )
  assert_eq(
    publisher.receive(@session.StreamData(40, request_update_bytes(5, []))),
    Ok([]),
  )
}

///|
test "cancellation supersedes a pending UPDATE_FAILED PUBLISH_DONE without aborting twice" {
  let publisher = publisher_started()
  let token = match
    publisher
    .open_object_stream(@session.StreamId(40), publisher_header(1, false))
    .unwrap() {
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
    publisher.reject_subscription_update(
      @session.StreamId(40),
      @session.RequestId(3),
      error,
    ),
    Ok([
      @session.SendChunk(40, @protocol.encode_request_error(error).unwrap()),
      @session.AbortSend(50, 0),
    ]),
  )
  assert_eq(
    publisher.cancel_request(1UL),
    Ok([
      @session.AbortSend(40, 1UL),
      @session.StopReceiving(40, 1UL),
      @session.EmitNotice(
        @session.SubscriptionEnded(1UL, 40, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(@session.SendCompleted(50, @session.Reset)),
    Ok([@session.EmitNotice(@session.StreamSendFinished(50, @session.Reset))]),
  )
}

///|
test "cancellation can supersede a pending UPDATE_FAILED PUBLISH_DONE on local PUBLISH" {
  let publisher = publisher_announced()
  let token = match
    publisher
    .open_object_stream(@session.StreamId(20), publisher_header(1, false))
    .unwrap() {
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
    Ok([
      @session.EmitNotice(
        @session.SubscriptionUpdateRequested(0, 3, 20, [], []),
      ),
    ]),
  )
  let error : @protocol.RequestErrorMessage = {
    code: 1,
    retry_interval: 0,
    reason: b"unsupported",
    redirect: b"",
  }
  assert_eq(
    publisher.reject_subscription_update(
      @session.StreamId(20),
      @session.RequestId(3),
      error,
    ),
    Ok([
      @session.SendChunk(20, @protocol.encode_request_error(error).unwrap()),
      @session.AbortSend(50, 0),
    ]),
  )
  assert_eq(
    publisher.cancel_request(0UL),
    Ok([
      @session.AbortSend(20, 1UL),
      @session.StopReceiving(20, 1UL),
      @session.EmitNotice(
        @session.SubscriptionEnded(0UL, 20, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(publisher.cancel_request(0UL), Ok([]))
  assert_eq(
    publisher.receive(@session.SendCompleted(50, @session.Reset)),
    Ok([@session.EmitNotice(@session.StreamSendFinished(50, @session.Reset))]),
  )
}

///|
test "failed local update requires UPDATE_FAILED PUBLISH_DONE" {
  let subscriber = subscriber_started()
  let (update_id, _) = subscriber
    .update_subscription(@session.RequestId(0), [forward_parameter(0)])
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
      @session.EmitNotice(
        @session.SubscriptionUpdateRejected(0, update_id, error),
      ),
      @session.SendFin(30, b""),
      @session.EmitNotice(@session.PublishDone(0, done)),
    ]),
  )
  assert_eq(
    subscriber.update_subscription(@session.RequestId(0), []),
    Err(@session.SubscriptionNotAccepted),
  )
}

///|
test "UPDATE_FAILED PUBLISH_DONE counts an Object stream opened after rejection" {
  let publisher = publisher_started()
  let token = match
    publisher
    .open_object_stream(@session.StreamId(40), publisher_header(1, false))
    .unwrap() {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("expected pending Object stream")
  }
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
    publisher.reject_subscription_update(
      @session.StreamId(40),
      @session.RequestId(3),
      error,
    ),
    Ok([@session.SendChunk(40, @protocol.encode_request_error(error).unwrap())]),
  )
  let done : @protocol.PublishDoneMessage = {
    code: 8,
    stream_count: 1,
    reason: b"",
  }
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(token), 50)),
    Ok([
      @session.AbortSend(50, 0UL),
      @session.SendFin(40, @protocol.encode_publish_done(done).unwrap()),
      @session.EmitNotice(@session.PublishDone(1UL, done)),
    ]),
  )
}

///|
test "Object stream opened after forwarding is paused counts toward later PUBLISH_DONE" {
  let publisher = publisher_started()
  let token = match
    publisher
    .open_object_stream(@session.StreamId(40), publisher_header(1, false))
    .unwrap() {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("expected pending Object stream")
  }
  let pause : Array[@protocol.MessageParameter] = [
    { kind: 0x10UL, value: @protocol.Byte(0), },
  ]
  ignore(
    publisher
    .receive(@session.StreamData(40, request_update_bytes(3, pause)))
    .unwrap(),
  )
  ignore(
    publisher
    .accept_subscription_update(
      @session.StreamId(40),
      @session.RequestId(3),
      update_ok(),
    )
    .unwrap(),
  )
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(token), 50)),
    Ok([@session.AbortSend(50, 0UL)]),
  )
  let done : @protocol.PublishDoneMessage = {
    code: 3,
    stream_count: 1,
    reason: b"",
  }
  assert_eq(
    publisher.publish_done(@session.StreamId(40), done.code, done.reason),
    Ok([
      @session.SendFin(40, @protocol.encode_publish_done(done).unwrap()),
      @session.EmitNotice(@session.PublishDone(1UL, done)),
    ]),
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
      @session.EmitNotice(
        @session.SubscriptionUpdateRequested(0, 1, 20, forward_off, forward_off),
      ),
    ]),
  )
  assert_eq(
    publisher.accept_subscription_update(
      @session.StreamId(20),
      @session.RequestId(1),
      update_ok(),
    ),
    Ok([
      @session.SendChunk(
        20,
        @protocol.encode_request_ok_for(
          update_ok(),
          @protocol.RequestOkContext::RequestUpdate,
        ).unwrap(),
      ),
      @session.EmitNotice(
        @session.SubscriptionUpdateApplied(0, 1, 20, forward_off),
      ),
    ]),
  )
  let inbound = publisher_started()
  assert_eq(inbound.receive(@session.InboundStreamFinished(40)), Ok([]))
  assert_eq(
    inbound.open_object_stream(
      @session.StreamId(40),
      publisher_header(1, false),
    ),
    Ok([@session.OpenStream(@session.Unidirectional, @session.Object(0))]),
  )
  assert_eq(
    inbound.receive(
      @session.StreamData(40, request_update_bytes(3, forward_off)),
    ),
    Ok([]),
  )
}

///|
test "datagrams route across aliases without ordered object ID state" {
  let subscriber = subscriber_started()
  let track_b : @protocol.TrackName = {
    namespace_fields: [b"demo"],
    name: b"audio",
  }
  let (request_b, actions) = subscriber.subscribe_track(track_b).unwrap()
  assert_eq(request_b, @session.RequestId(2UL))
  assert_eq(actions, [
    @session.OpenStream(@session.Bidirectional, @session.Request(2)),
  ])
  ignore(
    subscriber.receive(@session.StreamOpened(@session.Request(2), 31)).unwrap(),
  )
  assert_eq(
    subscriber.receive(
      @session.StreamData(
        31,
        @protocol.encode_subscribe_ok({
          track_alias: 2,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([@session.EmitNotice(@session.SubscribeEstablished(2, 2))]),
  )

  let late_a = update_datagram(1, 9, b"a9")
  let early_b = update_datagram(2, 0, b"b0")
  let reordered_a = update_datagram(1, 3, b"a3")
  assert_eq(
    subscriber.receive(
      @session.DatagramReceived(
        @protocol.encode_object_datagram(late_a).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.ObjectDatagramReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          late_a,
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.DatagramReceived(
        @protocol.encode_object_datagram(early_b).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(@session.ObjectDatagramReceived(2, track_b, early_b)),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.DatagramReceived(
        @protocol.encode_object_datagram(late_a).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.ObjectDatagramReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          late_a,
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      @session.DatagramReceived(
        @protocol.encode_object_datagram(reordered_a).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.ObjectDatagramReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          reordered_a,
        ),
      ),
    ]),
  )
}

///|
test "datagram object notices preserve forwarding mode, status, properties, and priority" {
  let subscriber = subscriber_started()
  let normal_with_properties : @protocol.ObjectDatagram = {
    track_alias: 1,
    group_id: 7,
    object_id: 4,
    publisher_priority: None,
    end_of_group: false,
    properties: b"\x02\x2a",
    status: @protocol.Normal,
    payload: b"payload",
  }
  let group_end : @protocol.ObjectDatagram = {
    track_alias: 1,
    group_id: 7,
    object_id: 5,
    publisher_priority: Some(9),
    end_of_group: true,
    properties: b"",
    status: @protocol.Normal,
    payload: b"last in group",
  }
  let end_of_group : @protocol.ObjectDatagram = {
    track_alias: 1,
    group_id: 7,
    object_id: 6,
    publisher_priority: None,
    end_of_group: false,
    properties: b"",
    status: @protocol.EndOfGroup,
    payload: b"",
  }
  let end_of_track : @protocol.ObjectDatagram = {
    track_alias: 1,
    group_id: 7,
    object_id: 0,
    publisher_priority: None,
    end_of_group: false,
    properties: b"",
    status: @protocol.EndOfTrack,
    payload: b"",
  }
  for
    datagram in [normal_with_properties, group_end, end_of_group, end_of_track] {
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
  }
}

///|
test "datagrams do not count toward PUBLISH_DONE and can arrive while streams drain" {
  let datagram_only = subscriber_started()
  let object = update_datagram(1, 4, b"datagram")
  assert_eq(
    datagram_only.receive(
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
  let empty_done : @protocol.PublishDoneMessage = {
    code: 3,
    stream_count: 0,
    reason: b"",
  }
  assert_eq(
    datagram_only.receive(
      @session.StreamData(
        30,
        @protocol.encode_publish_done(empty_done).unwrap(),
      ),
    ),
    Ok([
      @session.SendFin(30, b""),
      @session.EmitNotice(@session.PublishDone(0, empty_done)),
    ]),
  )
  assert_eq(
    datagram_only.receive(
      @session.DatagramReceived(
        @protocol.encode_object_datagram(object).unwrap(),
      ),
    ),
    Ok([]),
  )

  let mixed = subscriber_started()
  let header : @protocol.SubgroupHeader = {
    track_alias: 1,
    group_id: 8,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: false,
  }
  assert_eq(
    mixed.receive(@session.InboundStreamStarted(41, @session.Unidirectional)),
    Ok([]),
  )
  assert_eq(
    mixed.receive(
      @session.StreamData(
        41,
        @protocol.encode_subgroup_stream(header, [
          {
            object_id: 0,
            status: @protocol.Normal,
            properties: b"",
            payload: b"stream",
          },
        ]).unwrap(),
      ),
    ),
    Ok([
      @session.EmitNotice(
        @session.ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          header,
          [
            {
              object_id: 0,
              status: @protocol.Normal,
              properties: b"",
              payload: b"stream",
            },
          ],
        ),
      ),
    ]),
  )
  let mixed_done : @protocol.PublishDoneMessage = {
    code: 3,
    stream_count: 1,
    reason: b"",
  }
  assert_eq(
    mixed.receive(
      @session.StreamData(
        30,
        @protocol.encode_publish_done(mixed_done).unwrap(),
      ),
    ),
    Ok([@session.SendFin(30, b"")]),
  )
  assert_eq(
    mixed.receive(
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
  assert_eq(
    mixed.receive(@session.InboundStreamFinished(41)),
    Ok([@session.EmitNotice(@session.PublishDone(0, mixed_done))]),
  )
}

///|
test "object datagram sending reports local capability and maximum length" {
  let publisher = publisher_started()
  let datagram = update_datagram(1, 2, b"payload")
  let encoded = @protocol.encode_object_datagram(datagram).unwrap()
  assert_eq(
    publisher.receive(
      @session.DatagramSendCapabilityChanged(enabled=false, max_length=0),
    ),
    Ok([]),
  )
  assert_eq(
    publisher.send_object_datagram(@session.StreamId(40), datagram),
    Err(@session.DatagramSendUnavailable),
  )
  assert_eq(
    publisher.receive(
      @session.DatagramSendCapabilityChanged(
        enabled=true,
        max_length=encoded.length(),
      ),
    ),
    Ok([]),
  )
  assert_eq(
    publisher.send_object_datagram(@session.StreamId(40), datagram),
    Ok(
      (
        0UL,
        [
          @session.SendDatagram(0UL, encoded),
          @session.EmitNotice(@session.DatagramQueued(0UL)),
        ],
      ),
    ),
  )
  assert_eq(
    publisher.receive(
      @session.DatagramSendCapabilityChanged(
        enabled=true,
        max_length=encoded.length() - 1,
      ),
    ),
    Ok([]),
  )
  assert_eq(
    publisher.send_object_datagram(@session.StreamId(40), datagram),
    Err(@session.DatagramTooLarge(encoded.length(), encoded.length() - 1)),
  )
}

///|
test "datagram send failure reports its token without closing the session" {
  let publisher = publisher_started()
  let datagram = update_datagram(1, 2, b"payload")
  let encoded = @protocol.encode_object_datagram(datagram).unwrap()
  assert_eq(
    publisher.receive(
      @session.DatagramSendCapabilityChanged(
        enabled=true,
        max_length=encoded.length(),
      ),
    ),
    Ok([]),
  )
  assert_eq(publisher.max_datagram_length(), encoded.length())
  let (token, actions) = publisher
    .send_object_datagram(@session.StreamId(40), datagram)
    .unwrap()
  assert_eq(token, 0UL)
  assert_eq(actions, [
    @session.SendDatagram(0UL, encoded),
    @session.EmitNotice(@session.DatagramQueued(0UL)),
  ])
  assert_eq(
    publisher.receive(@session.DatagramSendFailed(token, -4L)),
    Ok([@session.EmitNotice(@session.DatagramLocalWriteFailed(token, -4L))]),
  )
  assert_eq(publisher.state(), @session.Active)
  assert_eq(
    publisher.send_object_datagram(@session.StreamId(40), datagram).unwrap().0,
    1UL,
  )
}

///|
test "datagram-only publisher completion reports zero opened Object streams" {
  let publisher = publisher_started()
  let datagram = update_datagram(1, 2, b"payload")
  assert_eq(
    publisher.send_object_datagram(@session.StreamId(40), datagram),
    Ok(
      (
        0UL,
        [
          @session.SendDatagram(
            0UL,
            @protocol.encode_object_datagram(datagram).unwrap(),
          ),
          @session.EmitNotice(@session.DatagramQueued(0UL)),
        ],
      ),
    ),
  )
  let done : @protocol.PublishDoneMessage = {
    code: 3,
    stream_count: 0,
    reason: b"",
  }
  assert_eq(
    publisher.publish_done(@session.StreamId(40), done.code, done.reason),
    Ok([
      @session.SendFin(40, @protocol.encode_publish_done(done).unwrap()),
      @session.EmitNotice(@session.PublishDone(1, done)),
    ]),
  )
}
