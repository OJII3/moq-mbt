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
  { kind: 0x20, value: Byte(value), }
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
    status: Normal,
    payload,
  }
}

///|
test "subscriber keeps its request stream open and matches one update response" {
  let subscriber = subscriber_started()
  let parameters = [forward_parameter(0)]
  let (update_id, actions) = subscriber
    .update_subscription(RequestId(0), parameters)
    .unwrap()
  assert_eq(update_id, RequestId(2UL))
  assert_eq(actions, [
    SendChunk(30, request_update_bytes(update_id, parameters)),
  ])
  assert_eq(
    subscriber.update_subscription(RequestId(0), [
      subscriber_priority_parameter(4),
    ]),
    Err(RequestUpdatePending),
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
    subscriber
    .update_subscription(RequestId(0), [subscriber_priority_parameter(4)])
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
  assert_eq(off_id, RequestId(2UL))
  ignore(
    subscriber.receive(
      StreamData(
        30,
        @protocol.encode_request_ok_for(update_ok(), RequestUpdate).unwrap(),
      ),
    ),
  )

  let (on_id, _) = subscriber
    .update_subscription(0UL, [forward_parameter(1)])
    .unwrap()
  assert_eq(on_id, RequestId(4UL))
  let (fetch_id, actions) = subscriber
    .fetch_joining_relative(subscription_request_id=0UL, groups_back=2UL, parameters=[])
    .unwrap()
  assert_eq(fetch_id, RequestId(6UL))
  assert_eq(actions, [OpenStream(Bidirectional, Request(6UL))])
  let fetch = @protocol.FetchMessage::{
    request_id: fetch_id.to_uint64(),
    fetch_type: RelativeJoiningFetch(0UL, 2UL),
    parameters: [],
  }
  assert_eq(
    subscriber.receive(StreamOpened(Request(fetch_id), 32)),
    Ok([SendChunk(32, @protocol.encode_fetch(fetch).unwrap())]),
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
    subscriber.receive(DatagramReceived(datagram_bytes)),
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
  let subgroup = @protocol.SubgroupHeader::{
    track_alias: 1,
    group_id: 1,
    subgroup_id_mode: Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: false,
  }
  assert_eq(subscriber.receive(InboundStreamStarted(9, Unidirectional)), Ok([]))
  assert_eq(
    subscriber.receive(
      StreamData(
        9,
        @protocol.encode_subgroup_stream(subgroup, [
          { object_id: 1, status: Normal, properties: b"", payload: b"stream", },
        ]).unwrap(),
      ),
    ),
    Ok([
      EmitNotice(
        ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          subgroup,
          [
            {
              object_id: 1,
              status: Normal,
              properties: b"",
              payload: b"stream",
            },
          ],
        ),
      ),
    ]),
  )

  let (disable_id, _) = subscriber
    .update_subscription(RequestId(0), forward_off)
    .unwrap()
  assert_eq(disable_id, RequestId(2UL))
  assert_eq(
    subscriber.receive(
      StreamData(
        30,
        @protocol.encode_request_ok_for(update_ok(), RequestUpdate).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscriptionUpdateAccepted(0, 2, update_ok()))]),
  )
  assert_eq(subscriber.receive(DatagramReceived(datagram_bytes)), Ok([]))
  assert_eq(
    subscriber.receive(InboundStreamStarted(13, Unidirectional)),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      StreamData(
        13,
        @protocol.encode_subgroup_stream(subgroup, [
          {
            object_id: 2,
            status: Normal,
            properties: b"",
            payload: b"dropped",
          },
        ]).unwrap(),
      ),
    ),
    Ok([]),
  )

  let (enable_id, _) = subscriber
    .update_subscription(RequestId(0), forward_on)
    .unwrap()
  assert_eq(enable_id, RequestId(4UL))
  ignore(
    subscriber
    .receive(
      StreamData(
        30,
        @protocol.encode_request_ok_for(update_ok(), RequestUpdate).unwrap(),
      ),
    )
    .unwrap(),
  )
  assert_eq(
    subscriber.receive(DatagramReceived(datagram_bytes)),
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
}

///|
test "publisher datagram send obeys the same FORWARD update and alias checks" {
  let publisher = publisher_started()
  let datagram = update_datagram(1, 2, b"payload")
  assert_eq(
    publisher.send_object_datagram(StreamId(20), datagram),
    Ok(
      (
        0UL,
        [
          SendDatagram(0UL, @protocol.encode_object_datagram(datagram).unwrap()),
          EmitNotice(DatagramQueued(0UL)),
        ],
      ),
    ),
  )
  let forward_off = [forward_parameter(0)]
  assert_eq(
    publisher.send_object_datagram(StreamId(40), datagram),
    Ok(
      (
        1UL,
        [
          SendDatagram(1UL, @protocol.encode_object_datagram(datagram).unwrap()),
          EmitNotice(DatagramQueued(1UL)),
        ],
      ),
    ),
  )
  assert_eq(
    publisher.receive(StreamData(40, request_update_bytes(3, forward_off))),
    Ok([
      EmitNotice(
        SubscriptionUpdateRequested(1, 3, 40, forward_off, forward_off),
      ),
    ]),
  )
  ignore(
    publisher
    .accept_subscription_update(StreamId(40), RequestId(3), update_ok())
    .unwrap(),
  )
  assert_eq(
    publisher.send_object_datagram(StreamId(40), datagram),
    Err(SubscriptionNotAccepted),
  )
  let forward_on = [forward_parameter(1)]
  assert_eq(
    publisher.receive(StreamData(40, request_update_bytes(5, forward_on))),
    Ok([
      EmitNotice(SubscriptionUpdateRequested(1, 5, 40, forward_on, forward_on)),
    ]),
  )
  ignore(
    publisher
    .accept_subscription_update(StreamId(40), RequestId(5), update_ok())
    .unwrap(),
  )
  assert_eq(
    publisher.send_object_datagram(StreamId(40), datagram),
    Ok(
      (
        2UL,
        [
          SendDatagram(2UL, @protocol.encode_object_datagram(datagram).unwrap()),
          EmitNotice(DatagramQueued(2UL)),
        ],
      ),
    ),
  )
  assert_eq(
    publisher.send_object_datagram(
      StreamId(40),
      update_datagram(99, 2, b"payload"),
    ),
    Err(Track(AliasMismatch)),
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
  ignore(publisher.receive(InboundStreamStarted(40, Bidirectional)))
  ignore(publisher.receive(StreamData(40, first_subscribe)))
  ignore(publisher.accept_subscription(StreamId(40)).unwrap())
  let header = @protocol.SubgroupHeader::{
    track_alias: 1UL,
    group_id: 10UL,
    subgroup_id_mode: Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: false,
  }
  ignore(publisher.open_object_stream(StreamId(40), header).unwrap())
  ignore(publisher.receive(StreamOpened(Object(0UL), 30)))
  ignore(
    publisher
    .send_object(StreamId(30), publisher_frame(3UL, b"object"))
    .unwrap(),
  )

  let second_subscribe = @protocol.encode_subscribe({
    request_id: 3UL,
    track,
    parameters: [forward_parameter(0)],
  }).unwrap()
  ignore(publisher.receive(InboundStreamStarted(42, Bidirectional)))
  ignore(publisher.receive(StreamData(42, second_subscribe)))
  ignore(publisher.accept_subscription(StreamId(42)).unwrap())
  let update = @protocol.encode_request_update({
    request_id: 5UL,
    parameters: [forward_parameter(1)],
  }).unwrap()
  assert_eq(
    publisher.receive(StreamData(42, update)),
    Ok([
      EmitNotice(
        SubscriptionUpdateRequested(3UL, 5UL, 42, [forward_parameter(1)], [
          forward_parameter(1),
        ]),
      ),
    ]),
  )
  ignore(publisher.receive(InboundStreamStarted(44, Bidirectional)))
  let joining_fetch = @protocol.encode_fetch({
    request_id: 9UL,
    fetch_type: RelativeJoiningFetch(3UL, 2UL),
    parameters: [],
  }).unwrap()
  ignore(publisher.receive(InboundStreamStarted(46, Bidirectional)))
  let update_id_as_join = @protocol.encode_fetch({
    request_id: 7UL,
    fetch_type: RelativeJoiningFetch(5UL, 0UL),
    parameters: [],
  }).unwrap()
  let invalid_joining_id = @protocol.RequestErrorMessage::{
    code: 0x32UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.receive(StreamData(46, update_id_as_join)),
    Ok([
      SendFin(46, @protocol.encode_request_error(invalid_joining_id).unwrap()),
      EmitNotice(FetchRejected(7UL, 46, invalid_joining_id)),
    ]),
  )
  assert_eq(publisher.receive(StreamData(44, joining_fetch)), Ok([]))
  let largest = @protocol.MessageParameter::{
    kind: 0x09UL,
    value: Location(10UL, 3UL),
  }
  let response = @protocol.RequestOkMessage::{
    parameters: [],
    properties: b"",
  }
  assert_eq(
    publisher.accept_subscription_update(42, 5UL, response),
    Ok([
      SendChunk(
        42,
        @protocol.encode_request_ok_for(
          { parameters: [largest], properties: b"", },
          RequestUpdate,
        ).unwrap(),
      ),
      EmitNotice(
        SubscriptionUpdateApplied(3UL, 5UL, 42, [forward_parameter(1)]),
      ),
      EmitNotice(
        FetchRequested(
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
      SendChunk(
        44,
        @protocol.encode_fetch_ok({
          end_of_track: 0,
          end_location: { group: 10UL, object: 4UL, },
          parameters: [],
          track_properties: b"",
        }).unwrap(),
      ),
      EmitNotice(FetchAccepted(9UL, 44)),
      OpenStream(Unidirectional, FetchData(9UL)),
    ]),
  )
  assert_eq(
    publisher.receive(StreamOpened(FetchData(9UL), 48)),
    Ok([SendChunk(48, @protocol.encode_fetch_header({ request_id: 9UL, }))]),
  )
  assert_eq(
    publisher.receive(
      StreamData(42, request_update_bytes(11UL, [forward_parameter(0)])),
    ),
    Ok([
      EmitNotice(
        SubscriptionUpdateRequested(3UL, 11UL, 42, [forward_parameter(0)], [
          forward_parameter(0),
        ]),
      ),
    ]),
  )
  assert_eq(
    publisher.accept_subscription_update(42, 11UL, update_ok()),
    Ok([
      SendChunk(
        42,
        @protocol.encode_request_ok_for(update_ok(), RequestUpdate).unwrap(),
      ),
      EmitNotice(
        SubscriptionUpdateApplied(3UL, 11UL, 42, [forward_parameter(0)]),
      ),
    ]),
  )
  ignore(publisher.receive(InboundStreamStarted(50, Bidirectional)))
  let after_forward_off = @protocol.encode_fetch({
    request_id: 13UL,
    fetch_type: RelativeJoiningFetch(3UL, 0UL),
    parameters: [],
  }).unwrap()
  let invalid_range = @protocol.RequestErrorMessage::{
    code: 0x10UL,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  assert_eq(
    publisher.receive(StreamData(50, after_forward_off)),
    Ok([
      SendFin(50, @protocol.encode_request_error(invalid_range).unwrap()),
      EmitNotice(FetchRejected(13UL, 50, invalid_range)),
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
      SendChunk(
        48,
        @protocol.FetchDataEncoder::new(Ascending)
        .encode(
          FetchDataObject({
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
    subscriber.receive(DatagramReceived(b"\xf0\x13\x2b\x3e\x29\x00\x00")),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(DatagramReceived(b"\xf0\x13\x2b\x3e\x29\x01")),
    Ok([]),
  )
  assert_eq(
    subscriber.receive(
      DatagramReceived(
        @protocol.encode_object_datagram(update_datagram(99, 1, b"unknown")).unwrap(),
      ),
    ),
    Ok([]),
  )
  let canceled = subscriber_started()
  assert_eq(
    canceled.cancel_request(RequestId(0)),
    Ok([
      AbortSend(30, 1),
      StopReceiving(30, 1),
      EmitNotice(
        SubscriptionEnded(0, 30, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    canceled.receive(
      DatagramReceived(
        @protocol.encode_object_datagram(update_datagram(1, 1, b"late")).unwrap(),
      ),
    ),
    Ok([]),
  )
  let malformed = subscriber_started()
  assert_eq(
    malformed.receive(DatagramReceived(b"\x0c\x01")),
    Ok([CloseConnection(3, b""), EmitNotice(SessionEnded(Local(3, b"")))]),
  )
  assert_eq(malformed.receive(DatagramReceived(b"\x02\x04")), Ok([]))
  assert_eq(malformed.receive(DatagramReceived(b"invalid")), Ok([]))
  assert_eq(malformed.state(), Closed)
  assert_eq(malformed.cancel_request(RequestId(0)), Err(SessionClosed))
}

///|
test "publisher accepts incremental queued updates and preserves omitted parameters" {
  let publisher = publisher_started()
  let token = match
    publisher
    .open_object_stream(StreamId(40), publisher_header(1, false))
    .unwrap() {
    [OpenStream(Unidirectional, Object(token))] => token
    _ => fail("expected object stream")
  }
  ignore(publisher.receive(StreamOpened(Object(token), 50)).unwrap())
  let forward_off = [forward_parameter(0)]
  let priority = [subscriber_priority_parameter(7)]
  let update_a = request_update_bytes(3, forward_off)
  let update_b = request_update_bytes(5, priority)
  assert_eq(
    publisher.receive(StreamData(40, update_a + update_b)),
    Ok([
      EmitNotice(
        SubscriptionUpdateRequested(1, 3, 40, forward_off, forward_off),
      ),
      EmitNotice(
        SubscriptionUpdateRequested(1, 5, 40, priority, [
          forward_parameter(0),
          subscriber_priority_parameter(7),
        ]),
      ),
    ]),
  )
  assert_eq(
    publisher.publish_done(StreamId(40), 3, b""),
    Err(RequestUpdatePending),
  )
  assert_eq(
    publisher.accept_subscription_update(
      StreamId(40),
      RequestId(5),
      update_ok(),
    ),
    Err(RequestUpdatePending),
  )
  assert_eq(
    publisher.accept_subscription_update(
      StreamId(40),
      RequestId(3),
      update_ok(),
    ),
    Ok([
      SendChunk(
        40,
        @protocol.encode_request_ok_for(update_ok(), RequestUpdate).unwrap(),
      ),
      AbortSend(50, 0),
      EmitNotice(SubscriptionUpdateApplied(1, 3, 40, forward_off)),
    ]),
  )
  assert_eq(
    publisher.receive(SendCompleted(50, Reset)),
    Ok([EmitNotice(StreamSendFinished(50, Reset))]),
  )
  assert_eq(
    publisher.open_object_stream(StreamId(40), publisher_header(1, false)),
    Err(SubscriptionNotAccepted),
  )
  assert_eq(
    publisher.accept_subscription_update(
      StreamId(40),
      RequestId(5),
      update_ok(),
    ),
    Ok([
      SendChunk(
        40,
        @protocol.encode_request_ok_for(update_ok(), RequestUpdate).unwrap(),
      ),
      EmitNotice(
        SubscriptionUpdateApplied(1, 5, 40, [
          forward_parameter(0),
          subscriber_priority_parameter(7),
        ]),
      ),
    ]),
  )
  let forward_on = [forward_parameter(1)]
  assert_eq(
    publisher.receive(StreamData(40, request_update_bytes(7, forward_on))),
    Ok([
      EmitNotice(
        SubscriptionUpdateRequested(1, 7, 40, forward_on, [
          forward_on[0],
          subscriber_priority_parameter(7),
        ]),
      ),
    ]),
  )
  assert_eq(
    publisher.accept_subscription_update(
      StreamId(40),
      RequestId(7),
      update_ok(),
    ),
    Ok([
      SendChunk(
        40,
        @protocol.encode_request_ok_for(update_ok(), RequestUpdate).unwrap(),
      ),
      EmitNotice(
        SubscriptionUpdateApplied(1, 7, 40, [
          forward_parameter(1),
          subscriber_priority_parameter(7),
        ]),
      ),
    ]),
  )
  assert_eq(
    publisher.open_object_stream(StreamId(40), publisher_header(1, false)),
    Ok([OpenStream(Unidirectional, Object(1))]),
  )
}

///|
test "duplicate peer update request ID closes the session once" {
  let publisher = publisher_started()
  let update = request_update_bytes(3, [forward_parameter(0)])
  assert_eq(
    publisher.receive(StreamData(40, update)),
    Ok([
      EmitNotice(
        SubscriptionUpdateRequested(1, 3, 40, [forward_parameter(0)], [
          forward_parameter(0),
        ]),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(StreamData(40, update)),
    Ok([CloseConnection(4, b""), EmitNotice(SessionEnded(Local(4, b"")))]),
  )
  assert_eq(publisher.receive(StreamData(40, update)), Ok([]))
}

///|
test "subscriber parses every split of a peer update on the established stream" {
  let wire = request_update_bytes(3, [forward_parameter(0)])
  for split = 0; split < wire.length(); split = split + 1 {
    let publisher = publisher_started()
    let first = publisher
      .receive(StreamData(40, wire[:split].to_owned()))
      .unwrap()
    let second = publisher
      .receive(StreamData(40, wire[split:].to_owned()))
      .unwrap()
    assert_eq(first + second, [
      EmitNotice(
        SubscriptionUpdateRequested(1, 3, 40, [forward_parameter(0)], [
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
    .open_object_stream(StreamId(40), publisher_header(1, false))
    .unwrap() {
    [OpenStream(Unidirectional, Object(token))] => token
    _ => fail("expected object stream")
  }
  ignore(publisher.receive(StreamOpened(Object(token), 50)).unwrap())
  let parameters : Array[@protocol.MessageParameter] = []
  ignore(
    publisher
    .receive(StreamData(40, request_update_bytes(3, parameters)))
    .unwrap(),
  )
  let error = @protocol.RequestErrorMessage::{
    code: 1,
    retry_interval: 0,
    reason: b"unsupported",
    redirect: b"",
  }
  assert_eq(
    publisher.reject_subscription_update(StreamId(40), RequestId(3), error),
    Ok([
      SendChunk(40, @protocol.encode_request_error(error).unwrap()),
      AbortSend(50, 0),
    ]),
  )
  let done = @protocol.PublishDoneMessage::{
    code: 8,
    stream_count: 1,
    reason: b"",
  }
  assert_eq(
    publisher.receive(SendCompleted(50, Reset)),
    Ok([
      EmitNotice(StreamSendFinished(50, Reset)),
      SendFin(40, @protocol.encode_publish_done(done).unwrap()),
      EmitNotice(PublishDone(1, done)),
    ]),
  )
  assert_eq(
    publisher.receive(StreamData(40, request_update_bytes(5, []))),
    Ok([]),
  )
}

///|
test "cancellation supersedes a pending UPDATE_FAILED PUBLISH_DONE without aborting twice" {
  let publisher = publisher_started()
  let token = match
    publisher
    .open_object_stream(StreamId(40), publisher_header(1, false))
    .unwrap() {
    [OpenStream(Unidirectional, Object(token))] => token
    _ => fail("expected object stream")
  }
  ignore(publisher.receive(StreamOpened(Object(token), 50)).unwrap())
  ignore(
    publisher.receive(StreamData(40, request_update_bytes(3, []))).unwrap(),
  )
  let error = @protocol.RequestErrorMessage::{
    code: 1,
    retry_interval: 0,
    reason: b"unsupported",
    redirect: b"",
  }
  assert_eq(
    publisher.reject_subscription_update(StreamId(40), RequestId(3), error),
    Ok([
      SendChunk(40, @protocol.encode_request_error(error).unwrap()),
      AbortSend(50, 0),
    ]),
  )
  assert_eq(
    publisher.cancel_request(1UL),
    Ok([
      AbortSend(40, 1UL),
      StopReceiving(40, 1UL),
      EmitNotice(
        SubscriptionEnded(1UL, 40, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(
    publisher.receive(SendCompleted(50, Reset)),
    Ok([EmitNotice(StreamSendFinished(50, Reset))]),
  )
}

///|
test "cancellation can supersede a pending UPDATE_FAILED PUBLISH_DONE on local PUBLISH" {
  let publisher = publisher_announced()
  let token = match
    publisher
    .open_object_stream(StreamId(20), publisher_header(1, false))
    .unwrap() {
    [OpenStream(Unidirectional, Object(token))] => token
    _ => fail("expected object stream")
  }
  ignore(publisher.receive(StreamOpened(Object(token), 50)).unwrap())
  assert_eq(
    publisher.receive(StreamData(20, request_update_bytes(3, []))),
    Ok([EmitNotice(SubscriptionUpdateRequested(0, 3, 20, [], []))]),
  )
  let error = @protocol.RequestErrorMessage::{
    code: 1,
    retry_interval: 0,
    reason: b"unsupported",
    redirect: b"",
  }
  assert_eq(
    publisher.reject_subscription_update(StreamId(20), RequestId(3), error),
    Ok([
      SendChunk(20, @protocol.encode_request_error(error).unwrap()),
      AbortSend(50, 0),
    ]),
  )
  assert_eq(
    publisher.cancel_request(0UL),
    Ok([
      AbortSend(20, 1UL),
      StopReceiving(20, 1UL),
      EmitNotice(
        SubscriptionEnded(0UL, 20, {
          namespace_fields: [b"demo"],
          name: b"video",
        }),
      ),
    ]),
  )
  assert_eq(publisher.cancel_request(0UL), Ok([]))
  assert_eq(
    publisher.receive(SendCompleted(50, Reset)),
    Ok([EmitNotice(StreamSendFinished(50, Reset))]),
  )
}

///|
test "failed local update requires UPDATE_FAILED PUBLISH_DONE" {
  let subscriber = subscriber_started()
  let (update_id, _) = subscriber
    .update_subscription(RequestId(0), [forward_parameter(0)])
    .unwrap()
  let error = @protocol.RequestErrorMessage::{
    code: 1,
    retry_interval: 0,
    reason: b"unsupported",
    redirect: b"",
  }
  let done = @protocol.PublishDoneMessage::{
    code: 8,
    stream_count: 0,
    reason: b"",
  }
  let wire = @protocol.encode_request_error(error).unwrap() +
    @protocol.encode_publish_done(done).unwrap()
  assert_eq(
    subscriber.receive(StreamData(30, wire)),
    Ok([
      EmitNotice(SubscriptionUpdateRejected(0, update_id, error)),
      SendFin(30, b""),
      EmitNotice(PublishDone(0, done)),
    ]),
  )
  assert_eq(
    subscriber.update_subscription(RequestId(0), []),
    Err(SubscriptionNotAccepted),
  )
}

///|
test "UPDATE_FAILED PUBLISH_DONE counts an Object stream opened after rejection" {
  let publisher = publisher_started()
  let token = match
    publisher
    .open_object_stream(StreamId(40), publisher_header(1, false))
    .unwrap() {
    [OpenStream(Unidirectional, Object(token))] => token
    _ => fail("expected pending Object stream")
  }
  ignore(
    publisher.receive(StreamData(40, request_update_bytes(3, []))).unwrap(),
  )
  let error = @protocol.RequestErrorMessage::{
    code: 1,
    retry_interval: 0,
    reason: b"unsupported",
    redirect: b"",
  }
  assert_eq(
    publisher.reject_subscription_update(StreamId(40), RequestId(3), error),
    Ok([SendChunk(40, @protocol.encode_request_error(error).unwrap())]),
  )
  let done = @protocol.PublishDoneMessage::{
    code: 8,
    stream_count: 1,
    reason: b"",
  }
  assert_eq(
    publisher.receive(StreamOpened(Object(token), 50)),
    Ok([
      AbortSend(50, 0UL),
      SendFin(40, @protocol.encode_publish_done(done).unwrap()),
      EmitNotice(PublishDone(1UL, done)),
    ]),
  )
}

///|
test "Object stream opened after forwarding is paused counts toward later PUBLISH_DONE" {
  let publisher = publisher_started()
  let token = match
    publisher
    .open_object_stream(StreamId(40), publisher_header(1, false))
    .unwrap() {
    [OpenStream(Unidirectional, Object(token))] => token
    _ => fail("expected pending Object stream")
  }
  let pause : Array[@protocol.MessageParameter] = [
    { kind: 0x10UL, value: Byte(0), },
  ]
  ignore(
    publisher.receive(StreamData(40, request_update_bytes(3, pause))).unwrap(),
  )
  ignore(
    publisher
    .accept_subscription_update(StreamId(40), RequestId(3), update_ok())
    .unwrap(),
  )
  assert_eq(
    publisher.receive(StreamOpened(Object(token), 50)),
    Ok([AbortSend(50, 0UL)]),
  )
  let done = @protocol.PublishDoneMessage::{
    code: 3,
    stream_count: 1,
    reason: b"",
  }
  assert_eq(
    publisher.publish_done(StreamId(40), done.code, done.reason),
    Ok([
      SendFin(40, @protocol.encode_publish_done(done).unwrap()),
      EmitNotice(PublishDone(1UL, done)),
    ]),
  )
}

///|
test "request FIN closes the update direction without ending the local PUBLISH subscription" {
  let publisher = publisher_announced()
  let forward_off = [forward_parameter(0)]
  assert_eq(
    publisher.receive(StreamData(20, request_update_bytes(1, forward_off))),
    Ok([
      EmitNotice(
        SubscriptionUpdateRequested(0, 1, 20, forward_off, forward_off),
      ),
    ]),
  )
  assert_eq(
    publisher.accept_subscription_update(
      StreamId(20),
      RequestId(1),
      update_ok(),
    ),
    Ok([
      SendChunk(
        20,
        @protocol.encode_request_ok_for(update_ok(), RequestUpdate).unwrap(),
      ),
      EmitNotice(SubscriptionUpdateApplied(0, 1, 20, forward_off)),
    ]),
  )
  let inbound = publisher_started()
  assert_eq(inbound.receive(InboundStreamFinished(40)), Ok([]))
  assert_eq(
    inbound.open_object_stream(StreamId(40), publisher_header(1, false)),
    Ok([OpenStream(Unidirectional, Object(0))]),
  )
  assert_eq(
    inbound.receive(StreamData(40, request_update_bytes(3, forward_off))),
    Ok([]),
  )
}

///|
test "datagrams route across aliases without ordered object ID state" {
  let subscriber = subscriber_started()
  let track_b = @protocol.TrackName::{
    namespace_fields: [b"demo"],
    name: b"audio",
  }
  let (request_b, actions) = subscriber.subscribe_track(track_b).unwrap()
  assert_eq(request_b, RequestId(2UL))
  assert_eq(actions, [OpenStream(Bidirectional, Request(2))])
  ignore(subscriber.receive(StreamOpened(Request(2), 31)).unwrap())
  assert_eq(
    subscriber.receive(
      StreamData(
        31,
        @protocol.encode_subscribe_ok({
          track_alias: 2,
          parameters: [],
          properties: b"",
        }).unwrap(),
      ),
    ),
    Ok([EmitNotice(SubscribeEstablished(2, 2))]),
  )

  let late_a = update_datagram(1, 9, b"a9")
  let early_b = update_datagram(2, 0, b"b0")
  let reordered_a = update_datagram(1, 3, b"a3")
  assert_eq(
    subscriber.receive(
      DatagramReceived(@protocol.encode_object_datagram(late_a).unwrap()),
    ),
    Ok([
      EmitNotice(
        ObjectDatagramReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          late_a,
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      DatagramReceived(@protocol.encode_object_datagram(early_b).unwrap()),
    ),
    Ok([EmitNotice(ObjectDatagramReceived(2, track_b, early_b))]),
  )
  assert_eq(
    subscriber.receive(
      DatagramReceived(@protocol.encode_object_datagram(late_a).unwrap()),
    ),
    Ok([
      EmitNotice(
        ObjectDatagramReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          late_a,
        ),
      ),
    ]),
  )
  assert_eq(
    subscriber.receive(
      DatagramReceived(@protocol.encode_object_datagram(reordered_a).unwrap()),
    ),
    Ok([
      EmitNotice(
        ObjectDatagramReceived(
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
  let normal_with_properties = @protocol.ObjectDatagram::{
    track_alias: 1,
    group_id: 7,
    object_id: 4,
    publisher_priority: None,
    end_of_group: false,
    properties: b"\x02\x2a",
    status: Normal,
    payload: b"payload",
  }
  let group_end = @protocol.ObjectDatagram::{
    track_alias: 1,
    group_id: 7,
    object_id: 5,
    publisher_priority: Some(9),
    end_of_group: true,
    properties: b"",
    status: Normal,
    payload: b"last in group",
  }
  let end_of_group = @protocol.ObjectDatagram::{
    track_alias: 1,
    group_id: 7,
    object_id: 6,
    publisher_priority: None,
    end_of_group: false,
    properties: b"",
    status: EndOfGroup,
    payload: b"",
  }
  let end_of_track = @protocol.ObjectDatagram::{
    track_alias: 1,
    group_id: 7,
    object_id: 0,
    publisher_priority: None,
    end_of_group: false,
    properties: b"",
    status: EndOfTrack,
    payload: b"",
  }
  for
    datagram in [normal_with_properties, group_end, end_of_group, end_of_track] {
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
  }
}

///|
test "datagrams do not count toward PUBLISH_DONE and can arrive while streams drain" {
  let datagram_only = subscriber_started()
  let object = update_datagram(1, 4, b"datagram")
  assert_eq(
    datagram_only.receive(
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
  let empty_done = @protocol.PublishDoneMessage::{
    code: 3,
    stream_count: 0,
    reason: b"",
  }
  assert_eq(
    datagram_only.receive(
      StreamData(30, @protocol.encode_publish_done(empty_done).unwrap()),
    ),
    Ok([SendFin(30, b""), EmitNotice(PublishDone(0, empty_done))]),
  )
  assert_eq(
    datagram_only.receive(
      DatagramReceived(@protocol.encode_object_datagram(object).unwrap()),
    ),
    Ok([]),
  )

  let mixed = subscriber_started()
  let header = @protocol.SubgroupHeader::{
    track_alias: 1,
    group_id: 8,
    subgroup_id_mode: Zero,
    publisher_priority: None,
    end_of_group: false,
    first_object: true,
    has_properties: false,
  }
  assert_eq(mixed.receive(InboundStreamStarted(41, Unidirectional)), Ok([]))
  assert_eq(
    mixed.receive(
      StreamData(
        41,
        @protocol.encode_subgroup_stream(header, [
          { object_id: 0, status: Normal, properties: b"", payload: b"stream", },
        ]).unwrap(),
      ),
    ),
    Ok([
      EmitNotice(
        ObjectReceived(
          0,
          { namespace_fields: [b"demo"], name: b"video", },
          header,
          [
            {
              object_id: 0,
              status: Normal,
              properties: b"",
              payload: b"stream",
            },
          ],
        ),
      ),
    ]),
  )
  let mixed_done = @protocol.PublishDoneMessage::{
    code: 3,
    stream_count: 1,
    reason: b"",
  }
  assert_eq(
    mixed.receive(
      StreamData(30, @protocol.encode_publish_done(mixed_done).unwrap()),
    ),
    Ok([SendFin(30, b"")]),
  )
  assert_eq(
    mixed.receive(
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
  assert_eq(
    mixed.receive(InboundStreamFinished(41)),
    Ok([EmitNotice(PublishDone(0, mixed_done))]),
  )
}

///|
test "object datagram sending reports local capability and maximum length" {
  let publisher = publisher_started()
  let datagram = update_datagram(1, 2, b"payload")
  let encoded = @protocol.encode_object_datagram(datagram).unwrap()
  assert_eq(
    publisher.receive(
      DatagramSendCapabilityChanged(enabled=false, max_length=0),
    ),
    Ok([]),
  )
  assert_eq(
    publisher.send_object_datagram(StreamId(40), datagram),
    Err(DatagramSendUnavailable),
  )
  assert_eq(
    publisher.receive(
      DatagramSendCapabilityChanged(enabled=true, max_length=encoded.length()),
    ),
    Ok([]),
  )
  assert_eq(
    publisher.send_object_datagram(StreamId(40), datagram),
    Ok((0UL, [SendDatagram(0UL, encoded), EmitNotice(DatagramQueued(0UL))])),
  )
  assert_eq(
    publisher.receive(
      DatagramSendCapabilityChanged(
        enabled=true,
        max_length=encoded.length() - 1,
      ),
    ),
    Ok([]),
  )
  assert_eq(
    publisher.send_object_datagram(StreamId(40), datagram),
    Err(DatagramTooLarge(encoded.length(), encoded.length() - 1)),
  )
}

///|
test "datagram send failure reports its token without closing the session" {
  let publisher = publisher_started()
  let datagram = update_datagram(1, 2, b"payload")
  let encoded = @protocol.encode_object_datagram(datagram).unwrap()
  assert_eq(
    publisher.receive(
      DatagramSendCapabilityChanged(enabled=true, max_length=encoded.length()),
    ),
    Ok([]),
  )
  assert_eq(publisher.max_datagram_length(), encoded.length())
  let (token, actions) = publisher
    .send_object_datagram(StreamId(40), datagram)
    .unwrap()
  assert_eq(token, 0UL)
  assert_eq(actions, [
    SendDatagram(0UL, encoded),
    EmitNotice(DatagramQueued(0UL)),
  ])
  assert_eq(
    publisher.receive(DatagramSendFailed(token, -4L)),
    Ok([EmitNotice(DatagramLocalWriteFailed(token, -4L))]),
  )
  assert_eq(publisher.state(), Active)
  assert_eq(
    publisher.send_object_datagram(StreamId(40), datagram).unwrap().0,
    1UL,
  )
}

///|
test "datagram-only publisher completion reports zero opened Object streams" {
  let publisher = publisher_started()
  let datagram = update_datagram(1, 2, b"payload")
  assert_eq(
    publisher.send_object_datagram(StreamId(40), datagram),
    Ok(
      (
        0UL,
        [
          SendDatagram(0UL, @protocol.encode_object_datagram(datagram).unwrap()),
          EmitNotice(DatagramQueued(0UL)),
        ],
      ),
    ),
  )
  let done = @protocol.PublishDoneMessage::{
    code: 3,
    stream_count: 0,
    reason: b"",
  }
  assert_eq(
    publisher.publish_done(StreamId(40), done.code, done.reason),
    Ok([
      SendFin(40, @protocol.encode_publish_done(done).unwrap()),
      EmitNotice(PublishDone(1, done)),
    ]),
  )
}
