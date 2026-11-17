///|
test "Joining FETCH accepts group IDs above 2^62" {
  let publisher = client_session_ready()
  let track = client_track(b"live")
  let group_id = 0x8000000000000000UL
  ignore(publisher.publish_track(track, 7UL).unwrap())
  ignore(publisher.receive(StreamOpened(Request(0UL), 20)))
  ignore(publisher.receive(StreamData(20, b"\x07\x00\x01\x00")))
  ignore(publisher.receive(InboundStreamStarted(40, Bidirectional)))
  ignore(
    publisher.receive(
      StreamData(
        40,
        @protocol.encode_subscribe({ request_id: 1UL, track, parameters: [], }).unwrap(),
      ),
    ),
  )
  ignore(publisher.accept_subscription(StreamId(40)).unwrap())
  ignore(
    publisher
    .open_object_stream(StreamId(40), {
      track_alias: 7UL,
      group_id,
      subgroup_id_mode: Zero,
      publisher_priority: None,
      end_of_group: false,
      first_object: true,
      has_properties: false,
    })
    .unwrap(),
  )
  ignore(publisher.receive(StreamOpened(Object(0UL), 30)))
  ignore(
    publisher
    .send_object(StreamId(30), {
      object_id: 0xffffffffffffffffUL,
      status: Normal,
      properties: b"",
      payload: b"edge",
    })
    .unwrap(),
  )
  ignore(publisher.receive(InboundStreamStarted(42, Bidirectional)))
  let fetch = @protocol.encode_fetch({
    request_id: 3UL,
    fetch_type: RelativeJoiningFetch(1UL, 0UL),
    parameters: [],
  }).unwrap()
  assert_eq(
    publisher.receive(StreamData(42, fetch)),
    Ok([
      EmitNotice(
        FetchRequested(
          3UL,
          42,
          track,
          { group: group_id, object: 0UL, },
          { group: group_id, object: 0UL, },
          [],
        ),
      ),
    ]),
  )
}

///|
test "local Joining FETCH accepts data above the old group limit" {
  let client = client_session_ready()
  let track = client_track(b"live")
  let group_id = 0x8000000000000000UL
  let (subscription_id, _) = client.subscribe_track(track).unwrap()
  ignore(client.receive(StreamOpened(Request(subscription_id), 20)))
  let subscribe_ok = @protocol.SubscribeOkMessage::{
    track_alias: 7UL,
    parameters: [{ kind: 0x09UL, value: Location(group_id, 0UL), }],
    properties: b"",
  }
  assert_eq(
    client.receive(
      StreamData(20, @protocol.encode_subscribe_ok(subscribe_ok).unwrap()),
    ),
    Ok([EmitNotice(SubscribeEstablished(subscription_id, 7UL))]),
  )
  let (fetch_id, _) = client
    .fetch_joining_relative(
      subscription_request_id=subscription_id.to_uint64(),
      groups_back=0UL,
      parameters=[],
    )
    .unwrap()
  ignore(client.receive(StreamOpened(Request(fetch_id), 22)))
  let data_stream : Int64 = 30
  ignore(client.receive(InboundStreamStarted(data_stream, Unidirectional)))
  let object = @protocol.FetchObject::{
    group_id,
    subgroup_id: None,
    object_id: 0UL,
    publisher_priority: 0,
    datagram_forwarding: true,
    properties: b"",
    payload: b"live",
  }
  let data = @protocol.encode_fetch_header({ request_id: fetch_id.to_uint64(), }) +
    @protocol.FetchDataEncoder::new(Ascending)
    .encode(FetchDataObject(object))
    .unwrap()
  assert_eq(
    client.receive(StreamData(data_stream, data)),
    Ok([EmitNotice(FetchObjectReceived(fetch_id, object))]),
  )
}
