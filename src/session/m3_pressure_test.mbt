///|
test "M3 rejected update drains a near-full stream set across cancellation" {
  let publisher = publisher_started()
  let active_streams : Array[Int64] = []
  for i = 0; i < 14; i = i + 1 {
    let token = match
      publisher
      .open_object_stream(
        @session.StreamId(40),
        publisher_header(i.to_uint64(), false),
      )
      .unwrap() {
      [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
        token
      _ => fail("expected object stream")
    }
    let stream = 50L + i.to_int64()
    active_streams.push(stream)
    ignore(
      publisher
      .receive(@session.StreamOpened(@session.Object(token), stream))
      .unwrap(),
    )
  }
  let delayed_token = match
    publisher
    .open_object_stream(@session.StreamId(40), publisher_header(14UL, false))
    .unwrap() {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("expected delayed object stream")
  }
  ignore(
    publisher
    .receive(@session.StreamData(40, request_update_bytes(3, [])))
    .unwrap(),
  )

  let error : @protocol.RequestErrorMessage = {
    code: 1UL,
    retry_interval: 0UL,
    reason: b"resource pressure",
    redirect: b"",
  }
  let reject_actions : Array[@session.SessionAction] = [
    @session.SendChunk(40, @protocol.encode_request_error(error).unwrap()),
  ]
  for stream in active_streams {
    reject_actions.push(@session.AbortSend(stream, 0UL))
  }
  assert_eq(
    publisher.reject_subscription_update(40, 3UL, error),
    Ok(reject_actions),
  )

  // The peer cancels while callbacks for the already-aborted streams are queued.
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
  for stream in active_streams {
    assert_eq(
      publisher.receive(@session.SendCompleted(stream, @session.Reset)),
      Ok([
        @session.EmitNotice(@session.StreamSendFinished(stream, @session.Reset)),
      ]),
    )
  }
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(delayed_token), 80)),
    Ok([@session.AbortSend(80, 1UL)]),
  )
  assert_eq(publisher.cancel_request(1UL), Ok([]))
  assert_eq(
    publisher.receive(@session.StreamData(40, request_update_bytes(5, []))),
    Ok([]),
  )
}
