///|
fn test_namespace(fields : Array[Bytes]) -> @protocol.TrackNamespace {
  { fields, }
}

///|
fn has_close_connection(actions : Array[@session.ClientSessionAction]) -> Bool {
  for action in actions {
    match action {
      @session.CloseConnection(3UL, _) => return true
      _ => ()
    }
  }
  false
}

///|
fn has_rejection_send(actions : Array[@session.ClientSessionAction]) -> Bool {
  for action in actions {
    match action {
      @session.Send(42, _, true) => return true
      _ => ()
    }
  }
  false
}

///|
fn has_fin_send(
  actions : Array[@session.ClientSessionAction],
  stream : Int64,
) -> Bool {
  for action in actions {
    match action {
      @session.Send(actual, _, true) if actual == stream => return true
      _ => ()
    }
  }
  false
}

///|
fn has_namespace_rejection(
  actions : Array[@session.ClientSessionAction],
) -> Bool {
  for action in actions {
    match action {
      @session.Notify(@session.NamespaceSubscriptionRejected(3UL, 42, _)) =>
        return true
      _ => ()
    }
  }
  false
}

///|
test "SUBSCRIBE_NAMESPACE restores full names, including empty suffixes, then withdraws them" {
  let client = client_session_ready()
  let prefix = test_namespace([b"example"])
  let full = test_namespace([b"example", b"room"])
  let (request_id, opened) = client.subscribe_namespace(prefix).unwrap()
  assert_eq(request_id, 0UL)
  assert_eq(opened, [
    @session.OpenStream(@session.Bidirectional, @session.Request(request_id)),
  ])
  assert_eq(
    client.receive(@session.StreamOpened(@session.Request(request_id), 30)),
    Ok([
      @session.Send(
        30,
        @protocol.encode_subscribe_namespace({
          request_id,
          track_namespace: prefix,
          parameters: [],
        }).unwrap(),
        false,
      ),
    ]),
  )
  let ok = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    @protocol.Namespace,
  ).unwrap()
  assert_eq(
    client.receive(@session.StreamData(30, ok)),
    Ok([
      @session.Notify(@session.NamespaceSubscriptionEstablished(request_id, 30)),
    ]),
  )
  assert_eq(
    client.receive(
      @session.StreamData(
        30,
        @protocol.encode_namespace({
          track_namespace: test_namespace([b"room"]),
        }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.NamespaceDiscovered(request_id, full))]),
  )
  assert_eq(
    client.receive(
      @session.StreamData(
        30,
        @protocol.encode_namespace({ track_namespace: test_namespace([]), }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.NamespaceDiscovered(request_id, prefix))]),
  )
  assert_eq(
    client.receive(
      @session.StreamData(
        30,
        @protocol.encode_namespace_done({ track_namespace: test_namespace([]), }).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(@session.NamespaceNoLongerAvailable(request_id, prefix)),
    ]),
  )
  assert_eq(
    client.receive(
      @session.StreamData(
        30,
        @protocol.encode_namespace_done({
          track_namespace: test_namespace([b"room"]),
        }).unwrap(),
      ),
    ),
    Ok([@session.Notify(@session.NamespaceNoLongerAvailable(request_id, full))]),
  )
}

///|
test "namespace subscription rejects DONE before NAMESPACE and closes on FIN with active names" {
  let client = client_session_ready()
  let prefix = test_namespace([b"a"])
  let full = test_namespace([b"a", b"b"])
  let (request_id, _) = client.subscribe_namespace(prefix).unwrap()
  ignore(
    client.receive(@session.StreamOpened(@session.Request(request_id), 30)),
  )
  let ok = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    @protocol.Namespace,
  ).unwrap()
  ignore(client.receive(@session.StreamData(30, ok)))
  let done_first = client
    .receive(
      @session.StreamData(
        30,
        @protocol.encode_namespace_done({
          track_namespace: test_namespace([b"b"]),
        }).unwrap(),
      ),
    )
    .unwrap()
  assert_true(has_close_connection(done_first))

  let second = client_session_ready()
  let (second_id, _) = second.subscribe_namespace(prefix).unwrap()
  ignore(second.receive(@session.StreamOpened(@session.Request(second_id), 31)))
  ignore(second.receive(@session.StreamData(31, ok)))
  ignore(
    second.receive(
      @session.StreamData(
        31,
        @protocol.encode_namespace({ track_namespace: test_namespace([b"b"]), }).unwrap(),
      ),
    ),
  )
  assert_eq(
    second.receive(@session.StreamFinished(31)),
    Ok([
      @session.Send(31, b"", true),
      @session.Notify(@session.NamespaceNoLongerAvailable(second_id, full)),
      @session.Notify(@session.NamespaceSubscriptionEnded(second_id, 31)),
    ]),
  )

  let peer_cancel = client_session_ready()
  ignore(
    peer_cancel.receive(
      @session.InboundStreamStarted(33, @session.Bidirectional),
    ),
  )
  ignore(
    peer_cancel.receive(
      @session.StreamData(
        33,
        @protocol.encode_subscribe_namespace({
          request_id: 1UL,
          track_namespace: prefix,
          parameters: [],
        }).unwrap(),
      ),
    ),
  )
  ignore(peer_cancel.accept_namespace_subscription(33, []).unwrap())
  assert_eq(
    peer_cancel.receive(@session.StreamFinished(33)),
    Ok([
      @session.Send(33, b"", true),
      @session.Notify(@session.NamespaceSubscriptionEnded(1UL, 33)),
    ]),
  )

  let canceled = client_session_ready()
  let (cancel_id, _) = canceled.subscribe_namespace(prefix).unwrap()
  ignore(
    canceled.receive(@session.StreamOpened(@session.Request(cancel_id), 32)),
  )
  ignore(canceled.receive(@session.StreamData(32, ok)))
  ignore(
    canceled.receive(
      @session.StreamData(
        32,
        @protocol.encode_namespace({ track_namespace: test_namespace([b"b"]), }).unwrap(),
      ),
    ),
  )
  assert_eq(
    canceled.cancel_request(cancel_id),
    Ok([
      @session.Send(32, b"", true),
      @session.StopReceiving(32, 1UL),
      @session.Notify(@session.NamespaceNoLongerAvailable(cancel_id, full)),
      @session.Notify(@session.NamespaceSubscriptionEnded(cancel_id, 32)),
    ]),
  )
  assert_eq(canceled.cancel_request(cancel_id), Ok([]))
}

///|
test "peer namespace subscriptions enumerate app names and reject tuple prefix overlap" {
  let publisher = client_session_ready()
  let prefix = test_namespace([b"root"])
  let child = test_namespace([b"root", b"child"])
  ignore(
    publisher.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
  )
  let request : @protocol.NamespaceRequestMessage = {
    request_id: 1UL,
    track_namespace: prefix,
    parameters: [],
  }
  assert_eq(
    publisher.receive(
      @session.StreamData(
        40,
        @protocol.encode_subscribe_namespace(request).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(@session.NamespaceSubscriptionRequested(1UL, 40, prefix)),
    ]),
  )
  let actions = publisher.accept_namespace_subscription(40, [child]).unwrap()
  assert_eq(actions, [
    @session.Send(
      40,
      @protocol.encode_request_ok_for(
        { parameters: [], properties: b"", },
        @protocol.Namespace,
      ).unwrap(),
      false,
    ),
    @session.Notify(@session.NamespaceSubscriptionEstablished(1UL, 40)),
    @session.Send(
      40,
      @protocol.encode_namespace({
        track_namespace: test_namespace([b"child"]),
      }).unwrap(),
      false,
    ),
    @session.Notify(@session.NamespaceDiscovered(1UL, child)),
  ])

  ignore(
    publisher.receive(@session.InboundStreamStarted(42, @session.Bidirectional)),
  )
  let overlapping : @protocol.NamespaceRequestMessage = {
    request_id: 3UL,
    track_namespace: child,
    parameters: [],
  }
  let reject = publisher
    .receive(
      @session.StreamData(
        42,
        @protocol.encode_subscribe_namespace(overlapping).unwrap(),
      ),
    )
    .unwrap()
  assert_true(has_rejection_send(reject))
  assert_true(has_namespace_rejection(reject))
}

///|
test "namespace prefix matching treats sibling tuple branches as disjoint" {
  let publisher = client_session_ready()
  let left = test_namespace([b"root", b"left"])
  let right = test_namespace([b"root", b"right"])
  ignore(
    publisher.receive(@session.InboundStreamStarted(50, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        50,
        @protocol.encode_subscribe_namespace({
          request_id: 1UL,
          track_namespace: left,
          parameters: [],
        }).unwrap(),
      ),
    ),
  )
  ignore(publisher.accept_namespace_subscription(50, []).unwrap())
  ignore(
    publisher.receive(@session.InboundStreamStarted(52, @session.Bidirectional)),
  )
  assert_eq(
    publisher.receive(
      @session.StreamData(
        52,
        @protocol.encode_subscribe_namespace({
          request_id: 3UL,
          track_namespace: right,
          parameters: [],
        }).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(@session.NamespaceSubscriptionRequested(3UL, 52, right)),
    ]),
  )
  ignore(
    publisher.receive(@session.InboundStreamStarted(54, @session.Bidirectional)),
  )
  let same_prefix = publisher
    .receive(
      @session.StreamData(
        54,
        @protocol.encode_subscribe_namespace({
          request_id: 5UL,
          track_namespace: left,
          parameters: [],
        }).unwrap(),
      ),
    )
    .unwrap()
  assert_true(has_fin_send(same_prefix, 54))
  ignore(
    publisher.receive(@session.InboundStreamStarted(56, @session.Bidirectional)),
  )
  let ancestor = publisher
    .receive(
      @session.StreamData(
        56,
        @protocol.encode_subscribe_namespace({
          request_id: 7UL,
          track_namespace: test_namespace([b"root"]),
          parameters: [],
        }).unwrap(),
      ),
    )
    .unwrap()
  assert_true(has_fin_send(ancestor, 56))
}

///|
test "accepted local namespace publications are enumerated and withdrawn to peer subscriptions" {
  let publisher = client_session_ready()
  let full = test_namespace([b"catalog", b"series"])
  let (publish_id, _) = publisher.publish_namespace(full, []).unwrap()
  ignore(
    publisher.receive(@session.StreamOpened(@session.Request(publish_id), 30)),
  )
  let ok = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    @protocol.Namespace,
  ).unwrap()
  ignore(publisher.receive(@session.StreamData(30, ok)))

  ignore(
    publisher.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        40,
        @protocol.encode_subscribe_namespace({
          request_id: 1UL,
          track_namespace: test_namespace([b"catalog"]),
          parameters: [],
        }).unwrap(),
      ),
    ),
  )
  assert_eq(publisher.accept_namespace_subscription(40, []).unwrap(), [
    @session.Send(40, ok, false),
    @session.Notify(@session.NamespaceSubscriptionEstablished(1UL, 40)),
    @session.Send(
      40,
      @protocol.encode_namespace({
        track_namespace: test_namespace([b"series"]),
      }).unwrap(),
      false,
    ),
    @session.Notify(@session.NamespaceDiscovered(1UL, full)),
  ])
  assert_eq(
    publisher.cancel_request(publish_id),
    Ok([
      @session.Send(
        40,
        @protocol.encode_namespace_done({
          track_namespace: test_namespace([b"series"]),
        }).unwrap(),
        false,
      ),
      @session.Notify(@session.NamespaceNoLongerAvailable(1UL, full)),
      @session.AbortSend(30, 1UL),
      @session.StopReceiving(30, 1UL),
      @session.Notify(@session.NamespaceWithdrawn(publish_id, 30)),
    ]),
  )
}

///|
test "empty namespace prefix overlaps every active namespace subscription" {
  let publisher = client_session_ready()
  ignore(
    publisher.receive(@session.InboundStreamStarted(60, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        60,
        @protocol.encode_subscribe_namespace({
          request_id: 1UL,
          track_namespace: test_namespace([]),
          parameters: [],
        }).unwrap(),
      ),
    ),
  )
  ignore(publisher.accept_namespace_subscription(60, []).unwrap())
  ignore(
    publisher.receive(@session.InboundStreamStarted(62, @session.Bidirectional)),
  )
  let overlap = publisher
    .receive(
      @session.StreamData(
        62,
        @protocol.encode_subscribe_namespace({
          request_id: 3UL,
          track_namespace: test_namespace([b"any"]),
          parameters: [],
        }).unwrap(),
      ),
    )
    .unwrap()
  assert_true(has_fin_send(overlap, 62))
}
