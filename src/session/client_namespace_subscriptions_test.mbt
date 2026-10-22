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
test "namespace prefix update switches suffix interpretation at REQUEST_OK in one chunk" {
  let client = client_session_ready()
  let old_prefix = test_namespace([b"old"])
  let new_prefix = test_namespace([b"new"])
  let old_name = test_namespace([b"old", b"before"])
  let new_name = test_namespace([b"new", b"after"])
  let (request_id, _) = client.subscribe_namespace(old_prefix).unwrap()
  ignore(
    client.receive(@session.StreamOpened(@session.Request(request_id), 70)),
  )
  let established = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    @protocol.Namespace,
  ).unwrap()
  ignore(client.receive(@session.StreamData(70, established)))
  let (update_id, update_actions) = client
    .update_namespace_subscription(request_id, new_prefix)
    .unwrap()
  assert_eq(update_id, 2UL)
  assert_true(update_actions.length() == 1)
  let old_namespace = @protocol.encode_namespace({
    track_namespace: test_namespace([b"before"]),
  }).unwrap()
  let update_ok = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    @protocol.RequestUpdate,
  ).unwrap()
  let new_namespace = @protocol.encode_namespace({
    track_namespace: test_namespace([b"after"]),
  }).unwrap()
  assert_eq(
    client.receive(
      @session.StreamData(70, old_namespace + update_ok + new_namespace),
    ),
    Ok([
      @session.Notify(@session.NamespaceDiscovered(request_id, old_name)),
      @session.Notify(
        @session.NamespaceSubscriptionUpdateAccepted(request_id, update_id, {
          parameters: [],
          properties: b"",
        }),
      ),
      @session.Notify(@session.NamespaceNoLongerAvailable(request_id, old_name)),
      @session.Notify(@session.NamespaceDiscovered(request_id, new_name)),
    ]),
  )
}

///|
test "namespace prefix update response split across chunks uses new prefix afterward" {
  let client = client_session_ready()
  let old_prefix = test_namespace([b"before"])
  let new_prefix = test_namespace([b"after"])
  let (request_id, _) = client.subscribe_namespace(old_prefix).unwrap()
  ignore(
    client.receive(@session.StreamOpened(@session.Request(request_id), 71)),
  )
  ignore(
    client.receive(
      @session.StreamData(
        71,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          @protocol.Namespace,
        ).unwrap(),
      ),
    ),
  )
  let (update_id, _) = client
    .update_namespace_subscription(request_id, new_prefix)
    .unwrap()
  let update_ok = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    @protocol.RequestUpdate,
  ).unwrap()
  assert_true(update_ok.length() > 1)
  let split = update_ok.length() / 2
  assert_eq(
    client.receive(@session.StreamData(71, update_ok[:split].to_owned())),
    Ok([]),
  )
  assert_eq(
    client.receive(
      @session.StreamData(
        71,
        update_ok[split:].to_owned() +
        @protocol.encode_namespace({
          track_namespace: test_namespace([b"child"]),
        }).unwrap(),
      ),
    ),
    Ok([
      @session.Notify(
        @session.NamespaceSubscriptionUpdateAccepted(request_id, update_id, {
          parameters: [],
          properties: b"",
        }),
      ),
      @session.Notify(
        @session.NamespaceDiscovered(
          request_id,
          test_namespace([b"after", b"child"]),
        ),
      ),
    ]),
  )
}

///|
test "peer namespace prefix update is accepted and re-enumerates under new prefix" {
  let publisher = client_session_ready()
  let old_prefix = test_namespace([b"old"])
  let new_prefix = test_namespace([b"new"])
  let current = test_namespace([b"new", b"room"])
  ignore(publisher.publish_application_namespace(current).unwrap())
  ignore(
    publisher.receive(@session.InboundStreamStarted(72, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        72,
        @protocol.encode_subscribe_namespace({
          request_id: 1UL,
          track_namespace: old_prefix,
          parameters: [],
        }).unwrap(),
      ),
    ),
  )
  ignore(publisher.accept_namespace_subscription(72, []).unwrap())
  let update_id = 3UL
  let update = @protocol.encode_request_update({
    request_id: update_id,
    parameters: [
      { kind: 0x34UL, value: @protocol.NamespacePrefix(new_prefix), },
    ],
  }).unwrap()
  assert_eq(
    publisher.receive(@session.StreamData(72, update)),
    Ok([
      @session.Notify(
        @session.NamespaceSubscriptionUpdateRequested(
          1UL, update_id, 72, new_prefix,
        ),
      ),
    ]),
  )
  assert_eq(
    publisher.accept_namespace_subscription_update(72, update_id, {
      parameters: [],
      properties: b"",
    }),
    Ok([
      @session.Send(
        72,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          @protocol.RequestUpdate,
        ).unwrap(),
        false,
      ),
      @session.Notify(
        @session.NamespaceSubscriptionUpdateAccepted(1UL, update_id, {
          parameters: [],
          properties: b"",
        }),
      ),
      @session.Send(
        72,
        @protocol.encode_namespace({
          track_namespace: test_namespace([b"room"]),
        }).unwrap(),
        false,
      ),
      @session.Notify(@session.NamespaceDiscovered(1UL, current)),
    ]),
  )
}

///|
test "namespace prefix updates reject overlap and rejection leaves other subscriptions active" {
  let client = client_session_ready()
  let (first_id, _) = client
    .subscribe_namespace(test_namespace([b"first"]))
    .unwrap()
  let (second_id, _) = client
    .subscribe_namespace(test_namespace([b"root", b"child"]))
    .unwrap()
  ignore(client.receive(@session.StreamOpened(@session.Request(first_id), 73)))
  ignore(client.receive(@session.StreamOpened(@session.Request(second_id), 74)))
  let established = @protocol.encode_request_ok_for(
    { parameters: [], properties: b"", },
    @protocol.Namespace,
  ).unwrap()
  ignore(client.receive(@session.StreamData(73, established)))
  ignore(client.receive(@session.StreamData(74, established)))
  assert_eq(
    client.update_namespace_subscription(first_id, test_namespace([b"root"])),
    Err(@session.PrefixOverlap),
  )
  assert_eq(
    client.update_namespace_subscription(
      first_id,
      test_namespace([b"root", b"child", b"x"]),
    ),
    Err(@session.PrefixOverlap),
  )

  let publisher = client_session_ready()
  let old_prefix = test_namespace([b"old"])
  let active_name = test_namespace([b"old", b"room"])
  ignore(
    publisher.receive(@session.InboundStreamStarted(75, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(@session.InboundStreamStarted(76, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        75,
        @protocol.encode_subscribe_namespace({
          request_id: 1UL,
          track_namespace: old_prefix,
          parameters: [],
        }).unwrap(),
      ),
    ),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        76,
        @protocol.encode_subscribe_namespace({
          request_id: 3UL,
          track_namespace: test_namespace([b"independent"]),
          parameters: [],
        }).unwrap(),
      ),
    ),
  )
  ignore(publisher.accept_namespace_subscription(75, [active_name]).unwrap())
  ignore(publisher.accept_namespace_subscription(76, []).unwrap())
  let update_id = 5UL
  ignore(
    publisher.receive(
      @session.StreamData(
        75,
        @protocol.encode_request_update({
          request_id: update_id,
          parameters: [
            {
              kind: 0x34UL,
              value: @protocol.NamespacePrefix(test_namespace([b"new"])),
            },
          ],
        }).unwrap(),
      ),
    ),
  )
  let error : @protocol.RequestErrorMessage = {
    code: 0x30UL,
    retry_interval: 0UL,
    reason: b"overlap",
    redirect: b"",
  }
  let rejected = publisher
    .reject_namespace_subscription_update(75, update_id, error)
    .unwrap()
  assert_true(
    rejected.contains(
      @session.Send(75, @protocol.encode_request_error(error).unwrap(), true),
    ),
  )
  assert_true(
    rejected.contains(
      @session.Notify(@session.NamespaceNoLongerAvailable(1UL, active_name)),
    ),
  )
  assert_true(
    rejected.contains(
      @session.Notify(
        @session.NamespaceSubscriptionUpdateRejected(1UL, update_id, error),
      ),
    ),
  )
  assert_true(
    rejected.contains(
      @session.Notify(@session.NamespaceSubscriptionEnded(1UL, 75)),
    ),
  )
  assert_true(rejected.contains(@session.StopReceiving(75, 1UL)))
  assert_true(
    publisher
    .publish_application_namespace(test_namespace([b"independent", b"next"]))
    .unwrap()
    .contains(
      @session.Send(
        76,
        @protocol.encode_namespace({
          track_namespace: test_namespace([b"next"]),
        }).unwrap(),
        false,
      ),
    ),
  )
}

///|
test "canceling a namespace subscription during prefix update ignores a late response" {
  let client = client_session_ready()
  let (request_id, _) = client
    .subscribe_namespace(test_namespace([b"old"]))
    .unwrap()
  ignore(
    client.receive(@session.StreamOpened(@session.Request(request_id), 77)),
  )
  ignore(
    client.receive(
      @session.StreamData(
        77,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          @protocol.Namespace,
        ).unwrap(),
      ),
    ),
  )
  ignore(
    client
    .update_namespace_subscription(request_id, test_namespace([b"new"]))
    .unwrap(),
  )
  assert_true(client.cancel_request(request_id) is Ok(_))
  assert_eq(
    client.receive(
      @session.StreamData(
        77,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          @protocol.RequestUpdate,
        ).unwrap(),
      ),
    ),
    Ok([]),
  )
}

///|
test "rejected namespace prefix update terminates only its request" {
  let client = client_session_ready()
  let old_prefix = test_namespace([b"old"])
  let active_name = test_namespace([b"old", b"room"])
  let (request_id, _) = client.subscribe_namespace(old_prefix).unwrap()
  ignore(
    client.receive(@session.StreamOpened(@session.Request(request_id), 78)),
  )
  ignore(
    client.receive(
      @session.StreamData(
        78,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          @protocol.Namespace,
        ).unwrap(),
      ),
    ),
  )
  ignore(
    client.receive(
      @session.StreamData(
        78,
        @protocol.encode_namespace({
          track_namespace: test_namespace([b"room"]),
        }).unwrap(),
      ),
    ),
  )
  let (update_id, _) = client
    .update_namespace_subscription(request_id, test_namespace([b"new"]))
    .unwrap()
  let error : @protocol.RequestErrorMessage = {
    code: 0x30UL,
    retry_interval: 0UL,
    reason: b"overlap",
    redirect: b"",
  }
  let actions = client
    .receive(
      @session.StreamData(78, @protocol.encode_request_error(error).unwrap()),
    )
    .unwrap()
  assert_true(
    actions.contains(
      @session.Notify(
        @session.NamespaceNoLongerAvailable(request_id, active_name),
      ),
    ),
  )
  assert_true(
    actions.contains(
      @session.Notify(@session.NamespaceSubscriptionEnded(request_id, 78)),
    ),
  )
  assert_true(
    actions.contains(
      @session.Notify(
        @session.NamespaceSubscriptionUpdateRejected(
          request_id, update_id, error,
        ),
      ),
    ),
  )
  assert_true(actions.contains(@session.AbortSend(78, 1UL)))
  assert_true(actions.contains(@session.StopReceiving(78, 1UL)))
  assert_eq(
    client.update_namespace_subscription(request_id, test_namespace([b"later"])),
    Err(@session.SubscriptionNotAccepted),
  )
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
