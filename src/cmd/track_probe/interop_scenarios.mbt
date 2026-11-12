///|
fn interop_namespace(label : String) -> @protocol.TrackNamespace {
  { fields: [b"moq-test", @utf8.encode("\{label}-\{@env.now()}")], }
}

///|
fn interop_child_namespace(
  parent : @protocol.TrackNamespace,
  child : String,
) -> @protocol.TrackNamespace {
  { fields: parent.fields + [@utf8.encode(child)], }
}

///|
fn interop_wait_namespace_accepted(
  peer : LifecyclePeer,
  request_id : UInt64,
  initial : Array[@session.SessionNotice],
  timeout_ms : UInt64,
) -> Unit raise {
  let mut accepted = false
  for notice in initial {
    match notice {
      @session.NamespacePublicationAccepted(id, _) if id.to_uint64() ==
        request_id => accepted = true
      @session.NamespacePublicationRejected(_, _, error) =>
        fail("PUBLISH_NAMESPACE rejected: \{error.to_repr()}")
      @session.SessionEnded(end) =>
        fail("relay ended the session: \{end.to_repr()}")
      _ => ()
    }
  }
  let deadline = @env.now() + timeout_ms
  while !accepted && @env.now() < deadline {
    for notice in lifecycle_poll(peer, 20) {
      match notice {
        @session.NamespacePublicationAccepted(id, _) if id.to_uint64() ==
          request_id => accepted = true
        @session.NamespacePublicationRejected(_, _, error) =>
          fail("PUBLISH_NAMESPACE rejected: \{error.to_repr()}")
        @session.SessionEnded(end) =>
          fail("relay ended the session: \{end.to_repr()}")
        _ => ()
      }
    }
  }
  guard accepted else {
    fail("timed out waiting for PUBLISH_NAMESPACE acceptance")
  }
}

///|
fn interop_publish_namespace(
  peer : LifecyclePeer,
  track_namespace : @protocol.TrackNamespace,
) -> (UInt64, Int64) raise {
  let (request_id, actions) = peer.session
    .publish_namespace(track_namespace, [])
    .unwrap()
  let request_id = request_id.to_uint64()
  let (notices, opened) = drive_session_actions(
    peer.client,
    peer.session,
    actions,
    peer.driver,
  )
  let stream = lifecycle_request_stream(opened, request_id)
  interop_wait_namespace_accepted(peer, request_id, notices, 2000UL)
  (request_id, stream)
}

///|
fn interop_cancel_request(
  peer : LifecyclePeer,
  request_id : UInt64,
) -> Unit raise {
  ignore(
    drive_session_result(
      peer.client,
      peer.session,
      peer.session.cancel_request(RequestId(request_id)),
      peer.driver,
    ),
  )
}

///|
fn interop_service_publisher(
  peer : LifecyclePeer,
  timeout_ms : Int32,
) -> Array[@session.SessionNotice] raise {
  let notices = lifecycle_poll(peer, timeout_ms)
  for notice in notices {
    match notice {
      @session.SubscriptionRequested(_, stream, _) =>
        ignore(
          drive_session_result(
            peer.client,
            peer.session,
            peer.session.accept_subscription(stream),
            peer.driver,
          ),
        )
      @session.NamespacePublicationRequested(_, stream, _, _) =>
        ignore(
          drive_session_result(
            peer.client,
            peer.session,
            peer.session.accept_namespace_publication(stream),
            peer.driver,
          ),
        )
      @session.SessionEnded(end) =>
        fail("publisher session ended: \{end.to_repr()}")
      _ => ()
    }
  }
  notices
}

///|
fn interop_setup_only(context : InteropContext) -> Unit raise {
  let peer = context.connect("client")
  defer peer.client.close()
}

///|
fn interop_announce_only(context : InteropContext) -> Unit raise {
  let publisher = context.connect("publisher")
  defer publisher.client.close()
  let track_namespace : @protocol.TrackNamespace = {
    fields: [b"moq-test", b"interop"],
  }
  let (request_id, actions) = publisher.session
    .publish_namespace(track_namespace, [])
    .unwrap()
  let request_id = request_id.to_uint64()
  let (notices, _) = drive_session_actions(
    publisher.client,
    publisher.session,
    actions,
    publisher.driver,
  )
  interop_wait_namespace_accepted(publisher, request_id, notices, 2000UL)
}

///|
fn interop_publish_namespace_done(context : InteropContext) -> Unit raise {
  let publisher = context.connect("publisher")
  defer publisher.client.close()
  let track_namespace : @protocol.TrackNamespace = {
    fields: [b"moq-test", b"interop"],
  }
  let (request_id, actions) = publisher.session
    .publish_namespace(track_namespace, [])
    .unwrap()
  let request_id = request_id.to_uint64()
  let (notices, opened) = drive_session_actions(
    publisher.client,
    publisher.session,
    actions,
    publisher.driver,
  )
  let stream = lifecycle_request_stream(opened, request_id)
  interop_wait_namespace_accepted(publisher, request_id, notices, 2000UL)
  interop_cancel_request(publisher, request_id)
  let mut withdrawn = false
  let mut ended = false
  let deadline = @env.now() + 2000UL
  while !withdrawn && !ended && @env.now() < deadline {
    for notice in lifecycle_poll(publisher, 20) {
      match notice {
        @session.NamespaceWithdrawn(id, _) if id.to_uint64() == request_id =>
          withdrawn = true
        @session.NamespacePublicationRejected(_, _, error) =>
          fail("PUBLISH_NAMESPACE withdrawal failed: \{error.to_repr()}")
        @session.SessionEnded(_) => ended = true
        _ => ()
      }
    }
    if publisher.closed_streams.get(stream) is Some(_) {
      withdrawn = true
    }
  }
  guard withdrawn || ended else {
    fail("timed out waiting for PUBLISH_NAMESPACE withdrawal")
  }
}

///|
fn interop_subscribe_error(context : InteropContext) -> Unit raise {
  let subscriber = context.connect("subscriber")
  defer subscriber.client.close()
  let track = @protocol.TrackName::from_utf8_fields(
    ["nonexistent", "namespace"],
    "test-track",
  )
  let (request_id, actions) = subscriber.session.subscribe_track(track).unwrap()
  let request_id = request_id.to_uint64()
  let (notices, _) = drive_session_actions(
    subscriber.client,
    subscriber.session,
    actions,
    subscriber.driver,
  )
  let mut rejected = false
  for notice in notices {
    match notice {
      @session.SubscribeRejected(id, _) if id.to_uint64() == request_id =>
        rejected = true
      @session.SubscribeEstablished(id, _) if id.to_uint64() == request_id =>
        fail("SUBSCRIBE unexpectedly succeeded")
      @session.SessionEnded(end) =>
        fail("relay ended the session: \{end.to_repr()}")
      _ => ()
    }
  }
  let deadline = @env.now() + 2000UL
  while !rejected && @env.now() < deadline {
    for notice in lifecycle_poll(subscriber, 20) {
      match notice {
        @session.SubscribeRejected(id, _) if id.to_uint64() == request_id =>
          rejected = true
        @session.SubscribeEstablished(id, _) if id.to_uint64() == request_id =>
          fail("SUBSCRIBE unexpectedly succeeded")
        @session.SessionEnded(end) =>
          fail("relay ended the session: \{end.to_repr()}")
        _ => ()
      }
    }
  }
  guard rejected else { fail("timed out waiting for SUBSCRIBE error") }
}

///|
fn interop_rendezvous_timeout(context : InteropContext) -> Unit raise {
  let subscriber = context.connect("subscriber")
  defer subscriber.client.close()
  let track = @protocol.TrackName::from_utf8_fields(
    ["nonexistent", "rendezvous"],
    "test-track",
  )
  let (request_id, actions) = subscriber.session
    .subscribe_track(track, parameters=[
      @protocol.rendezvous_timeout_parameter(500UL),
    ])
    .unwrap()
  let request_id = request_id.to_uint64()
  let (notices, _) = drive_session_actions(
    subscriber.client,
    subscriber.session,
    actions,
    subscriber.driver,
  )
  let mut timed_out = false
  for notice in notices {
    match notice {
      @session.SubscribeRejected(id, error) if id.to_uint64() == request_id =>
        if error.code == 0x02UL {
          timed_out = true
        } else {
          fail("SUBSCRIBE error code was not TIMEOUT: \{error.to_repr()}")
        }
      @session.SubscribeEstablished(id, _) if id.to_uint64() == request_id =>
        fail("SUBSCRIBE unexpectedly succeeded during the rendezvous window")
      @session.SessionEnded(end) =>
        fail("relay ended the session: \{end.to_repr()}")
      _ => ()
    }
  }
  let deadline = @env.now() + 2000UL
  while !timed_out && @env.now() < deadline {
    for notice in lifecycle_poll(subscriber, 20) {
      match notice {
        @session.SubscribeRejected(id, error) if id.to_uint64() == request_id =>
          if error.code == 0x02UL {
            timed_out = true
          } else {
            fail("SUBSCRIBE error code was not TIMEOUT: \{error.to_repr()}")
          }
        @session.SubscribeEstablished(id, _) if id.to_uint64() == request_id =>
          fail("SUBSCRIBE unexpectedly succeeded during the rendezvous window")
        @session.SessionEnded(end) =>
          fail("relay ended the session: \{end.to_repr()}")
        _ => ()
      }
    }
  }
  guard timed_out else {
    fail("timed out waiting for the rendezvous TIMEOUT error")
  }
}

///|
fn interop_subscribe_namespace_lifecycle(
  context : InteropContext,
) -> Unit raise {
  let subscriber = context.connect("subscriber")
  defer subscriber.client.close()
  let publisher = context.connect("publisher")
  defer publisher.client.close()
  let run_namespace = interop_namespace("subscribe-namespace-lifecycle")
  let target = interop_child_namespace(run_namespace, "child")
  let (subscription, actions) = subscriber.session
    .subscribe_namespace(run_namespace)
    .unwrap()
  let subscription = subscription.to_uint64()
  let (notices, _) = drive_session_actions(
    subscriber.client,
    subscriber.session,
    actions,
    subscriber.driver,
  )
  let mut established = false
  for notice in notices {
    match notice {
      @session.NamespaceSubscriptionEstablished(id, _) if id.to_uint64() ==
        subscription => established = true
      @session.NamespaceSubscriptionRejected(_, _, error) =>
        fail("SUBSCRIBE_NAMESPACE rejected: \{error.to_repr()}")
      @session.SessionEnded(end) =>
        fail("relay ended the session: \{end.to_repr()}")
      _ => ()
    }
  }
  let deadline = @env.now() + 2000UL
  while !established && @env.now() < deadline {
    for notice in lifecycle_poll(subscriber, 20) {
      match notice {
        @session.NamespaceSubscriptionEstablished(id, _) if id.to_uint64() ==
          subscription => established = true
        @session.NamespaceSubscriptionRejected(_, _, error) =>
          fail("SUBSCRIBE_NAMESPACE rejected: \{error.to_repr()}")
        @session.SessionEnded(end) =>
          fail("relay ended the session: \{end.to_repr()}")
        _ => ()
      }
    }
  }
  guard established else {
    fail("timed out waiting for SUBSCRIBE_NAMESPACE establishment")
  }

  let (publication, actions) = publisher.session
    .publish_namespace(target, [])
    .unwrap()
  let publication = publication.to_uint64()
  let (notices, _) = drive_session_actions(
    publisher.client,
    publisher.session,
    actions,
    publisher.driver,
  )
  interop_wait_namespace_accepted(publisher, publication, notices, 2000UL)

  let mut discovered = false
  let deadline = @env.now() + 2000UL
  while !discovered && @env.now() < deadline {
    for notice in lifecycle_poll(subscriber, 20) {
      match notice {
        @session.NamespaceDiscovered(id, discovered_namespace) if id.to_uint64() ==
          subscription =>
          if discovered_namespace == target {
            discovered = true
          } else {
            fail("discovered an unexpected namespace")
          }
        @session.NamespaceNoLongerAvailable(id, _) if id.to_uint64() ==
          subscription => fail("namespace was withdrawn before cancellation")
        @session.SessionEnded(end) =>
          fail("relay ended the subscriber session: \{end.to_repr()}")
        _ => ()
      }
    }
    for notice in lifecycle_poll(publisher, 10) {
      match notice {
        @session.SessionEnded(end) =>
          fail("relay ended the publisher session: \{end.to_repr()}")
        @session.NamespacePublicationRejected(_, _, error) =>
          fail("PUBLISH_NAMESPACE rejected: \{error.to_repr()}")
        _ => ()
      }
    }
  }
  guard discovered else {
    fail("timed out waiting for the subscriber to discover the namespace")
  }

  interop_cancel_request(publisher, publication)
  let mut unavailable = false
  let deadline = @env.now() + 2000UL
  while !unavailable && @env.now() < deadline {
    for notice in lifecycle_poll(subscriber, 20) {
      match notice {
        @session.NamespaceNoLongerAvailable(id, discovered_namespace) if id.to_uint64() ==
          subscription =>
          if discovered_namespace == target {
            unavailable = true
          } else {
            fail("withdrew an unexpected namespace")
          }
        @session.SessionEnded(end) =>
          fail("relay ended the subscriber session: \{end.to_repr()}")
        _ => ()
      }
    }
    for notice in lifecycle_poll(publisher, 10) {
      match notice {
        // The fixed relay closes the publisher session after withdrawal.
        @session.SessionEnded(_) => ()
        _ => ()
      }
    }
  }
  guard unavailable else {
    fail("timed out waiting for NAMESPACE_DONE after withdrawal")
  }
}

///|
priv struct InteropNamespaceWatch {
  request_id : UInt64
  shared : @protocol.TrackNamespace
  sync : @protocol.TrackNamespace
  mut established : Bool
  mut shared_seen : Bool
  mut sync_seen : Bool
  mut shared_done : Bool
  mut sync_done : Bool
  mut expect_shared_done : Bool
}

///|
fn InteropNamespaceWatch::observe(
  self : InteropNamespaceWatch,
  notices : Array[@session.SessionNotice],
) -> Unit raise {
  for notice in notices {
    match notice {
      @session.NamespaceSubscriptionEstablished(id, _) if id.to_uint64() ==
        self.request_id => self.established = true
      @session.NamespaceDiscovered(id, discovered_namespace) if id.to_uint64() ==
        self.request_id =>
        if discovered_namespace == self.shared {
          self.shared_seen = true
        } else if discovered_namespace == self.sync {
          self.sync_seen = true
        } else {
          fail("discovered an unexpected namespace")
        }
      @session.NamespaceNoLongerAvailable(id, discovered_namespace) if id.to_uint64() ==
        self.request_id =>
        if discovered_namespace == self.shared {
          guard self.expect_shared_done else {
            fail(
              "relay withdrew a namespace that another publisher still advertises",
            )
          }
          self.shared_done = true
        } else if discovered_namespace == self.sync {
          self.sync_done = true
        } else {
          fail("withdrew an unexpected namespace")
        }
      @session.NamespaceSubscriptionRejected(_, _, error) =>
        fail("SUBSCRIBE_NAMESPACE rejected: \{error.to_repr()}")
      @session.SessionEnded(end) =>
        fail("relay ended the namespace subscriber session: \{end.to_repr()}")
      _ => ()
    }
  }
}

///|
fn interop_open_namespace_watch(
  context : InteropContext,
  role : String,
  prefix : @protocol.TrackNamespace,
  shared : @protocol.TrackNamespace,
  sync : @protocol.TrackNamespace,
) -> (LifecyclePeer, InteropNamespaceWatch) raise {
  let peer = context.connect(role)
  let (request_id, actions) = peer.session.subscribe_namespace(prefix).unwrap()
  let request_id = request_id.to_uint64()
  let (notices, _) = drive_session_actions(
    peer.client,
    peer.session,
    actions,
    peer.driver,
  )
  let watch : InteropNamespaceWatch = {
    request_id,
    shared,
    sync,
    established: false,
    shared_seen: false,
    sync_seen: false,
    shared_done: false,
    sync_done: false,
    expect_shared_done: false,
  }
  watch.observe(notices)
  (peer, watch)
}

///|
fn interop_publish_namespace_multi_publisher_retention(
  context : InteropContext,
) -> Unit raise {
  let deadline = @env.now() + 8000UL
  let run_prefix = interop_namespace("publish-namespace-retention")
  let shared = interop_child_namespace(run_prefix, "shared")
  let sync = interop_child_namespace(run_prefix, "sync")
  let (observer, observer_state) = interop_open_namespace_watch(
    context, "observer", run_prefix, shared, sync,
  )
  defer observer.client.close()
  while !observer_state.established && @env.now() < deadline {
    observer_state.observe(lifecycle_poll(observer, 20))
  }
  guard observer_state.established else {
    fail("timed out waiting for the observer SUBSCRIBE_NAMESPACE")
  }

  let publisher = context.connect("publisher")
  defer publisher.client.close()
  let (shared_publication, _) = interop_publish_namespace(publisher, shared)
  while !observer_state.shared_seen && @env.now() < deadline {
    observer_state.observe(lifecycle_poll(observer, 20))
    for notice in lifecycle_poll(publisher, 10) {
      match notice {
        @session.SessionEnded(end) =>
          fail("relay ended the publisher session: \{end.to_repr()}")
        _ => ()
      }
    }
  }
  guard observer_state.shared_seen else {
    fail("the observer did not discover the shared namespace")
  }

  let publisher_2 = context.connect("publisher_2")
  defer publisher_2.client.close()
  let (shared_publication_2, _) = interop_publish_namespace(publisher_2, shared)
  let (sync_publication_2, _) = interop_publish_namespace(publisher_2, sync)
  while !observer_state.sync_seen && @env.now() < deadline {
    observer_state.observe(lifecycle_poll(observer, 20))
    for notice in lifecycle_poll(publisher_2, 10) {
      match notice {
        @session.SessionEnded(end) =>
          fail("relay ended the second publisher session: \{end.to_repr()}")
        _ => ()
      }
    }
  }
  guard observer_state.sync_seen else {
    fail("the observer did not discover the synchronization namespace")
  }

  interop_cancel_request(publisher_2, shared_publication_2)
  interop_cancel_request(publisher_2, sync_publication_2)
  while !observer_state.sync_done && @env.now() < deadline {
    observer_state.observe(lifecycle_poll(observer, 20))
  }
  guard observer_state.sync_done else {
    fail(
      "the observer did not receive NAMESPACE_DONE for the synchronization namespace",
    )
  }
  let retention_deadline = @env.now() + 300UL
  while @env.now() < retention_deadline {
    observer_state.observe(lifecycle_poll(observer, 10))
  }

  let (subscriber_1, subscriber_1_state) = interop_open_namespace_watch(
    context, "subscriber_1", run_prefix, shared, sync,
  )
  defer subscriber_1.client.close()
  let (subscriber_2, subscriber_2_state) = interop_open_namespace_watch(
    context, "subscriber_2", run_prefix, shared, sync,
  )
  defer subscriber_2.client.close()
  while (!subscriber_1_state.established || !subscriber_2_state.established) &&
        @env.now() < deadline {
    subscriber_1_state.observe(lifecycle_poll(subscriber_1, 20))
    subscriber_2_state.observe(lifecycle_poll(subscriber_2, 20))
  }
  guard subscriber_1_state.established && subscriber_2_state.established else {
    fail("timed out waiting for the fan-out subscriptions")
  }
  while (!subscriber_1_state.shared_seen || !subscriber_2_state.shared_seen) &&
        @env.now() < deadline {
    subscriber_1_state.observe(lifecycle_poll(subscriber_1, 20))
    subscriber_2_state.observe(lifecycle_poll(subscriber_2, 20))
  }
  guard subscriber_1_state.shared_seen && subscriber_2_state.shared_seen else {
    fail("the fan-out subscribers did not discover the retained namespace")
  }

  observer_state.expect_shared_done = true
  interop_cancel_request(publisher, shared_publication)
  while (
          !observer_state.shared_done ||
          !subscriber_1_state.shared_done ||
          !subscriber_2_state.shared_done
        ) &&
        @env.now() < deadline {
    observer_state.observe(lifecycle_poll(observer, 20))
    subscriber_1_state.observe(lifecycle_poll(subscriber_1, 20))
    subscriber_2_state.observe(lifecycle_poll(subscriber_2, 20))
  }
  guard observer_state.shared_done &&
    subscriber_1_state.shared_done &&
    subscriber_2_state.shared_done else {
    fail("the fan-out subscribers did not observe the final withdrawal")
  }
}

///|
fn interop_announce_subscribe(context : InteropContext) -> Unit raise {
  let deadline = @env.now() + 3000UL
  let publisher = context.connect("publisher")
  defer publisher.client.close()
  let subscriber = context.connect("subscriber")
  defer subscriber.client.close()
  let track_namespace : @protocol.TrackNamespace = {
    fields: [b"moq-test", b"interop"],
  }
  let (publication, actions) = publisher.session
    .publish_namespace(track_namespace, [])
    .unwrap()
  let publication = publication.to_uint64()
  let (notices, _) = drive_session_actions(
    publisher.client,
    publisher.session,
    actions,
    publisher.driver,
  )
  interop_wait_namespace_accepted(publisher, publication, notices, 2000UL)

  let track = @protocol.TrackName::from_utf8_fields(
    ["moq-test", "interop"],
    "test-track",
  )
  let (subscription, actions) = subscriber.session
    .subscribe_track(track)
    .unwrap()
  let subscription = subscription.to_uint64()
  let (notices, _) = drive_session_actions(
    subscriber.client,
    subscriber.session,
    actions,
    subscriber.driver,
  )
  let mut subscribed = false
  for notice in notices {
    match notice {
      @session.SubscribeEstablished(id, _) if id.to_uint64() == subscription =>
        subscribed = true
      @session.SubscribeRejected(_, error) =>
        fail("SUBSCRIBE rejected: \{error.to_repr()}")
      @session.SessionEnded(end) =>
        fail("relay ended the subscriber session: \{end.to_repr()}")
      _ => ()
    }
  }
  while !subscribed && @env.now() < deadline {
    ignore(interop_service_publisher(publisher, 10))
    for notice in lifecycle_poll(subscriber, 20) {
      match notice {
        @session.SubscribeEstablished(id, _) if id.to_uint64() == subscription =>
          subscribed = true
        @session.SubscribeRejected(_, error) =>
          fail("SUBSCRIBE rejected: \{error.to_repr()}")
        @session.SessionEnded(end) =>
          fail("relay ended the subscriber session: \{end.to_repr()}")
        _ => ()
      }
    }
  }
  guard subscribed else { fail("timed out waiting for SUBSCRIBE_OK") }
}

///|
fn interop_subscribe_before_announce(context : InteropContext) -> Unit raise {
  let deadline = @env.now() + 3500UL
  let subscriber = context.connect("subscriber")
  defer subscriber.client.close()
  let track = @protocol.TrackName::from_utf8_fields(
    ["moq-test", "interop"],
    "test-track",
  )
  let (subscription, actions) = subscriber.session
    .subscribe_track(track)
    .unwrap()
  let subscription = subscription.to_uint64()
  let (notices, _) = drive_session_actions(
    subscriber.client,
    subscriber.session,
    actions,
    subscriber.driver,
  )
  let mut subscribed = false
  let mut rejected = false
  for notice in notices {
    match notice {
      @session.SubscribeEstablished(id, _) if id.to_uint64() == subscription =>
        subscribed = true
      @session.SubscribeRejected(id, _) if id.to_uint64() == subscription =>
        rejected = true
      @session.SessionEnded(end) =>
        fail("relay ended the subscriber session: \{end.to_repr()}")
      _ => ()
    }
  }
  let early_deadline = @env.now() + 500UL
  while !subscribed && !rejected && @env.now() < early_deadline {
    for notice in lifecycle_poll(subscriber, 20) {
      match notice {
        @session.SubscribeEstablished(id, _) if id.to_uint64() == subscription =>
          subscribed = true
        @session.SubscribeRejected(id, _) if id.to_uint64() == subscription =>
          rejected = true
        @session.SessionEnded(end) =>
          fail("relay ended the subscriber session: \{end.to_repr()}")
        _ => ()
      }
    }
  }

  let publisher = context.connect("publisher")
  defer publisher.client.close()
  let track_namespace : @protocol.TrackNamespace = {
    fields: [b"moq-test", b"interop"],
  }
  let (publication, actions) = publisher.session
    .publish_namespace(track_namespace, [])
    .unwrap()
  let publication = publication.to_uint64()
  let (notices, _) = drive_session_actions(
    publisher.client,
    publisher.session,
    actions,
    publisher.driver,
  )
  let mut accepted = false
  for notice in notices {
    match notice {
      @session.NamespacePublicationAccepted(id, _) if id.to_uint64() ==
        publication => accepted = true
      @session.NamespacePublicationRejected(_, _, error) =>
        fail("PUBLISH_NAMESPACE rejected: \{error.to_repr()}")
      @session.SessionEnded(end) =>
        fail("relay ended the publisher session: \{end.to_repr()}")
      _ => ()
    }
  }
  while (!accepted || (!subscribed && !rejected)) && @env.now() < deadline {
    for notice in interop_service_publisher(publisher, 10) {
      match notice {
        @session.NamespacePublicationAccepted(id, _) if id.to_uint64() ==
          publication => accepted = true
        @session.NamespacePublicationRejected(_, _, error) =>
          fail("PUBLISH_NAMESPACE rejected: \{error.to_repr()}")
        _ => ()
      }
    }
    for notice in lifecycle_poll(subscriber, 20) {
      match notice {
        @session.SubscribeEstablished(id, _) if id.to_uint64() == subscription =>
          subscribed = true
        @session.SubscribeRejected(id, _) if id.to_uint64() == subscription =>
          rejected = true
        @session.SessionEnded(end) =>
          fail("relay ended the subscriber session: \{end.to_repr()}")
        _ => ()
      }
    }
  }
  guard accepted else {
    fail("timed out waiting for PUBLISH_NAMESPACE acceptance")
  }
  guard subscribed || rejected else {
    fail("subscriber did not handle the early SUBSCRIBE gracefully")
  }
}
