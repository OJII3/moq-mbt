///|
priv enum NamespaceProbeStage {
  Established
  Discovered
  Withdrawn
  Ended
}

///|
priv struct NamespaceProbeState {
  subscription : UInt64
  subscription_stream : Int64
  track_namespace : @protocol.TrackNamespace
  independent_publish : UInt64
  independent_subscribe : UInt64?
  mut independent_alias : UInt64?
  mut independent_published : Bool
  mut independent_subscribed : Bool
  mut independent_object_received : Bool
  mut independent_group_finished : Bool
  mut publication : UInt64?
  mut publication_stream : Int64?
  mut established : Bool
  mut accepted : Bool
  mut discovered : Bool
  mut cancelling_publication : Bool
  mut withdrawn : Bool
  mut unavailable : Bool
  mut cancelling_subscription : Bool
  mut subscriber_session_ended : Bool
  mut publisher_session_ended : Bool
  mut ended : Bool
}

///|
fn NamespaceProbeState::observe(
  self : NamespaceProbeState,
  notices : Array[@session.ClientSessionNotice],
  publisher : Bool,
) -> Unit raise {
  for notice in notices {
    match notice {
      @session.PublishAccepted(id) if publisher &&
        id == self.independent_publish => self.independent_published = true
      @session.SubscribeEstablished(id, track_alias) if !publisher &&
        Some(id) == self.independent_subscribe => {
        self.independent_alias = Some(track_alias)
        self.independent_subscribed = true
      }
      @session.ObjectReceived(id, header, frames) if !publisher &&
        Some(id) == self.independent_subscribe => {
        guard Some(header.track_alias) == self.independent_alias &&
          header.group_id == 0UL &&
          !self.independent_object_received else {
          fail(
            "independent track Object did not match after Namespace cancellation",
          )
        }
        for frame in frames {
          guard frame.object_id == 0UL &&
            frame.properties == probe_properties() &&
            frame.payload == probe_payload(0UL, 0UL) else {
            fail("independent track Object contents did not match")
          }
        }
        self.independent_object_received = true
        println(
          "Received independent track group 0 after Namespace cancellation",
        )
      }
      @session.GroupFinished(id, group) if !publisher &&
        Some(id) == self.independent_subscribe => {
        guard group == 0UL && self.independent_object_received else {
          fail("independent track Group finished before its Object")
        }
        self.independent_group_finished = true
      }
      @session.NamespaceSubscriptionEstablished(id, stream) if !publisher &&
        id == self.subscription => {
        guard stream == self.subscription_stream && !self.established else {
          fail("unexpected Namespace subscription establishment")
        }
        self.established = true
        println("SUBSCRIBE_NAMESPACE established")
      }
      @session.NamespacePublicationAccepted(id, stream) if publisher &&
        Some(id) == self.publication => {
        guard Some(stream) == self.publication_stream && !self.accepted else {
          fail("unexpected Namespace publication acceptance")
        }
        self.accepted = true
        println("PUBLISH_NAMESPACE accepted")
      }
      @session.NamespaceDiscovered(id, track_namespace) if !publisher &&
        id == self.subscription => {
        guard self.established &&
          track_namespace == self.track_namespace &&
          !self.discovered else {
          fail("unexpected Namespace discovery")
        }
        self.discovered = true
        println("NAMESPACE discovered with matching prefix and suffix")
      }
      @session.NamespaceWithdrawn(id, stream) if publisher &&
        Some(id) == self.publication => {
        guard self.cancelling_publication &&
          Some(stream) == self.publication_stream else {
          fail("Namespace publication ended before cancellation")
        }
        self.withdrawn = true
        println("Namespace publication cancelled")
      }
      @session.NamespaceNoLongerAvailable(id, track_namespace) if !publisher &&
        id == self.subscription => {
        // Cancellation also clears discoveries locally. Require this notice
        // while discovery is still active, before cancelling the subscription.
        guard self.cancelling_publication &&
          !self.cancelling_subscription &&
          self.discovered &&
          track_namespace == self.track_namespace &&
          !self.unavailable else {
          fail("unexpected Namespace removal")
        }
        self.unavailable = true
        println("NAMESPACE_DONE received for withdrawn namespace")
      }
      @session.NamespaceSubscriptionEnded(id, stream) if !publisher &&
        id == self.subscription => {
        guard self.cancelling_subscription && stream == self.subscription_stream else {
          fail("Namespace subscription ended before cancellation")
        }
        self.ended = true
        println("Namespace subscription cancelled")
      }
      @session.NamespacePublicationRejected(_, _, error) =>
        fail("PUBLISH_NAMESPACE rejected (code \{error.code})")
      @session.NamespaceSubscriptionRejected(_, _, error) =>
        fail("SUBSCRIBE_NAMESPACE rejected (code \{error.code})")
      @session.NamespaceAcceptanceRevoked(_, _) =>
        fail("Namespace publication acceptance revoked")
      @session.SessionEnded(end) =>
        if publisher && self.cancelling_publication && self.withdrawn {
          self.publisher_session_ended = true
          println(
            "moqtail closed the Namespace publisher session after withdrawal ({end.to_repr()})",
          )
        } else if !publisher && self.cancelling_subscription {
          self.subscriber_session_ended = true
          self.ended = true
          println(
            "moqtail closed the Namespace subscriber session after cancellation ({end.to_repr()})",
          )
        } else {
          let peer_role = if publisher { "publisher" } else { "subscriber" }
          fail("Namespace probe \{peer_role} session ended: \{end.to_repr()}")
        }
      _ => ()
    }
  }
}

///|
fn NamespaceProbeState::reached(
  self : NamespaceProbeState,
  publisher : LifecyclePeer,
  subscriber : LifecyclePeer,
  stage : NamespaceProbeStage,
) -> Bool {
  match stage {
    Established => self.established
    Discovered => self.accepted && self.discovered
    Withdrawn =>
      self.withdrawn &&
      self.unavailable &&
      publisher.closed_streams.get(self.publication_stream.unwrap()) is Some(_)
    Ended =>
      self.ended &&
      (
        subscriber.closed_streams.get(self.subscription_stream) is Some(_) ||
        self.subscriber_session_ended
      )
  }
}

///|
fn wait_namespace_stage(
  publisher : LifecyclePeer,
  subscriber : LifecyclePeer,
  state : NamespaceProbeState,
  stage : NamespaceProbeStage,
) -> Unit raise {
  let deadline = @env.now() + 30000UL
  while !state.reached(publisher, subscriber, stage) && @env.now() < deadline {
    state.observe(lifecycle_poll(publisher, 20), true)
    state.observe(lifecycle_poll(subscriber, 20), false)
  }
  guard state.reached(publisher, subscriber, stage) else {
    let description = match stage {
      Established => "subscription establishment"
      Discovered => "publication acceptance and discovery"
      Withdrawn => "withdrawal notification and publication stream shutdown"
      Ended => "subscription ending and stream shutdown"
    }
    fail("timed out waiting for Namespace \{description}")
  }
}

///|
pub fn run_namespace(
  host : String,
  port : Int32,
  path : String,
  insecure : Bool,
) -> Unit raise {
  let (subscriber_client, subscriber_session) = setup_client(
    host, port, path, insecure,
  )
  defer subscriber_client.close()
  let subscriber : LifecyclePeer = {
    client: subscriber_client,
    session: subscriber_session,
    driver: SessionActionDriver::new(),
    closed_streams: {},
  }
  let (publisher_client, publisher_session) = setup_client(
    host, port, path, insecure,
  )
  defer publisher_client.close()
  let publisher : LifecyclePeer = {
    client: publisher_client,
    session: publisher_session,
    driver: SessionActionDriver::new(),
    closed_streams: {},
  }
  let prefix : @protocol.TrackNamespace = {
    fields: [b"interop", @utf8.encode("namespace-\{@env.now()}")],
  }
  let track_namespace : @protocol.TrackNamespace = {
    fields: prefix.fields + [b"child"],
  }
  let independent_track = lifecycle_track("namespace-independent")
  let (independent_publish, publish_actions) = publisher_session
    .publish_track(independent_track, 7UL)
    .unwrap()
  let (publish_notices, _) = drive_session_actions(
    publisher_client,
    publisher_session,
    publish_actions,
    publisher.driver,
  )
  let (subscription, actions) = subscriber_session
    .subscribe_namespace(prefix)
    .unwrap()
  let (notices, opened) = drive_session_actions(
    subscriber_client,
    subscriber_session,
    actions,
    subscriber.driver,
  )
  let (independent_subscribe, subscribe_actions) = subscriber_session
    .subscribe_track(independent_track)
    .unwrap()
  let (subscribe_notices, _) = drive_session_actions(
    subscriber_client,
    subscriber_session,
    subscribe_actions,
    subscriber.driver,
  )
  let state : NamespaceProbeState = {
    subscription,
    subscription_stream: lifecycle_request_stream(opened, subscription),
    track_namespace,
    independent_publish,
    independent_subscribe: Some(independent_subscribe),
    independent_alias: None,
    independent_published: false,
    independent_subscribed: false,
    independent_object_received: false,
    independent_group_finished: false,
    publication: None,
    publication_stream: None,
    established: false,
    accepted: false,
    discovered: false,
    cancelling_publication: false,
    withdrawn: false,
    unavailable: false,
    cancelling_subscription: false,
    subscriber_session_ended: false,
    publisher_session_ended: false,
    ended: false,
  }
  state.observe(publish_notices, true)
  state.observe(subscribe_notices, false)
  state.observe(notices, false)
  wait_namespace_stage(publisher, subscriber, state, Established)
  let deadline = @env.now() + 30000UL
  while (!state.independent_published || !state.independent_subscribed) &&
        @env.now() < deadline {
    state.observe(lifecycle_poll(publisher, 20), true)
    state.observe(lifecycle_poll(subscriber, 20), false)
  }
  guard state.independent_published && state.independent_subscribed else {
    fail("independent track publication or subscription was not established")
  }

  let (publication, actions) = publisher_session
    .publish_namespace(track_namespace, [])
    .unwrap()
  state.publication = Some(publication)
  let (notices, opened) = drive_session_actions(
    publisher_client,
    publisher_session,
    actions,
    publisher.driver,
  )
  state.publication_stream = Some(lifecycle_request_stream(opened, publication))
  state.observe(notices, true)
  wait_namespace_stage(publisher, subscriber, state, Discovered)

  state.cancelling_publication = true
  let (notices, _) = drive_session_result(
    publisher_client,
    publisher_session,
    publisher_session.cancel_request(publication),
    publisher.driver,
  )
  state.observe(notices, true)
  wait_namespace_stage(publisher, subscriber, state, Withdrawn)
  println("Namespace publication request stream shutdown completed")
  if state.publisher_session_ended {
    println("moqtail closed the publisher session after Namespace withdrawal")
  }

  state.cancelling_subscription = true
  let (notices, _) = drive_session_result(
    subscriber_client,
    subscriber_session,
    subscriber_session.cancel_request(subscription),
    subscriber.driver,
  )
  state.observe(notices, false)
  wait_namespace_stage(publisher, subscriber, state, Ended)
  if subscriber.closed_streams.get(state.subscription_stream) is Some(_) {
    println("Namespace subscription request stream shutdown completed")
  } else {
    println(
      "Namespace subscriber session closed after subscription cancellation",
    )
  }
  if state.subscriber_session_ended {
    println("moqtail closed the subscriber session after Namespace cancellation")
    fail(
      "Namespace cancellation closed the session before independent track continuation",
    )
  }
  // Reconnect the independent track publisher after Namespace withdrawal and
  // verify that the separate track subscription remains active.
  let (continuation_client, continuation_session) = setup_client(
    host, port, path, insecure,
  )
  defer continuation_client.close()
  let continuation_publisher : LifecyclePeer = {
    client: continuation_client,
    session: continuation_session,
    driver: SessionActionDriver::new(),
    closed_streams: {},
  }
  let (continuation_publish, continuation_actions) = continuation_session
    .publish_track(independent_track, 7UL)
    .unwrap()
  let (continuation_notices, continuation_opened) = drive_session_actions(
    continuation_client,
    continuation_session,
    continuation_actions,
    continuation_publisher.driver,
  )
  let continuation_stream = lifecycle_request_stream(
    continuation_opened, continuation_publish,
  )
  let mut continuation_accepted = false
  for notice in continuation_notices {
    match notice {
      @session.PublishAccepted(id) if id == continuation_publish =>
        continuation_accepted = true
      @session.PublishRejected(_, error) =>
        fail("independent track republish rejected: \{error.to_repr()}")
      _ => ()
    }
  }
  let accept_deadline = @env.now() + 30000UL
  while !continuation_accepted && @env.now() < accept_deadline {
    for notice in lifecycle_poll(continuation_publisher, 20) {
      match notice {
        @session.PublishAccepted(id) if id == continuation_publish =>
          continuation_accepted = true
        @session.PublishRejected(_, error) =>
          fail("independent track republish rejected: \{error.to_repr()}")
        _ => ()
      }
    }
  }
  guard continuation_accepted else {
    fail("independent track republish was not accepted")
  }
  let (_, object_streams) = drive_session_result(
    continuation_client,
    continuation_session,
    continuation_session.open_object_stream(continuation_stream, {
      track_alias: 7UL,
      group_id: 0UL,
      subgroup_id_mode: @protocol.Zero,
      publisher_priority: Some(128),
      end_of_group: true,
      first_object: true,
      has_properties: true,
    }),
    continuation_publisher.driver,
  )
  let mut object_stream : Int64? = None
  for (purpose, stream) in object_streams {
    match purpose {
      @session.Object(_) => object_stream = Some(stream)
      _ => ()
    }
  }
  let object_stream = object_stream.unwrap()
  ignore(
    drive_session_result(
      continuation_client,
      continuation_session,
      continuation_session.send_object(object_stream, {
        object_id: 0UL,
        status: @protocol.Normal,
        properties: probe_properties(),
        payload: probe_payload(0UL, 0UL),
      }),
      continuation_publisher.driver,
    ),
  )
  ignore(
    drive_session_result(
      continuation_client,
      continuation_session,
      continuation_session.finish_object_stream(object_stream),
      continuation_publisher.driver,
    ),
  )
  let deadline = @env.now() + 30000UL
  while (
          !state.independent_object_received ||
          !state.independent_group_finished
        ) &&
        @env.now() < deadline {
    state.observe(lifecycle_poll(continuation_publisher, 20), true)
    state.observe(lifecycle_poll(subscriber, 20), false)
  }
  guard state.independent_object_received && state.independent_group_finished else {
    fail(
      "independent track did not continue after Namespace subscription ended",
    )
  }
  println("Independent track continued after Namespace subscription ended")
  println(
    "Verified Namespace discovery, withdrawal, and subscription cancellation",
  )
}
