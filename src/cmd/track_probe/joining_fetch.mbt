///|
priv struct JoiningFetchState {
  track : @protocol.TrackName
  publish_id : UInt64
  mut published : Bool
  mut subscribe_id : UInt64?
  aliases : Map[UInt64, UInt64]
  mut fetch_id : UInt64?
  mut upstream_fetch_id : UInt64?
  mut fetch_accepted : Bool
  mut fetched : UInt64
  mut fetch_finished : Bool
  mut fetch_rejected_as_unavailable : Bool
  mut live_received : Bool
  mut live_finished : Bool
}

///|
fn JoiningFetchState::observe_publisher(
  self : JoiningFetchState,
  peer : LifecyclePeer,
  notices : Array[@session.ClientSessionNotice],
) -> Unit raise {
  for notice in notices {
    match notice {
      @session.PublishAccepted(id, _, _) if id == self.publish_id =>
        self.published = true
      @session.PublishRejected(_, error) =>
        fail("joining FETCH PUBLISH rejected: \{error.to_repr()}")
      // moq5 forwards a SUBSCRIBE-originated joining FETCH to the origin as
      // a standalone, bounded FETCH. Serve exactly the latched history.
      @session.FetchRequested(id, _, track, start, end, _) => {
        guard self.upstream_fetch_id is None &&
          track == self.track &&
          start == { group: 0UL, object: 0UL, } &&
          end == { group: 1UL, object: 1UL, } else {
          fail(
            "joining FETCH upstream identity or resolved range did not match",
          )
        }
        self.upstream_fetch_id = Some(id)
        ignore(
          drive_session_result(
            peer.client,
            peer.session,
            peer.session.accept_fetch(id, {
              end_of_track: 0,
              end_location: end,
              parameters: [],
              track_properties: b"",
            }),
            peer.driver,
          ),
        )
        for group in [0UL, 1UL] {
          ignore(
            drive_session_result(
              peer.client,
              peer.session,
              peer.session.send_fetch_object(id, {
                group_id: group,
                subgroup_id: Some(0UL),
                object_id: 0UL,
                publisher_priority: 128,
                datagram_forwarding: false,
                properties: probe_properties(),
                payload: probe_payload(group, 0UL),
              }),
              peer.driver,
            ),
          )
        }
        ignore(
          drive_session_result(
            peer.client,
            peer.session,
            peer.session.finish_fetch(id),
            peer.driver,
          ),
        )
      }
      @session.FetchCancelled(_, _) | @session.FetchReset(_, _, _) =>
        fail("joining FETCH upstream transfer was interrupted")
      @session.SessionEnded(end) =>
        fail("joining FETCH publisher session ended: \{end.to_repr()}")
      _ => ()
    }
  }
}

///|
fn JoiningFetchState::observe_subscriber(
  self : JoiningFetchState,
  notices : Array[@session.ClientSessionNotice],
) -> Unit raise {
  for notice in notices {
    match notice {
      @session.SubscribeEstablished(id, track_alias) =>
        self.aliases[id] = track_alias
      @session.SubscribeRejected(_, error) =>
        fail("joining FETCH SUBSCRIBE rejected: \{error.to_repr()}")
      @session.SubscriptionUpdateRejected(_, _, error) =>
        fail("joining FETCH filter update rejected: \{error.to_repr()}")
      @session.ObjectReceived(id, _, header, frames) => {
        guard self.aliases.get(id) == Some(header.track_alias) else {
          fail("joining FETCH live Object alias did not match")
        }
        for frame in frames {
          guard frame.object_id == 0UL &&
            frame.status == @protocol.Normal &&
            frame.properties == probe_properties() &&
            frame.payload == probe_payload(header.group_id, 0UL) else {
            fail("joining FETCH live Object contents did not match")
          }
          guard Some(id) == self.subscribe_id &&
            header.group_id == 2UL &&
            !self.live_received &&
            self.fetch_finished else {
            fail("joining FETCH and live delivery overlapped or duplicated")
          }
          self.live_received = true
          println("Received joining live Object (group 2, object 0)")
        }
      }
      @session.GroupFinished(id, group) => {
        guard Some(id) == self.subscribe_id &&
          group == 2UL &&
          !self.live_finished else {
          fail("unexpected joining live Group FIN")
        }
        self.live_finished = true
      }
      @session.FetchAccepted(id, _) if Some(id) == self.fetch_id =>
        self.fetch_accepted = true
      @session.FetchObjectReceived(id, object) => {
        // QUIC may deliver the data stream before FETCH_OK on its bidi stream.
        guard Some(id) == self.fetch_id &&
          !self.fetch_finished &&
          self.fetched < 2UL &&
          object.group_id == self.fetched &&
          object.object_id == 0UL &&
          object.subgroup_id == Some(0UL) &&
          !object.datagram_forwarding &&
          object.properties == probe_properties() &&
          object.payload == probe_payload(self.fetched, 0UL) else {
          fail(
            "joining FETCH historical Object range, order or contents did not match",
          )
        }
        self.fetched += 1UL
        println(
          "Received joining FETCH Object (group \{object.group_id}, object 0)",
        )
      }
      @session.FetchFinished(id) if Some(id) == self.fetch_id => {
        guard self.fetch_accepted && self.fetched == 2UL && !self.fetch_finished else {
          fail("joining FETCH finished without the exact historical range")
        }
        self.fetch_finished = true
        println("Joining FETCH data FIN received")
      }
      @session.FetchRejected(id, _, error) if Some(id) == self.fetch_id => {
        guard error.code == 0x10UL else {
          fail("joining FETCH was rejected: \{error.to_repr()}")
        }
        self.fetch_rejected_as_unavailable = true
        println(
          "moq5 rejected Joining FETCH with INVALID_RANGE (0x10); no usable retained joining range was available",
        )
      }
      @session.FetchFailed(_, error) =>
        if error.code == 0x10UL {
          self.fetch_rejected_as_unavailable = true
          println(
            "moq5 rejected Joining FETCH with INVALID_RANGE (0x10); no usable retained joining range was available",
          )
        } else {
          fail("joining FETCH failed: \{error.to_repr()}")
        }
      @session.FetchRangeEnded(_, _)
      | @session.FetchEndOfTrack(_)
      | @session.FetchCancelled(_, _)
      | @session.FetchReset(_, _, _) =>
        fail("joining FETCH returned an unexpected terminal or range marker")
      @session.SessionEnded(end) =>
        fail("joining FETCH subscriber session ended: \{end.to_repr()}")
      _ => ()
    }
  }
}

///|
fn wait_joining_fetch(
  publisher : LifecyclePeer,
  subscriber : LifecyclePeer,
  state : JoiningFetchState,
  ready : () -> Bool,
  stage : String,
) -> Unit raise {
  let deadline = @env.now() + 30000UL
  while !ready() && @env.now() < deadline {
    state.observe_publisher(publisher, lifecycle_poll(publisher, 20))
    state.observe_subscriber(lifecycle_poll(subscriber, 20))
  }
  guard ready() else { fail("timed out waiting for joining FETCH \{stage}") }
}

///|
fn wait_joining_publisher(
  publisher : LifecyclePeer,
  state : JoiningFetchState,
  ready : () -> Bool,
  stage : String,
) -> Unit raise {
  let deadline = @env.now() + 30000UL
  while !ready() && @env.now() < deadline {
    state.observe_publisher(publisher, lifecycle_poll(publisher, 20))
  }
  guard ready() else { fail("timed out waiting for joining FETCH: " + stage) }
}

///|
fn send_joining_fetch_group(
  publisher : LifecyclePeer,
  publish_stream : Int64,
  group : UInt64,
) -> Int64 raise {
  let (_, opened) = drive_session_result(
    publisher.client,
    publisher.session,
    publisher.session.open_object_stream(publish_stream, {
      track_alias: 1UL,
      group_id: group,
      subgroup_id_mode: @protocol.Zero,
      publisher_priority: Some(128),
      end_of_group: true,
      first_object: true,
      has_properties: true,
    }),
    publisher.driver,
  )
  let mut object_stream : Int64? = None
  for (purpose, stream) in opened {
    match purpose {
      @session.Object(_) => object_stream = Some(stream)
      _ => ()
    }
  }
  let stream = object_stream.unwrap()
  ignore(
    drive_session_result(
      publisher.client,
      publisher.session,
      publisher.session.send_object(stream, {
        object_id: 0UL,
        status: @protocol.Normal,
        properties: probe_properties(),
        payload: probe_payload(group, 0UL),
      }),
      publisher.driver,
    ),
  )
  ignore(
    drive_session_result(
      publisher.client,
      publisher.session,
      publisher.session.finish_object_stream(stream),
      publisher.driver,
    ),
  )
  stream
}

///|
pub fn run_joining_fetch(
  host : String,
  port : Int32,
  path : String,
  insecure : Bool,
) -> Unit raise {
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
  let track = lifecycle_track("joining-fetch-\{@env.now()}")
  let (publish_id, actions) = publisher_session
    .publish_track(track, 1UL)
    .unwrap()
  let state : JoiningFetchState = {
    track,
    publish_id,
    published: false,
    subscribe_id: None,
    aliases: {},
    fetch_id: None,
    upstream_fetch_id: None,
    fetch_accepted: false,
    fetched: 0UL,
    fetch_finished: false,
    fetch_rejected_as_unavailable: false,
    live_received: false,
    live_finished: false,
  }
  let (notices, opened) = drive_session_actions(
    publisher_client,
    publisher_session,
    actions,
    publisher.driver,
  )
  state.observe_publisher(publisher, notices)
  let publish_stream = lifecycle_request_stream(opened, publish_id)
  wait_joining_publisher(
    publisher,
    state,
    fn() { state.published },
    "PUBLISH_OK",
  )
  println("Joining FETCH origin publication accepted")

  for group in [0UL, 1UL] {
    ignore(send_joining_fetch_group(publisher, publish_stream, group))
    println("Published joining FETCH history group \{group}")
  }

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
  let (subscribe_id, actions) = subscriber_session
    .subscribe_track_with_parameters(track, [
      { kind: 0x21UL, value: @protocol.Filter(@protocol.LargestObject), },
    ])
    .unwrap()
  state.subscribe_id = Some(subscribe_id)
  let (notices, _) = drive_session_actions(
    subscriber_client,
    subscriber_session,
    actions,
    subscriber.driver,
  )
  state.observe_subscriber(notices)
  wait_joining_fetch(
    publisher,
    subscriber,
    state,
    fn() { state.aliases.get(subscribe_id) is Some(_) },
    "joining SUBSCRIBE_OK",
  )
  println("Joining FETCH live subscription accepted")
  // The omitted GROUP_ORDER is ascending. The relay joins from its retained
  // history while the origin's publish streams remain unacknowledged.
  let (fetch_id, actions) = subscriber_session
    .fetch_relative(subscribe_id, 1UL, [])
    .unwrap()
  println("Joining FETCH request sent")
  state.fetch_id = Some(fetch_id)
  let (notices, _) = drive_session_actions(
    subscriber_client,
    subscriber_session,
    actions,
    subscriber.driver,
  )
  state.observe_subscriber(notices)
  wait_joining_fetch(
    publisher,
    subscriber,
    state,
    fn() { state.fetch_rejected_as_unavailable || state.fetch_finished },
    "historical FETCH response",
  )
  guard state.fetch_rejected_as_unavailable &&
    state.fetched == 0UL &&
    state.upstream_fetch_id is None &&
    !state.live_received else {
    fail("moq5 did not report its unsupported joining range consistently")
  }
  println(
    "Joining FETCH overlap remains covered by Sans-I/O; moq5 rejected this relay scenario before forwarding",
  )
}
