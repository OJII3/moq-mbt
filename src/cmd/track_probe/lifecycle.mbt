///|
priv struct LifecyclePeer {
  client : @transport.Client
  session : @session.ClientSession
  send_counts : Map[Int64, UInt64]
  closed_streams : Map[Int64, Bool]
}

///|
priv enum LifecycleStage {
  Subscribed
  ForwardingPaused
  TrackBGroup0
  ForwardingResumed
  TrackAGroup0
  TrackACancelled
  TrackBGroup1
  TrackBFinished
}

///|
priv struct LifecycleState {
  publish_a : UInt64
  publish_b : UInt64
  publish_stream_b : Int64
  subscribe_a : UInt64
  subscribe_b : UInt64
  subscribe_stream_a : Int64
  mut alias_a : UInt64?
  mut alias_b : UInt64?
  mut update_off : UInt64?
  mut update_on : UInt64?
  mut subscribed_a : Bool
  mut subscribed_b : Bool
  mut update_off_accepted : Bool
  mut update_on_accepted : Bool
  mut received_a0 : Bool
  mut received_b0 : Bool
  mut received_b1 : Bool
  mut finished_a0 : Bool
  mut finished_b0 : Bool
  mut finished_b1 : Bool
  mut cancelled_a : Bool
  mut cancel_transport_closed : Bool
  mut finished_a_streams : UInt64
  mut finished_b_streams : UInt64
  mut done_b_sent : Bool
  mut done_b_send_completed : Bool
  mut done_b_received : Bool
  object_stream_requests : Map[Int64, UInt64]
}

///|
fn LifecycleState::reached(
  self : LifecycleState,
  stage : LifecycleStage,
) -> Bool {
  match stage {
    Subscribed => self.subscribed_a && self.subscribed_b
    ForwardingPaused => self.update_off_accepted
    TrackBGroup0 =>
      self.received_b0 && self.finished_b0 && self.finished_b_streams >= 1UL
    ForwardingResumed => self.update_on_accepted
    TrackAGroup0 =>
      self.received_a0 && self.finished_a0 && self.finished_a_streams >= 1UL
    TrackACancelled => self.cancelled_a && self.cancel_transport_closed
    TrackBGroup1 =>
      self.received_b1 && self.finished_b1 && self.finished_b_streams >= 2UL
    TrackBFinished => self.done_b_received && self.done_b_send_completed
  }
}

///|
fn lifecycle_track(name : String) -> @protocol.TrackName {
  { namespace_fields: [@utf8.encode("interop")], name: @utf8.encode(name), }
}

///|
fn lifecycle_request_stream(
  opened : Array[(@session.ClientStreamPurpose, Int64)],
  request_id : UInt64,
) -> Int64 raise {
  for (purpose, stream) in opened {
    match purpose {
      @session.Request(opened_id) if opened_id == request_id => return stream
      _ => ()
    }
  }
  fail("request stream was not opened")
}

///|
fn lifecycle_poll(
  peer : LifecyclePeer,
  timeout_ms : Int32,
) -> Array[@session.ClientSessionNotice] raise {
  match peer.client.next_event(timeout_ms) {
    Connected => []
    StreamStarted(stream, direction) => {
      let (notices, _) = drive_session_result(
        peer.client,
        peer.session,
        receive_session_event(
          peer.session,
          @session.InboundStreamStarted(
            stream,
            client_stream_direction(direction),
          ),
        ),
        peer.send_counts,
      )
      notices
    }
    Data(stream, bytes) => {
      let (notices, _) = drive_session_result(
        peer.client,
        peer.session,
        receive_session_event(peer.session, @session.StreamData(stream, bytes)),
        peer.send_counts,
      )
      notices
    }
    Datagram(bytes) => {
      let (notices, _) = drive_session_result(
        peer.client,
        peer.session,
        receive_session_event(peer.session, @session.DatagramReceived(bytes)),
        peer.send_counts,
      )
      notices
    }
    StreamFinished(stream) => {
      let (notices, _) = drive_session_result(
        peer.client,
        peer.session,
        receive_session_event(peer.session, @session.StreamFinished(stream)),
        peer.send_counts,
      )
      notices
    }
    StreamClosed(stream) => {
      peer.closed_streams[stream] = true
      []
    }
    StreamReset(stream, code) => {
      let (notices, _) = drive_session_result(
        peer.client,
        peer.session,
        receive_session_event(
          peer.session,
          @session.StreamReset(stream, code.reinterpret_as_uint64()),
        ),
        peer.send_counts,
      )
      notices
    }
    SendComplete(stream, canceled) => {
      note_send_complete(peer.send_counts, stream)
      let (notices, _) = drive_session_result(
        peer.client,
        peer.session,
        receive_session_event(
          peer.session,
          @session.SendComplete(stream, canceled),
        ),
        peer.send_counts,
      )
      notices
    }
    SendStopped(stream, code) => {
      let (notices, _) = drive_session_result(
        peer.client,
        peer.session,
        receive_session_event(
          peer.session,
          @session.SendStopped(stream, code.reinterpret_as_uint64()),
        ),
        peer.send_counts,
      )
      notices
    }
    DatagramStateChanged(capability) => {
      let (notices, _) = drive_session_result(
        peer.client,
        peer.session,
        receive_session_event(
          peer.session,
          @session.DatagramSendCapabilityChanged(
            capability.enabled,
            capability.max_length,
          ),
        ),
        peer.send_counts,
      )
      notices
    }
    DatagramSendStateChanged(_, send_state) if send_state.is_final() => {
      let (notices, _) = drive_session_result(
        peer.client,
        peer.session,
        receive_session_event(peer.session, @session.DatagramSendComplete),
        peer.send_counts,
      )
      notices
    }
    DatagramSendStateChanged(_, _) => []
    Closed(code) => {
      let (notices, _) = drive_session_result(
        peer.client,
        peer.session,
        receive_session_event(
          peer.session,
          @session.PeerClosed(code.reinterpret_as_uint64()),
        ),
        peer.send_counts,
      )
      notices
    }
    TransportClosed(status) => {
      let (notices, _) = drive_session_result(
        peer.client,
        peer.session,
        receive_session_event(
          peer.session,
          @session.TransportFailed(status.to_int64()),
        ),
        peer.send_counts,
      )
      notices
    }
    QueueOverflow => fail("transport event buffer overflow")
    Timeout => {
      let (notices, _) = drive_session_result(
        peer.client,
        peer.session,
        receive_session_clock(peer.session),
        peer.send_counts,
      )
      notices
    }
  }
}

///|
fn LifecycleState::observe_publisher(
  self : LifecycleState,
  peer : LifecyclePeer,
  notices : Array[@session.ClientSessionNotice],
) -> Unit raise {
  for notice in notices {
    match notice {
      @session.PublishRejected(_, _) => fail("PUBLISH rejected")
      @session.ObjectStreamFinished(stream) =>
        match self.object_stream_requests.get(stream) {
          Some(request_id) if request_id == self.publish_a =>
            self.finished_a_streams = self.finished_a_streams + 1UL
          Some(request_id) if request_id == self.publish_b =>
            self.finished_b_streams = self.finished_b_streams + 1UL
          _ => fail("unknown lifecycle object stream finished")
        }
      @session.SendFinished(stream, canceled) if self.done_b_sent &&
        stream == self.publish_stream_b => {
        guard !canceled else { fail("track B PUBLISH_DONE send was canceled") }
        if peer.send_counts.get(stream) is None {
          self.done_b_send_completed = true
        }
      }
      @session.PublishDone(request_id, done) if request_id == self.publish_b => {
        guard done.stream_count == 2UL else {
          fail("track B PUBLISH_DONE reported an unexpected stream count")
        }
      }
      @session.SessionEnded(_) => fail("publisher session ended")
      _ => ()
    }
  }
}

///|
fn wait_lifecycle_publishes(
  publisher : LifecyclePeer,
  publish_a : UInt64,
  publish_b : UInt64,
) -> Unit raise {
  let mut accepted_a = false
  let mut accepted_b = false
  let deadline = @env.now() + 30000UL
  while !(accepted_a && accepted_b) && @env.now() < deadline {
    for notice in lifecycle_poll(publisher, 20) {
      match notice {
        @session.PublishAccepted(request_id) if request_id == publish_a =>
          accepted_a = true
        @session.PublishAccepted(request_id) if request_id == publish_b =>
          accepted_b = true
        @session.PublishRejected(_, _) => fail("PUBLISH rejected")
        @session.SessionEnded(_) => fail("publisher session ended")
        _ => ()
      }
    }
  }
  guard accepted_a && accepted_b else {
    fail("timed out waiting for both PUBLISH_OK responses")
  }
}

///|
fn LifecycleState::observe_subscriber(
  self : LifecycleState,
  notices : Array[@session.ClientSessionNotice],
) -> Unit raise {
  for notice in notices {
    match notice {
      @session.SubscribeEstablished(request_id, track_alias_value) if request_id ==
        self.subscribe_a => {
        self.alias_a = Some(track_alias_value)
        self.subscribed_a = true
        eprintln("SUBSCRIBE track A accepted (alias \{track_alias_value})")
      }
      @session.SubscribeEstablished(request_id, track_alias_value) if request_id ==
        self.subscribe_b => {
        self.alias_b = Some(track_alias_value)
        self.subscribed_b = true
        eprintln("SUBSCRIBE track B accepted (alias \{track_alias_value})")
      }
      @session.SubscribeRejected(_, _) => fail("SUBSCRIBE rejected")
      @session.SubscriptionUpdateAccepted(request_id, update_id, _) if request_id ==
        self.subscribe_a =>
        if self.update_off == Some(update_id) {
          self.update_off_accepted = true
          println("REQUEST_UPDATE FORWARD=0 accepted")
        } else if self.update_on == Some(update_id) {
          self.update_on_accepted = true
          println("REQUEST_UPDATE FORWARD=1 accepted")
        } else {
          fail("unexpected track A update response")
        }
      @session.SubscriptionUpdateRejected(_, _, _) =>
        fail("REQUEST_UPDATE rejected")
      @session.ObjectReceived(request_id, header, frames) => {
        let is_a = request_id == self.subscribe_a
        let track_alias_value = if is_a {
          self.alias_a.unwrap()
        } else if request_id == self.subscribe_b {
          self.alias_b.unwrap()
        } else {
          fail("Object arrived on an unknown subscription")
        }
        guard header.track_alias == track_alias_value && frames.length() == 1 else {
          fail("lifecycle Object alias or frame count did not match")
        }
        let frame = frames[0]
        guard frame.object_id == 0UL &&
          frame.status == @protocol.Normal &&
          frame.properties.is_empty() else {
          fail("lifecycle Object fields did not match")
        }
        if is_a && header.group_id == 0UL {
          guard frame.payload == @utf8.encode("lifecycle-a-group-0") else {
            fail("track A group 0 payload did not match")
          }
          self.received_a0 = true
          println("Received track A group 0 after forwarding resumed")
        } else if !is_a && header.group_id == 0UL {
          guard frame.payload == @utf8.encode("lifecycle-b-group-0") else {
            fail("track B group 0 payload did not match")
          }
          self.received_b0 = true
          println("Received track B group 0 while track A was paused")
        } else if !is_a && header.group_id == 1UL {
          guard frame.payload == @utf8.encode("lifecycle-b-group-1") else {
            fail("track B group 1 payload did not match")
          }
          self.received_b1 = true
          println("Received track B group 1 after track A cancellation")
        } else {
          fail("received an unexpected lifecycle Object")
        }
      }
      @session.GroupFinished(request_id, group_id) =>
        if request_id == self.subscribe_a && group_id == 0UL {
          self.finished_a0 = true
        } else if request_id == self.subscribe_b && group_id == 0UL {
          self.finished_b0 = true
        } else if request_id == self.subscribe_b && group_id == 1UL {
          self.finished_b1 = true
        } else {
          fail("received an unexpected lifecycle Group end")
        }
      @session.SubscriptionEnded(request_id, _) if request_id ==
        self.subscribe_a => {
        self.cancelled_a = true
        println("Track A subscription cancelled")
      }
      @session.PublishDone(request_id, done) if request_id == self.subscribe_b => {
        guard done.stream_count == 2UL else {
          fail("track B PUBLISH_DONE reported an unexpected stream count")
        }
        self.done_b_received = true
        println("Verified track B PUBLISH_DONE (2 object streams)")
      }
      @session.SessionEnded(_) => fail("subscriber session ended")
      _ => ()
    }
  }
}

///|
fn wait_lifecycle_stage(
  publisher : LifecyclePeer,
  subscriber : LifecyclePeer,
  state : LifecycleState,
  stage : LifecycleStage,
) -> Unit raise {
  let deadline = @env.now() + 30000UL
  while !state.reached(stage) && @env.now() < deadline {
    state.observe_publisher(publisher, lifecycle_poll(publisher, 20))
    if !state.reached(stage) {
      state.observe_subscriber(lifecycle_poll(subscriber, 20))
      if subscriber.closed_streams.get(state.subscribe_stream_a) is Some(_) &&
        !state.cancel_transport_closed {
        state.cancel_transport_closed = true
        println("Track A request stream shutdown completed")
      }
    }
  }
  guard state.reached(stage) else {
    fail("timed out waiting for lifecycle stage")
  }
}

///|
fn send_lifecycle_group(
  peer : LifecyclePeer,
  state : LifecycleState,
  request_id : UInt64,
  publish_stream : Int64,
  track_alias_value : UInt64,
  group_id : UInt64,
  payload : Bytes,
) -> Unit raise {
  let header : @protocol.SubgroupHeader = {
    track_alias: track_alias_value,
    group_id,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group: true,
    first_object: true,
    has_properties: false,
  }
  let (_, opened) = drive_session_result(
    peer.client,
    peer.session,
    peer.session.open_object_stream(publish_stream, header),
    peer.send_counts,
  )
  let mut object_stream : Int64? = None
  for (purpose, stream) in opened {
    match purpose {
      @session.Object(_) => object_stream = Some(stream)
      _ => ()
    }
  }
  let object_stream = object_stream.unwrap()
  state.object_stream_requests[object_stream] = request_id
  let frame : @protocol.SubgroupFrame = {
    object_id: 0UL,
    status: @protocol.Normal,
    properties: b"",
    payload,
  }
  ignore(
    drive_session_result(
      peer.client,
      peer.session,
      peer.session.send_object(object_stream, frame),
      peer.send_counts,
    ),
  )
  ignore(
    drive_session_result(
      peer.client,
      peer.session,
      peer.session.finish_object_stream(object_stream),
      peer.send_counts,
    ),
  )
}

///|
pub fn run_lifecycle(
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
    send_counts: Map([]),
    closed_streams: {},
  }
  let (publish_a, publish_a_actions) = publisher_session
    .publish_track(lifecycle_track("lifecycle-a"), 1UL)
    .unwrap()
  let (_, publish_a_opened) = drive_session_actions(
    publisher_client,
    publisher_session,
    publish_a_actions,
    publisher.send_counts,
  )
  let publish_stream_a = lifecycle_request_stream(publish_a_opened, publish_a)
  let (publish_b, publish_b_actions) = publisher_session
    .publish_track(lifecycle_track("lifecycle-b"), 2UL)
    .unwrap()
  let (_, publish_b_opened) = drive_session_actions(
    publisher_client,
    publisher_session,
    publish_b_actions,
    publisher.send_counts,
  )
  let publish_stream_b = lifecycle_request_stream(publish_b_opened, publish_b)
  wait_lifecycle_publishes(publisher, publish_a, publish_b)
  println("PUBLISH tracks A and B accepted")
  let (subscriber_client, subscriber_session) = setup_client(
    host, port, path, insecure,
  )
  defer subscriber_client.close()
  let subscriber : LifecyclePeer = {
    client: subscriber_client,
    session: subscriber_session,
    send_counts: Map([]),
    closed_streams: {},
  }
  let (subscribe_a, subscribe_a_actions) = subscriber_session
    .subscribe_track(lifecycle_track("lifecycle-a"))
    .unwrap()
  let (_, subscribe_a_opened) = drive_session_actions(
    subscriber_client,
    subscriber_session,
    subscribe_a_actions,
    subscriber.send_counts,
  )
  let subscribe_stream_a = lifecycle_request_stream(
    subscribe_a_opened, subscribe_a,
  )
  let (subscribe_b, subscribe_b_actions) = subscriber_session
    .subscribe_track(lifecycle_track("lifecycle-b"))
    .unwrap()
  ignore(
    drive_session_actions(
      subscriber_client,
      subscriber_session,
      subscribe_b_actions,
      subscriber.send_counts,
    ),
  )
  let state : LifecycleState = {
    publish_a,
    publish_b,
    publish_stream_b,
    subscribe_a,
    subscribe_b,
    subscribe_stream_a,
    alias_a: None,
    alias_b: None,
    update_off: None,
    update_on: None,
    subscribed_a: false,
    subscribed_b: false,
    update_off_accepted: false,
    update_on_accepted: false,
    received_a0: false,
    received_b0: false,
    received_b1: false,
    finished_a0: false,
    finished_b0: false,
    finished_b1: false,
    cancelled_a: false,
    cancel_transport_closed: false,
    finished_a_streams: 0UL,
    finished_b_streams: 0UL,
    done_b_sent: false,
    done_b_send_completed: false,
    done_b_received: false,
    object_stream_requests: {},
  }
  wait_lifecycle_stage(publisher, subscriber, state, Subscribed)
  println("SUBSCRIBE tracks A and B accepted")

  let (update_off, update_off_actions) = subscriber_session
    .update_subscription(subscribe_a, [
      { kind: 0x10UL, value: @protocol.Byte(0), },
    ])
    .unwrap()
  state.update_off = Some(update_off)
  let (update_off_notices, _) = drive_session_result(
    subscriber_client,
    subscriber_session,
    Ok(update_off_actions),
    subscriber.send_counts,
  )
  state.observe_subscriber(update_off_notices)
  wait_lifecycle_stage(publisher, subscriber, state, ForwardingPaused)

  send_lifecycle_group(
    publisher,
    state,
    publish_b,
    publish_stream_b,
    2UL,
    0UL,
    @utf8.encode("lifecycle-b-group-0"),
  )
  wait_lifecycle_stage(publisher, subscriber, state, TrackBGroup0)

  let (update_on, update_on_actions) = subscriber_session
    .update_subscription(subscribe_a, [
      { kind: 0x10UL, value: @protocol.Byte(1), },
    ])
    .unwrap()
  state.update_on = Some(update_on)
  let (update_on_notices, _) = drive_session_result(
    subscriber_client,
    subscriber_session,
    Ok(update_on_actions),
    subscriber.send_counts,
  )
  state.observe_subscriber(update_on_notices)
  wait_lifecycle_stage(publisher, subscriber, state, ForwardingResumed)

  send_lifecycle_group(
    publisher,
    state,
    publish_a,
    publish_stream_a,
    1UL,
    0UL,
    @utf8.encode("lifecycle-a-group-0"),
  )
  wait_lifecycle_stage(publisher, subscriber, state, TrackAGroup0)

  let cancel_actions = match subscriber_session.cancel_request(subscribe_a) {
    Ok(actions) => actions
    Err(error) => fail("track A cancellation failed: \{error.to_repr()}")
  }
  let (cancel_notices, _) = drive_session_result(
    subscriber_client,
    subscriber_session,
    Ok(cancel_actions),
    subscriber.send_counts,
  )
  state.observe_subscriber(cancel_notices)
  wait_lifecycle_stage(publisher, subscriber, state, TrackACancelled)

  send_lifecycle_group(
    publisher,
    state,
    publish_b,
    publish_stream_b,
    2UL,
    1UL,
    @utf8.encode("lifecycle-b-group-1"),
  )
  wait_lifecycle_stage(publisher, subscriber, state, TrackBGroup1)
  let done_actions = publisher_session.publish_done(publish_stream_b, 3UL, b"")
  state.done_b_sent = true
  let (done_notices, _) = drive_session_result(
    publisher_client,
    publisher_session,
    done_actions,
    publisher.send_counts,
  )
  state.observe_publisher(publisher, done_notices)
  wait_lifecycle_stage(publisher, subscriber, state, TrackBFinished)
  println("Lifecycle and independent-track scenario passed")
}
