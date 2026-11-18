///|
/// Fetch a retained group and then join the live track. The publisher sends
/// the fetched groups before the FETCH, and the remaining groups only after it
/// has served that FETCH.
pub fn run_late_join(
  host : String,
  port : Int32,
  path : String,
  namespace_field : String,
  track_name : String,
  insecure : Bool,
) -> Unit raise {
  let track = @protocol.TrackName::{
    namespace_fields: [@utf8.encode(namespace_field)],
    name: @utf8.encode(track_name),
  }
  let (client, session) = setup_client(host, port, path, insecure)
  defer client.close()
  let peer = LifecyclePeer::{
    client,
    session,
    driver: SessionActionDriver::new(),
    closed_streams: {},
  }
  let (fetch_id, fetch_actions) = session
    .fetch_track(
      track,
      { group: 0UL, object: 0UL, },
      { group: 1UL, object: 2UL, },
      [],
    )
    .unwrap()
  ignore(drive_session_actions(client, session, fetch_actions, peer.driver))
  println("Late-join FETCH request sent")

  let mut fetch_accepted = false
  let mut fetch_objects = 0UL
  let mut fetch_finished = false
  let fetch_deadline = @env.now() + 30000UL
  while !fetch_finished && @env.now() < fetch_deadline {
    for notice in lifecycle_poll(peer, 100) {
      match notice {
        FetchAccepted(id, _) if id == fetch_id => fetch_accepted = true
        FetchResponseAccepted(id, _, response) if id == fetch_id => {
          guard response.end_location.group == 0UL &&
            response.end_location.object == 2UL else {
            fail("late-join FETCH_OK returned an unexpected end location")
          }
          guard response.end_of_track == 0 else {
            fail("late-join FETCH_OK reported end-of-track")
          }
        }
        FetchRejected(id, _, error) if id == fetch_id =>
          fail("late-join FETCH rejected (code \{error.code})")
        FetchObjectReceived(id, object) if id == fetch_id => {
          guard fetch_accepted else {
            fail("late-join FETCH Object arrived before FETCH_OK")
          }
          guard object.group_id == 0UL &&
            object.object_id == fetch_objects &&
            object.properties == probe_properties() &&
            object.payload == probe_payload(0UL, object.object_id) else {
            fail("late-join FETCH Object did not match group 0")
          }
          fetch_objects = fetch_objects + 1UL
        }
        FetchFinished(id) if id == fetch_id => fetch_finished = true
        SessionEnded(end) => fail("session ended: \{end.to_repr()}")
        _ => ()
      }
    }
  }
  guard fetch_finished && fetch_objects == 2UL else {
    fail("late-join FETCH did not return both retained Objects")
  }
  println("Fetched group 0; joining the live track for group 1")

  let (subscribe_id, subscribe_actions) = session
    .subscribe_track(track)
    .unwrap()
  let (notices, _) = drive_session_actions(
    client,
    session,
    subscribe_actions,
    peer.driver,
  )
  let mut subscribed = false
  let mut live_alias : UInt64? = None
  let mut live_objects = 0UL
  let mut group_finished = false
  let mut done = false
  for notice in notices {
    match notice {
      SubscribeEstablished(id, track_alias_value) if id == subscribe_id => {
        subscribed = true
        live_alias = Some(track_alias_value.to_uint64())
      }
      SubscribeRejected(_, error) =>
        fail("late-join SUBSCRIBE rejected (code \{error.code})")
      SessionEnded(end) => fail("session ended: \{end.to_repr()}")
      _ => ()
    }
  }
  let live_deadline = @env.now() + 30000UL
  while !done && @env.now() < live_deadline {
    for notice in lifecycle_poll(peer, 100) {
      match notice {
        SubscribeEstablished(id, track_alias_value) if id == subscribe_id => {
          subscribed = true
          live_alias = Some(track_alias_value.to_uint64())
        }
        SubscribeRejected(_, error) =>
          fail("late-join SUBSCRIBE rejected (code \{error.code})")
        ObjectReceived(id, _, header, frames) if id == subscribe_id => {
          guard subscribed && live_alias == Some(header.track_alias) else {
            fail("late-join Object arrived before SUBSCRIBE_OK")
          }
          // The relay may replay the retained group to the late subscriber.
          // The FETCH already verified group 0, so only group 1 is counted.
          if header.group_id == 0UL {
            continue
          }
          guard header.group_id == 1UL else {
            fail("late-join live Object used group \{header.group_id}")
          }
          for frame in frames {
            guard frame.object_id == live_objects &&
              frame.properties == probe_properties() &&
              frame.payload == probe_payload(1UL, frame.object_id) else {
              fail("late-join live Object did not match group 1")
            }
            live_objects = live_objects + 1UL
          }
        }
        GroupFinished(id, group_id) if id == subscribe_id => {
          if group_id == 0UL {
            continue
          }
          guard group_id == 1UL && live_objects == 2UL else {
            fail("late-join live group ended before both Objects")
          }
          group_finished = true
        }
        PublishDone(id, _) if id == subscribe_id => done = true
        SessionEnded(end) => fail("session ended: \{end.to_repr()}")
        _ => ()
      }
    }
  }
  guard subscribed && group_finished && live_objects == 2UL && done else {
    fail("late-join live subscription did not complete")
  }
  println("Verified FETCH for group 0 and live delivery for group 1")
}
