///|
pub fn run_fetch(
  host : String,
  port : Int32,
  path : String,
  namespace_field : String,
  track_name : String,
  start_group : UInt64,
  end_group : UInt64,
  insecure : Bool,
) -> Unit raise {
  guard start_group < end_group else {
    fail("START_GROUP must be less than END_GROUP")
  }
  let track : @protocol.TrackName = {
    namespace_fields: [@utf8.encode(namespace_field)],
    name: @utf8.encode(track_name),
  }
  let (client, session) = setup_client(host, port, path, insecure)
  defer client.close()
  let peer : LifecyclePeer = {
    client,
    session,
    driver: SessionActionDriver::new(),
    closed_streams: {},
  }
  let (request_id, actions) = session
    .fetch_track(
      track,
      { group: start_group, object: 0UL, },
      { group: end_group, object: 0UL, },
      [],
    )
    .unwrap()
  ignore(drive_session_actions(client, session, actions, peer.driver))
  println("FETCH request sent")

  let mut expected_group = start_group
  let mut expected_object = 0UL
  let mut received = 0UL
  let mut accepted = false
  let mut finished = false
  let deadline = @env.now() + 30000UL
  while !finished && @env.now() < deadline {
    let notices = lifecycle_poll(peer, 100)
    for notice in notices {
      match notice {
        @session.FetchAccepted(id, _) if id == request_id => {
          accepted = true
          println("FETCH_OK received")
        }
        @session.FetchResponseAccepted(id, _, response) if id == request_id => {
          guard response.end_location.group == end_group &&
            response.end_location.object == 0UL else {
            fail(
              "FETCH_OK end location did not match the requested terminal group",
            )
          }
          guard response.end_of_track == 0 else {
            fail("unexpected FETCH_OK end-of-track flag from the pinned relay")
          }
          println(
            "Verified FETCH_OK end location (group \{response.end_location.group}, object \{response.end_location.object}, end-of-track \{response.end_of_track})",
          )
        }
        @session.FetchRejected(id, _, error) if id == request_id =>
          fail("FETCH rejected (code \{error.code})")
        @session.FetchObjectReceived(id, object) if id == request_id => {
          guard accepted else { fail("FETCH Object arrived before FETCH_OK") }
          guard object.group_id == expected_group &&
            object.object_id == expected_object else {
            fail(
              "unexpected FETCH Object (group \{object.group_id}, object \{object.object_id})",
            )
          }
          guard object.payload == probe_payload(expected_group, expected_object) else {
            fail(
              "FETCH payload mismatch for group \{expected_group}, object \{expected_object}",
            )
          }
          guard object.properties == probe_properties() else {
            fail(
              "FETCH properties mismatch for group \{expected_group}, object \{expected_object}",
            )
          }
          println(
            "Received FETCH Object (group \{object.group_id}, object \{object.object_id}, properties \{object.properties.length()} bytes)",
          )
          received = received + 1UL
          expected_object = expected_object + 1UL
          if expected_object == 2UL {
            expected_object = 0UL
            expected_group = expected_group + 1UL
          }
        }
        @session.FetchRangeEnded(id, _) if id == request_id =>
          fail("FETCH relay returned a range-end marker")
        @session.FetchFinished(id) if id == request_id => finished = true
        @session.SessionEnded(end) => fail("session ended: \{end.to_repr()}")
        _ => ()
      }
    }
  }
  guard finished else { fail("FETCH did not finish before the deadline") }
  guard expected_group == end_group + 1UL && expected_object == 0UL else {
    fail("FETCH ended before the requested group range was complete")
  }
  println("Verified \{received} FETCH Objects; FETCH data FIN received")
}
