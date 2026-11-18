///|
/// Run the FETCH probe for an Object range against `host`. An end object of 0
/// means the end of the end group, matching the publisher's group boundary.
pub fn run_fetch(
  host : String,
  port : Int32,
  path : String,
  namespace_field : String,
  track_name : String,
  start_group : UInt64,
  start_object : UInt64,
  end_group : UInt64,
  end_object : UInt64,
  objects_per_group : Int32,
  insecure : Bool,
) -> Unit raise {
  guard objects_per_group > 0 else {
    fail("OBJECTS_PER_GROUP must be positive")
  }
  let group_size = objects_per_group.to_uint64()
  guard start_group <= end_group &&
    start_object <= group_size &&
    (end_object == 0UL || end_object <= group_size) &&
    (
      start_group < end_group ||
      (start_group == end_group && start_object < end_object)
    ) else {
    fail("START and END must describe a non-empty Object range")
  }
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
  let (request_id, actions) = session
    .fetch_track(
      track,
      { group: start_group, object: start_object, },
      { group: end_group, object: end_object, },
      [],
    )
    .unwrap()
  ignore(drive_session_actions(client, session, actions, peer.driver))
  println("FETCH request sent")

  let mut expected_count = 0UL
  let mut group = start_group
  while group <= end_group {
    let first = if group == start_group { start_object } else { 0UL }
    let last = if group == end_group && end_object != 0UL {
      end_object
    } else {
      group_size
    }
    if last > first {
      expected_count = expected_count + (last - first)
    }
    group = group + 1UL
  }

  let mut expected_group = start_group
  let mut expected_object = start_object
  let mut received = 0UL
  let mut accepted = false
  let mut finished = false
  let deadline = @env.now() + 30000UL
  while !finished && @env.now() < deadline {
    let notices = lifecycle_poll(peer, 100)
    for notice in notices {
      match notice {
        FetchAccepted(id, _) if id == request_id => {
          accepted = true
          println("FETCH_OK received")
        }
        FetchResponseAccepted(id, _, response) if id == request_id => {
          let response_end_object = if end_object == 0UL {
            group_size
          } else {
            end_object
          }
          guard response.end_location.group == end_group &&
            response.end_location.object == response_end_object else {
            fail(
              "FETCH_OK end location did not match the requested terminal location",
            )
          }
          guard response.end_of_track == 0 else {
            fail("unexpected FETCH_OK end-of-track flag from the pinned relay")
          }
          println(
            "Verified FETCH_OK end location (group \{response.end_location.group}, object \{response.end_location.object}, end-of-track \{response.end_of_track})",
          )
        }
        FetchRejected(id, _, error) if id == request_id =>
          fail("FETCH rejected (code \{error.code})")
        FetchObjectReceived(id, object) if id == request_id => {
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
          let group_end = if expected_group == end_group && end_object != 0UL {
            end_object
          } else {
            group_size
          }
          if expected_object == group_end {
            expected_object = 0UL
            expected_group = expected_group + 1UL
          }
        }
        FetchRangeEnded(id, _) if id == request_id =>
          fail("FETCH relay returned a range-end marker")
        FetchFinished(id) if id == request_id => finished = true
        SessionEnded(end) => fail("session ended: \{end.to_repr()}")
        _ => ()
      }
    }
  }
  guard finished else { fail("FETCH did not finish before the deadline") }
  guard received == expected_count else {
    fail(
      "received \{received} FETCH Objects; expected \{expected_count} for the requested range",
    )
  }
  println("Verified \{received} FETCH Objects; FETCH data FIN received")
}
