///|
fn assert_request_lifetime_actions(
  actual : RequestLifetimeActions,
  expected : (Bool, Bool, Bool),
) -> Unit raise {
  assert_eq(actual.abort_send, expected.0)
  assert_eq(actual.stop_receiving, expected.1)
  assert_eq(actual.notify_end, expected.2)
}

///|
test "request lifetime applies crossed terminal events once" {
  let none = (false, false, false)
  let cancel = (true, true, true)
  let reset = (true, false, true)
  let stop = (false, true, true)
  let notify_only = (false, false, true)
  let cases : Array[(Array[RequestLifetimeEvent], Array[(Bool, Bool, Bool)])] = [
    (
      [Cancel(1UL), PeerReset(1UL), PeerStop(1UL), PeerFin],
      [cancel, none, none, none],
    ),
    (
      [PeerReset(2UL), PeerStop(2UL), PeerStop(2UL), PeerFin],
      [reset, none, none, none],
    ),
    (
      [PeerStop(3UL), PeerReset(3UL), PeerFin, Cancel(3UL)],
      [stop, none, none, none],
    ),
    (
      [PeerFin, Cancel(4UL), PeerReset(4UL), PeerStop(4UL)],
      [none, reset, none, none],
    ),
    ([LocalFin, Cancel(5UL), PeerReset(5UL)], [none, stop, none]),
    ([LocalFin, PeerFin, Cancel(6UL)], [none, none, notify_only]),
  ]
  for (events, expected) in cases {
    let lifetime = RequestLifetime::new()
    for index in 0..<events.length() {
      assert_request_lifetime_actions(
        lifetime.transition(events[index], true),
        expected[index],
      )
    }
  }
}

///|
test "request lifetime defers transport actions until a canceled stream opens" {
  let lifetime = RequestLifetime::new()
  assert_request_lifetime_actions(
    lifetime.transition(Cancel(1UL), false),
    (false, false, false),
  )
  assert_request_lifetime_actions(
    lifetime.transition(Cancel(1UL), true),
    (true, true, true),
  )
  assert_request_lifetime_actions(
    lifetime.transition(Cancel(1UL), true),
    (false, false, false),
  )
}
