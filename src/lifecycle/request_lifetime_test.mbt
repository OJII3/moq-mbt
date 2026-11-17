///|
test "lifecycle helpers preserve terminal fields" {
  let cancelled = @lifecycle.RequestLifetime::new().advance(Cancel(7UL), true)
  assert_true(cancelled.cancelled)
  assert_true(cancelled.local_send_closed)
  assert_true(cancelled.peer_send_closed)
  assert_true(cancelled.end_notified)
  assert_eq(cancelled.cancel_error_code, Some(7UL))

  let ended = @lifecycle.RequestLifetime::new().with_end_notified()
  assert_true(ended.end_notified)
  assert_false(ended.cancelled)
  assert_false(ended.local_send_closed)
  assert_false(ended.peer_send_closed)
  assert_eq(ended.cancel_error_code, None)

  let local_closed = ended.with_local_send_closed()
  assert_true(local_closed.local_send_closed)
  assert_false(local_closed.peer_send_closed)
  assert_false(local_closed.cancelled)
  assert_true(local_closed.end_notified)

  let terminal = local_closed.terminated()
  assert_true(terminal.local_send_closed)
  assert_true(terminal.peer_send_closed)
  assert_true(terminal.cancelled)
  assert_true(terminal.end_notified)
  assert_eq(terminal.cancel_error_code, None)
}
