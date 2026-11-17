///|
test "object write accepts a zero-length payload" {
  let state = @lifecycle.ObjectWriteState::new(0UL)
  assert_eq(state.remaining(), 0UL)
  assert_true(state.can_finish())
  let next = state.write(0UL).unwrap()
  assert_eq(next.declared, 0UL)
  assert_eq(next.written, 0UL)
  assert_eq(next.remaining(), 0UL)
  assert_true(next.can_finish())
}

///|
test "object write accepts the exact payload in one chunk" {
  let state = @lifecycle.ObjectWriteState::new(3UL)
  assert_eq(state.remaining(), 3UL)
  assert_false(state.can_finish())
  let done = state.write(3UL).unwrap()
  assert_eq(done.declared, 3UL)
  assert_eq(done.written, 3UL)
  assert_eq(done.remaining(), 0UL)
  assert_true(done.can_finish())
  assert_true(done.write(1UL) is Err(PayloadLengthMismatch))
}

///|
test "object write accounts multiple chunks" {
  let state = @lifecycle.ObjectWriteState::new(5UL)
  let partial = state.write(2UL).unwrap()
  assert_eq(partial.declared, 5UL)
  assert_eq(partial.written, 2UL)
  assert_eq(partial.remaining(), 3UL)
  assert_false(partial.can_finish())
  let done = partial.write(3UL).unwrap()
  assert_eq(done.declared, 5UL)
  assert_eq(done.written, 5UL)
  assert_eq(done.remaining(), 0UL)
  assert_true(done.can_finish())
}

///|
test "rejected over-write leaves the state reusable" {
  let state = @lifecycle.ObjectWriteState::new(4UL)
  assert_true(state.write(5UL) is Err(PayloadLengthMismatch))
  assert_eq(state.declared, 4UL)
  assert_eq(state.written, 0UL)
  assert_eq(state.remaining(), 4UL)
  assert_false(state.can_finish())
  let done = state.write(4UL).unwrap()
  assert_eq(done.written, 4UL)
  assert_eq(done.remaining(), 0UL)
  assert_true(done.can_finish())
}

///|
test "can_finish reports completeness before and after the last chunk" {
  let state = @lifecycle.ObjectWriteState::new(2UL)
  assert_false(state.can_finish())
  let half = state.write(1UL).unwrap()
  assert_false(half.can_finish())
  assert_eq(half.remaining(), 1UL)
  let done = half.write(1UL).unwrap()
  assert_true(done.can_finish())
  assert_eq(done.remaining(), 0UL)
}

///|
test "empty write leaves a nonempty payload unchanged" {
  let state = @lifecycle.ObjectWriteState::new(2UL)
  let same = state.write(0UL).unwrap()
  assert_eq(same.declared, 2UL)
  assert_eq(same.written, 0UL)
  assert_eq(same.remaining(), 2UL)
  assert_false(same.can_finish())
}
