///|
test "request id allocation starts at zero" {
  let allocation = @lifecycle.allocate_request_id(0UL)
  assert_eq(allocation.request_id, 0UL)
  assert_eq(allocation.next, 2UL)
}

///|
test "request id allocation continues from two" {
  let first = @lifecycle.allocate_request_id(0UL)
  let second = @lifecycle.allocate_request_id(first.next)
  assert_eq(second.request_id, 2UL)
  assert_eq(second.next, 4UL)
}

///|
test "request id allocation accepts a large even cursor" {
  let allocation = @lifecycle.allocate_request_id(0xfffffffffffffffcUL)
  assert_eq(allocation.request_id, 0xfffffffffffffffcUL)
  assert_eq(allocation.next, 0xfffffffffffffffeUL)
}

///|
test "request ids are even and strictly increase" {
  let first = @lifecycle.allocate_request_id(0UL)
  let second = @lifecycle.allocate_request_id(first.next)
  let third = @lifecycle.allocate_request_id(second.next)
  assert_eq(first.request_id % 2UL, 0UL)
  assert_eq(second.request_id % 2UL, 0UL)
  assert_eq(third.request_id % 2UL, 0UL)
  assert_eq(first.next % 2UL, 0UL)
  assert_eq(second.next % 2UL, 0UL)
  assert_eq(third.next % 2UL, 0UL)
  assert_true(first.request_id < first.next)
  assert_true(second.request_id < second.next)
  assert_true(third.request_id < third.next)
  assert_true(first.request_id < second.request_id)
  assert_true(second.request_id < third.request_id)
  assert_eq(first.next, second.request_id)
  assert_eq(second.next, third.request_id)
}

///|
test "peek does not advance the cursor" {
  assert_eq(@lifecycle.peek_request_id(6UL), 6UL)
  assert_eq(@lifecycle.peek_request_id(6UL), 6UL)
  let allocation = @lifecycle.allocate_request_id(
    @lifecycle.peek_request_id(6UL),
  )
  assert_eq(allocation.request_id, 6UL)
  assert_eq(allocation.next, 8UL)
}
