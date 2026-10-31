///|
test "send budget admits exact byte boundary and does not consume rejected checks" {
  let budget = SendQueueBudget::new(3, 10)
  assert_false(budget.has_capacity(11))
  assert_true(budget.has_capacity(10))
  budget.acquire(6)
  assert_false(budget.has_capacity(5))
  assert_true(budget.has_capacity(4))
  budget.acquire(4)
  assert_false(budget.has_capacity(1))
  // An empty FIN still consumes a pending operation, but no bytes.
  assert_true(budget.has_capacity(0))
  budget.acquire(0)
  assert_false(budget.has_capacity(0))
  budget.release(0)
  assert_true(budget.has_capacity(0))
  assert_false(budget.has_capacity(1))
}

///|
test "send budget enforces count independently and recovers after completion" {
  let budget = SendQueueBudget::new(2, 100)
  budget.acquire(2)
  budget.acquire(3)
  assert_false(budget.has_capacity(0))
  // Browser promises can settle out of submission order.
  budget.release(3)
  assert_true(budget.has_capacity(98))
  assert_false(budget.has_capacity(99))
  budget.acquire(98)
  assert_false(budget.has_capacity(0))
  budget.release(2)
  budget.release(98)
  assert_true(budget.has_capacity(100))
  assert_eq(budget.pending_count, 0)
  assert_eq(budget.pending_bytes, 0)
}
