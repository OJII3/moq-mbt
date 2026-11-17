///|
test "track alias starts at one when no alias is used" {
  let used : FixedArray[UInt64] = []
  let allocation = @lifecycle.allocate_track_alias(1UL, used)
  assert_eq(allocation.track_alias, 1UL)
  assert_eq(allocation.next, 2UL)
}

///|
test "track alias skips a used run" {
  let used : FixedArray[UInt64] = FixedArray::from_array([1UL, 2UL, 3UL])
  let allocation = @lifecycle.allocate_track_alias(1UL, used)
  assert_eq(allocation.track_alias, 4UL)
  assert_eq(allocation.next, 5UL)
}

///|
test "track alias uses the first hole" {
  let used : FixedArray[UInt64] = FixedArray::from_array([1UL, 3UL, 4UL])
  let allocation = @lifecycle.allocate_track_alias(1UL, used)
  assert_eq(allocation.track_alias, 2UL)
  assert_eq(allocation.next, 3UL)
}

///|
test "track alias does not depend on the used order" {
  let used : FixedArray[UInt64] = FixedArray::from_array([2UL, 1UL])
  let allocation = @lifecycle.allocate_track_alias(1UL, used)
  assert_eq(allocation.track_alias, 3UL)
  assert_eq(allocation.next, 4UL)
}

///|
test "track alias tolerates duplicates" {
  let used : FixedArray[UInt64] = FixedArray::from_array([1UL, 1UL, 2UL])
  let allocation = @lifecycle.allocate_track_alias(1UL, used)
  assert_eq(allocation.track_alias, 3UL)
  assert_eq(allocation.next, 4UL)
}

///|
test "track alias starts from a custom cursor" {
  let used : FixedArray[UInt64] = FixedArray::from_array([4UL, 5UL, 6UL])
  let allocation = @lifecycle.allocate_track_alias(4UL, used)
  assert_eq(allocation.track_alias, 7UL)
  assert_eq(allocation.next, 8UL)
  let free_start = @lifecycle.allocate_track_alias(5UL, used)
  assert_eq(free_start.track_alias, 7UL)
  assert_eq(free_start.next, 8UL)
}

///|
test "track alias keeps a cursor above every used alias" {
  let used : FixedArray[UInt64] = FixedArray::from_array([1UL, 2UL])
  let allocation = @lifecycle.allocate_track_alias(9UL, used)
  assert_eq(allocation.track_alias, 9UL)
  assert_eq(allocation.next, 10UL)
}

///|
test "track alias scans long consecutive runs" {
  let used : FixedArray[UInt64] = FixedArray::from_array([
    1UL, 2UL, 3UL, 4UL, 5UL, 6UL, 7UL, 8UL, 9UL, 10UL,
  ])
  let allocation = @lifecycle.allocate_track_alias(1UL, used)
  assert_eq(allocation.track_alias, 11UL)
  assert_eq(allocation.next, 12UL)
}

///|
test "track alias returned by allocation is absent from the used aliases" {
  let used : FixedArray[UInt64] = FixedArray::from_array([1UL, 3UL, 5UL])
  let allocation = @lifecycle.allocate_track_alias(1UL, used)
  assert_eq(allocation.track_alias, 2UL)
  let mut seen = false
  for i = 0; i < used.length(); i = i + 1 {
    if used[i] == allocation.track_alias {
      seen = true
    }
  }
  assert_false(seen)
}
