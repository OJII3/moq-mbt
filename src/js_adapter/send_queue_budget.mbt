///|
/// Admission policy and accounting for writes waiting on browser promises.
/// Keep transport validation and promise handling in the calling manager.
priv struct SendQueueBudget {
  max_count : Int32
  max_bytes : Int32
  mut pending_count : Int32
  mut pending_bytes : Int32
}

///|
fn SendQueueBudget::new(
  max_count : Int32,
  max_bytes : Int32,
) -> SendQueueBudget {
  { max_count, max_bytes, pending_count: 0, pending_bytes: 0, }
}

///|
fn SendQueueBudget::has_capacity(self : SendQueueBudget, size : Int32) -> Bool {
  self.pending_count < self.max_count &&
  size <= self.max_bytes - self.pending_bytes
}

///|
/// Call after has_capacity and transport validation, without yielding between
/// checking and acquiring. This keeps each transport's error precedence intact.
fn SendQueueBudget::acquire(self : SendQueueBudget, size : Int32) -> Unit {
  self.pending_count += 1
  self.pending_bytes += size
}

///|
/// Release once when the corresponding write settles, including failures.
fn SendQueueBudget::release(self : SendQueueBudget, size : Int32) -> Unit {
  self.pending_count -= 1
  self.pending_bytes -= size
}
