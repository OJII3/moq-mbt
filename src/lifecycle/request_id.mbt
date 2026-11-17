///|
/// One client request ID allocation step.
///
/// Client request IDs are even and each allocation advances the cursor by two,
/// so callers replace their stored cursor with `next` after sending the
/// request that uses `request_id`.
pub struct RequestIdAllocation {
  request_id : UInt64
  next : UInt64
}

///|
#proof_pure
fn RequestIdAllocation::request_id(self : RequestIdAllocation) -> UInt64 {
  self.request_id
}

///|
#proof_pure
fn RequestIdAllocation::next(self : RequestIdAllocation) -> UInt64 {
  self.next
}

///|
/// Reserve the current client request ID and advance the cursor by two.
///
/// The cursor precondition keeps both the reserved ID and the returned cursor
/// even, and keeps the returned cursor representable as a UInt64.
pub fn allocate_request_id(next : UInt64) -> RequestIdAllocation where {
  proof_require: request_id_cursor(next),
  proof_ensure: result => request_id_allocation_post(next, result),
} {
  { request_id: next, next: next + 2UL, }
}

///|
/// Read the request ID that the next allocation returns without advancing.
pub fn peek_request_id(next : UInt64) -> UInt64 where {
  proof_ensure: result => result == next,
} {
  next
}
