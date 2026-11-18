///|
/// Declared and written payload byte counts for one Object.
///
/// The fields stay immutable so each accepted write returns a new accounting
/// state. A rejected write leaves the caller's state usable for a later, smaller
/// chunk.
pub struct ObjectWriteState {
  declared : UInt64
  written : UInt64
}

///|
/// Why an Object payload write was rejected.
pub(all) enum ObjectWriteError {
  PayloadLengthMismatch
}

///|
/// Accounting result for one payload chunk.
///
/// `write` returns a `Result`, but the proof logic cannot inspect `Result`
/// values. This private carrier keeps the accepted flag and the next state in a
/// struct that the contracts can describe.
priv struct ObjectWriteAttempt {
  accepted : Bool
  state : ObjectWriteState
}

///|
#proof_pure
fn ObjectWriteState::declared(self : ObjectWriteState) -> UInt64 {
  self.declared
}

///|
#proof_pure
fn ObjectWriteState::written(self : ObjectWriteState) -> UInt64 {
  self.written
}

///|
#proof_pure
fn ObjectWriteAttempt::accepted(self : ObjectWriteAttempt) -> Bool {
  self.accepted
}

///|
#proof_pure
fn ObjectWriteAttempt::state(self : ObjectWriteAttempt) -> ObjectWriteState {
  self.state
}

///|
/// Record a declared Object payload length with no bytes written yet.
pub fn ObjectWriteState::new(
  declared : UInt64,
) -> ObjectWriteState where {
  proof_require: 0UL <= declared,
  proof_ensure: result => object_write_new_post(declared, result),
} {
  { declared, written: 0UL, }
}

///|
/// Payload bytes still accepted by `write`.
pub fn ObjectWriteState::remaining(
  self : ObjectWriteState,
) -> UInt64 where {
  proof_require: object_write_inv(self),
  proof_ensure: result => result == self.declared() - self.written(),
} {
  self.declared - self.written
}

///|
fn ObjectWriteState::attempt(
  self : ObjectWriteState,
  chunk_length : UInt64,
) -> ObjectWriteAttempt where {
  proof_require: object_write_inv(self),
  proof_require: 0UL <= chunk_length,
  proof_ensure: result => object_write_attempt_post(self, chunk_length, result),
} {
  let available = self.remaining()
  if chunk_length > available {
    { accepted: false, state: self, }
  } else {
    {
      accepted: true,
      state: { declared: self.declared, written: self.written + chunk_length, },
    }
  }
}

///|
/// Account one payload chunk.
///
/// A chunk larger than the remaining payload is rejected with
/// PayloadLengthMismatch, and `self` stays usable for a later, smaller chunk.
/// The accounting step is verified on `attempt`; this wrapper only maps the
/// accepted flag to `Ok` or `Err` because the proof logic cannot inspect
/// `Result` values.
pub fn ObjectWriteState::write(
  self : ObjectWriteState,
  chunk_length : UInt64,
) -> Result[ObjectWriteState, ObjectWriteError] {
  let attempt = self.attempt(chunk_length)
  if attempt.accepted {
    Ok(attempt.state)
  } else {
    Err(PayloadLengthMismatch)
  }
}

///|
/// Whether the declared payload length has been written completely.
pub fn ObjectWriteState::can_finish(
  self : ObjectWriteState,
) -> Bool where {
  proof_require: object_write_inv(self),
  proof_ensure: result => result == (self.declared() == self.written()),
} {
  self.remaining() == 0UL
}
