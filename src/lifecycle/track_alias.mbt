///|
/// One track alias allocation step.
///
/// The alias is the first value at or above `start` that is absent from the
/// active aliases, and `next` is the value to use as the next `start`.
pub struct TrackAliasAllocation {
  track_alias : UInt64
  next : UInt64
}

///|
#proof_pure
fn TrackAliasAllocation::track_alias(
  self : TrackAliasAllocation,
) -> UInt64 {
  self.track_alias
}

///|
#proof_pure
fn TrackAliasAllocation::next(self : TrackAliasAllocation) -> UInt64 {
  self.next
}

///|
/// Reserve the first track alias at or above `start` that is not in `used`.
///
/// The proof contract covers the returned range, the cursor progression, and
/// the empty `used` case. The stronger property that `track_alias` differs
/// from every `used[i]` is covered by tests only: the verifier cannot yet
/// discharge the pigeonhole argument that a scan of `used.length() + 1`
/// candidates must encounter a free alias.
pub fn allocate_track_alias(
  start : UInt64,
  used : FixedArray[UInt64],
) -> TrackAliasAllocation where {
  proof_require: 0UL < start,
  proof_ensure: result => track_alias_allocation_post(start, result) &&
    (used.length() == 0 → track_alias_empty_post(start, result)),
} {
  if used.length() == 0 {
    { track_alias: start, next: start + 1UL }
  } else {
    let mut chosen = start
    for step = 0; step < used.length() + 1; step = step + 1 {
      let mut found = false
      for i = 0; i < used.length(); i = i + 1 {
        if used[i] == chosen {
          found = true
        }
      } where {
        proof_invariant: 0 <= i,
        proof_invariant: i <= used.length(),
      }
      if found {
        chosen = chosen + 1UL
      }
    } where {
      proof_invariant: start <= chosen,
    }
    { track_alias: chosen, next: chosen + 1UL }
  }
}
