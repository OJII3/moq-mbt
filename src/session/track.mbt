///|
#derive(Debug, Eq)
pub(all) enum TrackSessionError {
  InvalidControlMessage
  TrackMismatch
  DuplicateSubscription
  InvalidRequestId
  AliasMismatch
  UnexpectedMessage
  BufferLimitExceeded
  IncompleteObjectStream
  ObjectIdNotIncreasing
  ObjectStreamsStillOpen
}

///|
pub extend TrackSessionError with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend TrackSessionError with Eq::{equal, not_equal}
