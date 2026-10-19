///|
pub(all) suberror TransportError {
  TransportError(String)
} derive(Debug)

///|
pub extend TransportError with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub(all) enum StreamDirection {
  Unidirectional
  Bidirectional
} derive(Debug, Eq)

///|
pub extend StreamDirection with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend StreamDirection with Eq::{equal, not_equal}

///|
pub(all) enum Event {
  Connected
  StreamStarted(Int64, StreamDirection)
  Data(Int64, Bytes)
  StreamFinished(Int64)
  StreamReset(Int64, Int64)
  SendStopped(Int64, Int64)
  SendComplete(Int64, Bool)
  Closed(Int64)
  TransportClosed(Int32)
  QueueOverflow
  Timeout
} derive(Debug)

///|
pub extend Event with @moonbitlang/core/debug.Debug::{to_repr}
