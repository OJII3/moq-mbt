///|
pub(all) suberror TransportError {
  TransportError(String)
  SendQueueFull
  SendTooLarge(Int32, Int32)
  DatagramSendUnavailable
  DatagramTooLarge(Int32, Int32)
  DatagramSendQueueFull
  DatagramSendAllocationFailed
  DatagramSendFailed(Int32)
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
pub(all) struct DatagramSendCapability {
  enabled : Bool
  max_length : Int32
} derive(Debug, Eq)

///|
pub extend DatagramSendCapability with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend DatagramSendCapability with Eq::{equal, not_equal}

///|
pub(all) enum DatagramSendState {
  Unknown(Int32)
  Sent
  LostSuspect
  LostDiscarded
  Acknowledged
  AcknowledgedSpurious
  Canceled
} derive(Debug, Eq)

///|
pub extend DatagramSendState with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend DatagramSendState with Eq::{equal, not_equal}

///|
pub fn DatagramSendState::is_final(self : DatagramSendState) -> Bool {
  match self {
    LostDiscarded | Acknowledged | AcknowledgedSpurious | Canceled => true
    Unknown(_) | Sent | LostSuspect => false
  }
}

///|
fn datagram_send_state_from_code(code : Int32) -> DatagramSendState {
  match code {
    1 => Sent
    2 => LostSuspect
    3 => LostDiscarded
    4 => Acknowledged
    5 => AcknowledgedSpurious
    6 => Canceled
    _ => Unknown(code)
  }
}

///|
pub(all) enum Event {
  Connected
  StreamStarted(Int64, StreamDirection)
  Data(Int64, Bytes)
  Datagram(Bytes)
  DatagramStateChanged(DatagramSendCapability)
  DatagramSendStateChanged(Int64, DatagramSendState)
  StreamFinished(Int64)
  /// MsQuic reported QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE for the stream.
  StreamClosed(Int64)
  StreamReset(Int64, Int64)
  SendStopped(Int64, Int64)
  /// Local send-buffer release; this is not peer acknowledgement.
  SendComplete(Int64, Bool)
  /// The peer acknowledged all stream data and the final offset.
  StreamDataCommitted(Int64)
  Closed(Int64)
  TransportClosed(Int32)
  QueueOverflow
  Timeout
} derive(Debug)

///|
pub extend Event with @moonbitlang/core/debug.Debug::{to_repr}
