///|
/// Calls made by `SessionDriver`, including attempts that return `Retry` or
/// `Fatal`. Send completion is controlled separately from these observations.
pub(all) enum TransportCall {
  OpenStream(@session.ClientStreamDirection, @session.ClientStreamPurpose)
  Send(Int64, Bytes, Bool)
  SendDatagram(Bytes)
  AbortSend(Int64, UInt64)
  StopReceiving(Int64, UInt64)
  CloseConnection(UInt64, Bytes)
} derive(Debug, Eq)

///|
/// In-memory transport for tests using the public `SessionDriver`.
///
/// Each operation kind consumes its own FIFO script of acceptance results and
/// defaults to `Accepted` when its script is empty. Accepted operations remain
/// pending until explicitly completed; `Retry` and `Fatal` retain no callback.
/// No protocol or session behavior is implemented here.
pub struct ScriptedTransport {
  priv open_script : Array[@driver.TransportAcceptance]
  priv send_script : Array[@driver.TransportAcceptance]
  priv datagram_script : Array[@driver.TransportAcceptance]
  priv mut calls : Array[TransportCall]
  priv open_callbacks : Array[(@driver.TransportOpenOutcome) -> Unit]
  priv send_callbacks : Array[(Int64, (@driver.TransportWriteOutcome) -> Unit)]
  priv datagram_callbacks : Array[(@driver.TransportWriteOutcome) -> Unit]
}

///|
pub fn ScriptedTransport::new() -> ScriptedTransport {
  {
    open_script: [],
    send_script: [],
    datagram_script: [],
    calls: [],
    open_callbacks: [],
    send_callbacks: [],
    datagram_callbacks: [],
  }
}

///|
/// Append an acceptance result for an open attempt.
/// Call `driver.pump()` to retry after a scripted `Retry`.
pub fn ScriptedTransport::script_open(
  self : ScriptedTransport,
  acceptance : @driver.TransportAcceptance,
) -> Unit {
  self.open_script.push(acceptance)
}

///|
/// Append an acceptance result for a send attempt, across all streams.
pub fn ScriptedTransport::script_send(
  self : ScriptedTransport,
  acceptance : @driver.TransportAcceptance,
) -> Unit {
  self.send_script.push(acceptance)
}

///|
/// Append an acceptance result for a datagram send attempt.
pub fn ScriptedTransport::script_datagram(
  self : ScriptedTransport,
  acceptance : @driver.TransportAcceptance,
) -> Unit {
  self.datagram_script.push(acceptance)
}

///|
/// Drain the call log without changing scripts or pending completions.
pub fn ScriptedTransport::take_calls(
  self : ScriptedTransport,
) -> Array[TransportCall] {
  let calls = self.calls
  self.calls = []
  calls
}

///|
/// Complete the oldest accepted open. Returns false if none is pending.
/// The callback is removed before invocation and can only be completed once.
pub fn ScriptedTransport::complete_open(
  self : ScriptedTransport,
  outcome : @driver.TransportOpenOutcome,
) -> Bool {
  if self.open_callbacks.is_empty() {
    return false
  }
  let callback = self.open_callbacks.remove(0)
  callback(outcome)
  true
}

///|
/// Complete the oldest accepted send on `stream`, allowing completion order
/// to differ between streams. Returns false if that stream has no pending send.
pub fn ScriptedTransport::complete_send(
  self : ScriptedTransport,
  stream : Int64,
  outcome : @driver.TransportWriteOutcome,
) -> Bool {
  for index = 0; index < self.send_callbacks.length(); index = index + 1 {
    let (candidate, callback) = self.send_callbacks[index]
    if candidate == stream {
      ignore(self.send_callbacks.remove(index))
      callback(outcome)
      return true
    }
  }
  false
}

///|
/// Complete the oldest accepted datagram. Returns false if none is pending.
pub fn ScriptedTransport::complete_datagram(
  self : ScriptedTransport,
  outcome : @driver.TransportWriteOutcome,
) -> Bool {
  if self.datagram_callbacks.is_empty() {
    return false
  }
  let callback = self.datagram_callbacks.remove(0)
  callback(outcome)
  true
}

///|
fn next_acceptance(
  script : Array[@driver.TransportAcceptance],
) -> @driver.TransportAcceptance {
  if script.is_empty() {
    @driver.Accepted
  } else {
    script.remove(0)
  }
}

///|
pub impl @driver.SessionTransport for ScriptedTransport with fn open_stream(
  self,
  direction,
  purpose,
  on_done,
) {
  self.calls.push(OpenStream(direction, purpose))
  let acceptance = next_acceptance(self.open_script)
  match acceptance {
    @driver.Accepted => self.open_callbacks.push(on_done)
    _ => ()
  }
  acceptance
}

///|
pub impl @driver.SessionTransport for ScriptedTransport with fn send(
  self,
  stream,
  bytes,
  fin,
  on_done,
) {
  self.calls.push(Send(stream, bytes, fin))
  let acceptance = next_acceptance(self.send_script)
  match acceptance {
    @driver.Accepted => self.send_callbacks.push((stream, on_done))
    _ => ()
  }
  acceptance
}

///|
pub impl @driver.SessionTransport for ScriptedTransport with fn send_datagram(
  self,
  bytes,
  on_done,
) {
  self.calls.push(SendDatagram(bytes))
  let acceptance = next_acceptance(self.datagram_script)
  match acceptance {
    @driver.Accepted => self.datagram_callbacks.push(on_done)
    _ => ()
  }
  acceptance
}

///|
pub impl @driver.SessionTransport for ScriptedTransport with fn abort_send(
  self,
  stream,
  code,
) -> Unit {
  self.calls.push(AbortSend(stream, code))
}

///|
pub impl @driver.SessionTransport for ScriptedTransport with fn stop_receiving(
  self,
  stream,
  code,
) -> Unit {
  self.calls.push(StopReceiving(stream, code))
}

///|
pub impl @driver.SessionTransport for ScriptedTransport with fn close_connection(
  self,
  code,
  reason,
) -> Unit {
  self.calls.push(CloseConnection(code, reason))
}

///|
pub extend ScriptedTransport with @driver.SessionTransport::{
  open_stream,
  send,
  send_datagram,
  abort_send,
  stop_receiving,
  close_connection,
}

///|
pub extend TransportCall with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend TransportCall with Eq::{equal, not_equal}
