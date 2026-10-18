///|
/// The transport selects the version before constructing the handshake.
pub(all) enum Version {
  Draft18
} derive(Debug, Eq)

///|
/// Errors carry the MoQT connection-close code to use at the transport boundary.
pub(all) enum HandshakeError {
  InvalidSetup(@protocol.CodecError)
  DuplicateControlStream
  ControlStreamClosed
  InvalidServerPath
  InvalidServerAuthority
  AlreadyCompleted
} derive(Debug, Eq)

///|
pub fn HandshakeError::close_code(self : HandshakeError) -> UInt64 {
  match self {
    InvalidServerPath => 0x8
    InvalidServerAuthority => 0x19
    AlreadyCompleted => 0x1
    _ => 0x3
  }
}

///|
/// Ready includes any bytes following SETUP on the same control stream.
/// The caller retains that stream for later control messages; it must not FIN it.
pub(all) enum HandshakeProgress {
  Pending
  Ready(@protocol.Setup, Bytes)
} derive(Debug, Eq)

///|
/// Client-side SETUP exchange only. No sockets, clocks, or transport stream IDs
/// are interpreted here. Stream references are opaque caller-provided values.
pub struct ClientHandshake {
  priv version : Version
  priv outbound : Bytes
  priv mut peer_stream : Int64?
  priv mut buffer : Bytes
  priv mut completed : Bool
}

///|
/// Validate and snapshot local SETUP before opening the transport control stream.
pub fn ClientHandshake::new(
  version : Version,
  setup : @protocol.Setup,
) -> Result[ClientHandshake, @protocol.CodecError] {
  match @protocol.encode_setup(setup) {
    Err(error) => Err(error)
    Ok(outbound) =>
      Ok({
        version,
        outbound,
        peer_stream: None,
        buffer: b"",
        completed: false,
      })
  }
}

///|
pub fn ClientHandshake::version(self : ClientHandshake) -> Version {
  self.version
}

///|
/// Write once to a newly opened unidirectional control stream, without FIN.
/// SETUP's message type is also the stream type; no extra prefix is required.
pub fn ClientHandshake::outbound_setup(self : ClientHandshake) -> Bytes {
  self.outbound
}

///|
/// Feed ordered bytes from the peer's unidirectional control stream.
/// Call only until Ready. The returned trailing bytes belong to the next
/// control-message handler, which is outside this initial handshake scope.
pub fn ClientHandshake::receive(
  self : ClientHandshake,
  stream : Int64,
  bytes : Bytes,
) -> Result[HandshakeProgress, HandshakeError] {
  match self.peer_stream {
    Some(previous) if previous != stream => return Err(DuplicateControlStream)
    None => self.peer_stream = Some(stream)
    _ => ()
  }
  if self.completed {
    return Err(AlreadyCompleted)
  }
  // A SETUP frame contains at most 9 type bytes, 2 length bytes and 65535
  // payload bytes. Consume no more than that even for a large receive chunk.
  let remaining = 65546 - self.buffer.length()
  let count = Int32::min(bytes.length(), remaining)
  self.buffer = self.buffer + bytes[:count].to_owned()
  match @protocol.decode_setup(self.buffer) {
    Incomplete => Ok(Pending)
    Invalid(error) => Err(InvalidSetup(error))
    Complete(setup, consumed) => {
      for option in setup.options {
        if option.kind == 1 {
          return Err(InvalidServerPath)
        }
        if option.kind == 5 {
          return Err(InvalidServerAuthority)
        }
      }
      let trailing = self.buffer[consumed:].to_owned() +
        bytes[count:].to_owned()
      self.buffer = b""
      self.completed = true
      Ok(Ready(setup, trailing))
    }
  }
}

///|
/// A control stream ending at any point is a protocol violation (Draft 18 §3.3).
pub fn ClientHandshake::control_closed(
  _self : ClientHandshake,
) -> HandshakeError {
  ControlStreamClosed
}

///|
pub extend Version with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend Version with Eq::{equal, not_equal}

///|
pub extend HandshakeError with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend HandshakeError with Eq::{equal, not_equal}

///|
pub extend HandshakeProgress with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend HandshakeProgress with Eq::{equal, not_equal}
