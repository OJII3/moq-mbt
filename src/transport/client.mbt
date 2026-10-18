///|
#external
priv type RawClient

///|
pub(all) suberror TransportError {
  TransportError(String)
} derive(Debug)

///|
pub extend TransportError with @moonbitlang/core/debug.Debug::{to_repr}

///|
/// Native QUIC client. Call `close` when finished; methods run on the calling thread.
pub struct Client {
  priv mut raw : RawClient?
}

///|
pub(all) enum Event {
  Connected
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

///|
#borrow(host)
extern "C" fn native_connect(
  host : Bytes,
  length : Int32,
  port : Int32,
  insecure : Bool,
) -> RawClient = "moq_client_connect"

///|
extern "C" fn native_valid(client : RawClient) -> Bool = "moq_client_valid"

///|
extern "C" fn native_open_uni(client : RawClient) -> Int64 = "moq_client_open_uni"

///|
#borrow(data)
extern "C" fn native_send(
  client : RawClient,
  stream : Int64,
  data : Bytes,
  length : Int32,
  fin : Bool,
) -> Int32 = "moq_client_send"

///|
extern "C" fn native_next(client : RawClient, timeout : Int32) -> Int32 = "moq_client_next"

///|
extern "C" fn native_event_id(client : RawClient) -> Int64 = "moq_client_event_id"

///|
extern "C" fn native_event_code(client : RawClient) -> Int64 = "moq_client_event_code"

///|
extern "C" fn native_event_data(client : RawClient) -> Bytes = "moq_client_event_data"

///|
extern "C" fn native_close(client : RawClient, error_code : Int64) -> Unit = "moq_client_close"

///|
/// Start a QUIC connection using ALPN `moqt-18`. Wait for `Connected` before sending.
/// Certificate validation is enabled unless `insecure=true` is explicitly supplied.
pub fn Client::connect(
  host : String,
  port : Int32,
  insecure~ : Bool = false,
) -> Client raise TransportError {
  if port <= 0 || port > 65535 || host.is_empty() {
    raise TransportError("invalid host or port")
  }
  let host = @utf8.encode(host)
  let raw = native_connect(host, host.length(), port, insecure)
  if !native_valid(raw) {
    raise TransportError("MsQuic connection initialization failed")
  }
  { raw: Some(raw), }
}

///|
fn Client::handle(self : Client) -> RawClient raise TransportError {
  match self.raw {
    Some(raw) => raw
    None => raise TransportError("client is closed")
  }
}

///|
/// Open a unidirectional stream. The returned value is the QUIC stream ID.
pub fn Client::open_uni(self : Client) -> Int64 raise TransportError {
  let stream = native_open_uni(self.handle())
  if stream < 0 {
    raise TransportError("cannot open unidirectional stream")
  }
  stream
}

///|
/// Copy bytes into a send buffer retained until MsQuic completes the send.
/// `SendComplete(stream, false)` confirms peer QUIC acknowledgement; `true`
/// means the send was canceled. This does not acknowledge application processing.
pub fn Client::send(
  self : Client,
  stream : Int64,
  data : Bytes,
  fin~ : Bool = false,
) -> Unit raise TransportError {
  let status = native_send(self.handle(), stream, data, data.length(), fin)
  if status != 0 {
    raise TransportError("stream send failed (status \{status})")
  }
}

///|
/// Wait at most `timeout_ms` for an event. Stream data may arrive in arbitrary chunks.
/// A connection buffers at most 1 MiB and 256 queued events; overflow closes it.
pub fn Client::next_event(
  self : Client,
  timeout_ms : Int32,
) -> Event raise TransportError {
  if timeout_ms < 0 {
    raise TransportError("negative event timeout")
  }
  let raw = self.handle()
  match native_next(raw, timeout_ms) {
    0 => Timeout
    1 => Connected
    2 => Data(native_event_id(raw), native_event_data(raw))
    3 => StreamFinished(native_event_id(raw))
    4 => Closed(native_event_id(raw))
    5 => TransportClosed(native_event_id(raw).to_int())
    6 => QueueOverflow
    7 => StreamReset(native_event_id(raw), native_event_code(raw))
    8 => SendComplete(native_event_id(raw), native_event_code(raw) != 0)
    9 => SendStopped(native_event_id(raw), native_event_code(raw))
    _ => raise TransportError("event queue failed")
  }
}

///|
/// Close streams and the connection, waiting for callbacks before freeing memory.
/// Calling this method more than once is harmless.
pub fn Client::close(self : Client, error_code~ : Int64 = 0) -> Unit {
  if self.raw is Some(raw) {
    self.raw = None
    native_close(raw, error_code)
  }
}
