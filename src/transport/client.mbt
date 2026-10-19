///|
let max_bytes : Int32 = 1024 * 1024

///|
let max_events : Int32 = 256

///|
let max_streams : Int32 = 16

///|
/// Native QUIC client. Call `close` when finished; methods run on the calling thread.
pub struct Client {
  priv mut raw : RawClient?
  priv streams : Map[Int64, RawStream]
  priv send_sizes : Array[(Int64, Int32)]
  priv mut pending_send_bytes : Int32
}

///|
/// Start a QUIC connection using ALPN `moqt-18`. Wait for `Connected` before sending.
/// Certificate validation is enabled unless `insecure=true` is explicitly supplied.
pub fn Client::connect(
  host : String,
  port : Int32,
  insecure~ : Bool = false,
) -> Client raise TransportError {
  if port <= 0 ||
    port > 65535 ||
    host.is_empty() ||
    host.length() > 65535 ||
    host.contains("\u0000") {
    raise TransportError("invalid host or port")
  }
  let host_cstr = @utf8.encode(host) + b"\x00"
  let alpn = b"moqt-18"
  let raw = native_connect(host_cstr, port, alpn, alpn.length(), insecure)
  if !native_valid(raw) {
    raise TransportError("MsQuic connection initialization failed")
  }
  { raw: Some(raw), streams: {}, send_sizes: [], pending_send_bytes: 0, }
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
  let raw = self.handle()
  if self.streams.length() >= max_streams {
    raise TransportError("cannot open unidirectional stream")
  }
  let stream = native_open_uni(raw)
  let id = native_stream_id(raw, stream)
  if id < 0L {
    native_stream_close(raw, stream)
    raise TransportError("cannot open unidirectional stream")
  }
  self.streams[id] = stream
  id
}

///|
/// Open a bidirectional request stream. The returned value is the QUIC stream ID.
pub fn Client::open_bidi(self : Client) -> Int64 raise TransportError {
  let raw = self.handle()
  if self.streams.length() >= max_streams {
    raise TransportError("cannot open bidirectional stream")
  }
  let stream = native_open_bidi(raw)
  let id = native_stream_id(raw, stream)
  if id < 0L {
    native_stream_close(raw, stream)
    raise TransportError("cannot open bidirectional stream")
  }
  self.streams[id] = stream
  id
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
  let raw = self.handle()
  let length = data.length()
  if length > max_bytes - self.pending_send_bytes ||
    self.send_sizes.length() >= max_events {
    raise TransportError("send buffer overflow")
  }
  guard self.streams.get(stream) is Some(handle) else {
    raise TransportError("unknown stream \{stream}")
  }
  let status = native_send(raw, handle, data, length, fin)
  if status != 0 {
    raise TransportError("stream send failed (status \{status})")
  }
  self.pending_send_bytes += length
  self.send_sizes.push((stream, length))
}

///|
/// Abort the local send side of a stream.
pub fn Client::abort_send(
  self : Client,
  stream : Int64,
  error_code : Int64,
) -> Unit raise TransportError {
  let raw = self.handle()
  guard self.streams.get(stream) is Some(handle) else {
    raise TransportError("unknown stream \{stream}")
  }
  let status = native_abort_send(raw, handle, error_code)
  if status != 0 {
    raise TransportError("stream send abort failed (status \{status})")
  }
}

///|
/// Stop receiving data on a stream.
pub fn Client::stop_receiving(
  self : Client,
  stream : Int64,
  error_code : Int64,
) -> Unit raise TransportError {
  let raw = self.handle()
  guard self.streams.get(stream) is Some(handle) else {
    raise TransportError("unknown stream \{stream}")
  }
  let status = native_stop_receiving(raw, handle, error_code)
  if status != 0 {
    raise TransportError("stream receive stop failed (status \{status})")
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
  let mut remaining = timeout_ms
  for ;; {
    match native_pop(raw) {
      0 => {
        if remaining <= 0 {
          return Timeout
        }
        let step = if remaining < 5 { remaining } else { 5 }
        native_sleep_ms(step)
        remaining -= step
      }
      1 =>
        if native_event_code(raw) != 0L {
          return Connected
        } else {
          native_shutdown(raw, 3L)
          return TransportClosed(-1)
        }
      2 => return Data(native_event_id(raw), native_event_data(raw))
      3 => return StreamFinished(native_event_id(raw))
      4 => return Closed(native_event_code(raw))
      5 => return TransportClosed(native_event_code(raw).to_int())
      6 => return QueueOverflow
      7 => return StreamReset(native_event_id(raw), native_event_code(raw))
      8 => {
        let id = native_event_id(raw)
        for i in 0..<self.send_sizes.length() {
          if self.send_sizes[i].0 == id {
            self.pending_send_bytes -= self.send_sizes[i].1
            ignore(self.send_sizes.remove(i))
            break
          }
        }
        return SendComplete(id, native_event_code(raw) != 0L)
      }
      9 => return SendStopped(native_event_id(raw), native_event_code(raw))
      10 => {
        let id = native_event_id(raw)
        if self.streams.get(id) is Some(stream) {
          self.streams.remove(id)
          native_stream_close(raw, stream)
        }
      }
      11 => {
        let id = native_event_id(raw)
        self.streams[id] = native_event_stream(raw)
        if self.streams.length() > max_streams {
          native_shutdown(raw, 3L)
          return QueueOverflow
        }
        return StreamStarted(id)
      }
      _ => raise TransportError("event queue failed")
    }
  }
}

///|
/// Close streams and the connection, waiting for callbacks before freeing memory.
/// Calling this method more than once is harmless.
pub fn Client::close(self : Client, error_code~ : Int64 = 0) -> Unit {
  if self.raw is Some(raw) {
    self.raw = None
    native_shutdown(raw, error_code)
    for _ in 0..<600 {
      let event = native_pop(raw)
      if event == 4 {
        break
      }
      if event == 11 {
        let id = native_event_id(raw)
        self.streams[id] = native_event_stream(raw)
      } else if event == 10 {
        let id = native_event_id(raw)
        if self.streams.get(id) is Some(stream) {
          self.streams.remove(id)
          native_stream_close(raw, stream)
        }
      }
      native_sleep_ms(5)
    }
    for _, stream in self.streams {
      native_stream_close(raw, stream)
    }
    self.streams.clear()
    native_free(raw)
  }
}
