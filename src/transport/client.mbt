///|
let max_bytes : Int32 = 1024 * 1024

///|
let max_events : Int32 = 256

///|
let max_streams : Int32 = 16

///|
let handshake_timeout_ms : Int32 = 10000

///|
let idle_timeout_ms : Int32 = 30000

///|
let max_datagram_sends : Int32 = 64

///|
fn validate_datagram_send(
  length : Int32,
  capability : DatagramSendCapability,
  pending_bytes : Int32,
  pending_sends : Int32,
) -> Unit raise TransportError {
  if !capability.enabled {
    raise DatagramSendUnavailable
  }
  if length > capability.max_length {
    raise DatagramTooLarge(length, capability.max_length)
  }
  if length > max_bytes - pending_bytes || pending_sends >= max_datagram_sends {
    raise DatagramSendQueueFull
  }
}

///|
/// Retire the buffer associated with an MsQuic stream send completion.
fn Client::record_send_complete(self : Client, stream : Int64) -> Unit {
  for i in 0..<self.send_sizes.length() {
    if self.send_sizes[i].0 == stream {
      self.pending_send_bytes -= self.send_sizes[i].1
      ignore(self.send_sizes.remove(i))
      break
    }
  }
}

///|
/// Retire a datagram buffer only once MsQuic reports a final send state.
fn Client::record_datagram_send_state(
  self : Client,
  id : Int64,
  state : DatagramSendState,
) -> Unit raise TransportError {
  if state.is_final() {
    match self.datagram_send_sizes.get(id) {
      Some(size) => {
        self.pending_datagram_bytes -= size
        self.pending_datagrams -= 1
        self.datagram_send_sizes.remove(id)
      }
      None => raise TransportError("unknown datagram send ID \{id}")
    }
  }
}

///|
/// Native QUIC client. Call `close` when finished; methods run on the calling thread.
pub struct Client {
  priv mut raw : RawClient?
  priv streams : Map[Int64, RawStream]
  priv send_sizes : Array[(Int64, Int32)]
  priv datagram_send_sizes : Map[Int64, Int32]
  priv mut next_datagram_id : Int64
  priv mut pending_send_bytes : Int32
  priv mut pending_datagram_bytes : Int32
  priv mut pending_datagrams : Int32
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
  let raw = native_connect(
    host_cstr,
    port,
    alpn,
    alpn.length(),
    insecure,
    max_bytes,
    max_events,
    max_streams,
    handshake_timeout_ms,
    idle_timeout_ms,
  )
  if !native_valid(raw) {
    raise TransportError("MsQuic connection initialization failed")
  }
  {
    raw: Some(raw),
    streams: {},
    send_sizes: [],
    datagram_send_sizes: {},
    next_datagram_id: 1L,
    pending_send_bytes: 0,
    pending_datagram_bytes: 0,
    pending_datagrams: 0,
  }
}

///|
fn Client::handle(self : Client) -> RawClient raise TransportError {
  match self.raw {
    Some(raw) => raw
    None => raise TransportError("client is closed")
  }
}

///|
/// Whether the local stream table has room for another stream.
pub fn Client::has_stream_capacity(self : Client) -> Bool raise TransportError {
  ignore(self.handle())
  self.streams.length() < max_streams
}

///|
/// Current peer datagram-send capability and the maximum size of one datagram.
pub fn Client::datagram_send_capability(
  self : Client,
) -> DatagramSendCapability raise TransportError {
  let raw = self.handle()
  let state = native_datagram_state(raw)
  { enabled: (state & 1L) != 0L, max_length: (state >> 1).to_int(), }
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
/// `SendComplete(stream, false)` releases the local send buffer; `true` means
/// the send was canceled. Only `StreamDataCommitted` confirms QUIC commitment.
pub fn Client::send(
  self : Client,
  stream : Int64,
  data : Bytes,
  fin~ : Bool = false,
) -> Unit raise TransportError {
  let raw = self.handle()
  let length = data.length()
  if length > max_bytes {
    raise SendTooLarge(length, max_bytes)
  }
  if length > max_bytes - self.pending_send_bytes ||
    self.send_sizes.length() >= max_events {
    raise SendQueueFull
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
/// Copy one unreliable datagram into a buffer retained until MsQuic reports a final state.
/// The returned ID is included in each `DatagramSendStateChanged` event. An
/// acknowledged state confirms QUIC acknowledgement only, not application delivery.
pub fn Client::send_datagram(
  self : Client,
  data : Bytes,
) -> Int64 raise TransportError {
  let raw = self.handle()
  let length = data.length()
  let capability = self.datagram_send_capability()
  validate_datagram_send(
    length,
    capability,
    self.pending_datagram_bytes,
    self.pending_datagrams,
  )
  let id = self.next_datagram_id
  let status = native_send_datagram(raw, data, length, id)
  match status {
    0 => ()
    1 => raise DatagramSendUnavailable
    2 => {
      let current = self.datagram_send_capability()
      raise DatagramTooLarge(length, current.max_length)
    }
    3 => raise DatagramSendAllocationFailed
    _ => raise DatagramSendFailed(status)
  }
  self.next_datagram_id += 1L
  self.pending_datagram_bytes += length
  self.pending_datagrams += 1
  self.datagram_send_sizes[id] = length
  id
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
      1 => return Connected
      2 => return Data(native_event_id(raw), native_event_data(raw))
      3 => return StreamFinished(native_event_id(raw))
      4 => return Closed(native_event_code(raw))
      5 => return TransportClosed(native_event_code(raw).to_int())
      6 => return QueueOverflow
      7 => return StreamReset(native_event_id(raw), native_event_code(raw))
      8 => {
        let id = native_event_id(raw)
        self.record_send_complete(id)
        return SendComplete(id, native_event_code(raw) != 0L)
      }
      9 => return SendStopped(native_event_id(raw), native_event_code(raw))
      10 => {
        let id = native_event_id(raw)
        if self.streams.get(id) is Some(stream) {
          self.streams.remove(id)
          native_stream_close(raw, stream)
        }
        return StreamClosed(id)
      }
      11 => {
        let id = native_event_id(raw)
        self.streams[id] = native_event_stream(raw)
        if self.streams.length() > max_streams {
          native_shutdown(raw, 3L)
          return QueueOverflow
        }
        // QUIC stream ID bit 1 distinguishes unidirectional streams.
        let direction = if (id & 0x2L) != 0L {
          Unidirectional
        } else {
          Bidirectional
        }
        return StreamStarted(id, direction)
      }
      12 => return Datagram(native_event_data(raw))
      13 => {
        let id = native_event_id(raw)
        let state = datagram_send_state_from_code(
          native_event_code(raw).to_int(),
        )
        self.record_datagram_send_state(id, state)
        return DatagramSendStateChanged(id, state)
      }
      14 =>
        return DatagramStateChanged({
          enabled: native_event_id(raw) != 0L,
          max_length: native_event_code(raw).to_int(),
        })
      15 => return StreamDataCommitted(native_event_id(raw))
      _ => raise TransportError("event queue failed")
    }
  }
}

///|
/// Close streams and the connection, waiting for `SHUTDOWN_COMPLETE` before freeing memory.
/// Calling this method more than once is harmless.
pub fn Client::close(self : Client, error_code~ : Int64 = 0) -> Unit {
  if self.raw is Some(raw) {
    self.raw = None
    native_shutdown(raw, error_code)
    while !native_shutdown_complete(raw) {
      let event = native_pop(raw)
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
      if !native_shutdown_complete(raw) {
        native_sleep_ms(5)
      }
    }
    for _, stream in self.streams {
      native_stream_close(raw, stream)
    }
    self.streams.clear()
    native_free(raw)
  }
}
