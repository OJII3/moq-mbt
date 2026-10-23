///|
pub(all) suberror WebTransportError {
  InvalidCloseCode(Int64)
  CloseReasonTooLong(Int32)
}

///|
pub(all) struct WebTransportFailure {
  name : String
  message : String
  source : String?
  stream_error_code : Int64?
}

///|
pub(all) struct WebTransportCloseInfo {
  close_code : Int64
  reason : String
}

///|
pub(all) enum WebTransportClose {
  LocalClose
  PeerClose(WebTransportCloseInfo)
  Failed(WebTransportFailure)
}

///|
pub(all) enum WebTransportStreamDirection {
  Unidirectional
  Bidirectional
}

///|
pub(all) enum WebTransportStreamEvent {
  StreamStarted(Int64, WebTransportStreamDirection)
  Data(Int64, Bytes)
  StreamFinished(Int64)
  StreamReset(Int64, WebTransportFailure)
  SendStopped(Int64, WebTransportFailure)
  StreamQueueOverflow(Int64)
  ConnectionClosed(WebTransportClose)
}

///|
struct WebTransportStreamState {
  mut reader : @streams.ReadableStreamDefaultReader?
  mut writer : @streams.WritableStreamDefaultWriter?
  read_cancelled : Ref[Bool]
  write_aborted : Ref[Bool]
}

///|
/// Browser WebTransport session for MOQT draft-18.
/// `closed` resolves once with the local or peer close and rejects on failure.
/// Closing while `ready` is pending cancels that connection.
pub struct WebTransportConnection {
  raw : @js.Any
  ready : @js.Promise[Result[String, WebTransportFailure]]
  closed : @js.Promise[WebTransportClose]
  locally_closed : Ref[Bool]
  streams : Map[Int64, WebTransportStreamState]
  events : Array[WebTransportStreamEvent]
  event_waiters : Array[(WebTransportStreamEvent) -> Unit]
  next_stream_ref : Ref[Int64]
  pending_stream_opens : Ref[Int32]
  pending_send_count : Ref[Int32]
  pending_send_bytes : Ref[Int32]
  pending_event_bytes : Ref[Int32]
  connection_ended : Ref[Bool]
  final_close : Ref[WebTransportClose?]
}

///|
let max_stream_events : Int32 = 256

///|
let max_stream_event_bytes : Int32 = 1024 * 1024

///|
let max_pending_stream_opens : Int32 = 16

///|
let max_active_webtransport_streams : Int32 = 16

///|
let max_pending_stream_sends : Int32 = 256

///|
let max_pending_stream_send_bytes : Int32 = 1024 * 1024

///|
/// Create a WebTransport session from an MOQT URI, preserving its path and
/// query while replacing `moqt:` with `https:`. The browser's default
/// certificate validation remains enabled.
pub fn webtransport_connect(
  moqt_uri : String,
) -> Result[WebTransportConnection, String] {
  webtransport_connect_with_certificate_hashes(moqt_uri, [])
}

///|
/// As `webtransport_connect`, with explicit SHA-256 server certificate hashes.
/// An empty list keeps the browser's default Web PKI validation.
pub fn webtransport_connect_with_certificate_hashes(
  moqt_uri : String,
  certificate_hashes : Array[@arraybuffer.Uint8Array],
) -> Result[WebTransportConnection, String] {
  let connection_box : Ref[WebTransportConnection?] = { val: None, }
  let _result = @js.try_sync(fn() {
    let url = ffi_webtransport_url(moqt_uri)
    let raw = ffi_webtransport_new(
      url,
      moqt_draft18_protocol,
      FixedArray::from_array(certificate_hashes[:]),
    )
    let ready_raw : @js.Any = raw["ready"]
    let closed_raw : @js.Any = raw["closed"]
    let locally_closed_box = Ref(false)
    let ready_settled = ffi_webtransport_settle(ready_raw)
    let ready : @js.Promise[Result[String, WebTransportFailure]] = ready_settled.then(fn(
        settled,
      ) {
        if !settled["ok"].cast() {
          @js.Promise::resolve(Err(webtransport_failure(settled["error"])))
        } else if locally_closed_box.val {
          @js.Promise::resolve(
            Err(WebTransportFailure::{
              name: "AbortError",
              message: "connection closed while waiting for ready",
              source: Some("session"),
              stream_error_code: None,
            }),
          )
        } else {
          let selected : String = raw["protocol"].cast()
          if selected != moqt_draft18_protocol {
            @js.Promise::resolve(
              Err(WebTransportFailure::{
                name: "ProtocolError",
                message: "server selected unsupported WebTransport protocol: " +
                selected,
                source: None,
                stream_error_code: None,
              }),
            )
          } else {
            @js.Promise::resolve(Ok(selected))
          }
        }
      },
    )
    let closed_settled = ffi_webtransport_settle(closed_raw)
    let closed : @js.Promise[WebTransportClose] = closed_settled.then(fn(
      settled,
    ) {
      let outcome = if !settled["ok"].cast() {
        Failed(webtransport_failure(settled["error"]))
      } else {
        let info = settled["value"]
        let close_info = WebTransportCloseInfo::{
          close_code: ffi_webtransport_close_code(info),
          reason: info["reason"].cast(),
        }
        if locally_closed_box.val {
          LocalClose
        } else {
          PeerClose(close_info)
        }
      }
      match connection_box.val {
        Some(connection) => {
          connection.connection_ended.val = true
          connection.final_close.val = Some(outcome)
          connection.shutdown_streams()
          connection.enqueue_stream_event(ConnectionClosed(outcome))
        }
        None => ()
      }
      @js.Promise::resolve(outcome)
    })
    let connection = WebTransportConnection::{
      raw,
      ready,
      closed,
      locally_closed: locally_closed_box,
      streams: {},
      events: [],
      event_waiters: [],
      next_stream_ref: Ref(1L),
      pending_stream_opens: Ref(0),
      pending_send_count: Ref(0),
      pending_send_bytes: Ref(0),
      pending_event_bytes: Ref(0),
      connection_ended: Ref(false),
      final_close: Ref(None),
    }
    connection_box.val = Some(connection)
    connection.start_accepting_streams()
    @js.undefined()
  }) catch {
    @js.JsError(message) => return Err(message)
  }
  match connection_box.val {
    Some(connection) => Ok(connection)
    None => Err("WebTransport constructor returned no connection")
  }
}

///|
fn WebTransportConnection::enqueue_stream_event(
  self : WebTransportConnection,
  event : WebTransportStreamEvent,
) -> Unit {
  if event is ConnectionClosed(_) {
    self.events.clear()
    self.pending_event_bytes.val = 0
    if self.event_waiters.length() > 0 {
      while self.event_waiters.length() > 0 {
        let waiter = self.event_waiters[0]
        ignore(self.event_waiters.remove(0))
        waiter(event)
      }
    } else {
      self.events.push(event)
    }
    return
  }
  if self.connection_ended.val {
    return
  }
  if self.event_waiters.length() > 0 {
    let waiter = self.event_waiters[0]
    ignore(self.event_waiters.remove(0))
    waiter(event)
  } else if self.events.length() < max_stream_events {
    match event {
      Data(stream, bytes) if bytes.length() >
        max_stream_event_bytes - self.pending_event_bytes.val => {
        self.events.push(StreamQueueOverflow(stream))
        self.stop_stream(stream)
      }
      _ if self.events.length() >= max_stream_events - 1 => {
        let stream = match event {
          StreamStarted(stream, _)
          | Data(stream, _)
          | StreamFinished(stream)
          | StreamReset(stream, _)
          | SendStopped(stream, _)
          | StreamQueueOverflow(stream) => stream
          ConnectionClosed(_) => 0L
        }
        self.events.push(StreamQueueOverflow(stream))
        if stream != 0L {
          self.stop_stream(stream)
        }
      }
      Data(_, bytes) => {
        self.pending_event_bytes.val += bytes.length()
        self.events.push(event)
      }
      _ => self.events.push(event)
    }
  }
}

///|
fn WebTransportConnection::stop_stream(
  self : WebTransportConnection,
  stream : Int64,
) -> Unit {
  match self.streams.get(stream) {
    Some(state) => {
      match state.reader {
        Some(reader) =>
          ignore(
            @js.from_async(async fn() {
              state.read_cancelled.val = true
              reader.cancel() catch {
                _ => ()
              }
              reader.release_lock()
            }),
          )
        None => ()
      }
      match state.writer {
        Some(writer) =>
          ignore(
            @js.from_async(async fn() {
              state.write_aborted.val = true
              writer.abort() catch {
                _ => ()
              }
              writer.release_lock()
            }),
          )
        None => ()
      }
      self.streams.remove(stream)
    }
    None => ()
  }
}

///|
/// Wait for the next stream lifecycle or data event. Data chunks preserve the
/// exact Uint8Array view range and are copied into owned MoonBit Bytes.
pub fn WebTransportConnection::next_stream_event(
  self : WebTransportConnection,
) -> @js.Promise[WebTransportStreamEvent] {
  if self.events.length() > 0 {
    let event = self.events[0]
    ignore(self.events.remove(0))
    match event {
      Data(_, bytes) => self.pending_event_bytes.val -= bytes.length()
      _ => ()
    }
    @js.Promise::resolve(event)
  } else {
    match self.final_close.val {
      Some(close) => @js.Promise::resolve(ConnectionClosed(close))
      None =>
        @js.Promise::new(fn(resolve, _reject) {
          self.event_waiters.push(resolve)
        })
    }
  }
}

///|
fn WebTransportConnection::allocate_stream_ref(
  self : WebTransportConnection,
) -> Int64 {
  let id = self.next_stream_ref.val
  self.next_stream_ref.val += 1L
  id
}

///|
fn WebTransportConnection::shutdown_streams(
  self : WebTransportConnection,
) -> Unit {
  for _, state in self.streams {
    match state.reader {
      Some(reader) =>
        ignore(
          @js.from_async(async fn() {
            state.read_cancelled.val = true
            reader.cancel() catch {
              _ => ()
            }
            reader.release_lock()
          }),
        )
      None => ()
    }
    match state.writer {
      Some(writer) =>
        ignore(
          @js.from_async(async fn() {
            state.write_aborted.val = true
            writer.abort() catch {
              _ => ()
            }
            writer.release_lock()
          }),
        )
      None => ()
    }
  }
  self.streams.clear()
}

///|
fn WebTransportConnection::register_stream(
  self : WebTransportConnection,
  stream : @js.Any,
  direction : WebTransportStreamDirection,
  locally_opened : Bool,
) -> Int64 {
  let has_reader = direction is Bidirectional || !locally_opened
  let has_writer = direction is Bidirectional || locally_opened
  if self.connection_ended.val {
    ffi_webtransport_discard_stream(
      stream,
      has_reader,
      has_writer,
      direction is Unidirectional,
    )
    return 0L
  }
  if self.streams.length() >= max_active_webtransport_streams {
    let id = self.allocate_stream_ref()
    self.enqueue_stream_event(StreamStarted(id, direction))
    self.enqueue_stream_event(StreamQueueOverflow(id))
    ffi_webtransport_discard_stream(
      stream,
      has_reader,
      has_writer,
      direction is Unidirectional,
    )
    return id
  }
  let reader = if has_reader {
    let readable : @streams.ReadableStream = if direction is Unidirectional {
      stream.cast()
    } else {
      stream["readable"].cast()
    }
    Some(readable.get_reader())
  } else {
    None
  }
  let writer = if has_writer {
    let writable : @streams.WritableStream = if direction is Unidirectional {
      stream.cast()
    } else {
      stream["writable"].cast()
    }
    Some(writable.get_writer())
  } else {
    None
  }
  let id = self.allocate_stream_ref()
  let state = {
    reader,
    writer,
    read_cancelled: Ref(false),
    write_aborted: Ref(false),
  }
  self.streams[id] = state
  self.enqueue_stream_event(StreamStarted(id, direction))
  match writer {
    Some(writer) =>
      ignore(
        writer
        .closed()
        .catch_(fn(error) {
          if !state.write_aborted.val {
            self.enqueue_stream_event(
              SendStopped(id, webtransport_failure(@js.any(error))),
            )
            self.release_writer(id)
          }
          @js.Promise::resolve(())
        }),
      )
    None => ()
  }
  match reader {
    Some(reader) => self.start_reading(id, reader)
    None => ()
  }
  id
}

///|
fn WebTransportConnection::start_reading(
  self : WebTransportConnection,
  stream : Int64,
  reader : @streams.ReadableStreamDefaultReader,
) -> Unit {
  ignore(
    @js.from_async(async fn() {
      let mut ended = false
      while !ended {
        try {
          let result = reader.read()
          if result["done"].cast() {
            ended = true
            reader.release_lock()
            match self.streams.get(stream) {
              Some(state) if state.read_cancelled.val => ()
              _ => self.enqueue_stream_event(StreamFinished(stream))
            }
            self.release_reader(stream)
          } else {
            let view : @arraybuffer.Uint8Array = result["value"].cast()
            self.enqueue_stream_event(
              Data(
                stream,
                uint8array_to_bytes(ffi_webtransport_copy_view(view)),
              ),
            )
          }
        } catch {
          error => {
            ended = true
            reader.release_lock()
            self.enqueue_stream_event(
              StreamReset(stream, webtransport_failure(@js.any(error))),
            )
            self.release_reader(stream)
          }
        }
      }
    }),
  )
}

///|
fn WebTransportConnection::release_reader(
  self : WebTransportConnection,
  stream : Int64,
) -> Unit {
  match self.streams.get(stream) {
    Some(state) => {
      state.reader = None
      if state.writer is None {
        self.streams.remove(stream)
      } else {
        self.streams[stream] = state
      }
    }
    None => ()
  }
}

///|
fn WebTransportConnection::start_accepting_streams(
  self : WebTransportConnection,
) -> Unit {
  let incoming_uni : @streams.ReadableStream = self.raw["incomingUnidirectionalStreams"].cast()
  let incoming_bidi : @streams.ReadableStream = self.raw["incomingBidirectionalStreams"].cast()
  self.start_accepting(incoming_uni, Unidirectional)
  self.start_accepting(incoming_bidi, Bidirectional)
}

///|
fn WebTransportConnection::start_accepting(
  self : WebTransportConnection,
  incoming : @streams.ReadableStream,
  direction : WebTransportStreamDirection,
) -> Unit {
  let reader = incoming.get_reader()
  ignore(
    @js.from_async(async fn() {
      let mut ended = false
      while !ended {
        try {
          let result = reader.read()
          if result["done"].cast() {
            ended = true
            reader.release_lock()
          } else {
            let stream : @js.Any = result["value"]
            ignore(self.register_stream(stream, direction, false))
          }
        } catch {
          _ => {
            ended = true
            reader.release_lock()
          }
        }
      }
    }),
  )
}

///|
/// Open a local unidirectional stream and return its opaque session reference.
/// The corresponding StreamStarted event is queued before any data event.
pub fn WebTransportConnection::open_unidirectional(
  self : WebTransportConnection,
) -> @js.Promise[Result[Int64, WebTransportFailure]] {
  self.open_stream("createUnidirectionalStream", Unidirectional)
}

///|
/// Open a local bidirectional stream and return its opaque session reference.
pub fn WebTransportConnection::open_bidirectional(
  self : WebTransportConnection,
) -> @js.Promise[Result[Int64, WebTransportFailure]] {
  self.open_stream("createBidirectionalStream", Bidirectional)
}

///|
fn WebTransportConnection::open_stream(
  self : WebTransportConnection,
  method_name : String,
  direction : WebTransportStreamDirection,
) -> @js.Promise[Result[Int64, WebTransportFailure]] {
  if self.locally_closed.val || self.connection_ended.val {
    return @js.Promise::resolve(
      Err(WebTransportFailure::{
        name: "InvalidStateError",
        message: "connection is closed",
        source: Some("session"),
        stream_error_code: None,
      }),
    )
  }
  if self.pending_stream_opens.val >= max_pending_stream_opens {
    return @js.Promise::resolve(
      Err(WebTransportFailure::{
        name: "QuotaExceededError",
        message: "stream open queue is full",
        source: Some("session"),
        stream_error_code: None,
      }),
    )
  }
  self.pending_stream_opens.val += 1
  let result = ffi_webtransport_settle(self.raw._call(method_name, []))
  result.then(fn(settled) {
    self.pending_stream_opens.val -= 1
    if !settled["ok"].cast() {
      @js.Promise::resolve(Err(webtransport_failure(settled["error"])))
    } else if self.locally_closed.val || self.connection_ended.val {
      let stream : @js.Any = settled["value"]
      ffi_webtransport_discard_stream(
        stream,
        direction is Bidirectional,
        true,
        direction is Unidirectional,
      )
      @js.Promise::resolve(
        Err(WebTransportFailure::{
          name: "AbortError",
          message: "connection closed while opening stream",
          source: Some("session"),
          stream_error_code: None,
        }),
      )
    } else {
      let stream : @js.Any = settled["value"]
      @js.Promise::resolve(Ok(self.register_stream(stream, direction, true)))
    }
  })
}

///|
/// Write bytes in call order, respecting writer.ready. `fin=true` closes the
/// send side after the write. Completion means the browser accepted the write;
/// it does not report a QUIC acknowledgement.
pub fn WebTransportConnection::write_stream(
  self : WebTransportConnection,
  stream : Int64,
  bytes : Bytes,
  fin~ : Bool = false,
) -> @js.Promise[Result[Unit, WebTransportFailure]] {
  if self.locally_closed.val || self.connection_ended.val {
    return @js.Promise::resolve(
      Err(WebTransportFailure::{
        name: "InvalidStateError",
        message: "connection is closed",
        source: Some("session"),
        stream_error_code: None,
      }),
    )
  }
  if self.pending_send_count.val >= max_pending_stream_sends ||
    bytes.length() > max_pending_stream_send_bytes - self.pending_send_bytes.val {
    return @js.Promise::resolve(
      Err(WebTransportFailure::{
        name: "QuotaExceededError",
        message: "stream send queue is full",
        source: Some("session"),
        stream_error_code: None,
      }),
    )
  }
  let state = match self.streams.get(stream) {
    Some(state) => state
    None =>
      return @js.Promise::resolve(
        Err(WebTransportFailure::{
          name: "InvalidStateError",
          message: "unknown stream",
          source: Some("session"),
          stream_error_code: None,
        }),
      )
  }
  let writer = match state.writer {
    Some(writer) => writer
    None =>
      return @js.Promise::resolve(
        Err(WebTransportFailure::{
          name: "InvalidStateError",
          message: "stream has no send side",
          source: Some("session"),
          stream_error_code: None,
        }),
      )
  }
  self.pending_send_count.val += 1
  self.pending_send_bytes.val += bytes.length()
  @js.from_async(async fn() {
    try {
      writer.ready().wait()
      let data = bytes_to_uint8array(bytes)
      writer.write(data.as_any())
      if fin {
        writer.close()
        writer.release_lock()
        self.release_writer(stream)
      }
      Ok(())
    } catch {
      error => Err(webtransport_failure(@js.any(error)))
    }
  }).then(fn(result) {
    self.pending_send_count.val -= 1
    self.pending_send_bytes.val -= bytes.length()
    @js.Promise::resolve(result)
  })
}

///|
/// Cancel the receive side. This maps to reader.cancel and reports a stream
/// reset only when the browser rejects the operation.
pub fn WebTransportConnection::cancel_stream_read(
  self : WebTransportConnection,
  stream : Int64,
  error_code : Int64,
) -> @js.Promise[Result[Unit, WebTransportFailure]] {
  match self.streams.get(stream) {
    Some(state) =>
      match state.reader {
        Some(reader) =>
          @js.from_async(async fn() {
            try {
              state.read_cancelled.val = true
              ffi_webtransport_reader_cancel(reader.as_any(), error_code).wait()
              reader.release_lock()
              self.release_reader(stream)
              Ok(())
            } catch {
              error => Err(webtransport_failure(@js.any(error)))
            }
          })
        None =>
          @js.Promise::resolve(
            Err(WebTransportFailure::{
              name: "InvalidStateError",
              message: "stream has no receive side",
              source: Some("session"),
              stream_error_code: None,
            }),
          )
      }
    None =>
      @js.Promise::resolve(
        Err(WebTransportFailure::{
          name: "InvalidStateError",
          message: "unknown stream",
          source: Some("session"),
          stream_error_code: None,
        }),
      )
  }
}

///|
/// Abort the send side with a WebTransport stream error code.
pub fn WebTransportConnection::abort_stream_write(
  self : WebTransportConnection,
  stream : Int64,
  error_code : Int64,
) -> @js.Promise[Result[Unit, WebTransportFailure]] {
  match self.streams.get(stream) {
    Some(state) =>
      match state.writer {
        Some(writer) =>
          @js.from_async(async fn() {
            try {
              state.write_aborted.val = true
              ffi_webtransport_writer_abort(writer.as_any(), error_code).wait()
              writer.release_lock()
              self.release_writer(stream)
              Ok(())
            } catch {
              error => Err(webtransport_failure(@js.any(error)))
            }
          })
        None =>
          @js.Promise::resolve(
            Err(WebTransportFailure::{
              name: "InvalidStateError",
              message: "stream has no send side",
              source: Some("session"),
              stream_error_code: None,
            }),
          )
      }
    None =>
      @js.Promise::resolve(
        Err(WebTransportFailure::{
          name: "InvalidStateError",
          message: "unknown stream",
          source: Some("session"),
          stream_error_code: None,
        }),
      )
  }
}

///|
fn WebTransportConnection::release_writer(
  self : WebTransportConnection,
  stream : Int64,
) -> Unit {
  match self.streams.get(stream) {
    Some(state) => {
      state.writer = None
      if state.reader is None {
        self.streams.remove(stream)
      } else {
        self.streams[stream] = state
      }
    }
    None => ()
  }
}

///|
extern "js" fn ffi_webtransport_writer_abort(
  writer : @js.Any,
  error_code : Int64,
) -> @js.Promise[Unit] =
  #| (writer, errorCode) => writer.abort(Number(errorCode))

///|
extern "js" fn ffi_webtransport_reader_cancel(
  reader : @js.Any,
  error_code : Int64,
) -> @js.Promise[Unit] =
  #| (reader, errorCode) => reader.cancel(Number(errorCode))

///|
extern "js" fn ffi_webtransport_copy_view(
  view : @arraybuffer.Uint8Array,
) -> @arraybuffer.Uint8Array =
  #| (view) => view.slice()

///|
extern "js" fn ffi_webtransport_discard_stream(
  stream : @js.Any,
  has_reader : Bool,
  has_writer : Bool,
  unidirectional : Bool,
) -> Unit =
  #| (stream, hasReader, hasWriter, unidirectional) => {
  #|   const readable = unidirectional ? stream : stream.readable;
  #|   const writable = unidirectional ? stream : stream.writable;
  #|   if (hasReader) void readable.cancel().catch(() => {});
  #|   if (hasWriter) void writable.abort().catch(() => {});
  #| }

///|
/// Resolves `true` when Draft 18 negotiation succeeds. If the timeout expires,
/// the pending WebTransport connection is closed and this resolves `false`.
/// Rejections from the browser's `ready` promise are preserved.
pub fn WebTransportConnection::wait_ready(
  self : WebTransportConnection,
  timeout_ms : Int32,
) -> @js.Promise[Result[Bool, WebTransportFailure]] {
  let ready : @js.Promise[Result[Bool, WebTransportFailure]] = self.ready.then(fn(
    result,
  ) {
    @js.Promise::resolve(result.map(_ => true))
  })
  let timeout : @js.Promise[Result[Bool, WebTransportFailure]] = @js.from_async(async fn() {
      @js.sleep(timeout_ms)
      Ok(false)
    },
  )
  let raced : @js.Promise[Result[Bool, WebTransportFailure]] = @js.Promise::race([
      ready, timeout,
    ],
  )
  raced.then(fn(result) {
    if result is Ok(false) {
      ignore(self.close(0L, ""))
    }
    @js.Promise::resolve(result)
  })
}

///|
pub fn WebTransportConnection::ready(
  self : WebTransportConnection,
) -> @js.Promise[Result[String, WebTransportFailure]] {
  self.ready
}

///|
pub fn WebTransportConnection::closed(
  self : WebTransportConnection,
) -> @js.Promise[WebTransportClose] {
  self.closed
}

///|
pub fn WebTransportConnection::protocol(
  self : WebTransportConnection,
) -> String {
  self.raw["protocol"].cast()
}

///|
fn webtransport_failure(error : @js.Any) -> WebTransportFailure {
  let source = error["source"]
  let source = if @js.is_nullish(source) { None } else { Some(source.cast()) }
  let code = error["streamErrorCode"]
  let stream_error_code = if @js.is_nullish(code) {
    None
  } else {
    Some(ffi_webtransport_stream_error_code(code))
  }
  WebTransportFailure::{
    name: error["name"].cast(),
    message: error["message"].cast(),
    source,
    stream_error_code,
  }
}

///|
/// Close the session once. Close codes must fit the Web API's unsigned-long
/// range and reason must fit its 1024-byte UTF-8 limit.
pub fn WebTransportConnection::close(
  self : WebTransportConnection,
  close_code : Int64,
  reason : String,
) -> Result[Unit, WebTransportError] {
  if close_code < 0L || close_code > 4294967295L {
    return Err(InvalidCloseCode(close_code))
  }
  let reason_bytes = @utf8.encode(reason).length()
  if reason_bytes > 1024 {
    return Err(CloseReasonTooLong(reason_bytes))
  }
  if !self.locally_closed.val {
    self.locally_closed.val = true
    ffi_webtransport_close(self.raw, close_code, reason)
  }
  Ok(())
}

///|
pub fn WebTransportConnection::was_closed_locally(
  self : WebTransportConnection,
) -> Bool {
  self.locally_closed.val
}

///|
extern "js" fn ffi_webtransport_url(moqt_uri : String) -> String =
  #| (moqtUri) => {
  #|   const url = new URL(moqtUri);
  #|   if (url.protocol !== "moqt:" || !url.hostname || url.hash || url.username || url.password) {
  #|     throw new TypeError("expected a moqt:// URL without credentials or fragment");
  #|   }
  #|   return url.href.replace(/^moqt:/, "https:");
  #| }

///|
extern "js" fn ffi_webtransport_new(
  url : String,
  protocol : String,
  certificate_hashes : FixedArray[@arraybuffer.Uint8Array],
) -> @js.Any =
  #| (url, protocol, certificateHashes) => {
  #|   const WebTransport = globalThis["WebTransport"];
  #|   if (typeof WebTransport !== "function") {
  #|     throw new Error("WebTransport API is unavailable; use a supporting browser in a secure context");
  #|   }
  #|   return new WebTransport(url, {
  #|     protocols: [protocol],
  #|     serverCertificateHashes: certificateHashes.map(value => ({ algorithm: "sha-256", value })),
  #|   });
  #| }

///|
extern "js" fn ffi_webtransport_close(
  transport : @js.Any,
  close_code : Int64,
  reason : String,
) -> Unit =
  #| (transport, closeCode, reason) => transport.close({ closeCode: Number(closeCode), reason })

///|
extern "js" fn ffi_webtransport_close_code(info : @js.Any) -> Int64 =
  #| (info) => BigInt(info.closeCode)

///|
extern "js" fn ffi_webtransport_stream_error_code(code : @js.Any) -> Int64 =
  #| (code) => BigInt(code)

///|
extern "js" fn ffi_webtransport_settle(
  promise : @js.Any,
) -> @js.Promise[@js.Any] =
  #| (promise) => Promise.resolve(promise).then(
  #|   value => ({ ok: true, value }),
  #|   error => ({
  #|     ok: false,
  #|     error: {
  #|       name: String(error?.name ?? "Error"),
  #|       message: String(error?.message ?? error),
  #|       source: error?.source ?? null,
  #|       streamErrorCode: error?.streamErrorCode ?? null,
  #|     },
  #|   }),
  #| )
