///|
pub(all) enum WebTransportStreamDirection {
  Unidirectional
  Bidirectional
}

///|
pub(all) enum WebTransportStreamEvent {
  StreamStarted(Int64, WebTransportStreamDirection)
  LocalStreamOpened(Int64, WebTransportStreamDirection)
  Data(Int64, Bytes)
  StreamFinished(Int64)
  /// WebTransport close fulfilled after reaching all data committed.
  StreamDataCommitted(Int64)
  StreamReset(Int64, WebTransportFailure)
  SendStopped(Int64, WebTransportFailure)
  StreamQueueOverflow(Int64)
  ConnectionClosed(WebTransportClose)
}

///|
priv struct WebTransportStreamState {
  mut reader : @streams.ReadableStreamDefaultReader?
  mut writer : @streams.WritableStreamDefaultWriter?
  read_cancelled : Ref[Bool]
  write_aborted : Ref[Bool]
  write_closed : Ref[Bool]
}

///|
struct WebTransportStreamManager {
  raw : @js.Any
  locally_closed : Ref[Bool]
  streams : Map[Int64, WebTransportStreamState]
  events : Array[WebTransportStreamEvent]
  event_waiters : Array[(WebTransportStreamEvent) -> Unit]
  next_stream_ref : Ref[Int64]
  pending_stream_opens : Ref[Int32]
  send_budget : SendQueueBudget
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
fn WebTransportStreamManager::connection_closed(
  self : WebTransportStreamManager,
  outcome : WebTransportClose,
) -> Unit {
  self.connection_ended.val = true
  self.final_close.val = Some(outcome)
  self.shutdown_streams()
  self.enqueue_stream_event(ConnectionClosed(outcome))
}

///|
fn WebTransportStreamManager::enqueue_stream_event(
  self : WebTransportStreamManager,
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
  if self.connection_ended.val || self.locally_closed.val {
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
          | LocalStreamOpened(stream, _)
          | Data(stream, _)
          | StreamFinished(stream)
          | StreamDataCommitted(stream)
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
fn WebTransportStreamManager::stop_stream(
  self : WebTransportStreamManager,
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
fn WebTransportStreamManager::next_stream_event(
  self : WebTransportStreamManager,
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
fn WebTransportStreamManager::allocate_stream_ref(
  self : WebTransportStreamManager,
) -> Int64 {
  let id = self.next_stream_ref.val
  self.next_stream_ref.val += 1L
  id
}

///|
fn WebTransportStreamManager::shutdown_streams(
  self : WebTransportStreamManager,
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
fn WebTransportStreamManager::register_stream(
  self : WebTransportStreamManager,
  stream : @js.Any,
  direction : WebTransportStreamDirection,
  locally_opened : Bool,
) -> Int64 {
  let has_reader = direction is Bidirectional || !locally_opened
  let has_writer = direction is Bidirectional || locally_opened
  if self.connection_ended.val || self.locally_closed.val {
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
    self.enqueue_stream_event(
      if locally_opened {
        LocalStreamOpened(id, direction)
      } else {
        StreamStarted(id, direction)
      },
    )
    self.enqueue_stream_event(StreamQueueOverflow(id))
    ffi_webtransport_discard_stream(
      stream,
      has_reader,
      has_writer,
      direction is Unidirectional,
    )
    return 0L
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
    write_closed: Ref(false),
  }
  self.streams[id] = state
  self.enqueue_stream_event(
    if locally_opened {
      LocalStreamOpened(id, direction)
    } else {
      StreamStarted(id, direction)
    },
  )
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
            writer.release_lock()
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
fn WebTransportStreamManager::start_reading(
  self : WebTransportStreamManager,
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
              Some(state) if !state.read_cancelled.val =>
                self.enqueue_stream_event(StreamFinished(stream))
              _ => ()
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
            match self.streams.get(stream) {
              Some(state) if !state.read_cancelled.val =>
                self.enqueue_stream_event(
                  StreamReset(stream, webtransport_failure(@js.any(error))),
                )
              _ => ()
            }
            self.release_reader(stream)
          }
        }
      }
    }),
  )
}

///|
fn WebTransportStreamManager::release_reader(
  self : WebTransportStreamManager,
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
fn WebTransportStreamManager::start_accepting_streams(
  self : WebTransportStreamManager,
) -> Unit {
  let incoming_uni : @streams.ReadableStream = self.raw["incomingUnidirectionalStreams"].cast()
  let incoming_bidi : @streams.ReadableStream = self.raw["incomingBidirectionalStreams"].cast()
  self.start_accepting(incoming_uni, Unidirectional)
  self.start_accepting(incoming_bidi, Bidirectional)
}

///|
fn WebTransportStreamManager::start_accepting(
  self : WebTransportStreamManager,
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
fn WebTransportStreamManager::open_unidirectional(
  self : WebTransportStreamManager,
) -> @js.Promise[Result[Int64, WebTransportFailure]] {
  self.open_stream("createUnidirectionalStream", Unidirectional)
}

///|
/// Open a local bidirectional stream and return its opaque session reference.
fn WebTransportStreamManager::open_bidirectional(
  self : WebTransportStreamManager,
) -> @js.Promise[Result[Int64, WebTransportFailure]] {
  self.open_stream("createBidirectionalStream", Bidirectional)
}

///|
fn WebTransportStreamManager::open_stream(
  self : WebTransportStreamManager,
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
      let id = self.register_stream(stream, direction, true)
      if id == 0L {
        @js.Promise::resolve(
          Err(WebTransportFailure::{
            name: "QuotaExceededError",
            message: "active stream limit reached",
            source: Some("session"),
            stream_error_code: None,
          }),
        )
      } else {
        @js.Promise::resolve(Ok(id))
      }
    }
  })
}

///|
/// Write bytes in call order, respecting writer.ready. `fin=true` closes the
/// send side after the write. Completion means the browser accepted the write;
/// non-final writes do not report a QUIC acknowledgement. A final write also
/// waits for close, which the WebTransport specification defines to wait for
/// all data committed.
fn WebTransportStreamManager::write_stream(
  self : WebTransportStreamManager,
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
  if !self.send_budget.has_capacity(bytes.length()) {
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
  if state.write_closed.val {
    return @js.Promise::resolve(
      Err(WebTransportFailure::{
        name: "InvalidStateError",
        message: "stream send side is already closed",
        source: Some("session"),
        stream_error_code: None,
      }),
    )
  }
  if fin {
    state.write_closed.val = true
  }
  self.send_budget.acquire(bytes.length())
  @js.from_async(async fn() {
    try {
      writer.ready().wait()
      let data = bytes_to_uint8array(bytes)
      writer.write(data.as_any())
      if fin {
        writer.close()
        self.enqueue_stream_event(StreamDataCommitted(stream))
        writer.release_lock()
        self.release_writer(stream)
      }
      Ok(())
    } catch {
      error => {
        state.write_aborted.val = true
        writer.release_lock()
        self.release_writer(stream)
        Err(webtransport_failure(@js.any(error)))
      }
    }
  }).then(fn(result) {
    self.send_budget.release(bytes.length())
    @js.Promise::resolve(result)
  })
}

///|
/// Cancel the receive side. This maps to reader.cancel and reports a stream
/// reset only when the browser rejects the operation.
fn WebTransportStreamManager::cancel_stream_read(
  self : WebTransportStreamManager,
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
fn WebTransportStreamManager::abort_stream_write(
  self : WebTransportStreamManager,
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
              state.write_closed.val = true
              ffi_webtransport_writer_abort(writer.as_any(), error_code).wait()
              writer.release_lock()
              self.release_writer(stream)
              Ok(())
            } catch {
              error => {
                writer.release_lock()
                self.release_writer(stream)
                Err(webtransport_failure(@js.any(error)))
              }
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
fn WebTransportStreamManager::release_writer(
  self : WebTransportStreamManager,
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
  #| (writer, errorCode) => writer.abort(new WebTransportError({ streamErrorCode: Number(errorCode) }))

///|
extern "js" fn ffi_webtransport_reader_cancel(
  reader : @js.Any,
  error_code : Int64,
) -> @js.Promise[Unit] =
  #| (reader, errorCode) => reader.cancel(new WebTransportError({ streamErrorCode: Number(errorCode) }))

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
