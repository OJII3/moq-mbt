///|
/// The direction of a WebTransport stream.
pub(all) enum WebTransportStreamDirection {
  Unidirectional
  Bidirectional
}

///|
/// Why a WebTransport stream was stopped locally.
#derive(Debug, Eq)
pub(all) enum WebTransportStreamOverflowReason {
  EventQueueCount
  EventQueueBytes
  ActiveStreamLimit
}

///|
/// The stream and local limit that stopped it.
#derive(Debug, Eq)
pub(all) struct WebTransportStreamOverflow {
  stream : Int64
  reason : WebTransportStreamOverflowReason
}

///|
/// Current stream counts and overflow counters for one WebTransport connection.
/// `active_incoming` counts peer-opened streams and `active_outgoing` counts
/// locally opened streams, including bidirectional streams.
#derive(Debug, Eq)
pub(all) struct WebTransportStreamStatistics {
  active_incoming : Int32
  active_outgoing : Int32
  event_queue_count_overflows : Int32
  event_queue_byte_overflows : Int32
  active_stream_limit_overflows : Int32
  last_overflow : WebTransportStreamOverflow?
}

///|
/// An event from a browser WebTransport stream.
pub(all) enum WebTransportStreamEvent {
  StreamStarted(Int64, WebTransportStreamDirection)
  LocalStreamOpened(Int64, WebTransportStreamDirection)
  Data(Int64, Bytes)
  StreamFinished(Int64)
  /// WebTransport close fulfilled after reaching all data committed.
  StreamDataCommitted(Int64)
  StreamReset(Int64, WebTransportFailure)
  SendStopped(Int64, WebTransportFailure)
  /// The adapter stopped this stream because a local limit was exceeded.
  StreamOverflow(Int64, WebTransportStreamOverflowReason)
  ConnectionClosed(WebTransportClose)
}

///|
priv struct WebTransportStreamState {
  locally_opened : Bool
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
  mut event_queue_count_overflows : Int32
  mut event_queue_byte_overflows : Int32
  mut active_stream_limit_overflows : Int32
  mut last_overflow : WebTransportStreamOverflow?
}

///|
let max_stream_events : Int32 = 256

///|
let max_stream_event_bytes : Int32 = 1024 * 1024

///|
let max_pending_stream_opens : Int32 = 64

///|
/// Outgoing and incoming streams have independent limits so local subscriptions
/// and Object streams cannot consume the capacity reserved for peer publishers.
let max_active_incoming_streams : Int32 = 64

///|
let max_active_outgoing_streams : Int32 = 64

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
fn stream_event_stream(event : WebTransportStreamEvent) -> Int64 {
  match event {
    StreamStarted(stream, _)
    | LocalStreamOpened(stream, _)
    | Data(stream, _)
    | StreamFinished(stream)
    | StreamDataCommitted(stream)
    | StreamReset(stream, _)
    | SendStopped(stream, _)
    | StreamOverflow(stream, _) => stream
    ConnectionClosed(_) => 0L
  }
}

///|
fn WebTransportStreamManager::active_stream_count(
  self : WebTransportStreamManager,
  locally_opened : Bool,
) -> Int32 {
  let mut count = 0
  for _, state in self.streams {
    if state.locally_opened == locally_opened {
      count += 1
    }
  }
  count
}

///|
fn WebTransportStreamManager::statistics(
  self : WebTransportStreamManager,
) -> WebTransportStreamStatistics {
  {
    active_incoming: self.active_stream_count(false),
    active_outgoing: self.active_stream_count(true),
    event_queue_count_overflows: self.event_queue_count_overflows,
    event_queue_byte_overflows: self.event_queue_byte_overflows,
    active_stream_limit_overflows: self.active_stream_limit_overflows,
    last_overflow: self.last_overflow,
  }
}

///|
fn WebTransportStreamManager::has_outgoing_stream_capacity(
  self : WebTransportStreamManager,
) -> Bool {
  if self.connection_ended.val || self.locally_closed.val {
    return false
  }
  self.active_stream_count(true) + self.pending_stream_opens.val <
  max_active_outgoing_streams
}

///|
fn WebTransportStreamManager::record_stream_overflow(
  self : WebTransportStreamManager,
  stream : Int64,
  reason : WebTransportStreamOverflowReason,
) -> Unit {
  match reason {
    EventQueueCount => self.event_queue_count_overflows += 1
    EventQueueBytes => self.event_queue_byte_overflows += 1
    ActiveStreamLimit => self.active_stream_limit_overflows += 1
  }
  self.last_overflow = Some({ stream, reason, })
}

///|
/// Stop one stream and enqueue its overflow notice even when the event queue is
/// full. The session must learn about the drop to release that stream's state.
fn WebTransportStreamManager::drop_stream_for_overflow(
  self : WebTransportStreamManager,
  stream : Int64,
  reason : WebTransportStreamOverflowReason,
) -> Unit {
  self.record_stream_overflow(stream, reason)
  self.stop_stream(stream)
  let event = StreamOverflow(stream, reason)
  if !self.event_waiters.is_empty() {
    let waiter = self.event_waiters[0]
    ignore(self.event_waiters.remove(0))
    waiter(event)
  } else {
    self.events.push(event)
  }
}

///|
fn WebTransportStreamManager::enqueue_stream_event(
  self : WebTransportStreamManager,
  event : WebTransportStreamEvent,
) -> Unit {
  if event is ConnectionClosed(_) {
    self.events.clear()
    self.pending_event_bytes.val = 0
    if !self.event_waiters.is_empty() {
      while !self.event_waiters.is_empty() {
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
  if !self.event_waiters.is_empty() {
    let waiter = self.event_waiters[0]
    ignore(self.event_waiters.remove(0))
    waiter(event)
    return
  }
  match event {
    Data(stream, bytes) if bytes.length() >
      max_stream_event_bytes - self.pending_event_bytes.val =>
      self.drop_stream_for_overflow(stream, EventQueueBytes)
    _ if self.events.length() >= max_stream_events - 1 => {
      let stream = stream_event_stream(event)
      if stream != 0L {
        self.drop_stream_for_overflow(stream, EventQueueCount)
      }
    }
    Data(_, bytes) => {
      self.pending_event_bytes.val += bytes.length()
      self.events.push(event)
    }
    _ => self.events.push(event)
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
  if !self.events.is_empty() {
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
  let active_limit = if locally_opened {
    max_active_outgoing_streams
  } else {
    max_active_incoming_streams
  }
  if self.active_stream_count(locally_opened) >= active_limit {
    let id = self.allocate_stream_ref()
    self.drop_stream_for_overflow(id, ActiveStreamLimit)
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
  let state = WebTransportStreamState::{
    locally_opened,
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
  if reader is Some(reader) {
    self.start_reading(id, reader)
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
fn WebTransportStreamManager::open_unidirectional(
  self : WebTransportStreamManager,
) -> @js.Promise[Result[Int64, WebTransportFailure]] {
  self.open_stream("createUnidirectionalStream", Unidirectional)
}

///|
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
      Err({
        name: "InvalidStateError",
        message: "connection is closed",
        source: Some("session"),
        stream_error_code: None,
      }),
    )
  }
  if self.pending_stream_opens.val >= max_pending_stream_opens {
    return @js.Promise::resolve(
      Err({
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
        Err({
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
          Err({
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
fn WebTransportStreamManager::write_stream(
  self : WebTransportStreamManager,
  stream : Int64,
  bytes : Bytes,
  fin~ : Bool,
) -> @js.Promise[Result[Unit, WebTransportFailure]] {
  if self.locally_closed.val || self.connection_ended.val {
    return @js.Promise::resolve(
      Err({
        name: "InvalidStateError",
        message: "connection is closed",
        source: Some("session"),
        stream_error_code: None,
      }),
    )
  }
  if !self.send_budget.has_capacity(bytes.length()) {
    return @js.Promise::resolve(
      Err({
        name: "QuotaExceededError",
        message: "stream send queue is full",
        source: Some("session"),
        stream_error_code: None,
      }),
    )
  }
  guard self.streams.get(stream) is Some(state) else {
    return @js.Promise::resolve(
      Err({
        name: "InvalidStateError",
        message: "unknown stream",
        source: Some("session"),
        stream_error_code: None,
      }),
    )
  }
  guard state.writer is Some(writer) else {
    return @js.Promise::resolve(
      Err({
        name: "InvalidStateError",
        message: "stream has no send side",
        source: Some("session"),
        stream_error_code: None,
      }),
    )
  }
  if state.write_closed.val {
    return @js.Promise::resolve(
      Err({
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
            Err({
              name: "InvalidStateError",
              message: "stream has no receive side",
              source: Some("session"),
              stream_error_code: None,
            }),
          )
      }
    None =>
      @js.Promise::resolve(
        Err({
          name: "InvalidStateError",
          message: "unknown stream",
          source: Some("session"),
          stream_error_code: None,
        }),
      )
  }
}

///|
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
            Err({
              name: "InvalidStateError",
              message: "stream has no send side",
              source: Some("session"),
              stream_error_code: None,
            }),
          )
      }
    None =>
      @js.Promise::resolve(
        Err({
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

///|
pub extend WebTransportStreamOverflowReason with @moonbitlang/core/debug.Debug::{
  to_repr,
}

///|
pub extend WebTransportStreamOverflowReason with Eq::{equal, not_equal}

///|
pub extend WebTransportStreamOverflow with @moonbitlang/core/debug.Debug::{
  to_repr,
}

///|
pub extend WebTransportStreamOverflow with Eq::{equal, not_equal}

///|
pub extend WebTransportStreamStatistics with @moonbitlang/core/debug.Debug::{
  to_repr,
}

///|
pub extend WebTransportStreamStatistics with Eq::{equal, not_equal}
