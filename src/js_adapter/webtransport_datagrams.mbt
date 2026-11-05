///|
pub(all) struct WebTransportDatagramCapability {
  enabled : Bool
  max_length : Int32
}

///|
pub(all) enum WebTransportDatagramError {
  DatagramUnavailable
  DatagramTooLarge(Int32, Int32)
  DatagramQueueFull
  DatagramConnectionClosed
  DatagramWriteFailed(WebTransportFailure)
}

///|
pub(all) enum WebTransportDatagramEvent {
  DatagramReceived(Bytes)
  DatagramReceiveQueueFull
  DatagramsClosed
}

///|
struct WebTransportDatagramManager {
  mut reader : @streams.ReadableStreamDefaultReader?
  mut writer : @streams.WritableStreamDefaultWriter?
  datagrams : @js.Any
  queue : Array[Bytes]
  waiters : Array[(WebTransportDatagramEvent) -> Unit]
  send_budget : SendQueueBudget
  pending_receive_bytes : Ref[Int32]
  receive_queue_full : Ref[Bool]
  ended : Ref[Bool]
}

///|
let max_pending_datagram_sends : Int32 = 64

///|
let max_pending_datagram_send_bytes : Int32 = 1024 * 1024

///|
let max_pending_datagram_receives : Int32 = 256

///|
let max_pending_datagram_receive_bytes : Int32 = 1024 * 1024

///|
fn WebTransportDatagramManager::new(
  raw : @js.Any,
) -> WebTransportDatagramManager {
  let datagrams = raw["datagrams"]
  let enabled = !@js.is_nullish(datagrams) &&
    !@js.is_nullish(datagrams["readable"]) &&
    !@js.is_nullish(datagrams["writable"])
  let reader = if enabled {
    let readable : @streams.ReadableStream = datagrams["readable"].cast()
    Some(readable.get_reader())
  } else {
    None
  }
  let writer = if enabled {
    let writable : @streams.WritableStream = datagrams["writable"].cast()
    Some(writable.get_writer())
  } else {
    None
  }
  {
    reader,
    writer,
    datagrams,
    queue: [],
    waiters: [],
    send_budget: SendQueueBudget::new(
      max_pending_datagram_sends, max_pending_datagram_send_bytes,
    ),
    pending_receive_bytes: Ref(0),
    receive_queue_full: Ref(false),
    ended: Ref(false),
  }
}

///|
fn WebTransportDatagramManager::capability(
  self : WebTransportDatagramManager,
) -> WebTransportDatagramCapability {
  {
    enabled: self.writer is Some(_) && !self.ended.val,
    max_length: if self.writer is Some(_) {
      self.datagrams["maxDatagramSize"].cast()
    } else {
      0
    },
  }
}

///|
fn WebTransportDatagramManager::start_reading(
  self : WebTransportDatagramManager,
) -> Unit {
  match self.reader {
    Some(reader) =>
      ignore(
        @js.from_async(async fn() {
          let mut done = false
          while !done && !self.ended.val {
            try {
              let result = reader.read()
              if result["done"].cast() {
                done = true
                self.finish_reading(reader)
              } else {
                let view : @arraybuffer.Uint8Array = result["value"].cast()
                let bytes = uint8array_to_bytes(
                  ffi_webtransport_copy_view(view),
                )
                if !self.waiters.is_empty() {
                  let waiter = self.waiters[0]
                  ignore(self.waiters.remove(0))
                  waiter(DatagramReceived(bytes))
                } else if self.queue.length() < max_pending_datagram_receives &&
                  bytes.length() <=
                  max_pending_datagram_receive_bytes -
                  self.pending_receive_bytes.val {
                  self.queue.push(bytes)
                  self.pending_receive_bytes.val += bytes.length()
                } else {
                  done = true
                  self.receive_queue_full.val = true
                  self.finish_reading(reader)
                }
              }
            } catch {
              _ => {
                done = true
                self.finish_reading(reader)
              }
            }
          }
        }),
      )
    None => ()
  }
}

///|
fn WebTransportDatagramManager::finish_reading(
  self : WebTransportDatagramManager,
  reader : @streams.ReadableStreamDefaultReader,
) -> Unit {
  reader.release_lock()
  self.reader = None
  self.notify_closed()
}

///|
fn WebTransportDatagramManager::next_datagram(
  self : WebTransportDatagramManager,
) -> @js.Promise[WebTransportDatagramEvent] {
  if !self.queue.is_empty() {
    let bytes = self.queue[0]
    ignore(self.queue.remove(0))
    self.pending_receive_bytes.val -= bytes.length()
    return @js.Promise::resolve(DatagramReceived(bytes))
  }
  if self.receive_queue_full.val {
    self.receive_queue_full.val = false
    return @js.Promise::resolve(DatagramReceiveQueueFull)
  }
  if self.ended.val || self.reader is None {
    return @js.Promise::resolve(DatagramsClosed)
  }
  @js.Promise::new(fn(resolve, _reject) { self.waiters.push(resolve) })
}

///|
fn WebTransportDatagramManager::send_datagram(
  self : WebTransportDatagramManager,
  bytes : Bytes,
) -> @js.Promise[Result[Unit, WebTransportDatagramError]] {
  guard self.writer is Some(writer) else {
    return @js.Promise::resolve(
      Err(
        if self.ended.val {
          DatagramConnectionClosed
        } else {
          DatagramUnavailable
        },
      ),
    )
  }
  if self.ended.val {
    return @js.Promise::resolve(Err(DatagramConnectionClosed))
  }
  let max_length : Int32 = self.datagrams["maxDatagramSize"].cast()
  if bytes.length() > max_length {
    return @js.Promise::resolve(
      Err(DatagramTooLarge(bytes.length(), max_length)),
    )
  }
  if !self.send_budget.has_capacity(bytes.length()) {
    return @js.Promise::resolve(Err(DatagramQueueFull))
  }
  self.send_budget.acquire(bytes.length())
  let start_write = fn() -> @js.Promise[Result[Unit, WebTransportDatagramError]] {
    if self.ended.val {
      self.send_budget.release(bytes.length())
      return @js.Promise::resolve(Err(DatagramConnectionClosed))
    }
    let current_max_length : Int32 = self.datagrams["maxDatagramSize"].cast()
    if bytes.length() > current_max_length {
      self.send_budget.release(bytes.length())
      return @js.Promise::resolve(
        Err(DatagramTooLarge(bytes.length(), current_max_length)),
      )
    }
    let data = bytes_to_uint8array(bytes)
    // Call the browser's write directly instead of the async wrapper, which
    // awaits the write promise and serializes datagram sends.
    let write : @js.Promise[Unit] = @js.any(writer)
      ._call("write", [data.as_any()])
      .cast()
    let ok : Result[Unit, WebTransportDatagramError] = Ok(())
    let written = write.then(fn(_) {
      self.send_budget.release(bytes.length())
      @js.Promise::resolve(ok)
    })
    written.catch_(fn(error) {
      self.send_budget.release(bytes.length())
      @js.Promise::resolve(
        Err(DatagramWriteFailed(webtransport_failure(@js.any(error)))),
      )
    })
  }
  // `writer.ready` is only needed when the stream has no capacity. Waiting for
  // it unconditionally costs a promise round trip per datagram.
  let desired_size : Int32 = writer.desiredSize().cast()
  if desired_size > 0 {
    start_write()
  } else {
    writer
    .ready()
    .then(fn(_) { start_write() })
    .catch_(fn(error) {
      self.send_budget.release(bytes.length())
      @js.Promise::resolve(
        Err(DatagramWriteFailed(webtransport_failure(@js.any(error)))),
      )
    })
  }
}

///|
fn WebTransportDatagramManager::connection_closed(
  self : WebTransportDatagramManager,
) -> Unit {
  if self.ended.val {
    return
  }
  self.ended.val = true
  self.queue.clear()
  self.pending_receive_bytes.val = 0
  match self.reader {
    Some(reader) =>
      ignore(
        @js.from_async(async fn() {
          reader.cancel() catch {
            _ => ()
          }
          reader.release_lock()
          self.reader = None
          self.notify_closed()
        }),
      )
    None => self.notify_closed()
  }
  match self.writer {
    Some(writer) =>
      ignore(
        @js.from_async(async fn() {
          ffi_webtransport_abort_datagram_writer(writer.as_any()).wait() catch {
            _ => ()
          }
          writer.release_lock()
          self.writer = None
        }),
      )
    None => ()
  }
}

///|
extern "js" fn ffi_webtransport_abort_datagram_writer(
  writer : @js.Any,
) -> @js.Promise[Unit] =
  #| writer => writer.abort(new Error("datagram connection closed"))

///|
fn WebTransportDatagramManager::notify_closed(
  self : WebTransportDatagramManager,
) -> Unit {
  while !self.waiters.is_empty() {
    let waiter = self.waiters[0]
    ignore(self.waiters.remove(0))
    waiter(DatagramsClosed)
  }
}
