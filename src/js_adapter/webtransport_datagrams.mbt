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
  pending_send_count : Ref[Int32]
  pending_send_bytes : Ref[Int32]
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
    pending_send_count: Ref(0),
    pending_send_bytes: Ref(0),
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
                if self.waiters.length() > 0 {
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
  if self.queue.length() > 0 {
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
  let writer = match self.writer {
    Some(writer) => writer
    None =>
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
  if self.pending_send_count.val >= max_pending_datagram_sends ||
    bytes.length() >
    max_pending_datagram_send_bytes - self.pending_send_bytes.val {
    return @js.Promise::resolve(Err(DatagramQueueFull))
  }
  self.pending_send_count.val += 1
  self.pending_send_bytes.val += bytes.length()
  @js.from_async(async fn() {
    let result = try {
      writer.ready().wait()
      if self.ended.val {
        Err(DatagramConnectionClosed)
      } else {
        let current_max_length : Int32 = self.datagrams["maxDatagramSize"].cast()
        if bytes.length() > current_max_length {
          Err(DatagramTooLarge(bytes.length(), current_max_length))
        } else {
          let data = bytes_to_uint8array(bytes)
          writer.write(data.as_any())
          Ok(())
        }
      }
    } catch {
      error => Err(DatagramWriteFailed(webtransport_failure(@js.any(error))))
    }
    self.pending_send_count.val -= 1
    self.pending_send_bytes.val -= bytes.length()
    result
  })
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
  while self.waiters.length() > 0 {
    let waiter = self.waiters[0]
    ignore(self.waiters.remove(0))
    waiter(DatagramsClosed)
  }
}
