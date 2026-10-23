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
/// Browser WebTransport session for MOQT draft-18.
/// `closed` resolves once with the local or peer close and rejects on failure.
/// Closing while `ready` is pending cancels that connection.
pub struct WebTransportConnection {
  raw : @js.Any
  ready : @js.Promise[Result[String, WebTransportFailure]]
  closed : @js.Promise[WebTransportClose]
  locally_closed : Ref[Bool]
  stream_manager : WebTransportStreamManager
  datagram_manager : WebTransportDatagramManager
}

///|
/// Create a WebTransport session from an HTTPS URL or an MOQT URI. For an
/// MOQT URI, its path and query are preserved while replacing `moqt:` with
/// `https:`. The browser's default certificate validation remains enabled.
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
            let error = WebTransportFailure::{
              name: "ProtocolError",
              message: "server selected unsupported WebTransport protocol: " +
              selected,
              source: None,
              stream_error_code: None,
            }
            match connection_box.val {
              Some(connection) => ignore(connection.close(0L, ""))
              None => ()
            }
            @js.Promise::resolve(Err(error))
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
          connection.stream_manager.connection_closed(outcome)
          connection.datagram_manager.connection_closed()
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
      stream_manager: WebTransportStreamManager::{
        raw,
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
      },
      datagram_manager: WebTransportDatagramManager::new(raw),
    }
    connection_box.val = Some(connection)
    connection.stream_manager.start_accepting_streams()
    connection.datagram_manager.start_reading()
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
/// Wait for the next stream lifecycle or data event. Data chunks preserve the
/// exact Uint8Array view range and are copied into owned MoonBit Bytes.
pub fn WebTransportConnection::next_stream_event(
  self : WebTransportConnection,
) -> @js.Promise[WebTransportStreamEvent] {
  self.stream_manager.next_stream_event()
}

///|
/// Report whether unreliable datagrams are available and the largest payload
/// accepted by this WebTransport session.
pub fn WebTransportConnection::datagram_capability(
  self : WebTransportConnection,
) -> WebTransportDatagramCapability {
  self.datagram_manager.capability()
}

///|
/// Wait for one incoming datagram. Each event preserves the browser's original
/// datagram boundary; payload bytes are copied into MoonBit-owned storage.
pub fn WebTransportConnection::next_datagram(
  self : WebTransportConnection,
) -> @js.Promise[WebTransportDatagramEvent] {
  self.datagram_manager.next_datagram()
}

///|
/// Send one datagram without adding MoQ or HTTP Datagram framing. Completion
/// reports local Web Streams write completion, not QUIC acknowledgement or
/// peer delivery.
pub fn WebTransportConnection::send_datagram(
  self : WebTransportConnection,
  bytes : Bytes,
) -> @js.Promise[Result[Unit, WebTransportDatagramError]] {
  self.datagram_manager.send_datagram(bytes)
}

///|
/// Open a local unidirectional stream and return its opaque session reference.
/// The corresponding StreamStarted event is queued before any data event.
pub fn WebTransportConnection::open_unidirectional(
  self : WebTransportConnection,
) -> @js.Promise[Result[Int64, WebTransportFailure]] {
  self.stream_manager.open_unidirectional()
}

///|
/// Open a local bidirectional stream and return its opaque session reference.
pub fn WebTransportConnection::open_bidirectional(
  self : WebTransportConnection,
) -> @js.Promise[Result[Int64, WebTransportFailure]] {
  self.stream_manager.open_bidirectional()
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
  self.stream_manager.write_stream(stream, bytes, fin~)
}

///|
/// Cancel the receive side. This maps to reader.cancel and reports a stream
/// reset only when the browser rejects the operation.
pub fn WebTransportConnection::cancel_stream_read(
  self : WebTransportConnection,
  stream : Int64,
  error_code : Int64,
) -> @js.Promise[Result[Unit, WebTransportFailure]] {
  self.stream_manager.cancel_stream_read(stream, error_code)
}

///|
/// Abort the send side with a WebTransport stream error code.
pub fn WebTransportConnection::abort_stream_write(
  self : WebTransportConnection,
  stream : Int64,
  error_code : Int64,
) -> @js.Promise[Result[Unit, WebTransportFailure]] {
  self.stream_manager.abort_stream_write(stream, error_code)
}

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
#warnings("-result_error_return")
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
  #|   if (!["moqt:", "https:"].includes(url.protocol) || !url.hostname || url.hash || url.username || url.password) {
  #|     throw new TypeError("expected an https:// or moqt:// URL without credentials or fragment");
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
