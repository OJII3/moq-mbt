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
}

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
      if !settled["ok"].cast() {
        @js.Promise::resolve(Failed(webtransport_failure(settled["error"])))
      } else {
        let info = settled["value"]
        let close_info = WebTransportCloseInfo::{
          close_code: ffi_webtransport_close_code(info),
          reason: info["reason"].cast(),
        }
        let outcome = if locally_closed_box.val {
          LocalClose
        } else {
          PeerClose(close_info)
        }
        @js.Promise::resolve(outcome)
      }
    })
    let connection = WebTransportConnection::{
      raw,
      ready,
      closed,
      locally_closed: locally_closed_box,
    }
    connection_box.val = Some(connection)
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
