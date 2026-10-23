///|
async test "WebTransport connection, negotiation, timeout, and close lifecycle" {
  ffi_install_fake_webtransport("moqt-18", false, false)
  let connection = match
    @js_adapter.webtransport_connect("moqt://example.test/live?q=1") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  assert_eq(ffi_fake_url(), "https://example.test/live?q=1")
  assert_eq(ffi_fake_protocol(), "moqt-18")
  assert_true(connection.ready().wait() is Ok("moqt-18"))
  ffi_fake_peer_close(37, "peer done")
  match connection.closed().wait() {
    PeerClose(info) => {
      assert_eq(info.close_code, 37L)
      assert_eq(info.reason, "peer done")
    }
    LocalClose => fail("peer close was reported as local")
    Failed(error) => fail("unexpected close failure: " + error.message)
  }

  ffi_install_fake_webtransport("not-moqt-18", false, false)
  let mismatch = match
    @js_adapter.webtransport_connect("moqt://example.test/mismatch") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  match mismatch.ready().wait() {
    Err(error) => assert_eq(error.name, "ProtocolError")
    Ok(_) => fail("a different subprotocol was accepted")
  }

  ffi_install_fake_webtransport("moqt-18", false, true)
  let failed = match
    @js_adapter.webtransport_connect("moqt://example.test/failure") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  match failed.ready().wait() {
    Err(error) => assert_eq(error.message, "ready failed")
    Ok(_) => fail("a rejected ready promise was accepted")
  }

  ffi_install_fake_webtransport("moqt-18", true, false)
  let pending = match
    @js_adapter.webtransport_connect("moqt://example.test/path") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  assert_true(pending.wait_ready(1).wait() is Ok(false))
  assert_true(pending.was_closed_locally())
  assert_true(pending.close(4L, "again") is Ok(_))
  assert_eq(ffi_fake_close_calls(), 1)
  ffi_fake_resolve_ready()
  match pending.ready().wait() {
    Err(error) => assert_eq(error.name, "AbortError")
    Ok(_) => fail("delayed ready succeeded after local close")
  }
  match pending.closed().wait() {
    LocalClose => ()
    PeerClose(_) => fail("local close was reported as peer close")
    Failed(error) => fail("unexpected close failure: " + error.message)
  }

  ffi_install_fake_webtransport("moqt-18", false, false)
  ffi_remove_fake_webtransport()
  assert_true(
    @js_adapter.webtransport_connect("moqt://example.test/unavailable")
    is Err(_),
  )
  ffi_install_fake_webtransport("moqt-18", false, false)
  assert_true(
    @js_adapter.webtransport_connect("https://example.test/path") is Err(_),
  )
  let validation = match
    @js_adapter.webtransport_connect("moqt://example.test/path") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  assert_true(validation.close(-1L, "") is Err(_))
  assert_true(validation.close(0L, "界".repeat(342)) is Err(_))
  let _with_hash = match
    @js_adapter.webtransport_connect_with_certificate_hashes(
      "moqt://example.test/local",
      [@js_adapter.bytes_to_uint8array(b"test hash")],
    ) {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  assert_eq(ffi_fake_certificate_hash_algorithm(), "sha-256")
}

///|
extern "js" fn ffi_install_fake_webtransport(
  protocol : String,
  pending_ready : Bool,
  reject_ready : Bool,
) -> Unit =
  #| (protocol, pendingReady, rejectReady) => {
  #|   globalThis.__moqWtLast = null;
  #|   globalThis.WebTransport = class {
  #|     constructor(url, options) {
  #|       this.url = url;
  #|       this.offered = options.protocols[0];
  #|       this.certificateHashes = options.serverCertificateHashes;
  #|       this.protocol = protocol;
  #|       this.closeCalls = 0;
  #|       this.resolveReady = undefined;
  #|       this.resolveClosed = undefined;
  #|       this.ready = pendingReady
  #|         ? new Promise(resolve => { this.resolveReady = resolve; })
  #|         : rejectReady ? Promise.reject(new Error("ready failed")) : Promise.resolve();
  #|       this.closed = new Promise(resolve => { this.resolveClosed = resolve; });
  #|       globalThis.__moqWtLast = this;
  #|     }
  #|     close(info) {
  #|       this.closeCalls++;
  #|       this.resolveClosed(info);
  #|     }
  #|   };
  #| }

///|
extern "js" fn ffi_fake_url() -> String =
  #| () => globalThis.__moqWtLast.url

///|
extern "js" fn ffi_fake_protocol() -> String =
  #| () => globalThis.__moqWtLast.offered

///|
extern "js" fn ffi_fake_certificate_hash_algorithm() -> String =
  #| () => globalThis.__moqWtLast.certificateHashes[0].algorithm

///|
extern "js" fn ffi_fake_close_calls() -> Int32 =
  #| () => globalThis.__moqWtLast.closeCalls

///|
extern "js" fn ffi_fake_peer_close(code : Int32, reason : String) -> Unit =
  #| (code, reason) => globalThis.__moqWtLast.resolveClosed({ closeCode: code, reason })

///|
extern "js" fn ffi_fake_resolve_ready() -> Unit =
  #| () => globalThis.__moqWtLast.resolveReady()

///|
extern "js" fn ffi_remove_fake_webtransport() -> Unit =
  #| () => { delete globalThis.WebTransport; }

///|
