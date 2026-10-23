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
  ffi_install_fake_webtransport("moqt-18", false, false)
  let connection = match
    @js_adapter.webtransport_connect("moqt://example.test/streams") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  let stream = match connection.open_bidirectional().wait() {
    Ok(stream) => stream
    Err(error) => fail(error.message)
  }
  match connection.next_stream_event().wait() {
    StreamStarted(id, Bidirectional) => assert_eq(id, stream)
    _ => fail("local stream start was not announced first")
  }
  let view = ffi_fake_offset_view("payload")
  ffi_fake_push_created_data(view)
  match connection.next_stream_event().wait() {
    Data(id, bytes) => {
      assert_eq(id, stream)
      ffi_fake_mutate_view(view)
      assert_eq(bytes, b"payload")
    }
    _ => fail("local stream data was not received")
  }
  assert_true(connection.write_stream(stream, b"reply").wait() is Ok(_))
  assert_eq(ffi_fake_created_write_count(), 1)
  assert_eq(ffi_fake_created_write_bytes(), "reply")
  assert_true(connection.write_stream(stream, b"", fin=true).wait() is Ok(_))
  ffi_fake_finish_created_readable()
  match connection.next_stream_event().wait() {
    StreamFinished(id) => assert_eq(id, stream)
    _ => fail("receive FIN was not reported independently")
  }

  ffi_fake_push_peer_unidirectional(@js_adapter.bytes_to_uint8array(b"peer"))
  let peer_stream = match connection.next_stream_event().wait() {
    StreamStarted(id, Unidirectional) => id
    _ => fail("peer unidirectional stream start was not reported")
  }
  match connection.next_stream_event().wait() {
    Data(id, bytes) => {
      assert_eq(id, peer_stream)
      assert_eq(bytes, b"peer")
    }
    _ => fail("peer stream data was not received")
  }
  match connection.next_stream_event().wait() {
    StreamFinished(id) => assert_eq(id, peer_stream)
    _ => fail("peer unidirectional FIN was not reported")
  }

  let uni_stream = match connection.open_unidirectional().wait() {
    Ok(stream) => stream
    Err(error) => fail(error.message)
  }
  match connection.next_stream_event().wait() {
    StreamStarted(id, Unidirectional) => assert_eq(id, uni_stream)
    _ => fail("local unidirectional stream start was not reported")
  }
  assert_true(
    connection.write_stream(uni_stream, b"uni", fin=true).wait() is Ok(_),
  )

  let cancelled_stream = match connection.open_bidirectional().wait() {
    Ok(stream) => stream
    Err(error) => fail(error.message)
  }
  match connection.next_stream_event().wait() {
    StreamStarted(id, Bidirectional) => assert_eq(id, cancelled_stream)
    _ => fail("cancellable stream start was not reported")
  }
  assert_true(
    connection.cancel_stream_read(cancelled_stream, 23L).wait() is Ok(_),
  )
  assert_true(
    connection.abort_stream_write(cancelled_stream, 24L).wait() is Ok(_),
  )
  let next_event = connection
    .next_stream_event()
    .then(fn(_) { @js.Promise::resolve(false) })
  let timeout = @js.from_async(async fn() {
    @js.sleep(5)
    true
  })
  assert_true(@js.Promise::race([next_event, timeout]).wait())
  ignore(connection.close(0L, "done"))
  ignore(connection.closed().wait())

  ffi_install_fake_webtransport("moqt-18", false, false)
  let pending_connection = match
    @js_adapter.webtransport_connect("moqt://example.test/open-race") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  ffi_fake_delay_stream_opens()
  let pending_opens : Array[
    @js.Promise[Result[Int64, @js_adapter.WebTransportFailure]],
  ] = []
  for _ in 0..<16 {
    pending_opens.push(pending_connection.open_bidirectional())
  }
  match pending_connection.open_unidirectional().wait() {
    Err(error) => assert_eq(error.name, "QuotaExceededError")
    Ok(_) => fail("the stream open queue exceeded its limit")
  }
  ignore(pending_connection.close(0L, "done"))
  ffi_fake_resolve_pending_stream_opens()
  for pending in pending_opens {
    assert_true(pending.wait() is Err(_))
  }
  match pending_connection.next_stream_event().wait() {
    ConnectionClosed(LocalClose) => ()
    _ => fail("connection close event was not retained")
  }
}

///|
extern "js" fn ffi_install_fake_webtransport(
  protocol : String,
  pending_ready : Bool,
  reject_ready : Bool,
) -> Unit =
  #| (protocol, pendingReady, rejectReady) => {
  #|   globalThis.__moqWtLast = null;
  #|   globalThis.__moqWtDelayOpen = false;
  #|   globalThis.WebTransport = class {
  #|     constructor(url, options) {
  #|       this.url = url;
  #|       this.offered = options.protocols[0];
  #|       this.certificateHashes = options.serverCertificateHashes;
  #|       this.protocol = protocol;
  #|       this.closeCalls = 0;
  #|       this.resolveReady = undefined;
  #|       this.resolveClosed = undefined;
  #|       globalThis.__moqWtPendingOpenResolvers = [];
  #|       this.incomingUnidirectionalStreams = new ReadableStream({
  #|         start: controller => { this.incomingUni = controller; },
  #|       });
  #|       this.incomingBidirectionalStreams = new ReadableStream({
  #|         start: controller => { this.incomingBidi = controller; },
  #|       });
  #|       this.createUnidirectionalStream = () => Promise.resolve(
  #|         new WritableStream({ write: chunk => { this.uniWrites = chunk; } }),
  #|       );
  #|       this.createBidirectionalStream = () => new Promise(resolve => {
  #|         let readableController;
  #|         const readable = new ReadableStream({ start: c => { readableController = c; } });
  #|         const writes = [];
  #|         const writable = new WritableStream({ write: chunk => { writes.push(chunk); } });
  #|         const stream = { readable, writable };
  #|         this.created = { readableController, writes };
  #|         if (globalThis.__moqWtDelayOpen) globalThis.__moqWtPendingOpenResolvers.push(() => resolve(stream));
  #|         else resolve(stream);
  #|       });
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
extern "js" fn ffi_fake_push_created_data(
  data : @arraybuffer.Uint8Array,
) -> Unit =
  #| (data) => globalThis.__moqWtLast.created.readableController.enqueue(data)

///|
extern "js" fn ffi_fake_offset_view(value : String) -> @arraybuffer.Uint8Array =
  #| (value) => {
  #|   const encoded = new TextEncoder().encode(value);
  #|   const backing = new Uint8Array(encoded.length + 2);
  #|   backing[0] = 0xff;
  #|   backing.set(encoded, 1);
  #|   backing[backing.length - 1] = 0xee;
  #|   return backing.subarray(1, backing.length - 1);
  #| }

///|
extern "js" fn ffi_fake_mutate_view(view : @arraybuffer.Uint8Array) -> Unit =
  #| (view) => { view[0] = 0; }

///|
extern "js" fn ffi_fake_finish_created_readable() -> Unit =
  #| () => globalThis.__moqWtLast.created.readableController.close()

///|
extern "js" fn ffi_fake_push_peer_unidirectional(
  data : @arraybuffer.Uint8Array,
) -> Unit =
  #| (data) => {
  #|   const readable = new ReadableStream({ start(c) { c.enqueue(data); c.close(); } });
  #|   globalThis.__moqWtLast.incomingUni.enqueue(readable);
  #| }

///|
extern "js" fn ffi_fake_created_write_count() -> Int32 =
  #| () => globalThis.__moqWtLast.created.writes.length

///|
extern "js" fn ffi_fake_created_write_bytes() -> String =
  #| () => new TextDecoder().decode(globalThis.__moqWtLast.created.writes[0])

///|
extern "js" fn ffi_fake_delay_stream_opens() -> Unit =
  #| () => { globalThis.__moqWtDelayOpen = true; }

///|
extern "js" fn ffi_fake_resolve_pending_stream_opens() -> Unit =
  #| () => {
  #|   globalThis.__moqWtPendingOpenResolvers.forEach(resolve => resolve());
  #|   globalThis.__moqWtPendingOpenResolvers = [];
  #| }

///|
extern "js" fn ffi_remove_fake_webtransport() -> Unit =
  #| () => { delete globalThis.WebTransport; }

///|
