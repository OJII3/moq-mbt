///|
async test "WebTransport connection and session lifecycle" {
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
  assert_true(mismatch.was_closed_locally())
  assert_eq(ffi_fake_close_calls(), 1)
  assert_true(mismatch.close(0L, "again") is Ok(_))
  assert_eq(ffi_fake_close_calls(), 1)
  let mismatch_closed = mismatch.closed().wait()
  assert_true(mismatch_closed is LocalClose)

  ffi_install_fake_webtransport("moqt-18", false, true)
  let failed = match
    @js_adapter.webtransport_connect("moqt://example.test/failure") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  match failed.ready().wait() {
    Err(error) => assert_eq(error.message, "server rejected request")
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
  let https_connection = match
    @js_adapter.webtransport_connect("https://example.test/path") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  assert_true(https_connection.close(0L, "done") is Ok(_))
  assert_true(https_connection.closed().wait() is LocalClose)
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
    LocalStreamOpened(id, Bidirectional) => assert_eq(id, stream)
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
  ffi_fake_delay_stream_writes()
  let delayed_write = connection.write_stream(stream, b"blocked")
  let delayed_write_done = delayed_write.then(fn(_) {
    @js.Promise::resolve(true)
  })
  let write_timeout = @js.from_async(async fn() {
    @js.sleep(5)
    false
  })
  assert_true(!@js.Promise::race([delayed_write_done, write_timeout]).wait())
  let excess_writes : Array[
    @js.Promise[Result[Unit, @js_adapter.WebTransportFailure]],
  ] = []
  for _ in 0..<255 {
    excess_writes.push(connection.write_stream(stream, b"queued"))
  }
  match connection.write_stream(stream, b"overflow").wait() {
    Err(error) => assert_eq(error.name, "QuotaExceededError")
    Ok(_) => fail("the pending send queue exceeded its limit")
  }
  ffi_fake_resolve_pending_stream_writes()
  assert_true(delayed_write.wait() is Ok(_))
  for pending in excess_writes {
    assert_true(pending.wait() is Ok(_))
  }
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
    LocalStreamOpened(id, Unidirectional) => assert_eq(id, uni_stream)
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
    LocalStreamOpened(id, Bidirectional) => assert_eq(id, cancelled_stream)
    _ => fail("cancellable stream start was not reported")
  }
  assert_true(
    connection.cancel_stream_read(cancelled_stream, 23L).wait() is Ok(_),
  )
  assert_eq(ffi_fake_cancel_error_code(), 23)
  assert_true(
    connection.abort_stream_write(cancelled_stream, 24L).wait() is Ok(_),
  )
  assert_eq(ffi_fake_abort_error_code(), 24)
  let pending_event = connection.next_stream_event()
  let next_event = pending_event.then(fn(_) { @js.Promise::resolve(false) })
  let timeout = @js.from_async(async fn() {
    @js.sleep(5)
    true
  })
  assert_true(@js.Promise::race([next_event, timeout]).wait())
  let fin_stream = match connection.open_bidirectional().wait() {
    Ok(stream) => stream
    Err(error) => fail(error.message)
  }
  match pending_event.wait() {
    LocalStreamOpened(id, Bidirectional) => assert_eq(id, fin_stream)
    _ => fail("FIN test stream was not announced")
  }
  ffi_fake_delay_stream_writes()
  let finishing_write = connection.write_stream(fin_stream, b"last", fin=true)
  match connection.write_stream(fin_stream, b"after FIN").wait() {
    Err(error) => assert_eq(error.name, "InvalidStateError")
    Ok(_) => fail("a write was accepted after FIN was queued")
  }
  ffi_fake_resolve_pending_stream_writes()
  assert_true(finishing_write.wait() is Ok(_))
  assert_true(connection.cancel_stream_read(fin_stream, 25L).wait() is Ok(_))
  ignore(connection.close(0L, "done"))
  ignore(connection.closed().wait())

  ffi_install_fake_webtransport("moqt-18", false, false)
  let capped_connection = match
    @js_adapter.webtransport_connect("moqt://example.test/active-limit") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  for _ in 0..<16 {
    assert_true(capped_connection.open_bidirectional().wait() is Ok(_))
    match capped_connection.next_stream_event().wait() {
      LocalStreamOpened(_, Bidirectional) => ()
      _ => fail("active stream was not announced")
    }
  }
  match capped_connection.open_bidirectional().wait() {
    Err(error) => assert_eq(error.name, "QuotaExceededError")
    Ok(_) => fail("the active stream limit was reported as success")
  }
  ignore(capped_connection.close(0L, "done"))
  ignore(capped_connection.closed().wait())

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
  ffi_install_fake_webtransport("moqt-18", false, false)
  let connection = match
    @js_adapter.webtransport_connect("moqt://example.test/datagrams") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  let capability = connection.datagram_capability()
  assert_true(capability.enabled)
  assert_eq(capability.max_length, 7)
  assert_true(connection.send_datagram(b"1234567").wait() is Ok(_))
  assert_eq(ffi_fake_datagram_write(), "1234567")
  match connection.send_datagram(b"12345678").wait() {
    Err(DatagramTooLarge(actual, maximum)) => {
      assert_eq(actual, 8)
      assert_eq(maximum, 7)
    }
    _ => fail("oversized datagram was accepted")
  }
  ffi_fake_set_max_datagram_size(12)
  assert_eq(connection.datagram_capability().max_length, 12)
  assert_true(connection.send_datagram(b"12345678").wait() is Ok(_))
  ffi_fake_set_max_datagram_size(6)
  assert_eq(connection.datagram_capability().max_length, 6)
  match connection.send_datagram(b"1234567").wait() {
    Err(DatagramTooLarge(actual, maximum)) => {
      assert_eq(actual, 7)
      assert_eq(maximum, 6)
    }
    _ => fail("datagram above the reduced maximum was accepted")
  }
  ffi_fake_set_max_datagram_size(7)
  let pending_size_check = connection.send_datagram(b"1234567")
  ffi_fake_set_max_datagram_size(6)
  match pending_size_check.wait() {
    Err(DatagramTooLarge(actual, maximum)) => {
      assert_eq(actual, 7)
      assert_eq(maximum, 6)
    }
    _ => fail("datagram accepted using the stale maximum after ready")
  }
  ffi_fake_set_max_datagram_size(7)
  let view = ffi_fake_offset_view("one")
  ffi_fake_push_datagram(view)
  let first = match connection.next_datagram().wait() {
    DatagramReceived(bytes) => bytes
    DatagramReceiveQueueFull => fail("datagram receive queue overflowed")
    DatagramsClosed => fail("datagram reader closed unexpectedly")
  }
  ffi_fake_mutate_view(view)
  assert_eq(first, b"one")
  ffi_fake_push_datagram(@js_adapter.bytes_to_uint8array(b"two"))
  match connection.next_datagram().wait() {
    DatagramReceived(bytes) => assert_eq(bytes, b"two")
    DatagramReceiveQueueFull => fail("datagram receive queue overflowed")
    DatagramsClosed => fail("datagram reader closed unexpectedly")
  }

  ffi_fake_reject_datagram_writes()
  match connection.send_datagram(b"reject").wait() {
    Err(DatagramWriteFailed(error)) =>
      assert_eq(error.message, "datagram write failed")
    _ => fail("datagram write rejection was reported as success")
  }
  ffi_fake_allow_datagram_writes()

  let waiting = connection.next_datagram()
  ignore(connection.close(0L, "done"))
  match connection.closed().wait() {
    LocalClose => ()
    PeerClose(_) => fail("local close was reported as peer close")
    Failed(error) => fail("unexpected close failure: " + error.message)
  }
  assert_true(waiting.wait() is DatagramsClosed)
  assert_true(!connection.datagram_capability().enabled)
  match connection.send_datagram(b"late").wait() {
    Err(DatagramConnectionClosed) => ()
    _ => fail("send after connection close was not rejected")
  }

  ffi_install_fake_webtransport("moqt-18", false, false)
  ffi_fake_disable_next_datagrams()
  let unavailable = match
    @js_adapter.webtransport_connect("moqt://example.test/no-datagrams") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  assert_true(!unavailable.datagram_capability().enabled)
  match unavailable.send_datagram(b"unavailable").wait() {
    Err(DatagramUnavailable) => ()
    _ => fail("unavailable datagram API was reported as available")
  }
  assert_true(unavailable.next_datagram().wait() is DatagramsClosed)
  ignore(unavailable.close(0L, "done"))

  ffi_install_fake_webtransport("moqt-18", false, false)
  let blocked = match
    @js_adapter.webtransport_connect(
      "moqt://example.test/datagram-backpressure",
    ) {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  ffi_fake_delay_datagram_writes()
  let pending_sends : Array[
    @js.Promise[Result[Unit, @js_adapter.WebTransportDatagramError]],
  ] = []
  for _ in 0..<64 {
    pending_sends.push(blocked.send_datagram(b"queued"))
  }
  match blocked.send_datagram(b"full").wait() {
    Err(DatagramQueueFull) => ()
    _ => fail("datagram send queue exceeded its limit")
  }
  ignore(blocked.close(0L, "done"))
  ffi_fake_resolve_pending_datagram_writes()
  ignore(blocked.closed().wait())
  for pending in pending_sends {
    ignore(pending.wait())
  }
  webtransport_session_driver_routes_setup()
  webtransport_session_driver_reports_close()
}

///|
async fn webtransport_session_driver_routes_setup() -> Unit {
  ffi_install_fake_webtransport("moqt-18", false, false)
  let connection = match
    @js_adapter.webtransport_connect("moqt://example.test/session") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  let session = match
    @session.ClientSession::new(@session.Draft18, { options: [], }) {
    Ok(session) => session
    Err(_) => fail("session creation failed")
  }
  let driver = @js_adapter.WebTransportSessionDriver::new(connection, session)
  assert_true(driver.start() is Ok(_))
  let peer_setup = match @protocol.encode_setup({ options: [], }) {
    Ok(bytes) => bytes
    Err(_) => fail("SETUP encoding failed")
  }
  ffi_fake_push_peer_unidirectional(@js_adapter.bytes_to_uint8array(peer_setup))
  match driver.next_notice().wait() {
    Some(@session.HandshakeComplete(setup)) =>
      assert_eq(setup.options.length(), 0)
    Some(_) => fail("session driver emitted an unexpected notice")
    None => fail("session driver stopped before SETUP completed")
  }
  ignore(connection.close(0L, "done"))
  ignore(connection.closed().wait())
}

///|
async fn webtransport_session_driver_reports_close() -> Unit {
  ffi_install_fake_webtransport("moqt-18", false, false)
  let connection = match
    @js_adapter.webtransport_connect("moqt://example.test/session-close") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  let session = match
    @session.ClientSession::new(@session.Draft18, { options: [], }) {
    Ok(session) => session
    Err(_) => fail("session creation failed")
  }
  let driver = @js_adapter.WebTransportSessionDriver::new(connection, session)
  assert_true(driver.start() is Ok(_))
  @js.sleep(5)

  ffi_fake_peer_close(37, "peer done")
  ignore(connection.closed().wait())
  let mut peer_ended = false
  while !peer_ended {
    match driver.next_notice().wait() {
      Some(@session.SessionEnded(@session.Peer(37UL))) => peer_ended = true
      Some(@session.SendFinished(_, _)) => ()
      Some(_) => fail("peer close did not produce the expected session end")
      None => fail("session ended without a SessionEnded notice")
    }
  }
  assert_true(session.start() is Err(@session.SessionClosed))
  match driver.next_notice().wait() {
    None => ()
    Some(_) => fail("session driver emitted more than one terminal notice")
  }

  ffi_install_fake_webtransport("moqt-18", false, false)
  let failed_connection = match
    @js_adapter.webtransport_connect("moqt://example.test/session-failure") {
    Ok(connection) => connection
    Err(message) => fail(message)
  }
  let failed_session = match
    @session.ClientSession::new(@session.Draft18, { options: [], }) {
    Ok(session) => session
    Err(_) => fail("session creation failed")
  }
  let failed_driver = @js_adapter.WebTransportSessionDriver::new(
    failed_connection, failed_session,
  )
  assert_true(failed_driver.start() is Ok(_))
  @js.sleep(5)
  ffi_fake_reject_closed("transport failed")
  ignore(failed_connection.closed().wait())
  let mut failure_ended = false
  while !failure_ended {
    match failed_driver.next_notice().wait() {
      Some(@session.SessionEnded(@session.Transport(-1L))) =>
        failure_ended = true
      Some(@session.SendFinished(_, _)) => ()
      Some(_) =>
        fail("transport failure did not produce the expected session end")
      None => fail("transport failure ended without a SessionEnded notice")
    }
  }
  assert_true(failed_session.start() is Err(@session.SessionClosed))
  match failed_driver.next_notice().wait() {
    None => ()
    Some(_) => fail("session driver emitted more than one terminal notice")
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
  #|   globalThis.__moqWtDelayWrites = false;
  #|   globalThis.__moqWtDatagramsEnabled = true;
  #|   globalThis.__moqWtRejectDatagramWrites = false;
  #|   globalThis.__moqWtDelayDatagramWrites = false;
  #|   globalThis.WebTransportError = class extends Error {
  #|     constructor(options) { super(); this.streamErrorCode = options.streamErrorCode; }
  #|   };
  #|   globalThis.WebTransport = class {
  #|     constructor(url, options) {
  #|       this.url = url;
  #|       this.offered = options.protocols[0];
  #|       this.certificateHashes = options.serverCertificateHashes;
  #|       this.protocol = protocol;
  #|       this.closeCalls = 0;
  #|       this.resolveReady = undefined;
  #|       this.resolveClosed = undefined;
  #|       this.rejectClosed = undefined;
  #|       this.pendingWriteResolvers = [];
  #|       this.pendingDatagramWriteResolvers = [];
  #|       globalThis.__moqWtPendingOpenResolvers = [];
  #|       this.incomingUnidirectionalStreams = new ReadableStream({
  #|         start: controller => { this.incomingUni = controller; },
  #|       });
  #|       this.incomingBidirectionalStreams = new ReadableStream({
  #|         start: controller => { this.incomingBidi = controller; },
  #|       });
  #|       if (globalThis.__moqWtDatagramsEnabled) {
  #|         this.datagrams = {
  #|           maxDatagramSize: 7,
  #|           readable: new ReadableStream({
  #|             start: controller => { this.datagramController = controller; },
  #|           }),
  #|           writable: new WritableStream({
  #|             write: chunk => {
  #|               if (globalThis.__moqWtRejectDatagramWrites) throw new Error("datagram write failed");
  #|               this.lastDatagram = chunk;
  #|               if (globalThis.__moqWtDelayDatagramWrites) return new Promise(resolve => this.pendingDatagramWriteResolvers.push(resolve));
  #|             },
  #|           }),
  #|         };
  #|       }
  #|       globalThis.__moqWtDatagramsEnabled = true;
  #|       this.createUnidirectionalStream = () => Promise.resolve(
  #|         new WritableStream({ write: chunk => { this.uniWrites = chunk; } }),
  #|       );
  #|       this.createBidirectionalStream = () => new Promise(resolve => {
  #|         let readableController;
  #|         const readable = new ReadableStream({
  #|           start: c => { readableController = c; },
  #|           cancel: reason => { this.lastCancel = reason; },
  #|         });
  #|         const writes = [];
  #|         const writable = new WritableStream({ write: chunk => new Promise(resolve => {
  #|           writes.push(chunk);
  #|           if (globalThis.__moqWtDelayWrites) this.pendingWriteResolvers.push(resolve);
  #|           else resolve();
  #|         }), abort: reason => { this.lastAbort = reason; } });
  #|         const stream = { readable, writable };
  #|         this.created = { readableController, writes };
  #|         if (globalThis.__moqWtDelayOpen) globalThis.__moqWtPendingOpenResolvers.push(() => resolve(stream));
  #|         else resolve(stream);
  #|       });
  #|       this.ready = pendingReady
  #|         ? new Promise(resolve => { this.resolveReady = resolve; })
  #|         : rejectReady ? Promise.reject(new Error("server rejected request")) : Promise.resolve();
  #|       this.closed = new Promise((resolve, reject) => { this.resolveClosed = resolve; this.rejectClosed = reject; });
  #|       globalThis.__moqWtLast = this;
  #|     }
  #|     close(info) {
  #|       this.closeCalls++;
  #|       this.resolveClosed(info);
  #|     }
  #|   };
  #| }

///|
extern "js" fn ffi_fake_delay_datagram_writes() -> Unit =
  #| () => { globalThis.__moqWtDelayDatagramWrites = true; }

///|
extern "js" fn ffi_fake_resolve_pending_datagram_writes() -> Unit =
  #| () => {
  #|   globalThis.__moqWtLast.pendingDatagramWriteResolvers.forEach(resolve => resolve());
  #|   globalThis.__moqWtLast.pendingDatagramWriteResolvers = [];
  #|   globalThis.__moqWtDelayDatagramWrites = false;
  #| }

///|
extern "js" fn ffi_fake_push_datagram(data : @arraybuffer.Uint8Array) -> Unit =
  #| (data) => globalThis.__moqWtLast.datagramController.enqueue(data)

///|
extern "js" fn ffi_fake_datagram_write() -> String =
  #| () => new TextDecoder().decode(globalThis.__moqWtLast.lastDatagram)

///|
extern "js" fn ffi_fake_set_max_datagram_size(size : Int32) -> Unit =
  #| size => { globalThis.__moqWtLast.datagrams.maxDatagramSize = size; }

///|
extern "js" fn ffi_fake_reject_datagram_writes() -> Unit =
  #| () => { globalThis.__moqWtRejectDatagramWrites = true; }

///|
extern "js" fn ffi_fake_allow_datagram_writes() -> Unit =
  #| () => { globalThis.__moqWtRejectDatagramWrites = false; }

///|
extern "js" fn ffi_fake_disable_next_datagrams() -> Unit =
  #| () => { globalThis.__moqWtDatagramsEnabled = false; }

///|
extern "js" fn ffi_fake_delay_stream_writes() -> Unit =
  #| () => { globalThis.__moqWtDelayWrites = true; }

///|
extern "js" fn ffi_fake_resolve_pending_stream_writes() -> Unit =
  #| () => {
  #|   globalThis.__moqWtLast.pendingWriteResolvers.forEach(resolve => resolve());
  #|   globalThis.__moqWtLast.pendingWriteResolvers = [];
  #|   globalThis.__moqWtDelayWrites = false;
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
extern "js" fn ffi_fake_reject_closed(reason : String) -> Unit =
  #| reason => globalThis.__moqWtLast.rejectClosed(new Error(reason))

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
extern "js" fn ffi_fake_cancel_error_code() -> Int32 =
  #| () => globalThis.__moqWtLast.lastCancel.streamErrorCode

///|
extern "js" fn ffi_fake_abort_error_code() -> Int32 =
  #| () => globalThis.__moqWtLast.lastAbort.streamErrorCode

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
