///|
/// A small browser-facing handle that owns the WebTransport connection and
/// the Sans-I/O client session driver.
pub struct BrowserClient {
  connection : WebTransportConnection
  driver : WebTransportSessionDriver
}

///|
/// Open a WebTransport connection and create a Draft 18 client session.
/// Call `start` before issuing session operations. Optional certificate hashes
/// enable certificate pinning; desired subscriptions are restored after SETUP.
pub fn browser_connect(
  url : String,
  setup : @protocol.Setup,
  certificate_hashes~ : Array[@arraybuffer.Uint8Array] = [],
  desired_subscriptions~ : Array[@session.DesiredSubscription] = [],
) -> Result[BrowserClient, String] {
  let connection = match webtransport_connect(url, certificate_hashes~) {
    Ok(connection) => connection
    Err(message) => return Err(message)
  }
  match
    WebTransportSessionDriver::from_setup(
      connection,
      setup,
      desired_subscriptions~,
    ) {
    Ok(driver) => Ok({ connection, driver, })
    Err(error) => {
      ignore(connection.close(0L, "invalid client setup"))
      Err("\{error.to_repr()}")
    }
  }
}

///|
/// Start driving the session over WebTransport.
pub fn BrowserClient::start(
  self : BrowserClient,
) -> Result[Unit, @driver.SessionDriverFailureReason] {
  self.driver.start()
}

///|
/// Access the session to issue publish, subscribe, and other MoQ operations.
/// Dispatch the returned actions with `dispatch`.
pub fn BrowserClient::session(self : BrowserClient) -> @session.ClientSession {
  self.driver.session()
}

///|
/// Apply actions returned by a session operation.
pub fn BrowserClient::dispatch(
  self : BrowserClient,
  actions : Array[@session.SessionAction],
) -> Unit {
  self.driver.dispatch(actions)
}

///|
/// Wait for the next session notice. Resolves to `None` when the session ends.
pub fn BrowserClient::next_notice(
  self : BrowserClient,
) -> @js.Promise[@session.SessionNotice?] {
  self.driver.next_notice()
}

///|
/// Wait for the next transport stream opened by a session action.
pub fn BrowserClient::next_opened_stream(
  self : BrowserClient,
) -> @js.Promise[(@session.ClientStreamPurpose, Int64)?] {
  self.driver.next_opened_stream()
}

///|
/// Wait for the WebTransport connection to close.
pub fn BrowserClient::closed(
  self : BrowserClient,
) -> @js.Promise[WebTransportClose] {
  self.connection.closed()
}

///|
/// Close the connection and release its WebTransport streams and readers.
#warnings("-result_error_return")
pub fn BrowserClient::close(
  self : BrowserClient,
  close_code : Int64,
  reason : String,
) -> Result[Unit, WebTransportError] {
  self.connection.close(close_code, reason)
}
