///|
/// A small browser-facing handle that owns the WebTransport connection and
/// the Sans-I/O client session driver.
pub struct BrowserClient {
  connection : WebTransportConnection
  driver : WebTransportSessionDriver
}

///|
/// Why `browser_connect` could not create a session.
pub(all) enum BrowserConnectError {
  /// The WebTransport constructor or URL conversion failed.
  Connection(String)
  /// The local SETUP could not be encoded for a Draft 18 client.
  InvalidSetup(@protocol.CodecError)
} derive(Debug, Eq)

///|
pub fn BrowserConnectError::message(self : BrowserConnectError) -> String {
  match self {
    Connection(message) => message
    InvalidSetup(error) => "invalid client setup: \{error.to_repr()}"
  }
}

///|
pub impl Show for BrowserConnectError with fn output(self, logger) {
  logger.write_string(self.message())
}

///|
/// Open a WebTransport connection and create a Draft 18 client session.
/// Optional certificate hashes enable certificate pinning; desired
/// subscriptions are restored after SETUP.
///
/// This only constructs the session: it does not wait for the WebTransport
/// handshake or send SETUP. Call `start` and then consume notices until
/// `HandshakeComplete` before issuing session operations.
pub fn browser_connect(
  url : String,
  setup : @protocol.Setup,
  certificate_hashes~ : Array[@arraybuffer.Uint8Array] = [],
  desired_subscriptions~ : Array[@session.DesiredSubscription] = [],
) -> Result[BrowserClient, BrowserConnectError] {
  let connection = match webtransport_connect(url, certificate_hashes~) {
    Ok(connection) => connection
    Err(message) => return Err(Connection(message))
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
      Err(InvalidSetup(error))
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
) -> @driver.DriverDispatchResult {
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
) -> @js.Promise[(@session.ClientStreamPurpose, @session.StreamId)?] {
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

///|
pub extend BrowserConnectError with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend BrowserConnectError with Eq::{equal, not_equal}

///|
pub extend BrowserConnectError with Show::{to_string, output}
