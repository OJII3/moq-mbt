///|
/// A user-facing description of the error, including available cause details.
pub fn ClientSessionError::message(self : ClientSessionError) -> String {
  match self {
    Handshake(InvalidSetup(_)) => "Peer SETUP is malformed"
    Handshake(DuplicateControlStream) =>
      "Peer opened more than one control stream"
    Handshake(ControlStreamClosed) => "The control stream was closed"
    Handshake(InvalidServerPath) => "Server SETUP must not contain PATH"
    Handshake(InvalidServerAuthority) =>
      "Server SETUP must not contain AUTHORITY"
    Handshake(AlreadyCompleted) => "The handshake has already completed"
    Track(InvalidControlMessage) => "Track control message is malformed"
    Track(TrackMismatch) => "Track does not match the request"
    Track(DuplicateSubscription) => "Subscription already exists"
    Track(InvalidRequestId) => "Track request ID is invalid"
    Track(AliasMismatch) => "Track alias does not match the request"
    Track(UnexpectedMessage) =>
      "Track message is unexpected in the current state"
    Track(BufferLimitExceeded) => "Track receive buffer limit exceeded"
    Track(IncompleteObjectStream) =>
      "Object stream ended before it was complete"
    Track(ObjectIdNotIncreasing) => "Object IDs must be strictly increasing"
    Track(ObjectStreamsStillOpen) => "Object streams are still open"
    Encoding(_) => "Cannot encode a protocol message"
    HandshakeNotComplete => "The session handshake has not completed"
    InvalidRequestId => "Request ID is invalid for this operation"
    DuplicateRequestId => "Request ID has already been used"
    DuplicateStream => "Stream is already registered with this session"
    UnknownStream => "Stream is not recognized by this session"
    UnknownRequest => "Request is unknown or is not in the required state"
    InvalidFetchRange => "Fetch range is invalid for this request"
    SessionDraining => "Session is draining and cannot start new requests"
    SessionClosed => "Session is closed"
    TrackNotPublished => "The requested track has not been published"
    AliasInUse => "Track alias is already in use"
    SubscriptionNotAccepted =>
      "Subscription is not in a state that allows this operation"
    RequestUpdatePending => "A request update is already pending"
    PrefixOverlap => "Namespace prefix overlaps another active subscription"
    UnexpectedMessage =>
      "Message or operation is unexpected in the current state"
    DeliveryTimeoutUnsupported =>
      "Transport does not support committed delivery and abort after FIN"
    ClockNotInitialized =>
      "Supply a monotonic Clock event before timed publisher operations"
    ObjectStreamNotReady =>
      "Object stream is still opening; retry after StreamOpened"
    ObjectPayloadLengthMismatch =>
      "Object payload length does not match its declared length"
    BufferLimitExceeded => "Session receive buffer limit exceeded"
    DatagramSendUnavailable =>
      "Datagram sending is unavailable on this transport"
    DatagramTooLarge(length, limit) =>
      "Encoded datagram is \{length} bytes, exceeding the transport limit of \{limit} bytes"
  }
}

///|
pub impl Show for ClientSessionError with fn output(self, logger) {
  logger.write_string(self.message())
}

///|
pub extend ClientSessionError with Show::{output}

///|
pub extend ClientSessionError with Show::{to_string}

///|
/// Return a connection-close code only when carried by a handshake error.
/// Other errors may describe local API misuse or stream-level failures, so
/// their disposition requires session and event context.
pub fn ClientSessionError::close_code(self : ClientSessionError) -> UInt64? {
  match self {
    Handshake(error) => Some(error.close_code())
    _ => None
  }
}
