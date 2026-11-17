///|
/// A SUBSCRIBE request identifying the track to subscribe to.
#derive(Debug, Eq)
pub(all) struct SubscribeMessage {
  request_id : UInt64
  track : TrackName
  parameters : Array[MessageParameter]
}

///|
/// A PUBLISH request that announces a track and the alias used by its data streams.
#derive(Debug, Eq)
pub(all) struct PublishMessage {
  request_id : UInt64
  track : TrackName
  track_alias : UInt64
  parameters : Array[MessageParameter]
  properties : Bytes
}

///|
/// A SUBSCRIBE_OK response carrying the Track Alias assigned by the publisher.
#derive(Debug, Eq)
pub(all) struct SubscribeOkMessage {
  track_alias : UInt64
  parameters : Array[MessageParameter]
  properties : Bytes
}

///|
/// A REQUEST_OK response carrying request-scoped parameters and properties.
#derive(Debug, Eq)
pub(all) struct RequestOkMessage {
  parameters : Array[MessageParameter]
  properties : Bytes
}

///|
/// A REQUEST_UPDATE that supplies a new Request ID and replacement parameters.
#derive(Debug, Eq)
pub(all) struct RequestUpdateMessage {
  request_id : UInt64
  parameters : Array[MessageParameter]
}

///|
/// A Group and Object position in a FETCH range.
#derive(Debug, Eq)
pub(all) struct FetchLocation {
  group : UInt64
  object : UInt64
}

///|
/// End Location stores the wire value: Object is exclusive (last Object + 1),
/// and zero requests the entire Group.
#derive(Debug, Eq)
pub(all) enum FetchType {
  StandaloneFetch(TrackName, FetchLocation, FetchLocation)
  RelativeJoiningFetch(UInt64, UInt64)
  AbsoluteJoiningFetch(UInt64, UInt64)
}

///|
/// A FETCH request for Objects in a track.
#derive(Debug, Eq)
pub(all) struct FetchMessage {
  request_id : UInt64
  fetch_type : FetchType
  parameters : Array[MessageParameter]
}

///|
/// A FETCH_OK response with the end location and track properties.
#derive(Debug, Eq)
pub(all) struct FetchOkMessage {
  end_of_track : Byte
  end_location : FetchLocation
  parameters : Array[MessageParameter]
  track_properties : Bytes
}

///|
/// A GOAWAY sent on the control stream, including the URI and timeout.
#derive(Debug, Eq)
pub(all) struct GoAwayControlMessage {
  uri : Bytes
  timeout_ms : UInt64
  request_id : UInt64
}

///|
/// A GOAWAY sent on a request stream, where the Request ID is implied.
#derive(Debug, Eq)
pub(all) struct GoAwayRequestMessage {
  uri : Bytes
  timeout_ms : UInt64
}

///|
/// The request type answered by a REQUEST_OK, used to select its parameter scope.
#derive(Debug, Eq)
pub(all) enum RequestOkContext {
  Publish
  RequestUpdate
  FetchUpdate
  TrackStatus
  Namespace
}

///|

///|
/// Where a control message was read from, and which request a response answers.
/// Context matters for REQUEST_OK parameter scope and for GOAWAY fields that
/// are only present on request streams.
#derive(Debug, Eq)
pub(all) enum ControlStreamContext {
  /// Context is unknown; context-specific validation is skipped.
  AnyStream
  ControlStream
  RequestStream
  /// A response on a request stream; identifies the request type.
  RequestStreamFor(RequestOkContext)
  /// A FETCH request stream, whose REQUEST_UPDATE uses the FETCH scope.
  FetchRequestStream
}

///|
priv enum ParameterScope {
  Subscribe
  Publish
  SubscribeOk
  RequestUpdate
  Fetch
  FetchUpdate
  FetchOk
  NamespaceRequest
  RequestOk(RequestOkContext)
}

///|
/// A REQUEST_ERROR response with a code, retry interval, reason, and redirect.
#derive(Debug, Eq)
pub(all) struct RequestErrorMessage {
  code : UInt64
  retry_interval : UInt64
  reason : Bytes
  redirect : Bytes
}

///|
/// A PUBLISH_DONE notification that ends a subscription and reports its stream count.
#derive(Debug, Eq)
pub(all) struct PublishDoneMessage {
  code : UInt64
  stream_count : UInt64
  reason : Bytes
}

///|
/// A PUBLISH_NAMESPACE or SUBSCRIBE_NAMESPACE request.
#derive(Debug, Eq)
pub(all) struct NamespaceRequestMessage {
  request_id : UInt64
  track_namespace : TrackNamespace
  parameters : Array[MessageParameter]
}

///|
/// A NAMESPACE or NAMESPACE_DONE notification for a namespace suffix.
#derive(Debug, Eq)
pub(all) struct NamespaceSuffixMessage {
  track_namespace : TrackNamespace
}

///|
/// A control message decoded from a control stream or request stream.
#derive(Debug, Eq)
pub(all) enum ControlMessage {
  Subscribe(SubscribeMessage)
  Publish(PublishMessage)
  SubscribeOk(SubscribeOkMessage)
  RequestOk(RequestOkMessage)
  RequestUpdate(RequestUpdateMessage)
  Fetch(FetchMessage)
  FetchOk(FetchOkMessage)
  GoAwayControl(GoAwayControlMessage)
  GoAwayRequest(GoAwayRequestMessage)
  RequestError(RequestErrorMessage)
  PublishDone(PublishDoneMessage)
  PublishNamespace(NamespaceRequestMessage)
  SubscribeNamespace(NamespaceRequestMessage)
  Namespace(NamespaceSuffixMessage)
  NamespaceDone(NamespaceSuffixMessage)
}

///|
fn validate_parameters_scope(
  parameters : Array[MessageParameter],
  context : ParameterScope,
) -> Result[Unit, CodecError] {
  for parameter in parameters {
    if !parameter_allowed(parameter.kind, context) {
      return Err(InvalidParameterScope(parameter.kind))
    }
  }
  Ok(())
}

///|
fn parameter_allowed(kind : UInt64, context : ParameterScope) -> Bool {
  match context {
    Subscribe =>
      kind == 0x02 ||
      kind == 0x03 ||
      kind == 0x04 ||
      kind == 0x06 ||
      kind == 0x10 ||
      kind == 0x20 ||
      kind == 0x21 ||
      kind == 0x22 ||
      kind == 0x32
    Publish => kind == 0x03 || kind == 0x08 || kind == 0x09 || kind == 0x10
    SubscribeOk => kind == 0x08 || kind == 0x09
    RequestUpdate =>
      kind == 0x02 ||
      kind == 0x03 ||
      kind == 0x06 ||
      kind == 0x10 ||
      kind == 0x20 ||
      kind == 0x21 ||
      kind == 0x32 ||
      kind == 0x34
    Fetch => kind == 0x03 || kind == 0x0a || kind == 0x20 || kind == 0x22
    FetchUpdate => kind == 0x03 || kind == 0x20
    FetchOk => false
    NamespaceRequest => kind == 0x03
    RequestOk(context) =>
      match context {
        Publish =>
          kind == 0x02 ||
          kind == 0x06 ||
          kind == 0x08 ||
          kind == 0x10 ||
          kind == 0x20 ||
          kind == 0x21 ||
          kind == 0x22 ||
          kind == 0x32
        RequestUpdate => kind == 0x08 || kind == 0x09
        FetchUpdate => false
        TrackStatus => kind == 0x09
        Namespace => false
      }
  }
}

///|
fn validate_request_ok(
  message : RequestOkMessage,
  context : RequestOkContext,
) -> Result[Unit, CodecError] {
  match validate_parameters_scope(message.parameters, RequestOk(context)) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if context != TrackStatus && !message.properties.is_empty() {
    return Err(InvalidRequestOkProperties)
  }
  Ok(())
}

///|
pub extend SubscribeMessage with Debug::{to_repr}

///|
pub extend SubscribeMessage with Eq::{equal, not_equal}

///|
pub extend PublishMessage with Debug::{to_repr}

///|
pub extend PublishMessage with Eq::{equal, not_equal}

///|
pub extend SubscribeOkMessage with Debug::{to_repr}

///|
pub extend SubscribeOkMessage with Eq::{equal, not_equal}

///|
pub extend RequestOkMessage with Debug::{to_repr}

///|
pub extend RequestOkMessage with Eq::{equal, not_equal}

///|
pub extend RequestUpdateMessage with Debug::{to_repr}

///|
pub extend RequestUpdateMessage with Eq::{equal, not_equal}

///|
pub extend FetchLocation with Debug::{to_repr}

///|
pub extend FetchLocation with Eq::{equal, not_equal}

///|
pub extend FetchType with Debug::{to_repr}

///|
pub extend FetchType with Eq::{equal, not_equal}

///|
pub extend FetchMessage with Debug::{to_repr}

///|
pub extend FetchMessage with Eq::{equal, not_equal}

///|
pub extend FetchOkMessage with Debug::{to_repr}

///|
pub extend FetchOkMessage with Eq::{equal, not_equal}

///|
pub extend GoAwayControlMessage with Debug::{to_repr}

///|
pub extend GoAwayControlMessage with Eq::{equal, not_equal}

///|
pub extend GoAwayRequestMessage with Debug::{to_repr}

///|
pub extend GoAwayRequestMessage with Eq::{equal, not_equal}

///|
pub extend RequestOkContext with Debug::{to_repr}

///|
pub extend RequestOkContext with Eq::{equal, not_equal}

///|
pub extend ControlStreamContext with Debug::{to_repr}

///|
pub extend ControlStreamContext with Eq::{equal, not_equal}

///|
pub extend RequestErrorMessage with Debug::{to_repr}

///|
pub extend RequestErrorMessage with Eq::{equal, not_equal}

///|
pub extend PublishDoneMessage with Debug::{to_repr}

///|
pub extend PublishDoneMessage with Eq::{equal, not_equal}

///|
pub extend NamespaceRequestMessage with Debug::{to_repr}

///|
pub extend NamespaceRequestMessage with Eq::{equal, not_equal}

///|
pub extend NamespaceSuffixMessage with Debug::{to_repr}

///|
pub extend NamespaceSuffixMessage with Eq::{equal, not_equal}

///|
pub extend ControlMessage with Debug::{to_repr}

///|
pub extend ControlMessage with Eq::{equal, not_equal}
