///|
pub(all) enum TrackSessionError {
  InvalidControlMessage
  TrackMismatch
  DuplicateSubscription
  InvalidRequestId
  AliasMismatch
  UnexpectedMessage
  BufferLimitExceeded
} derive(Debug, Eq)

///|
pub(all) enum PublisherProgress {
  Pending
  SubscribeRequested(Int64, @protocol.SubscribeMessage, Bytes)
} derive(Debug, Eq)

///|
pub(all) enum SubscriberProgress {
  Pending
  Established(UInt64, Bytes)
  Rejected(@protocol.RequestErrorMessage, Bytes)
  Finished(@protocol.PublishDoneMessage, Bytes)
} derive(Debug, Eq)

///|
pub(all) enum ObjectProgress {
  Pending
  Received(@protocol.SubgroupObject, Bytes)
} derive(Debug, Eq)

///|
/// Sans-I/O publisher state for one named track and one subscriber.
pub struct SingleTrackPublisher {
  priv track : @protocol.TrackName
  priv track_alias : UInt64
  priv mut receive_stream : Int64?
  priv mut receive_buffer : Bytes
  priv mut subscription_stream : Int64?
  priv mut established : Bool
  priv mut object_sent : Bool
}

///|
pub fn SingleTrackPublisher::new(
  track : @protocol.TrackName,
  track_alias : UInt64,
) -> SingleTrackPublisher {
  {
    track,
    track_alias,
    receive_stream: None,
    receive_buffer: b"",
    subscription_stream: None,
    established: false,
    object_sent: false,
  }
}

///|
/// Build the publisher's PUBLISH request. Request ID 0 is the first client ID.
pub fn SingleTrackPublisher::publish_request(
  self : SingleTrackPublisher,
) -> Result[Bytes, @protocol.CodecError] {
  @protocol.encode_publish({
    request_id: 0,
    track: self.track,
    track_alias: self.track_alias,
    parameters: [],
    properties: b"",
  })
}

///|
/// Feed bytes from the relay's publisher-initiated SUBSCRIBE request stream.
pub fn SingleTrackPublisher::receive_subscribe(
  self : SingleTrackPublisher,
  stream : Int64,
  bytes : Bytes,
) -> Result[PublisherProgress, TrackSessionError] {
  if self.subscription_stream is Some(_) {
    return Err(DuplicateSubscription)
  }
  match self.receive_stream {
    Some(previous) if previous != stream => return Err(DuplicateSubscription)
    None => self.receive_stream = Some(stream)
    _ => ()
  }
  if bytes.length() > 65546 - self.receive_buffer.length() {
    return Err(BufferLimitExceeded)
  }
  self.receive_buffer = self.receive_buffer + bytes
  match @protocol.decode_control(self.receive_buffer) {
    @protocol.Incomplete => Ok(Pending)
    @protocol.Invalid(_) => Err(InvalidControlMessage)
    @protocol.Complete(message, consumed) =>
      match message {
        @protocol.Subscribe(request) => {
          if (request.request_id & 1UL) != 1UL {
            return Err(InvalidRequestId)
          }
          if request.track != self.track {
            return Err(TrackMismatch)
          }
          self.subscription_stream = Some(stream)
          let trailing = self.receive_buffer[consumed:].to_owned()
          self.receive_buffer = b""
          Ok(SubscribeRequested(stream, request, trailing))
        }
        _ => Err(UnexpectedMessage)
      }
  }
}

///|
/// Accept the received SUBSCRIBE with SUBSCRIBE_OK and establish the track alias.
pub fn SingleTrackPublisher::accept_subscribe(
  self : SingleTrackPublisher,
  stream : Int64,
) -> Result[Bytes, TrackSessionError] {
  if self.subscription_stream != Some(stream) || self.established {
    return Err(UnexpectedMessage)
  }
  self.established = true
  match
    @protocol.encode_subscribe_ok({
      track_alias: self.track_alias,
      parameters: [],
      properties: b"",
    }) {
    Ok(bytes) => Ok(bytes)
    Err(_) => Err(InvalidControlMessage)
  }
}

///|
/// Mark the PUBLISH request as accepted so the publisher can send its object.
pub fn SingleTrackPublisher::publish_accepted(
  self : SingleTrackPublisher,
) -> Unit {
  self.established = true
}

///|
/// Produce one reliable Subgroup stream object after the subscription is accepted.
pub fn SingleTrackPublisher::object_stream(
  self : SingleTrackPublisher,
  group_id : UInt64,
  object_id : UInt64,
  payload : Bytes,
) -> Result[Bytes, TrackSessionError] {
  if !self.established || self.object_sent {
    return Err(UnexpectedMessage)
  }
  self.object_sent = true
  Ok(
    @protocol.encode_one_object(self.track_alias, group_id, object_id, payload),
  )
}

///|
/// Sans-I/O subscriber state for one named track and one object.
pub struct SingleTrackSubscriber {
  priv track : @protocol.TrackName
  priv mut request_stream : Int64?
  priv mut response_buffer : Bytes
  priv mut track_alias : UInt64?
  priv mut object_stream : Int64?
  priv mut object_buffer : Bytes
  priv mut object_received : Bool
}

///|
pub fn SingleTrackSubscriber::new(
  track : @protocol.TrackName,
) -> SingleTrackSubscriber {
  {
    track,
    request_stream: None,
    response_buffer: b"",
    track_alias: None,
    object_stream: None,
    object_buffer: b"",
    object_received: false,
  }
}

///|
/// Build the subscriber's unfiltered SUBSCRIBE request. Request ID 0 is the
/// first client ID; an unfiltered request receives newly published Objects.
pub fn SingleTrackSubscriber::subscribe_request(
  self : SingleTrackSubscriber,
) -> Result[Bytes, @protocol.CodecError] {
  @protocol.encode_subscribe({
    request_id: 0,
    track: self.track,
    parameters: [],
  })
}

///|
/// Feed the first response from the publisher on the SUBSCRIBE request stream.
pub fn SingleTrackSubscriber::receive_response(
  self : SingleTrackSubscriber,
  stream : Int64,
  bytes : Bytes,
) -> Result[SubscriberProgress, TrackSessionError] {
  match self.request_stream {
    Some(previous) if previous != stream => return Err(DuplicateSubscription)
    None => self.request_stream = Some(stream)
    _ => ()
  }
  if bytes.length() > 65546 - self.response_buffer.length() {
    return Err(BufferLimitExceeded)
  }
  self.response_buffer = self.response_buffer + bytes
  match @protocol.decode_control(self.response_buffer) {
    @protocol.Incomplete => Ok(Pending)
    @protocol.Invalid(_) => Err(InvalidControlMessage)
    @protocol.Complete(message, consumed) => {
      let trailing = self.response_buffer[consumed:].to_owned()
      self.response_buffer = b""
      match message {
        @protocol.SubscribeOk(response) if self.track_alias is None => {
          self.track_alias = Some(response.track_alias)
          Ok(Established(response.track_alias, trailing))
        }
        @protocol.RequestError(response) if self.track_alias is None =>
          Ok(Rejected(response, trailing))
        @protocol.PublishDone(response) if self.track_alias is Some(_) =>
          Ok(Finished(response, trailing))
        _ => Err(UnexpectedMessage)
      }
    }
  }
}

///|
/// Feed data from the first Subgroup stream and return its single Object.
pub fn SingleTrackSubscriber::receive_object(
  self : SingleTrackSubscriber,
  stream : Int64,
  bytes : Bytes,
) -> Result[ObjectProgress, TrackSessionError] {
  if self.object_received {
    return Err(DuplicateSubscription)
  }
  let track_alias = match self.track_alias {
    Some(value) => value
    None => return Err(UnexpectedMessage)
  }
  match self.object_stream {
    Some(previous) if previous != stream => return Err(DuplicateSubscription)
    None => self.object_stream = Some(stream)
    _ => ()
  }
  if bytes.length() > 1024 * 1024 - self.object_buffer.length() {
    return Err(BufferLimitExceeded)
  }
  self.object_buffer = self.object_buffer + bytes
  match @protocol.decode_subgroup_object(self.object_buffer) {
    @protocol.Incomplete => Ok(Pending)
    @protocol.Invalid(_) => Err(InvalidControlMessage)
    @protocol.Complete(object, consumed) => {
      if object.track_alias != track_alias {
        return Err(AliasMismatch)
      }
      self.object_received = true
      let trailing = self.object_buffer[consumed:].to_owned()
      self.object_buffer = b""
      Ok(Received(object, trailing))
    }
  }
}

///|
pub extend TrackSessionError with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend TrackSessionError with Eq::{equal, not_equal}

///|
pub extend PublisherProgress with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend PublisherProgress with Eq::{equal, not_equal}

///|
pub extend SubscriberProgress with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend SubscriberProgress with Eq::{equal, not_equal}

///|
pub extend ObjectProgress with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ObjectProgress with Eq::{equal, not_equal}
