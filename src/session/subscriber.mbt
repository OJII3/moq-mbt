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
pub extend SubscriberProgress with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend SubscriberProgress with Eq::{equal, not_equal}

///|
pub extend ObjectProgress with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ObjectProgress with Eq::{equal, not_equal}
