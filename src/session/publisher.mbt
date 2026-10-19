///|
pub(all) enum PublisherProgress {
  Pending
  SubscribeRequested(Int64, @protocol.SubscribeMessage, Bytes)
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
  match
    @protocol.encode_subgroup_stream(
      {
        track_alias: self.track_alias,
        group_id,
        subgroup_id_mode: @protocol.Zero,
        publisher_priority: None,
        end_of_group: true,
        first_object: true,
        has_properties: false,
      },
      [{ object_id, status: @protocol.Normal, properties: b"", payload, }],
    ) {
    Ok(bytes) => Ok(bytes)
    Err(_) => Err(InvalidControlMessage)
  }
}

///|
pub extend PublisherProgress with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend PublisherProgress with Eq::{equal, not_equal}
