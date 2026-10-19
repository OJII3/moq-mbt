///|
pub(all) enum PublisherProgress {
  Pending
  SubscribeRequested(Int64, @protocol.SubscribeMessage, Bytes)
} derive(Debug, Eq)

///|
priv struct PublisherSubscription {
  mut opened_stream_count : UInt64
  mut open_stream_count : UInt64
  mut publish_done_sent : Bool
}

///|
priv struct PublisherObjectStream {
  subscription_stream : Int64
  header : @protocol.SubgroupHeader
  mut previous_object_id : UInt64?
  mut closed : Bool
}

///|
/// Sans-I/O publisher state for one named track and its subscriptions.
pub struct SingleTrackPublisher {
  priv track : @protocol.TrackName
  priv track_alias : UInt64
  priv mut receive_stream : Int64?
  priv mut receive_buffer : Bytes
  priv mut subscription_stream : Int64?
  priv subscriptions : Map[Int64, PublisherSubscription]
  priv object_streams : Map[Int64, PublisherObjectStream]
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
    subscriptions: {},
    object_streams: {},
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
/// Accept a received SUBSCRIBE with SUBSCRIBE_OK.
pub fn SingleTrackPublisher::accept_subscribe(
  self : SingleTrackPublisher,
  stream : Int64,
) -> Result[Bytes, TrackSessionError] {
  if self.subscription_stream != Some(stream) ||
    self.subscriptions.get(stream) is Some(_) {
    return Err(UnexpectedMessage)
  }
  self.subscriptions[stream] = {
    opened_stream_count: 0UL,
    open_stream_count: 0UL,
    publish_done_sent: false,
  }
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
/// Mark a PUBLISH request as accepted and start its per-subscription state.
pub fn SingleTrackPublisher::publish_accepted(
  self : SingleTrackPublisher,
  stream : Int64,
) -> Result[Unit, TrackSessionError] {
  if self.subscriptions.get(stream) is Some(_) {
    return Err(DuplicateSubscription)
  }
  self.subscriptions[stream] = {
    opened_stream_count: 0UL,
    open_stream_count: 0UL,
    publish_done_sent: false,
  }
  Ok(())
}

///|
/// Open a Subgroup transport stream for a subscription and write its header.
/// This counts even when no Object is later written to the stream.
pub fn SingleTrackPublisher::open_object_stream(
  self : SingleTrackPublisher,
  subscription_stream : Int64,
  stream : Int64,
  header : @protocol.SubgroupHeader,
) -> Result[Bytes, TrackSessionError] {
  let subscription = match self.subscriptions.get(subscription_stream) {
    Some(value) => value
    None => return Err(UnexpectedMessage)
  }
  if subscription.publish_done_sent ||
    self.object_streams.get(stream) is Some(_) {
    return Err(UnexpectedMessage)
  }
  if header.track_alias != self.track_alias {
    return Err(AliasMismatch)
  }
  self.object_streams[stream] = {
    subscription_stream,
    header,
    previous_object_id: None,
    closed: false,
  }
  subscription.opened_stream_count = subscription.opened_stream_count + 1UL
  subscription.open_stream_count = subscription.open_stream_count + 1UL
  Ok(@protocol.encode_subgroup_header(header))
}

///|
/// Encode the next Object frame on an already opened Subgroup stream.
pub fn SingleTrackPublisher::object_stream(
  self : SingleTrackPublisher,
  stream : Int64,
  frame : @protocol.SubgroupFrame,
) -> Result[Bytes, TrackSessionError] {
  let writer = match self.object_streams.get(stream) {
    Some(value) => value
    None => return Err(UnexpectedMessage)
  }
  if writer.closed {
    return Err(UnexpectedMessage)
  }
  let delta = match writer.previous_object_id {
    None => frame.object_id
    Some(previous) => {
      if frame.object_id <= previous {
        return Err(ObjectIdNotIncreasing)
      }
      frame.object_id - previous - 1UL
    }
  }
  let delta_frame : @protocol.SubgroupFrame = {
    object_id: delta,
    status: frame.status,
    properties: frame.properties,
    payload: frame.payload,
  }
  let wire = match
    @protocol.encode_subgroup_stream(writer.header, [delta_frame]) {
    Ok(value) => value
    Err(_) => return Err(InvalidControlMessage)
  }
  let header_length = @protocol.encode_subgroup_header(writer.header).length()
  writer.previous_object_id = Some(frame.object_id)
  Ok(wire[header_length:].to_owned())
}

///|
/// Mark a Subgroup stream as gracefully finished after its FIN is sent.
pub fn SingleTrackPublisher::finish_object_stream(
  self : SingleTrackPublisher,
  stream : Int64,
) -> Result[Unit, TrackSessionError] {
  self.close_object_stream(stream)
}

///|
/// Mark a Subgroup stream as reset. Reset streams count, but do not imply that
/// their Group ended.
pub fn SingleTrackPublisher::reset_object_stream(
  self : SingleTrackPublisher,
  stream : Int64,
) -> Result[Unit, TrackSessionError] {
  self.close_object_stream(stream)
}

///|
fn SingleTrackPublisher::close_object_stream(
  self : SingleTrackPublisher,
  stream : Int64,
) -> Result[Unit, TrackSessionError] {
  let writer = match self.object_streams.get(stream) {
    Some(value) => value
    None => return Err(UnexpectedMessage)
  }
  if writer.closed {
    return Err(UnexpectedMessage)
  }
  let subscription = self.subscriptions.get(writer.subscription_stream).unwrap()
  writer.closed = true
  subscription.open_stream_count = subscription.open_stream_count - 1UL
  Ok(())
}

///|
/// Encode PUBLISH_DONE after every data stream for this subscription is closed.
pub fn SingleTrackPublisher::publish_done(
  self : SingleTrackPublisher,
  subscription_stream : Int64,
  code : UInt64,
  reason : Bytes,
) -> Result[Bytes, TrackSessionError] {
  let subscription = match self.subscriptions.get(subscription_stream) {
    Some(value) => value
    None => return Err(UnexpectedMessage)
  }
  if subscription.publish_done_sent {
    return Err(UnexpectedMessage)
  }
  if subscription.open_stream_count != 0UL {
    return Err(ObjectStreamsStillOpen)
  }
  let bytes = match
    @protocol.encode_publish_done({
      code,
      stream_count: subscription.opened_stream_count,
      reason,
    }) {
    Ok(value) => value
    Err(_) => return Err(InvalidControlMessage)
  }
  subscription.publish_done_sent = true
  Ok(bytes)
}

///|
pub extend PublisherProgress with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend PublisherProgress with Eq::{equal, not_equal}
