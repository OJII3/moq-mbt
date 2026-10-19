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
  Received(@protocol.SubgroupHeader, Array[@protocol.SubgroupFrame])
} derive(Debug, Eq)

///|
priv enum ObjectStreamEnd {
  Open
  Finished
  Reset
} derive(Eq)

///|
priv struct SubscriberObjectStream {
  mut buffer : Bytes
  mut header : @protocol.SubgroupHeader?
  mut decoder : @protocol.SubgroupDecoder?
  mut end_state : ObjectStreamEnd
}

///|
/// Sans-I/O subscriber state for one named track and its data streams.
pub struct SingleTrackSubscriber {
  priv track : @protocol.TrackName
  priv request_id : UInt64
  priv mut request_stream : Int64?
  priv mut response_buffer : Bytes
  priv mut track_alias : UInt64?
  priv object_streams : Map[Int64, SubscriberObjectStream]
}

///|
pub fn SingleTrackSubscriber::new(
  track : @protocol.TrackName,
) -> SingleTrackSubscriber {
  SingleTrackSubscriber::new_with_request_id(track, 0)
}

///|
/// Construct a subscriber with the even Request ID assigned by its session.
pub fn SingleTrackSubscriber::new_with_request_id(
  track : @protocol.TrackName,
  request_id : UInt64,
) -> SingleTrackSubscriber {
  {
    track,
    request_id,
    request_stream: None,
    response_buffer: b"",
    track_alias: None,
    object_streams: {},
  }
}

///|
/// Build the subscriber's unfiltered SUBSCRIBE request. An unfiltered request
/// receives newly published Objects.
pub fn SingleTrackSubscriber::subscribe_request(
  self : SingleTrackSubscriber,
) -> Result[Bytes, @protocol.CodecError] {
  @protocol.encode_subscribe({
    request_id: self.request_id,
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
/// Feed bytes from one Subgroup stream. Complete frames are returned together;
/// an incomplete trailing frame stays buffered for the next call on this stream.
pub fn SingleTrackSubscriber::receive_object(
  self : SingleTrackSubscriber,
  stream : Int64,
  bytes : Bytes,
) -> Result[ObjectProgress, TrackSessionError] {
  let track_alias = match self.track_alias {
    Some(value) => value
    None => return Err(UnexpectedMessage)
  }
  let state = match self.object_streams.get(stream) {
    Some(value) => value
    None => {
      let value : SubscriberObjectStream = {
        buffer: b"",
        header: None,
        decoder: None,
        end_state: Open,
      }
      self.object_streams[stream] = value
      value
    }
  }
  if state.end_state != Open {
    return Err(UnexpectedMessage)
  }
  if bytes.length() > 1024 * 1024 - state.buffer.length() {
    return Err(BufferLimitExceeded)
  }
  state.buffer = state.buffer + bytes
  match state.header {
    Some(_) => ()
    None =>
      match @protocol.decode_subgroup_header(state.buffer) {
        @protocol.Incomplete => return Ok(Pending)
        @protocol.Invalid(_) => return Err(InvalidControlMessage)
        @protocol.Complete(header, header_length) => {
          if header.track_alias != track_alias {
            return Err(AliasMismatch)
          }
          state.header = Some(header)
          state.decoder = Some(@protocol.SubgroupDecoder::new(header))
          state.buffer = state.buffer[header_length:].to_owned()
        }
      }
  }
  let header = state.header.unwrap()
  let decoder = state.decoder.unwrap()
  let frames : Array[@protocol.SubgroupFrame] = []
  let mut remaining = state.buffer
  while !remaining.is_empty() {
    match decoder.decode_next(remaining) {
      @protocol.Incomplete => break
      @protocol.Invalid(_) => return Err(InvalidControlMessage)
      @protocol.Complete(frame, consumed) => {
        frames.push(frame)
        remaining = remaining[consumed:].to_owned()
      }
    }
  }
  state.buffer = remaining
  if frames.is_empty() {
    Ok(Pending)
  } else {
    Ok(Received(header, frames))
  }
}

///|
pub fn SingleTrackSubscriber::object_stream_group_id(
  self : SingleTrackSubscriber,
  stream : Int64,
) -> Result[UInt64, TrackSessionError] {
  match self.object_streams.get(stream) {
    Some(state) if state.header is Some(header) => Ok(header.group_id)
    _ => Err(IncompleteObjectStream)
  }
}

///|
/// Mark a data stream's FIN. The return value says whether its header marks
/// the end of its Group; a reset never carries that meaning.
pub fn SingleTrackSubscriber::finish_object_stream(
  self : SingleTrackSubscriber,
  stream : Int64,
) -> Result[Bool, TrackSessionError] {
  match self.object_streams.get(stream) {
    None => Err(IncompleteObjectStream)
    Some(state) =>
      match state.end_state {
        Finished => Ok(false)
        Reset => Ok(false)
        Open => {
          if state.header is None || !state.buffer.is_empty() {
            return Err(IncompleteObjectStream)
          }
          state.end_state = Finished
          Ok(state.header.unwrap().end_of_group)
        }
      }
  }
}

///|
/// Mark a data stream as reset without inferring End of Group from its header.
pub fn SingleTrackSubscriber::reset_object_stream(
  self : SingleTrackSubscriber,
  stream : Int64,
) -> Unit {
  match self.object_streams.get(stream) {
    Some(state) if state.end_state == Finished => ()
    Some(state) => {
      state.buffer = b""
      state.end_state = Reset
    }
    None =>
      self.object_streams[stream] = {
        buffer: b"",
        header: None,
        decoder: None,
        end_state: Reset,
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
