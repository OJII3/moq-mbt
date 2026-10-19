///|
priv struct PendingObjectStream {
  subscription_stream : Int64
  header : @protocol.SubgroupHeader
  mut cancelled_error_code : UInt64?
}

///|
priv struct PublishedSubscription {
  mut opened_stream_count : UInt64
  mut open_stream_count : UInt64
}

///|
priv struct OutgoingObjectStream {
  subscription_stream : Int64
  header : @protocol.SubgroupHeader
  mut previous_object_id : UInt64?
  mut finishing : Bool
  mut pending_send_count : UInt64
}

///|
/// request_id, header, and decoder are set together once the header is routed.
priv struct IncomingObjectStream {
  mut request_id : UInt64?
  mut buffer : Bytes
  mut header : @protocol.SubgroupHeader?
  mut decoder : @protocol.SubgroupDecoder?
}

///|
priv struct IgnoredObjectStream {
  request_id : UInt64
  counted_active : Bool
}

///|
fn encode_outgoing_object(
  writer : OutgoingObjectStream,
  frame : @protocol.SubgroupFrame,
) -> Result[Bytes, TrackSessionError] {
  match writer.previous_object_id {
    Some(previous) if frame.object_id <= previous =>
      return Err(ObjectIdNotIncreasing)
    _ => ()
  }
  let encoded = match
    @protocol.encode_subgroup_frame(
      writer.header,
      writer.previous_object_id,
      frame,
    ) {
    Ok(value) => value
    Err(_) => return Err(InvalidControlMessage)
  }
  writer.previous_object_id = Some(frame.object_id)
  Ok(encoded)
}

///|
fn ClientSession::close_outgoing_object_stream(
  self : ClientSession,
  stream : Int64,
) -> Unit {
  match self.outgoing_object_streams.get(stream) {
    Some(outgoing) => {
      match self.published_subscriptions.get(outgoing.subscription_stream) {
        Some(subscription) =>
          subscription.open_stream_count = subscription.open_stream_count - 1UL
        None => ()
      }
      self.outgoing_object_streams.remove(stream)
    }
    None => ()
  }
}

///|
fn ClientSession::receive_subgroup(
  self : ClientSession,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  if self.ignored_object_streams.get(stream) is Some(_) {
    return Ok([])
  }
  let state = match self.incoming_object_streams.get(stream) {
    Some(value) => value
    None => {
      let value : IncomingObjectStream = {
        request_id: None,
        buffer: b"",
        header: None,
        decoder: None,
      }
      self.incoming_object_streams[stream] = value
      value
    }
  }
  if bytes.length() > 1024 * 1024 - state.buffer.length() {
    return Err(BufferLimitExceeded)
  }
  state.buffer = state.buffer + bytes
  match state.header {
    None =>
      match @protocol.decode_subgroup_header(state.buffer) {
        @protocol.Incomplete => return Ok([])
        @protocol.Invalid(_) => return Err(UnexpectedMessage)
        @protocol.Complete(header, consumed) => {
          let request_id = match self.aliases.get(header.track_alias) {
            Some(value) => value
            None => return Err(UnknownRequest)
          }
          let request = self.requests.get(request_id).unwrap()
          if request.terminal && !request.publish_done_received {
            self.incoming_object_streams.remove(stream)
            self.ignored_object_streams[stream] = {
              request_id,
              counted_active: false,
            }
            return Ok([StopReceiving(stream, 0UL)])
          }
          match request.pending_publish_done {
            Some(done) if request.observed_object_streams >= done.stream_count =>
              return Err(UnexpectedMessage)
            _ => ()
          }
          state.request_id = Some(request_id)
          state.header = Some(header)
          state.decoder = Some(@protocol.SubgroupDecoder::new(header))
          state.buffer = state.buffer[consumed:].to_owned()
          request.observed_object_streams = request.observed_object_streams +
            1UL
          request.active_object_streams = request.active_object_streams + 1UL
        }
      }
    Some(_) => ()
  }
  let request_id = state.request_id.unwrap()
  let header = state.header.unwrap()
  let decoder = state.decoder.unwrap()
  let frames : Array[@protocol.SubgroupFrame] = []
  let mut remaining = state.buffer
  while !remaining.is_empty() {
    match decoder.decode_next(remaining) {
      @protocol.Incomplete => break
      @protocol.Invalid(_) => return Err(Track(InvalidControlMessage))
      @protocol.Complete(frame, consumed) => {
        frames.push(frame)
        remaining = remaining[consumed:].to_owned()
      }
    }
  }
  state.buffer = remaining
  if frames.is_empty() {
    Ok([])
  } else {
    Ok([Notify(ObjectReceived(request_id, header, frames))])
  }
}
