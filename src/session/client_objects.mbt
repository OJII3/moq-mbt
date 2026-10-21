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
  mut parameters : Array[@protocol.MessageParameter]
  mut pending_updates : Array[PendingSubscriptionUpdate]
  mut pending_failure : @protocol.PublishDoneMessage?
  request_id : UInt64
}

///|
priv struct OutgoingObjectStream {
  subscription_stream : Int64
  header : @protocol.SubgroupHeader
  mut previous_object_id : UInt64?
  mut finishing : Bool
  mut abort_sent : Bool
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
          if request.lifetime.cancelled {
            // Cancellation retains the alias so delayed Object streams can be
            // attributed to this request and discarded.
            self.incoming_object_streams.remove(stream)
            self.ignored_object_streams[stream] = {
              request_id,
              counted_active: false,
            }
            return Ok([StopReceiving(stream, 1UL)])
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
    let request = self.requests.get(request_id).unwrap()
    if request.kind == Subscribing &&
      request.accepted &&
      !request.update_failed &&
      subscription_forward(request.parameters) {
      Ok([Notify(ObjectReceived(request_id, header, frames))])
    } else {
      Ok([])
    }
  }
}

///|
/// Route a received Object Datagram through the accepted subscription Alias.
fn ClientSession::receive_object_datagram(
  self : ClientSession,
  bytes : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  let datagram = match @protocol.decode_datagram(bytes) {
    @protocol.Complete(@protocol.Object(value), consumed) =>
      if consumed == bytes.length() {
        value
      } else {
        return Err(UnexpectedMessage)
      }
    @protocol.Complete(@protocol.Padding, consumed) =>
      if consumed == bytes.length() {
        return Ok([])
      } else {
        return Err(UnexpectedMessage)
      }
    _ => return Err(UnexpectedMessage)
  }
  let request_id = match self.aliases.get(datagram.track_alias) {
    Some(value) => value
    None => return Ok([])
  }
  let request = self.requests.get(request_id).unwrap()
  if request.kind != Subscribing ||
    !request.accepted ||
    (request.terminal && request.pending_publish_done is None) ||
    request.lifetime.cancelled ||
    request.update_failed ||
    !subscription_forward(request.parameters) {
    return Ok([])
  }
  Ok([Notify(ObjectDatagramReceived(request_id, datagram))])
}

///|
/// Ask the adapter to open an object stream for an accepted subscription,
/// whether this endpoint accepted an inbound SUBSCRIBE or sent a PUBLISH.
pub fn ClientSession::open_object_stream(
  self : ClientSession,
  subscription_stream : Int64,
  header : @protocol.SubgroupHeader,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let publisher_request_id = match
    self.inbound_subscriptions.get(subscription_stream) {
    Some(inbound) => {
      if inbound.state != Active {
        return Err(SubscriptionNotAccepted)
      }
      inbound.publisher_request_id
    }
    None =>
      match self.request_streams.get(subscription_stream) {
        Some(request_id) => {
          let request = self.requests.get(request_id).unwrap()
          if request.kind != Publishing || !request.accepted || request.terminal {
            return Err(SubscriptionNotAccepted)
          }
          request_id
        }
        None => return Err(UnknownStream)
      }
  }
  let publisher_request = self.requests.get(publisher_request_id).unwrap()
  if !self.subscription_is_active(subscription_stream) {
    return Err(SubscriptionNotAccepted)
  }
  if header.track_alias != publisher_request.track_alias.unwrap() {
    return Err(Track(AliasMismatch))
  }
  let token = self.next_object_token
  self.next_object_token = token + 1UL
  self.pending_object_streams[token] = {
    subscription_stream,
    header,
    cancelled_error_code: None,
  }
  Ok(self.finish_actions([OpenStream(Unidirectional, Object(token))]))
}

///|
/// Encode and send an Object on an open outgoing Subgroup stream.
pub fn ClientSession::send_object(
  self : ClientSession,
  stream : Int64,
  frame : @protocol.SubgroupFrame,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let outgoing = match self.outgoing_object_streams.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  if !self.subscription_is_active(outgoing.subscription_stream) {
    return Err(SubscriptionNotAccepted)
  }
  if outgoing.finishing {
    return Err(UnexpectedMessage)
  }
  let bytes = match encode_outgoing_object(outgoing, frame) {
    Ok(value) => value
    Err(error) => return Err(Track(error))
  }
  outgoing.pending_send_count = outgoing.pending_send_count + 1UL
  Ok(self.finish_actions([Send(stream, bytes, false)]))
}

///|
/// Send one Object Datagram for an active local or peer subscription.
pub fn ClientSession::send_object_datagram(
  self : ClientSession,
  subscription_stream : Int64,
  datagram : @protocol.ObjectDatagram,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if !self.subscription_is_active(subscription_stream) {
    return Err(SubscriptionNotAccepted)
  }
  let publisher_request_id = match
    self.inbound_subscriptions.get(subscription_stream) {
    Some(inbound) if inbound.state == Active => inbound.publisher_request_id
    Some(_) => return Err(SubscriptionNotAccepted)
    None =>
      match self.request_streams.get(subscription_stream) {
        Some(request_id) => {
          let request = self.requests.get(request_id).unwrap()
          if request.kind != Publishing || !request.accepted || request.terminal {
            return Err(SubscriptionNotAccepted)
          }
          request_id
        }
        None => return Err(UnknownStream)
      }
  }
  let publisher = self.requests.get(publisher_request_id).unwrap()
  if datagram.track_alias != publisher.track_alias.unwrap() {
    return Err(Track(AliasMismatch))
  }
  let bytes = match @protocol.encode_object_datagram(datagram) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  if !self.datagram_send_enabled {
    return Err(DatagramSendUnavailable)
  }
  if bytes.length() > self.max_datagram_length {
    return Err(DatagramTooLarge(bytes.length(), self.max_datagram_length))
  }
  Ok(self.finish_actions([SendDatagram(bytes)]))
}

///|
/// Finish an outgoing Subgroup stream after all of its Objects have been sent.
pub fn ClientSession::finish_object_stream(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let outgoing = match self.outgoing_object_streams.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  if outgoing.finishing {
    return Err(UnexpectedMessage)
  }
  outgoing.finishing = true
  outgoing.pending_send_count = outgoing.pending_send_count + 1UL
  Ok(self.finish_actions([Send(stream, b"", true)]))
}

///|
/// Send PUBLISH_DONE after all object streams for this subscription finish.
pub fn ClientSession::publish_done(
  self : ClientSession,
  subscription_stream : Int64,
  code : UInt64,
  reason : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let (request_id, local_publish) = match
    self.inbound_subscriptions.get(subscription_stream) {
    Some(inbound) => {
      if inbound.state != Active {
        return Err(SubscriptionNotAccepted)
      }
      (inbound.request_id, false)
    }
    None =>
      match self.request_streams.get(subscription_stream) {
        Some(request_id) => {
          let request = self.requests.get(request_id).unwrap()
          if request.kind != Publishing || !request.accepted || request.terminal {
            return Err(SubscriptionNotAccepted)
          }
          (request_id, true)
        }
        None => return Err(UnknownStream)
      }
  }
  let subscription = match
    self.published_subscriptions.get(subscription_stream) {
    Some(value) => value
    None => return Err(UnexpectedMessage)
  }
  if !subscription.pending_updates.is_empty() {
    return Err(RequestUpdatePending)
  }
  if subscription.open_stream_count != 0UL {
    return Err(Track(ObjectStreamsStillOpen))
  }
  let message : @protocol.PublishDoneMessage = {
    code,
    stream_count: subscription.opened_stream_count,
    reason,
  }
  let bytes = match @protocol.encode_publish_done(message) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  if local_publish {
    let request = self.requests.get(request_id).unwrap()
    request.terminal = true
  } else {
    let inbound = self.inbound_subscriptions.get(subscription_stream).unwrap()
    inbound.state = Ended
  }
  self.note_request_local_fin(subscription_stream)
  Ok(
    self.finish_actions([
      Send(subscription_stream, bytes, true),
      Notify(PublishDone(request_id, message)),
    ]),
  )
}
