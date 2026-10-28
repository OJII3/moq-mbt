///|
priv struct PartialObject {
  object_id : UInt64
  mut remaining : UInt64
  mut first_byte_ms : UInt64?
}

///|
/// Incremental payload writer. Each write returns its own send action and does
/// not retain payload bytes in the session. Finish validates the declared length.
pub struct ObjectWriter {
  priv session : ClientSession
  priv stream : Int64
  priv object_id : UInt64
  priv mut finished : Bool
}

///|
/// Begin a normal Object with a declared payload length. The subgroup stream
/// must already be open; ObjectStreamNotReady asks the caller to retry after
/// StreamOpened. Properties require has_properties=true on open_object_group.
/// The returned actions contain only the Object header. The delivery timer
/// starts with the first nonempty payload write, after a fresh Clock event.
pub fn ObjectGroupWriter::begin_object(
  self : ObjectGroupWriter,
  payload_length : UInt64,
  properties~ : Bytes = b"",
) -> Result[(ObjectWriter, Array[SessionAction]), ClientSessionError] {
  if self.finished || self.aborted {
    return Err(UnexpectedMessage)
  }
  match self.session.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let stream = match self.session.group_streams.get(self.token) {
    Some(value) => value
    None => return Err(ObjectStreamNotReady)
  }
  let outgoing = self.session.outgoing_object_streams.get(stream).unwrap()
  if outgoing.finishing || outgoing.partial_object is Some(_) {
    return Err(UnexpectedMessage)
  }
  if !self.session.subscription_is_active(outgoing.subscription_stream) {
    return Err(SubscriptionNotAccepted)
  }
  if !subscription_allows_location(
      self.session.published_subscriptions
      .get(outgoing.subscription_stream)
      .unwrap().resolved_filter,
      outgoing.header.group_id,
      self.next_object_id,
    ) {
    return Err(SubscriptionNotAccepted)
  }
  let bytes = match
    @protocol.encode_subgroup_object_header(
      outgoing.header,
      outgoing.previous_object_id,
      self.next_object_id,
      properties,
      payload_length,
    ) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  let object_id = self.next_object_id
  outgoing.previous_object_id = Some(object_id)
  outgoing.partial_object = Some({
    object_id,
    remaining: payload_length,
    first_byte_ms: None,
  })
  outgoing.pending_send_count = outgoing.pending_send_count + 1UL
  self.next_object_id = object_id + 1UL
  Ok(
    (
      { session: self.session, stream, object_id, finished: false, },
      self.session.finish_actions([SendChunk(stream, bytes)]),
    ),
  )
}

///|
/// Append payload bytes without exceeding the declared length. Empty writes
/// produce no action and do not start the Object delivery timer.
/// Supply a fresh Clock event immediately before every nonempty write.
pub fn ObjectWriter::write(
  self : ObjectWriter,
  chunk : Bytes,
) -> Result[Array[SessionAction], ClientSessionError] {
  if self.finished {
    return Err(UnexpectedMessage)
  }
  match self.session.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let outgoing = match self.session.outgoing_object_streams.get(self.stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  if !self.session.subscription_is_active(outgoing.subscription_stream) {
    return Err(SubscriptionNotAccepted)
  }
  let partial = match outgoing.partial_object {
    Some(value) if value.object_id == self.object_id => value
    _ => return Err(UnexpectedMessage)
  }
  if chunk.length().to_uint64() > partial.remaining {
    return Err(ObjectPayloadLengthMismatch)
  }
  if chunk.is_empty() {
    return Ok([])
  }
  match self.session.ensure_delivery_clock(outgoing.subscription_stream) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if partial.first_byte_ms is None {
    partial.first_byte_ms = Some(self.session.now_ms)
  }
  partial.remaining = partial.remaining - chunk.length().to_uint64()
  outgoing.pending_send_count = outgoing.pending_send_count + 1UL
  Ok(
    self.session.finish_actions([
      self.session.object_chunk_action(
        self.stream,
        outgoing.subscription_stream,
        chunk,
        partial.first_byte_ms.unwrap(),
      ),
    ]),
  )
}

///|
/// End this Object after exactly its declared payload length has been written.
/// An incomplete Object can still be written after this returns a length error.
pub fn ObjectWriter::finish(
  self : ObjectWriter,
) -> Result[Array[SessionAction], ClientSessionError] {
  if self.finished {
    return Err(UnexpectedMessage)
  }
  let outgoing = match self.session.outgoing_object_streams.get(self.stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  let partial = match outgoing.partial_object {
    Some(value) if value.object_id == self.object_id => value
    _ => return Err(UnexpectedMessage)
  }
  if partial.remaining != 0UL {
    return Err(ObjectPayloadLengthMismatch)
  }
  outgoing.partial_object = None
  self.finished = true
  match
    self.session.publisher_request_for_subscription(
      outgoing.subscription_stream,
    ) {
    Some(request_id) =>
      self.session.note_published_location(
        request_id,
        outgoing.header.group_id,
        self.object_id,
      )
    None => ()
  }
  Ok([])
}
