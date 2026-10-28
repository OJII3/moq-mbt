///|
/// Thin writer for one Object group. The session owns the stream, Object ID
/// allocation, FIN waiting, and abort state; this handle only carries the
/// group token and the next Object ID.
pub struct ObjectGroupWriter {
  priv session : ClientSession
  priv subscription_stream : Int64
  priv mut group_id : UInt64
  priv mut token : UInt64
  priv mut next_object_id : UInt64
  priv mut finished : Bool
  priv has_properties : Bool
  priv mut aborted : Bool
}

///|
/// Start a new Object group on an accepted subscription. The returned actions
/// contain the `OpenStream` request. Wait for StreamOpened before submitting
/// Objects; earlier sends return ObjectStreamNotReady without retaining payload.
pub fn ClientSession::open_object_group(
  self : ClientSession,
  subscription_stream : Int64,
  group_id : UInt64,
  end_of_group~ : Bool = false,
  has_properties~ : Bool = false,
) -> Result[(ObjectGroupWriter, Array[SessionAction]), ClientSessionError] {
  let publisher_request_id = match
    self.publisher_request_for_subscription(subscription_stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  let track_alias = match
    self.requests.get(publisher_request_id).unwrap().track_alias {
    Some(value) => value
    None => return Err(UnknownRequest)
  }
  let header : @protocol.SubgroupHeader = {
    track_alias,
    group_id,
    subgroup_id_mode: @protocol.Zero,
    publisher_priority: None,
    end_of_group,
    first_object: true,
    has_properties,
  }
  let actions = match self.open_object_stream(subscription_stream, header) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  let mut token : UInt64? = None
  for action in actions {
    match action {
      OpenStream(_, Object(value)) => token = Some(value)
      _ => ()
    }
  }
  let token = match token {
    Some(value) => value
    None => return Err(UnexpectedMessage)
  }
  Ok(
    (
      {
        session: self,
        subscription_stream,
        group_id,
        token,
        next_object_id: 0UL,
        finished: false,
        aborted: false,
        has_properties,
      },
      actions,
    ),
  )
}

///|
/// Send one normal Object. The Object ID is allocated by the writer.
/// Supply a fresh monotonic Clock event immediately before this operation.
pub fn ObjectGroupWriter::send(
  self : ObjectGroupWriter,
  payload : Bytes,
) -> Result[Array[SessionAction], ClientSessionError] {
  self.send_frame({
    object_id: self.next_object_id,
    status: @protocol.Normal,
    properties: b"",
    payload,
  })
}

///|
/// Send one Object with an explicit status and properties. `frame.object_id`
/// must be the writer's next Object ID. Supply a fresh Clock before sending.
pub fn ObjectGroupWriter::send_frame(
  self : ObjectGroupWriter,
  frame : @protocol.SubgroupFrame,
) -> Result[Array[SessionAction], ClientSessionError] {
  if self.finished || self.aborted {
    return Err(UnexpectedMessage)
  }
  if frame.object_id != self.next_object_id {
    return Err(Track(ObjectIdNotIncreasing))
  }
  let actions = match self.session.group_streams.get(self.token) {
    Some(stream) => self.session.send_object(stream, frame)
    None => Err(ObjectStreamNotReady)
  }
  match actions {
    Ok(value) => {
      self.next_object_id = self.next_object_id + 1UL
      Ok(value)
    }
    Err(error) => Err(error)
  }
}

///|
/// Finish the current group and start the next one with an incremented group
/// ID. The returned actions contain the FIN and the next `OpenStream`.
pub fn ObjectGroupWriter::rotate(
  self : ObjectGroupWriter,
) -> Result[Array[SessionAction], ClientSessionError] {
  let finish_actions = match self.finish() {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  let (next, open_actions) = match
    self.session.open_object_group(
      self.subscription_stream,
      self.group_id + 1UL,
      has_properties=self.has_properties,
    ) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  self.group_id = next.group_id
  self.token = next.token
  self.next_object_id = 0UL
  self.finished = false
  Ok(finish_actions + open_actions)
}

///|
/// Finish the current group. Supply a fresh Clock immediately beforehand;
/// the Subgroup delivery timer starts here.
pub fn ObjectGroupWriter::finish(
  self : ObjectGroupWriter,
) -> Result[Array[SessionAction], ClientSessionError] {
  if self.finished || self.aborted {
    return Err(UnexpectedMessage)
  }
  let result = match self.session.group_streams.get(self.token) {
    Some(stream) => self.session.finish_object_stream(stream)
    None => self.session.finish_pending_group(self.token)
  }
  if result is Ok(_) {
    self.finished = true
  }
  result
}

///|
/// Abort the group, including after FIN was requested while delivery is pending.
/// Repeated aborts and aborts after the stream has completed are harmless.
pub fn ObjectGroupWriter::abort(
  self : ObjectGroupWriter,
  code : UInt64,
) -> Result[Array[SessionAction], ClientSessionError] {
  if self.aborted {
    return Ok([])
  }
  match self.session.group_streams.get(self.token) {
    Some(stream) => {
      let outgoing = self.session.outgoing_object_streams.get(stream).unwrap()
      if outgoing.finishing && !self.session.abort_after_fin_supported {
        return Err(DeliveryTimeoutUnsupported)
      }
    }
    None => ()
  }
  self.aborted = true
  match self.session.group_streams.get(self.token) {
    Some(stream) => self.session.abort_outgoing_object_stream(stream, code)
    None =>
      if self.finished &&
        self.session.pending_object_streams.get(self.token) is None {
        Ok([])
      } else {
        self.session.cancel_pending_group(self.token, code)
      }
  }
}

///|
/// The Track Alias and request that a subscription forwards from.
fn ClientSession::publisher_request_for_subscription(
  self : ClientSession,
  subscription_stream : Int64,
) -> UInt64? {
  match self.inbound_subscriptions.get(subscription_stream) {
    Some(inbound) => Some(inbound.publisher_request_id)
    None => self.request_streams.get(subscription_stream)
  }
}

///|
fn ClientSession::finish_pending_group(
  self : ClientSession,
  token : UInt64,
) -> Result[Array[SessionAction], ClientSessionError] {
  match self.pending_object_streams.get(token) {
    Some(pending) => {
      if self.delivery_timeout(pending.subscription_stream, 0x06UL) != 0UL &&
        (!self.stream_committed_supported || !self.abort_after_fin_supported) {
        return Err(DeliveryTimeoutUnsupported)
      }
      match self.ensure_delivery_clock(pending.subscription_stream) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      pending.finish_requested = true
      pending.finish_time_ms = Some(self.now_ms)
      Ok([])
    }
    None => Err(UnknownStream)
  }
}

///|
fn ClientSession::cancel_pending_group(
  self : ClientSession,
  token : UInt64,
  code : UInt64,
) -> Result[Array[SessionAction], ClientSessionError] {
  match self.pending_object_streams.get(token) {
    Some(pending) => {
      pending.cancelled_error_code = Some(code)
      Ok([])
    }
    None => Err(UnknownStream)
  }
}

///|
fn ClientSession::abort_outgoing_object_stream(
  self : ClientSession,
  stream : Int64,
  code : UInt64,
) -> Result[Array[SessionAction], ClientSessionError] {
  let outgoing = match self.outgoing_object_streams.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  let subscription_stream = outgoing.subscription_stream
  self.close_outgoing_object_stream(stream)
  let extra = self.finish_failed_subscription(subscription_stream)
  Ok(
    self.finish_actions(
      [AbortSend(stream, code), EmitNotice(StreamSendFinished(stream, Reset))] +
      extra,
    ),
  )
}
