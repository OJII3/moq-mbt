///|
fn delivery_deadline(start : UInt64, duration : UInt64) -> UInt64 {
  if duration > 0xffffffffffffffffUL - start {
    0xffffffffffffffffUL
  } else {
    start + duration
  }
}

///|
fn ClientSession::delivery_timeout(
  self : ClientSession,
  subscription_stream : Int64,
  kind : UInt64,
) -> UInt64 {
  match self.published_subscriptions.get(subscription_stream) {
    Some(subscription) => {
      let publisher_timeout = match
        self.publisher_request_for_subscription(subscription_stream) {
        Some(request_id) =>
          match self.requests.get(request_id) {
            Some(request) => track_property_timeout(request.properties, kind)
            None => 0UL
          }
        None => 0UL
      }
      for parameter in subscription.parameters {
        if parameter.kind == kind {
          if parameter.value is @protocol.VarInt(value) {
            if value == 0UL {
              return publisher_timeout
            }
            if publisher_timeout == 0UL || value < publisher_timeout {
              return value
            }
            return publisher_timeout
          }
        }
      }
      publisher_timeout
    }
    None => 0UL
  }
}

///|
fn ClientSession::subgroup_deadline(
  self : ClientSession,
  subscription_stream : Int64,
  finished_ms : UInt64,
) -> UInt64? {
  let duration = self.delivery_timeout(subscription_stream, 0x06UL)
  if duration == 0UL {
    None
  } else {
    Some(delivery_deadline(finished_ms, duration))
  }
}

///|
fn ClientSession::object_chunk_action(
  self : ClientSession,
  stream : Int64,
  subscription_stream : Int64,
  bytes : Bytes,
  first_byte_ms : UInt64,
) -> SessionAction {
  let duration = self.delivery_timeout(subscription_stream, 0x02UL)
  if duration == 0UL {
    SendChunk(stream, bytes)
  } else {
    SendObjectChunk(stream, bytes, delivery_deadline(first_byte_ms, duration))
  }
}

///|
fn ClientSession::object_datagram_action(
  self : ClientSession,
  token : UInt64,
  subscription_stream : Int64,
  bytes : Bytes,
  first_byte_ms : UInt64,
) -> SessionAction {
  let object_timeout = self.delivery_timeout(subscription_stream, 0x02UL)
  let subgroup_timeout = self.delivery_timeout(subscription_stream, 0x06UL)
  let duration = if object_timeout == 0UL {
    subgroup_timeout
  } else if subgroup_timeout == 0UL {
    object_timeout
  } else if object_timeout < subgroup_timeout {
    object_timeout
  } else {
    subgroup_timeout
  }
  if duration == 0UL {
    SendDatagram(token, bytes)
  } else {
    SendObjectDatagram(token, bytes, delivery_deadline(first_byte_ms, duration))
  }
}

///|
fn ClientSession::expire_object_streams(
  self : ClientSession,
) -> Array[SessionAction] {
  let expired : Array[Int64] = []
  for (stream, outgoing) in self.outgoing_object_streams {
    if outgoing.delivery_deadline_ms is Some(deadline) &&
      self.now_ms >= deadline {
      expired.push(stream)
    }
  }
  for (_, pending) in self.pending_object_streams {
    if pending.cancelled_error_code is None {
      match pending.finish_time_ms {
        Some(time) =>
          if self.subgroup_deadline(pending.subscription_stream, time)
            is Some(deadline) &&
            self.now_ms >= deadline {
            pending.cancelled_error_code = Some(2UL)
          }
        None => ()
      }
    }
  }
  let actions : Array[SessionAction] = []
  for stream in expired {
    match self.abort_outgoing_object_stream(stream, 2UL) {
      Ok(extra) =>
        for action in extra {
          actions.push(action)
        }
      Err(_) => ()
    }
  }
  actions
}

///|
fn ClientSession::object_stream_committed(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[SessionAction], ClientSessionError] {
  match self.outgoing_object_streams.get(stream) {
    Some(outgoing) if outgoing.finishing => {
      let subscription_stream = outgoing.subscription_stream
      self.close_outgoing_object_stream(stream)
      Ok(
        [EmitNotice(ObjectStreamSendFinished(stream))] +
        self.finish_failed_subscription(subscription_stream),
      )
    }
    _ => Ok([])
  }
}

///|
/// Describe transport guarantees. Set these before opening Object streams.
/// A nonzero Subgroup timeout requires both acknowledgement of all data and
/// FIN, and the ability to reset a stream after FIN has been requested.
pub fn ClientSession::set_stream_delivery_capability(
  self : ClientSession,
  committed~ : Bool,
  abort_after_fin~ : Bool,
) -> Unit {
  self.stream_committed_supported = committed
  self.abort_after_fin_supported = abort_after_fin
}

///|
fn ClientSession::ensure_delivery_clock(
  self : ClientSession,
  subscription_stream : Int64,
) -> Result[Unit, ClientSessionError] {
  if !self.clock_initialized &&
    (
      self.delivery_timeout(subscription_stream, 0x02UL) != 0UL ||
      self.delivery_timeout(subscription_stream, 0x06UL) != 0UL
    ) {
    Err(ClockNotInitialized)
  } else {
    Ok(())
  }
}

///|
/// Publisher Track Properties are validated when the PUBLISH is created.
fn track_property_timeout(properties : Bytes, wanted : UInt64) -> UInt64 {
  let mut offset = 0
  let mut previous = 0UL
  while offset < properties.length() {
    let (delta, next) = read_track_property_vi64(properties, offset).unwrap()
    let kind = previous + delta
    offset = next
    if (kind & 1UL) == 0UL {
      let (value, next) = read_track_property_vi64(properties, offset).unwrap()
      if kind == wanted {
        return value
      }
      offset = next
    } else {
      let (length, next) = read_track_property_vi64(properties, offset).unwrap()
      offset = next + length.to_int()
    }
    previous = kind
  }
  0UL
}
