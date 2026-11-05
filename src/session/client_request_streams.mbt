///|
fn ClientSession::request_stream_exists(
  self : ClientSession,
  stream : Int64,
) -> Bool {
  self.request_streams.get(stream) is Some(_) ||
  self.inbound_subscriptions.get(stream) is Some(_) ||
  self.fetch_streams.get(stream) is Some(_) ||
  self.namespace_request_streams.get(stream) is Some(_) ||
  self.namespace_subscription_streams.get(stream) is Some(_) ||
  self.inbound_namespace_subscriptions.get(stream) is Some(_)
}

///|
fn ClientSession::request_stream_requires_cancel(
  self : ClientSession,
  stream : Int64,
) -> Bool {
  self.request_streams.get(stream) is Some(_) ||
  self.inbound_subscriptions.get(stream) is Some(_) ||
  self.namespace_request_streams.get(stream) is Some(_)
}

///|
fn ClientSession::has_active_request_family_work(self : ClientSession) -> Bool {
  for (_, request) in self.requests {
    if request.has_active_work() {
      return true
    }
  }
  for (_, inbound) in self.inbound_subscriptions {
    if inbound.state != Ended {
      return true
    }
  }
  for (_, fetch) in self.fetches {
    if fetch.state != FetchTerminal {
      return true
    }
  }
  self.has_active_namespace_work()
}

///|
fn ClientSession::request_stream_id(
  self : ClientSession,
  stream : Int64,
) -> UInt64 {
  if self.request_streams.get(stream) is Some(request_id) {
    return request_id
  }
  if self.namespace_request_streams.get(stream) is Some(request_id) {
    return request_id
  }
  if self.namespace_subscription_streams.get(stream) is Some(request_id) {
    return request_id
  }
  if self.inbound_subscriptions.get(stream) is Some(inbound) {
    return inbound.request_id
  }
  if self.inbound_namespace_subscriptions.get(stream) is Some(inbound) {
    return inbound.request_id
  }
  self.fetch_streams.get(stream).unwrap_or(0UL)
}

///|
fn ClientSession::finish_request_stream(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[SessionAction], ClientSessionError]? {
  if self.finish_namespace_stream(stream) is Some(result) {
    return Some(result)
  }
  if self.inbound_subscriptions.get(stream) is Some(_) ||
    self.request_streams.get(stream) is Some(_) {
    match self.inbound_subscriptions.get(stream) {
      Some(inbound) => ignore(inbound.lifetime.transition(PeerFin, true))
      None =>
        match self.request_streams.get(stream) {
          Some(request_id) => {
            let request = self.requests.get(request_id).unwrap()
            ignore(request.lifetime.transition(PeerFin, true))
            request.response_buffer = b""
          }
          None => ()
        }
    }
    self.inbound_buffers.remove(stream)
    return Some(Ok([]))
  }
  None
}

///|
fn ClientSession::reset_request_stream(
  self : ClientSession,
  stream : Int64,
  error_code : UInt64,
) -> Result[Array[SessionAction], ClientSessionError]? {
  if self.reset_namespace_stream(stream, error_code) is Some(result) {
    return Some(result)
  }
  if self.request_stream_exists(stream) {
    return Some(Ok(self.end_request_stream(stream, PeerReset(error_code))))
  }
  None
}
