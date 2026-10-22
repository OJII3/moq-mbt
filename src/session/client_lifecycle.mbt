///|
let request_error_going_away : UInt64 = 6UL

///|
let request_reset_going_away : UInt64 = 4UL

///|
let close_protocol_violation : UInt64 = 3UL

///|
let close_invalid_request_id : UInt64 = 4UL

///|
let close_goaway_timeout : UInt64 = 0x10UL

///|
fn ClientSession::ensure_open(
  self : ClientSession,
) -> Result[Unit, ClientSessionError] {
  if self.state == Closed {
    Err(SessionClosed)
  } else {
    Ok(())
  }
}

///|
fn ClientSession::ensure_new_request_allowed(
  self : ClientSession,
) -> Result[Unit, ClientSessionError] {
  if self.state == Closed {
    Err(SessionClosed)
  } else if self.peer_goaway_boundary is Some(_) {
    Err(SessionDraining)
  } else {
    Ok(())
  }
}

///|
fn ClientSession::finish_actions(
  self : ClientSession,
  actions : Array[ClientSessionAction],
) -> Array[ClientSessionAction] {
  for action in actions {
    match action {
      Send(stream, _, _) => {
        let count = match self.pending_sends.get(stream) {
          Some(value) => value
          None => 0UL
        }
        self.pending_sends[stream] = count + 1UL
      }
      SendDatagram(_) =>
        self.pending_datagram_sends = self.pending_datagram_sends + 1UL
      _ => ()
    }
  }
  actions + self.maybe_finish_drain()
}

///|
fn ClientSession::note_send_complete(
  self : ClientSession,
  stream : Int64,
) -> Unit {
  match self.pending_sends.get(stream) {
    Some(count) if count > 1UL => self.pending_sends[stream] = count - 1UL
    Some(_) => self.pending_sends.remove(stream)
    None => ()
  }
}

///|
fn ClientSession::has_active_requests(self : ClientSession) -> Bool {
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
  for (_, request) in self.namespace_requests {
    if request.state != NamespaceTerminal {
      return true
    }
  }
  for (_, request) in self.namespace_subscriptions {
    if !request.lifetime.cancelled && !request.lifetime.peer_send_closed {
      return true
    }
  }
  for (_, fetch) in self.fetches {
    if fetch.state != FetchTerminal {
      return true
    }
  }
  for (_, subscription) in self.inbound_namespace_subscriptions {
    if subscription.state != Ended {
      return true
    }
  }
  self.incoming_object_streams.length() > 0 ||
  self.outgoing_fetch_streams.length() > 0 ||
  self.ignored_object_streams.length() > 0 ||
  self.pending_object_streams.length() > 0 ||
  self.outgoing_object_streams.length() > 0
}

///|
fn ClientSession::has_drain_work(self : ClientSession) -> Bool {
  self.has_active_requests() ||
  self.pending_sends.length() > 0 ||
  self.pending_datagram_sends > 0UL
}

///|
fn ClientSession::maybe_finish_drain(
  self : ClientSession,
) -> Array[ClientSessionAction] {
  if self.state != Draining || self.has_drain_work() {
    return []
  }
  self.end_session(Local(0UL, b""), Some(0UL), b"")
}

///|
fn ClientSession::end_session(
  self : ClientSession,
  end : ClientSessionEnd,
  close_code : UInt64?,
  reason : Bytes,
) -> Array[ClientSessionAction] {
  if self.state == Closed {
    return []
  }
  self.state = Closed
  self.control_goaway_deadline = None
  self.request_goaway_deadlines.clear()
  self.fetches.clear()
  self.fetch_streams.clear()
  self.inbound_fetch_streams.clear()
  self.inbound_fetch_request_ids.clear()
  self.outgoing_fetch_streams.clear()
  self.pending_joining_fetches.clear()
  let actions : Array[ClientSessionAction] = []
  match close_code {
    Some(code) => actions.push(CloseConnection(code, reason))
    None => ()
  }
  actions.push(Notify(SessionEnded(end)))
  actions
}

///|
fn ClientSession::clock(
  self : ClientSession,
  now_ms : UInt64,
) -> Array[ClientSessionAction] {
  if !self.clock_initialized || now_ms > self.now_ms {
    self.now_ms = now_ms
  }
  self.clock_initialized = true
  let actions : Array[ClientSessionAction] = self.expire_pending_joining_fetches()
  let expired_requests : Array[Int64] = []
  for (stream, deadline) in self.request_goaway_deadlines {
    if self.now_ms >= deadline {
      expired_requests.push(stream)
    }
  }
  for stream in expired_requests {
    self.request_goaway_deadlines.remove(stream)
    for
      action in self.end_request_stream(
        stream,
        Cancel(request_reset_going_away),
      ) {
      actions.push(action)
    }
  }
  match self.control_goaway_deadline {
    Some(deadline) if self.now_ms >= deadline && self.has_active_requests() =>
      return actions +
        self.end_session(
          Local(close_goaway_timeout, b""),
          Some(close_goaway_timeout),
          b"",
        )
    _ => ()
  }
  actions
}

///|
pub fn ClientSession::state(self : ClientSession) -> ClientSessionState {
  self.state
}

///|
/// Send a client control GOAWAY with an empty New Session URI.
pub fn ClientSession::send_goaway(
  self : ClientSession,
  timeout_ms : UInt64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  if self.state == Closed {
    return Err(SessionClosed)
  }
  if !self.ready {
    return Err(HandshakeNotComplete)
  }
  if self.control_goaway_sent {
    return Err(UnexpectedMessage)
  }
  let stream = match self.outbound_control_stream {
    Some(value) => value
    None => return Err(HandshakeNotComplete)
  }
  let request_id = match self.max_peer_request_id {
    Some(value) => value + 2UL
    None => 1UL
  }
  let bytes = match
    @protocol.encode_goaway_control({ uri: b"", timeout_ms, request_id, }) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  self.control_goaway_sent = true
  self.state = Draining
  self.control_goaway_deadline = if timeout_ms == 0UL {
    None
  } else {
    Some(self.now_ms + timeout_ms)
  }
  Ok(self.finish_actions([Send(stream, bytes, false)]))
}

///|
fn ClientSession::note_peer_request_id(
  self : ClientSession,
  request_id : UInt64,
) -> Unit {
  match self.max_peer_request_id {
    Some(value) if request_id > value =>
      self.max_peer_request_id = Some(request_id)
    None => self.max_peer_request_id = Some(request_id)
    _ => ()
  }
}

///|
fn ClientSession::reject_local_request_for_goaway(
  _self : ClientSession,
  request_id : UInt64,
  request : ClientRequest,
) -> Array[ClientSessionAction] {
  if request.is_accepted() || request.is_terminal() {
    return []
  }
  request.mark_terminal()
  let effects = request.lifetime.transition(
    Cancel(request_reset_going_away),
    request.stream is Some(_),
  )
  request.response_buffer = b""
  request.pending_local_update = None
  let error : @protocol.RequestErrorMessage = {
    code: request_error_going_away,
    retry_interval: 0UL,
    reason: b"",
    redirect: b"",
  }
  let actions : Array[ClientSessionAction] = []
  match request.stream {
    Some(stream) => {
      if effects.abort_send {
        actions.push(AbortSend(stream, request_reset_going_away))
      }
      if effects.stop_receiving {
        actions.push(StopReceiving(stream, request_reset_going_away))
      }
    }
    None => ()
  }
  if effects.notify_end || !request.lifetime.end_notified {
    match request.kind {
      Publishing => actions.push(Notify(PublishRejected(request_id, error)))
      Subscribing => actions.push(Notify(SubscribeRejected(request_id, error)))
    }
    request.lifetime.end_notified = true
  }
  actions
}

///|
fn ClientSession::receive_control_goaway(
  self : ClientSession,
  message : @protocol.GoAwayControlMessage,
) -> Array[ClientSessionAction] {
  if self.control_goaway_received {
    return self.end_session(
      Local(close_protocol_violation, b""),
      Some(close_protocol_violation),
      b"",
    )
  }
  if (message.request_id & 1UL) != 0UL {
    return self.end_session(
      Local(close_invalid_request_id, b""),
      Some(close_invalid_request_id),
      b"",
    )
  }
  self.control_goaway_received = true
  self.peer_goaway_boundary = Some(message.request_id)
  self.state = Draining
  let actions : Array[ClientSessionAction] = [
    Notify(
      ControlGoAwayReceived(message.uri, message.timeout_ms, message.request_id),
    ),
  ]
  for (request_id, request) in self.requests {
    if request_id >= message.request_id && !request.is_accepted() {
      for action in self.reject_local_request_for_goaway(request_id, request) {
        actions.push(action)
      }
    }
  }
  for (request_id, fetch) in self.fetches {
    if fetch.locally_initiated &&
      request_id >= message.request_id &&
      fetch.state == FetchPending {
      let effects = fetch.lifetime.transition(
        Cancel(request_reset_going_away),
        fetch.stream is Some(_),
      )
      match fetch.stream {
        Some(stream) => {
          if effects.abort_send {
            actions.push(AbortSend(stream, request_reset_going_away))
          }
          if effects.stop_receiving {
            actions.push(StopReceiving(stream, request_reset_going_away))
          }
        }
        None => ()
      }
      for
        action in self.terminate_fetch(
          fetch,
          [],
          Some(
            Notify(
              FetchFailed(request_id, {
                code: request_error_going_away,
                retry_interval: 0UL,
                reason: b"",
                redirect: b"",
              }),
            ),
          ),
          StopFetchData(request_reset_going_away),
        ) {
        actions.push(action)
      }
    }
  }
  for (request_id, request) in self.namespace_requests {
    if request.locally_initiated &&
      request_id >= message.request_id &&
      request.state == NamespacePending {
      request.state = NamespaceTerminal
      request.pending_local_update_id = None
      request.pending_local_update_parameters = []
      request.pending_updates = []
      let effects = request.lifetime.transition(
        Cancel(request_reset_going_away),
        request.stream is Some(_),
      )
      request.lifetime.end_notified = true
      if request.stream is Some(stream) {
        if effects.abort_send {
          actions.push(AbortSend(stream, request_reset_going_away))
        }
        if effects.stop_receiving {
          actions.push(StopReceiving(stream, request_reset_going_away))
        }
        actions.push(
          Notify(
            NamespacePublicationRejected(request_id, stream, {
              code: request_error_going_away,
              retry_interval: 0UL,
              reason: b"",
              redirect: b"",
            }),
          ),
        )
      }
    }
  }
  actions
}

///|
fn ClientSession::receive_request_goaway(
  self : ClientSession,
  stream : Int64,
  message : @protocol.GoAwayRequestMessage,
) -> Array[ClientSessionAction] {
  if self.request_goaway_streams.get(stream) is Some(_) {
    return self.end_session(
      Local(close_protocol_violation, b""),
      Some(close_protocol_violation),
      b"",
    )
  }
  self.request_goaway_streams[stream] = true
  if message.timeout_ms > 0UL {
    self.request_goaway_deadlines[stream] = self.now_ms + message.timeout_ms
  }
  let request_id = match self.request_streams.get(stream) {
    Some(value) => value
    None =>
      match self.namespace_request_streams.get(stream) {
        Some(value) => value
        None =>
          match self.inbound_subscriptions.get(stream) {
            Some(inbound) => inbound.request_id
            None => self.fetch_streams.get(stream).unwrap_or(0UL)
          }
      }
  }
  [
    Notify(
      RequestGoAwayReceived(request_id, stream, message.uri, message.timeout_ms),
    ),
  ]
}

///|
pub extend ClientSessionState with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ClientSessionState with Eq::{equal, not_equal}

///|
pub extend ClientSessionEnd with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ClientSessionEnd with Eq::{equal, not_equal}
