///|
priv enum ClientRequestKind {
  Publishing
  Subscribing
} derive(Eq)

///|
priv enum ClientRequestState {
  RequestPending
  RequestAccepted
  RequestTerminal
} derive(Eq)

///|
priv struct ClientRequest {
  kind : ClientRequestKind
  track : @protocol.TrackName
  mut track_alias : UInt64?
  mut stream : Int64?
  mut response_buffer : Bytes
  mut state : ClientRequestState
  mut dynamic_groups : Bool
  lifetime : RequestLifetime
  object_drain : ObjectDrain
  mut parameters : Array[@protocol.MessageParameter]
  mut joining_location : @protocol.FetchLocation?
  mut pending_local_update : PendingSubscriptionUpdate?
  mut update_failed : Bool
}

///|
priv struct ObjectDrain {
  mut observed_streams : UInt64
  mut active_streams : UInt64
  mut pending_done : @protocol.PublishDoneMessage?
}

///|
fn ClientRequest::is_accepted(self : ClientRequest) -> Bool {
  self.state == RequestAccepted
}

///|
fn ClientRequest::is_terminal(self : ClientRequest) -> Bool {
  self.state == RequestTerminal
}

///|
fn ClientRequest::is_active(self : ClientRequest) -> Bool {
  self.state == RequestAccepted
}

///|
fn ClientRequest::is_fully_terminal(self : ClientRequest) -> Bool {
  self.is_terminal() && !self.object_drain.has_pending_done()
}

///|
fn ClientRequest::mark_accepted(self : ClientRequest) -> Unit {
  self.state = RequestAccepted
}

///|
fn ClientRequest::mark_terminal(self : ClientRequest) -> Unit {
  self.state = RequestTerminal
}

///|
fn ClientRequest::has_active_work(self : ClientRequest) -> Bool {
  !self.is_terminal() ||
  self.object_drain.has_work() ||
  self.pending_local_update is Some(_)
}

///|
fn ClientRequest::can_receive_object_data(self : ClientRequest) -> Bool {
  self.kind == Subscribing &&
  (
    self.is_accepted() ||
    (self.is_terminal() && self.object_drain.has_pending_done())
  ) &&
  !self.lifetime.cancelled &&
  !self.update_failed &&
  subscription_forward(self.parameters)
}

///|
fn ObjectDrain::has_work(self : ObjectDrain) -> Bool {
  self.active_streams > 0UL || self.pending_done is Some(_)
}

///|
fn ObjectDrain::has_pending_done(self : ObjectDrain) -> Bool {
  self.pending_done is Some(_)
}

///|
fn ObjectDrain::note_stream_open(self : ObjectDrain) -> Unit {
  self.observed_streams = self.observed_streams + 1UL
  self.active_streams = self.active_streams + 1UL
}

///|
fn ObjectDrain::begin(
  self : ObjectDrain,
  done : @protocol.PublishDoneMessage,
) -> Unit {
  self.pending_done = Some(done)
}

///|
fn ObjectDrain::cancel(self : ObjectDrain) -> Unit {
  self.pending_done = None
}

///|
fn ObjectDrain::note_stream_closed(self : ObjectDrain) -> Unit {
  self.active_streams = self.active_streams - 1UL
}

///|
fn ObjectDrain::can_observe_stream(self : ObjectDrain) -> Bool {
  match self.pending_done {
    Some(done) => self.observed_streams < done.stream_count
    None => true
  }
}

///|
fn ObjectDrain::can_begin(
  self : ObjectDrain,
  done : @protocol.PublishDoneMessage,
) -> Bool {
  done.stream_count >= self.observed_streams
}

///|
fn ObjectDrain::take_ready_done(
  self : ObjectDrain,
) -> @protocol.PublishDoneMessage? {
  match self.pending_done {
    Some(done) if self.observed_streams == done.stream_count &&
      self.active_streams == 0UL => {
      self.pending_done = None
      Some(done)
    }
    _ => None
  }
}

///|
priv struct PendingSubscriptionUpdate {
  request_id : UInt64
  effective_parameters : Array[@protocol.MessageParameter]
  filter_updated : Bool
}

///|
priv struct InboundSubscription {
  request_id : UInt64
  publisher_request_id : UInt64
  mut state : SubscriptionState
  parameters : Array[@protocol.MessageParameter]
  lifetime : RequestLifetime
}

///|
priv enum SubscriptionState {
  Pending
  Active
  Ended
} derive(Eq)

///|
fn ClientSession::allocate_request_id(self : ClientSession) -> UInt64 {
  let request_id = self.next_request_id
  self.next_request_id = self.next_request_id + 2UL
  request_id
}

///|
fn ClientSession::subscription_is_active(
  self : ClientSession,
  stream : Int64,
) -> Bool {
  let active = match self.inbound_subscriptions.get(stream) {
    Some(inbound) => inbound.state == Active
    None =>
      match self.request_streams.get(stream) {
        Some(request_id) => {
          let request = self.requests.get(request_id).unwrap()
          request.kind == Publishing && request.is_active()
        }
        None => false
      }
  }
  if !active {
    return false
  }
  match self.published_subscriptions.get(stream) {
    Some(subscription) => subscription_forward(subscription.parameters)
    None => true
  }
}

///|
fn ClientSession::receive_request(
  self : ClientSession,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[SessionAction], ClientSessionError] {
  match self.request_stream_route(stream) {
    NamespaceSubscriptionResponse(request_id) =>
      self.receive_namespace_subscription_response(request_id, stream, bytes)
    NamespacePublicationResponse(request_id) =>
      self.receive_namespace_response(request_id, stream, bytes)
    FetchResponse(request_id) =>
      self.receive_fetch_response(request_id, stream, bytes)
    TrackResponse(request_id) =>
      self.receive_response(request_id, stream, bytes)
    UnknownRequestStream => self.receive_peer_request(stream, bytes)
  }
}

///|
/// Resolve the protocol owner of a bidirectional request stream from the
/// session's request indexes. Keep the precedence in one place because some
/// Namespace requests also have entries in the general request index.
fn ClientSession::request_stream_route(
  self : ClientSession,
  stream : Int64,
) -> RequestStreamRoute {
  match self.namespace_subscription_streams.get(stream) {
    Some(request_id) => return NamespaceSubscriptionResponse(request_id)
    None => ()
  }
  match self.namespace_request_streams.get(stream) {
    Some(request_id) if self.namespace_requests.get(request_id).unwrap().locally_initiated =>
      return NamespacePublicationResponse(request_id)
    _ => ()
  }
  match self.fetch_streams.get(stream) {
    Some(request_id) => return FetchResponse(request_id)
    None => ()
  }
  match self.request_streams.get(stream) {
    Some(request_id) => TrackResponse(request_id)
    None => UnknownRequestStream
  }
}

///|
fn ClientSession::receive_response(
  self : ClientSession,
  request_id : UInt64,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[SessionAction], ClientSessionError] {
  let request = self.requests.get(request_id).unwrap()
  if request.lifetime.cancelled {
    if request.kind == Subscribing && request.track_alias is None {
      self.remember_cancelled_subscribe_alias(request_id, request, bytes)
    }
    return Ok([])
  }
  if request.lifetime.peer_send_closed {
    return Ok([])
  }
  if request.is_terminal() {
    return Err(UnexpectedMessage)
  }
  if bytes.length() > 65546 - request.response_buffer.length() {
    return Err(BufferLimitExceeded)
  }
  request.response_buffer = request.response_buffer + bytes
  let context = match (request.kind, request.is_accepted()) {
    (Publishing, false) =>
      @protocol.RequestStreamFor(@protocol.RequestOkContext::Publish)
    (Subscribing, true) if request.pending_local_update is Some(_) =>
      @protocol.RequestStreamFor(@protocol.RequestOkContext::RequestUpdate)
    _ => @protocol.RequestStream
  }
  match
    @protocol.decode_control_with_context(request.response_buffer, context) {
    @protocol.Incomplete => Ok([])
    @protocol.Invalid(_) => Err(UnexpectedMessage)
    @protocol.Complete(message, consumed) => {
      let trailing = request.response_buffer[consumed:].to_owned()
      request.response_buffer = b""
      let actions = match (request.kind, message) {
        (Publishing, @protocol.RequestOk(response)) if !request.is_accepted() => {
          request.mark_accepted()
          request.parameters = response.parameters
          self.published_subscriptions[stream] = {
            opened_stream_count: 0UL,
            open_stream_count: 0UL,
            parameters: response.parameters,
            pending_updates: [],
            pending_failure: None,
            resolved_filter: resolve_subscription_filter(
              response.parameters,
              self.largest_published_locations.get(request_id),
            ),
            request_id,
            joining_location: if subscription_forward(response.parameters) {
              match subscription_largest_location(response.parameters) {
                Some((group_id, object_id)) =>
                  Some({ group: group_id, object: object_id, })
                None => None
              }
            } else {
              None
            },
          }
          [
            EmitNotice(
              PublishAccepted(
                request_id,
                request.track,
                request.track_alias.unwrap(),
              ),
            ),
          ] +
          self.drain_joining_fetches(request_id, stream)
        }
        (Subscribing, @protocol.SubscribeOk(response)) if !request.is_accepted() => {
          let dynamic_groups = match
            track_supports_dynamic_groups(response.properties) {
            Some(value) => value
            None =>
              return Ok(
                self.end_session(
                  Local(close_protocol_violation, b""),
                  Some(close_protocol_violation),
                  b"",
                ),
              )
          }
          if self.aliases.get(response.track_alias) is Some(_) {
            return Err(AliasInUse)
          }
          self.aliases[response.track_alias] = request_id
          request.track_alias = Some(response.track_alias)
          request.dynamic_groups = dynamic_groups
          let old_parameters = request.parameters
          request.parameters = response.parameters
          self.update_desired_subscription_parameters(
            request.track,
            old_parameters,
            request.parameters,
          )
          request.joining_location = match
            subscription_largest_location(response.parameters) {
            Some((group_id, object_id)) =>
              Some({ group: group_id, object: object_id, })
            None => None
          }
          request.mark_accepted()
          [EmitNotice(SubscribeEstablished(request_id, response.track_alias))]
        }
        (Publishing, @protocol.RequestError(error)) if !request.is_accepted() => {
          request.mark_terminal()
          self.request_local_fin_actions(stream) +
          [EmitNotice(PublishRejected(request_id, error))] +
          self.fail_pending_joining_fetches(
            request_id, request_error_invalid_joining_request_id,
          )
        }
        (Subscribing, @protocol.RequestError(error)) if !request.is_accepted() => {
          request.mark_terminal()
          self.request_local_fin_actions(stream) +
          [EmitNotice(SubscribeRejected(request_id, error))]
        }
        (Publishing, @protocol.RequestUpdate(update)) if request.is_accepted() =>
          match self.receive_peer_subscription_update(stream, update) {
            Ok(values) => values
            Err(error) => return Err(error)
          }
        (Subscribing, @protocol.RequestOk(response)) if request.pending_local_update
          is Some(pending) => {
          let was_forwarding = subscription_forward(request.parameters)
          let forwarding = subscription_forward(pending.effective_parameters)
          let old_parameters = request.parameters
          request.parameters = pending.effective_parameters
          self.update_desired_subscription_parameters(
            request.track,
            old_parameters,
            request.parameters,
          )
          if !was_forwarding && forwarding {
            request.joining_location = match
              subscription_largest_location(response.parameters) {
              Some((group_id, object_id)) =>
                Some({ group: group_id, object: object_id, })
              None => request.joining_location
            }
          }
          request.pending_local_update = None
          [
            EmitNotice(
              SubscriptionUpdateAccepted(
                request_id,
                pending.request_id,
                response,
              ),
            ),
          ]
        }
        (Subscribing, @protocol.RequestError(error)) if request.pending_local_update
          is Some(pending) => {
          request.pending_local_update = None
          request.update_failed = true
          [
            EmitNotice(
              SubscriptionUpdateRejected(request_id, pending.request_id, error),
            ),
          ]
        }
        (Subscribing, @protocol.PublishDone(message)) if request.is_accepted() =>
          match self.receive_publish_done(request_id, message) {
            Ok(values) => values
            Err(error) => return Err(error)
          }
        (_, @protocol.GoAwayRequest(goaway)) =>
          self.receive_request_goaway(stream, goaway)
        _ => return Err(UnexpectedMessage)
      }
      if trailing.is_empty() {
        Ok(actions)
      } else {
        match self.receive_response(request_id, stream, trailing) {
          Ok(more) => Ok(actions + more)
          Err(error) => Err(error)
        }
      }
    }
  }
}

///|
fn ClientSession::remember_cancelled_subscribe_alias(
  self : ClientSession,
  request_id : UInt64,
  request : ClientRequest,
  bytes : Bytes,
) -> Unit {
  if bytes.length() > 65546 - request.response_buffer.length() {
    request.response_buffer = b""
    return
  }
  request.response_buffer = request.response_buffer + bytes
  match
    @protocol.decode_control_with_context(
      request.response_buffer,
      @protocol.RequestStream,
    ) {
    @protocol.Incomplete => ()
    @protocol.Invalid(_) => request.response_buffer = b""
    @protocol.Complete(@protocol.SubscribeOk(response), _) => {
      if self.aliases.get(response.track_alias) is None {
        self.aliases[response.track_alias] = request_id
        request.track_alias = Some(response.track_alias)
      }
      request.response_buffer = b""
    }
    @protocol.Complete(_, _) => request.response_buffer = b""
  }
}

///|
fn ClientSession::receive_publish_done(
  self : ClientSession,
  request_id : UInt64,
  message : @protocol.PublishDoneMessage,
) -> Result[Array[SessionAction], ClientSessionError] {
  let request = self.requests.get(request_id).unwrap()
  if !request.is_accepted() ||
    request.is_terminal() ||
    !request.object_drain.can_begin(message) ||
    (request.update_failed && message.code != 8UL) ||
    (!request.update_failed && message.code == 8UL) {
    return Err(UnexpectedMessage)
  }
  request.mark_terminal()
  request.pending_local_update = None
  request.object_drain.begin(message)
  let fin_actions = match request.stream {
    Some(stream) => self.request_local_fin_actions(stream)
    None => []
  }
  match self.take_ready_publish_done(request_id) {
    Some(done) => Ok(fin_actions + [EmitNotice(PublishDone(request_id, done))])
    None => Ok(fin_actions)
  }
}

///|
/// Close the local direction of a completed request stream exactly once.
fn ClientSession::request_local_fin_actions(
  self : ClientSession,
  stream : Int64,
) -> Array[SessionAction] {
  if self.namespace_request_streams.get(stream) is Some(_) {
    let request = self.namespace_requests
      .get(self.namespace_request_streams.get(stream).unwrap())
      .unwrap()
    if request.lifetime.local_send_closed {
      return []
    }
    self.note_request_local_fin(stream)
    return [SendFin(stream, b"")]
  }
  let request_id = match self.request_streams.get(stream) {
    Some(value) => value
    None => return []
  }
  let request = self.requests.get(request_id).unwrap()
  if request.lifetime.local_send_closed {
    return []
  }
  self.note_request_local_fin(stream)
  [SendFin(stream, b"")]
}

///|
/// Decode one variable-length integer from raw Track Properties.
fn read_track_property_vi64(input : Bytes, offset : Int32) -> (UInt64, Int32)? {
  let end = if offset + 9 < input.length() {
    offset + 9
  } else {
    input.length()
  }
  match @protocol.decode_vi64(input[offset:end].to_owned()) {
    @protocol.Complete(value, consumed) => Some((value, offset + consumed))
    _ => None
  }
}

///|
/// Scan Draft 18 Track Properties, ignoring unknown properties safely.
fn track_supports_dynamic_groups(properties : Bytes) -> Bool? {
  let mut offset = 0
  let mut previous = 0UL
  let mut supported = false
  while offset < properties.length() {
    let (delta, after_delta) = match
      read_track_property_vi64(properties, offset) {
      Some(value) => value
      None => return None
    }
    if delta > 0xffffffffffffffffUL - previous {
      return None
    }
    let kind = previous + delta
    offset = after_delta
    if (kind & 1UL) == 0UL {
      let (value, next) = match read_track_property_vi64(properties, offset) {
        Some(value) => value
        None => return None
      }
      if kind == 0x30UL {
        if value > 1UL {
          return None
        }
        supported = value == 1UL
      }
      offset = next
    } else {
      let (length, after_length) = match
        read_track_property_vi64(properties, offset) {
        Some(value) => value
        None => return None
      }
      if length > 65535UL ||
        length > (properties.length() - after_length).to_uint64() {
        return None
      }
      offset = after_length + length.to_int()
    }
    previous = kind
  }
  Some(supported)
}

///|
fn ClientSession::take_ready_publish_done(
  self : ClientSession,
  request_id : UInt64,
) -> @protocol.PublishDoneMessage? {
  let request = self.requests.get(request_id).unwrap()
  match request.object_drain.take_ready_done() {
    Some(message) => {
      match request.track_alias {
        Some(track_alias_value) if self.aliases.get(track_alias_value) ==
          Some(request_id) => self.aliases.remove(track_alias_value)
        _ => ()
      }
      request.track_alias = None
      Some(message)
    }
    None => None
  }
}

///|
fn ClientSession::note_request_local_fin(
  self : ClientSession,
  stream : Int64,
) -> Unit {
  match self.inbound_subscriptions.get(stream) {
    Some(inbound) => ignore(inbound.lifetime.transition(LocalFin, true))
    None if self.namespace_request_streams.get(stream) is Some(request_id) =>
      ignore(
        self.namespace_requests.get(request_id).unwrap().lifetime.transition(
          LocalFin,
          true,
        ),
      )
    None =>
      match self.request_streams.get(stream) {
        Some(request_id) =>
          ignore(
            self.requests.get(request_id).unwrap().lifetime.transition(
              LocalFin,
              true,
            ),
          )
        None => ()
      }
  }
}

///|
/// Start a local PUBLISH request. IDs are shared with SUBSCRIBE and increase by
/// two, starting at the client-reserved even ID 0.
pub fn ClientSession::publish_track(
  self : ClientSession,
  track : @protocol.TrackName,
  track_alias : UInt64,
) -> Result[(UInt64, Array[SessionAction]), ClientSessionError] {
  match self.ensure_new_request_allowed() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if !self.ready {
    return Err(HandshakeNotComplete)
  }
  for request_id in self.published_requests {
    let request = self.requests.get(request_id).unwrap()
    if !request.is_terminal() &&
      (request.track == track || request.track_alias == Some(track_alias)) {
      return Err(AliasInUse)
    }
  }
  let request_id = self.allocate_request_id()
  self.requests[request_id] = {
    kind: Publishing,
    track,
    track_alias: Some(track_alias),
    stream: None,
    response_buffer: b"",
    state: RequestPending,
    dynamic_groups: false,
    lifetime: RequestLifetime::new(),
    object_drain: {
      observed_streams: 0UL,
      active_streams: 0UL,
      pending_done: None,
    },
    parameters: [],
    joining_location: None,
    pending_local_update: None,
    update_failed: false,
  }
  self.published_requests.push(request_id)
  Ok(
    (
      request_id,
      self.finish_actions([OpenStream(Bidirectional, Request(request_id))]),
    ),
  )
}

///|
/// Start a local unfiltered SUBSCRIBE request.
pub fn ClientSession::subscribe_track(
  self : ClientSession,
  track : @protocol.TrackName,
) -> Result[(UInt64, Array[SessionAction]), ClientSessionError] {
  self.subscribe_track_with_parameters(track, [])
}

///|
/// Start a local SUBSCRIBE request with initial parameters.
pub fn ClientSession::subscribe_track_with_parameters(
  self : ClientSession,
  track : @protocol.TrackName,
  parameters : Array[@protocol.MessageParameter],
) -> Result[(UInt64, Array[SessionAction]), ClientSessionError] {
  match self.start_subscription_request(track, parameters) {
    Ok((request_id, actions)) => {
      self.remember_desired_subscription(track, parameters)
      Ok((request_id, actions))
    }
    Err(error) => Err(error)
  }
}

///|
fn ClientSession::start_subscription_request(
  self : ClientSession,
  track : @protocol.TrackName,
  parameters : Array[@protocol.MessageParameter],
) -> Result[(UInt64, Array[SessionAction]), ClientSessionError] {
  match self.ensure_new_request_allowed() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if !self.ready {
    return Err(HandshakeNotComplete)
  }
  let request_id = self.allocate_request_id()
  self.requests[request_id] = {
    kind: Subscribing,
    track,
    track_alias: None,
    stream: None,
    response_buffer: b"",
    state: RequestPending,
    dynamic_groups: false,
    lifetime: RequestLifetime::new(),
    object_drain: {
      observed_streams: 0UL,
      active_streams: 0UL,
      pending_done: None,
    },
    parameters,
    joining_location: None,
    pending_local_update: None,
    update_failed: false,
  }
  Ok(
    (
      request_id,
      self.finish_actions([OpenStream(Bidirectional, Request(request_id))]),
    ),
  )
}

///|
/// Finish sending updates on a local SUBSCRIBE request stream.
/// The response direction remains open for SUBSCRIBE_OK, Objects, and
/// PUBLISH_DONE. Calling this more than once has no effect.
pub fn ClientSession::finish_subscription_updates(
  self : ClientSession,
  request_id : UInt64,
) -> Result[Array[SessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let request = match self.requests.get(request_id) {
    Some(value) => value
    None => return Err(UnknownRequest)
  }
  if request.kind != Subscribing {
    return Err(SubscriptionNotAccepted)
  }
  if request.lifetime.local_send_closed {
    return Ok([])
  }
  if request.is_terminal() {
    return Err(SubscriptionNotAccepted)
  }
  let stream = match request.stream {
    Some(value) => value
    None => return Err(SubscriptionNotAccepted)
  }
  Ok(self.finish_actions(self.request_local_fin_actions(stream)))
}

///|
/// Cancel a local PUBLISH/SUBSCRIBE, namespace subscription, or peer SUBSCRIBE
/// by Request ID. Namespace subscriptions use request-side FIN as cancellation;
/// track subscriptions may use FIN only to finish their update direction.
/// Repeating cancellation after termination is a no-op.
pub fn ClientSession::cancel_request(
  self : ClientSession,
  request_id : UInt64,
) -> Result[Array[SessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  match self.fetches.get(request_id) {
    Some(fetch) if fetch.locally_initiated =>
      return self.cancel_fetch(request_id)
    _ => ()
  }
  match self.requests.get(request_id) {
    Some(request) => {
      if request.kind == Subscribing {
        self.forget_desired_subscription(request.track, request.parameters)
      }
      let publisher_pending_publish_done = match request.stream {
        Some(stream) => self.has_pending_publish_done(stream)
        None => false
      }
      if request.lifetime.cancelled ||
        (request.is_fully_terminal() && !publisher_pending_publish_done) {
        return Ok([])
      }
      match request.stream {
        Some(stream) =>
          Ok(self.finish_actions(self.end_request_stream(stream, Cancel(1UL))))
        None => {
          request.mark_terminal()
          ignore(request.lifetime.transition(Cancel(1UL), false))
          request.response_buffer = b""
          request.pending_local_update = None
          Ok(self.finish_actions([]))
        }
      }
    }
    None if self.namespace_requests.get(request_id) is Some(request) => {
      if request.lifetime.cancelled || request.state == NamespaceTerminal {
        return Ok([])
      }
      match request.stream {
        Some(stream) =>
          Ok(
            self.finish_actions(self.end_namespace_request(stream, Cancel(1UL))),
          )
        None => {
          request.state = NamespaceTerminal
          ignore(request.lifetime.transition(Cancel(1UL), false))
          Ok(self.finish_actions([]))
        }
      }
    }
    None if self.namespace_subscriptions.get(request_id) is Some(_) =>
      self.cancel_namespace_subscription(request_id)
    None =>
      match self.peer_request_ids.get(request_id) {
        Some(stream) => {
          let actions = if self.inbound_namespace_subscriptions.get(stream)
            is Some(_) {
            self.end_inbound_namespace_subscription(stream, Cancel(1UL))
          } else {
            self.end_request_stream(stream, Cancel(1UL))
          }
          Ok(self.finish_actions(actions))
        }
        None => Err(UnknownRequest)
      }
  }
}

///|
fn ClientSession::end_request_stream(
  self : ClientSession,
  stream : Int64,
  event : RequestLifetimeEvent,
) -> Array[SessionAction] {
  self.request_goaway_deadlines.remove(stream)
  match self.fetch_streams.get(stream) {
    Some(request_id) => {
      let fetch = self.fetches.get(request_id).unwrap()
      let effects = fetch.lifetime.transition(event, true)
      if !effects.abort_send && !effects.stop_receiving && !effects.notify_end {
        return []
      }
      let code = fetch.lifetime.cancel_error_code.unwrap()
      let actions : Array[SessionAction] = []
      if effects.abort_send {
        actions.push(AbortSend(stream, code))
      }
      if effects.stop_receiving {
        actions.push(StopReceiving(stream, code))
      }
      return self.terminate_fetch(
        fetch,
        actions,
        Some(EmitNotice(FetchCancelled(request_id, code))),
        if fetch.locally_initiated {
          StopFetchData(code)
        } else {
          AbortFetchData(code)
        },
      )
    }
    None => ()
  }
  match self.inbound_subscriptions.get(stream) {
    Some(_) => return self.end_inbound_subscription(stream, event)
    None => ()
  }
  if self.inbound_namespace_subscriptions.get(stream) is Some(_) {
    return self.end_inbound_namespace_subscription(stream, event)
  }
  if self.namespace_subscription_streams.get(stream) is Some(_) {
    return self.end_namespace_subscription(stream, event)
  }
  if self.namespace_request_streams.get(stream) is Some(_) {
    return self.end_namespace_request(stream, event)
  }
  let request_id = match self.request_streams.get(stream) {
    Some(value) => value
    None => return []
  }
  let request = self.requests.get(request_id).unwrap()
  let publisher_pending_publish_done = self.has_pending_publish_done(stream)
  if request.is_fully_terminal() &&
    !publisher_pending_publish_done &&
    !request.lifetime.cancelled {
    return []
  }
  let effects = request.lifetime.transition(event, true)
  if !effects.abort_send && !effects.stop_receiving && !effects.notify_end {
    return []
  }
  let error_code = request.lifetime.cancel_error_code.unwrap()
  request.mark_terminal()
  if request.kind != Subscribing ||
    request.is_accepted() ||
    request.lifetime.peer_send_closed {
    request.response_buffer = b""
  }
  request.pending_local_update = None
  request.object_drain.cancel()

  let actions : Array[SessionAction] = if request.kind == Publishing {
    self.fail_pending_joining_fetches(
      request_id, request_error_invalid_joining_request_id,
    )
  } else {
    []
  }
  if effects.abort_send {
    actions.push(AbortSend(stream, error_code))
  }
  if effects.stop_receiving {
    actions.push(StopReceiving(stream, error_code))
  }
  for action in self.cancel_outgoing_object_streams(stream, error_code) {
    actions.push(action)
  }
  if request.kind == Subscribing {
    for action in self.cancel_incoming_object_streams(request_id, error_code) {
      actions.push(action)
    }
  }
  if request.kind == Publishing {
    let child_streams : Array[Int64] = []
    for (child_stream, inbound) in self.inbound_subscriptions {
      if inbound.publisher_request_id == request_id &&
        (inbound.state != Ended || self.has_pending_publish_done(child_stream)) {
        child_streams.push(child_stream)
      }
    }
    for child_stream in child_streams {
      for
        action in self.end_inbound_subscription(
          child_stream,
          Cancel(error_code),
        ) {
        actions.push(action)
      }
    }
  }
  if effects.notify_end {
    actions.push(
      EmitNotice(SubscriptionEnded(request_id, stream, request.track)),
    )
  }
  actions
}

///|
fn ClientSession::cancel_outgoing_object_streams(
  self : ClientSession,
  subscription_stream : Int64,
  error_code : UInt64,
) -> Array[SessionAction] {
  match self.published_subscriptions.get(subscription_stream) {
    Some(subscription) => {
      subscription.pending_updates = []
      subscription.pending_failure = None
    }
    None => ()
  }
  for (_, pending) in self.pending_object_streams {
    if pending.subscription_stream == subscription_stream {
      pending.cancelled_error_code = Some(error_code)
    }
  }
  let object_streams : Array[Int64] = []
  for (object_stream, outgoing) in self.outgoing_object_streams {
    if outgoing.subscription_stream == subscription_stream {
      object_streams.push(object_stream)
    }
  }
  let actions : Array[SessionAction] = []
  for object_stream in object_streams {
    match self.outgoing_object_streams.get(object_stream) {
      Some(outgoing) if !outgoing.abort_sent => {
        outgoing.abort_sent = true
        actions.push(AbortSend(object_stream, error_code))
      }
      _ => ()
    }
    self.close_outgoing_object_stream(object_stream)
  }
  self.published_subscriptions.remove(subscription_stream)
  actions
}

///|
fn ClientSession::has_pending_publish_done(
  self : ClientSession,
  subscription_stream : Int64,
) -> Bool {
  match self.published_subscriptions.get(subscription_stream) {
    Some(subscription) => subscription.pending_failure is Some(_)
    None => false
  }
}

///|
fn ClientSession::cancel_incoming_object_streams(
  self : ClientSession,
  request_id : UInt64,
  error_code : UInt64,
) -> Array[SessionAction] {
  let object_streams : Array[Int64] = []
  for (object_stream, state) in self.incoming_object_streams {
    if state.request_id == Some(request_id) {
      object_streams.push(object_stream)
    }
  }
  let actions : Array[SessionAction] = []
  for object_stream in object_streams {
    self.incoming_object_streams.remove(object_stream)
    self.ignored_object_streams[object_stream] = {
      request_id,
      counted_active: true,
    }
    actions.push(StopReceiving(object_stream, error_code))
  }
  actions
}
