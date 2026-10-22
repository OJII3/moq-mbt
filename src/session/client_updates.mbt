///|
fn merge_subscription_parameters(
  current : Array[@protocol.MessageParameter],
  updates : Array[@protocol.MessageParameter],
) -> Array[@protocol.MessageParameter] {
  let merged : Array[@protocol.MessageParameter] = []
  let mut current_index = 0
  let mut update_index = 0
  while current_index < current.length() || update_index < updates.length() {
    if current_index == current.length() {
      merged.push(updates[update_index])
      update_index = update_index + 1
    } else if update_index == updates.length() {
      merged.push(current[current_index])
      current_index = current_index + 1
    } else {
      let current_kind = current[current_index].kind
      let update_kind = updates[update_index].kind
      if current_kind < update_kind {
        merged.push(current[current_index])
        current_index = current_index + 1
      } else if update_kind < current_kind {
        merged.push(updates[update_index])
        update_index = update_index + 1
      } else {
        while current_index < current.length() &&
              current[current_index].kind == current_kind {
          current_index = current_index + 1
        }
        while update_index < updates.length() &&
              updates[update_index].kind == update_kind {
          merged.push(updates[update_index])
          update_index = update_index + 1
        }
      }
    }
  }
  merged
}

///|
fn subscription_forward(parameters : Array[@protocol.MessageParameter]) -> Bool {
  for parameter in parameters {
    if parameter.kind == 0x10 {
      match parameter.value {
        @protocol.Byte(value) => return value != 0
        _ => ()
      }
    }
  }
  true
}

///|
fn ClientSession::receive_peer_subscription_update(
  self : ClientSession,
  stream : Int64,
  message : @protocol.RequestUpdateMessage,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  if (message.request_id & 1UL) != 1UL {
    return Err(InvalidRequestId)
  }
  if self.peer_request_ids.get(message.request_id) is Some(_) {
    return Err(DuplicateRequestId)
  }
  let request_id = match self.inbound_subscriptions.get(stream) {
    Some(inbound) if inbound.state == Active => inbound.request_id
    Some(_) => return Err(SubscriptionNotAccepted)
    None =>
      match self.request_streams.get(stream) {
        Some(request_id) => {
          let request = self.requests.get(request_id).unwrap()
          if request.kind != Publishing ||
            !request.is_accepted() ||
            request.is_terminal() {
            return Err(SubscriptionNotAccepted)
          }
          request_id
        }
        None => return Err(UnknownStream)
      }
  }
  let subscription = match self.published_subscriptions.get(stream) {
    Some(value) => value
    None => return Err(SubscriptionNotAccepted)
  }
  self.peer_request_ids[message.request_id] = stream
  self.note_peer_request_id(message.request_id)
  if self.control_goaway_sent {
    let error : @protocol.RequestErrorMessage = {
      code: request_error_going_away,
      retry_interval: 0UL,
      reason: b"",
      redirect: b"",
    }
    let bytes = match @protocol.encode_request_error(error) {
      Ok(value) => value
      Err(value) => return Err(Encoding(value))
    }
    let rejected_updates : Array[UInt64] = []
    for pending in subscription.pending_updates {
      rejected_updates.push(pending.request_id)
    }
    rejected_updates.push(message.request_id)
    subscription.pending_updates = []
    match self.inbound_subscriptions.get(stream) {
      Some(inbound) => inbound.state = Ended
      None =>
        match self.requests.get(request_id) {
          Some(request) => request.mark_terminal()
          None => ()
        }
    }
    subscription.pending_failure = Some({
      code: 8UL,
      stream_count: subscription.opened_stream_count,
      reason: b"",
    })
    let actions : Array[ClientSessionAction] = []
    for update_id in rejected_updates {
      actions.push(Send(stream, bytes, false))
      actions.push(
        Notify(SubscriptionUpdateRejected(request_id, update_id, error)),
      )
    }
    for action in self.stop_subscription_object_sends(stream, 0UL) {
      actions.push(action)
    }
    for action in self.finish_failed_subscription(stream) {
      actions.push(action)
    }
    return Ok(actions)
  }
  let previous = if subscription.pending_updates.is_empty() {
    subscription.parameters
  } else {
    subscription.pending_updates[subscription.pending_updates.length() - 1].effective_parameters
  }
  let effective_parameters = merge_subscription_parameters(
    previous,
    message.parameters,
  )
  subscription.pending_updates.push({
    request_id: message.request_id,
    effective_parameters,
    filter_updated: subscription_filter_parameter(message.parameters) is Some(_),
  })
  Ok([
    Notify(
      SubscriptionUpdateRequested(
        request_id,
        message.request_id,
        stream,
        message.parameters,
        effective_parameters,
      ),
    ),
  ])
}

///|
/// Send REQUEST_UPDATE on an accepted local SUBSCRIBE. Only one outgoing update
/// can be pending because its response carries no Request ID.
pub fn ClientSession::update_subscription(
  self : ClientSession,
  request_id : UInt64,
  parameters : Array[@protocol.MessageParameter],
) -> Result[(UInt64, Array[ClientSessionAction]), ClientSessionError] {
  match self.ensure_new_request_allowed() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let request = match self.requests.get(request_id) {
    Some(value) => value
    None => return Err(UnknownRequest)
  }
  if request.kind != Subscribing ||
    !request.is_accepted() ||
    request.lifetime.local_send_closed ||
    request.is_terminal() ||
    request.update_failed {
    return Err(SubscriptionNotAccepted)
  }
  if request.pending_local_update is Some(_) {
    return Err(RequestUpdatePending)
  }
  for parameter in parameters {
    if parameter.kind == 0x32 && !request.dynamic_groups {
      return Err(Encoding(@protocol.InvalidParameterValue(0x32)))
    }
  }
  let stream = match request.stream {
    Some(value) => value
    None => return Err(SubscriptionNotAccepted)
  }
  let update_id = self.allocate_request_id()
  let bytes = match
    @protocol.encode_request_update({ request_id: update_id, parameters, }) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  request.pending_local_update = Some({
    request_id: update_id,
    effective_parameters: merge_subscription_parameters(
      request.parameters,
      parameters,
    ),
    filter_updated: subscription_filter_parameter(parameters) is Some(_),
  })
  Ok((update_id, self.finish_actions([Send(stream, bytes, false)])))
}

///|
/// Accept the next peer update waiting on a published subscription.
pub fn ClientSession::accept_subscription_update(
  self : ClientSession,
  stream : Int64,
  update_id : UInt64,
  response : @protocol.RequestOkMessage,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let subscription = match self.published_subscriptions.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  if subscription.pending_failure is Some(_) {
    return Err(SubscriptionNotAccepted)
  }
  if subscription.pending_updates.is_empty() ||
    subscription.pending_updates[0].request_id != update_id {
    return Err(RequestUpdatePending)
  }
  let pending = subscription.pending_updates[0]
  let filter_was_updated = pending.filter_updated
  let publisher_request_id = match self.inbound_subscriptions.get(stream) {
    Some(inbound) => inbound.publisher_request_id
    None => self.request_streams.get(stream).unwrap()
  }
  let response_largest = match
    subscription_largest_location(response.parameters) {
    Some(value) => Some(value)
    None => self.largest_published_locations.get(publisher_request_id)
  }
  let mut response_parameters = response.parameters
  let was_forwarding = subscription_forward(subscription.parameters)
  let forwarding = subscription_forward(pending.effective_parameters)
  if !was_forwarding &&
    forwarding &&
    subscription_largest_location(response.parameters) is None {
    match response_largest {
      Some((group_id, object_id)) =>
        response_parameters = merge_subscription_parameters(
          response_parameters,
          [{ kind: 0x09UL, value: @protocol.Location(group_id, object_id), }],
        )
      None => ()
    }
  }
  if filter_was_updated &&
    subscription_has_relative_filter(pending.effective_parameters) &&
    subscription_largest_location(response.parameters) is None {
    match response_largest {
      Some((group_id, object_id)) =>
        response_parameters = merge_subscription_parameters(
          response_parameters,
          [{ kind: 0x09UL, value: @protocol.Location(group_id, object_id), }],
        )
      None => ()
    }
  }
  let response : @protocol.RequestOkMessage = {
    parameters: response_parameters,
    properties: response.properties,
  }
  let response_bytes = match
    @protocol.encode_request_ok_for(
      response,
      @protocol.RequestOkContext::RequestUpdate,
    ) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  ignore(subscription.pending_updates.remove(0))
  subscription.parameters = pending.effective_parameters
  if !was_forwarding && forwarding {
    subscription.joining_location = match response_largest {
      Some((group_id, object_id)) =>
        Some({ group: group_id, object: object_id, })
      None => None
    }
  }
  if filter_was_updated {
    subscription.resolved_filter = resolve_subscription_filter(
      subscription.parameters,
      response_largest,
    )
  }
  let actions : Array[ClientSessionAction] = [
    Send(stream, response_bytes, false),
  ]
  if was_forwarding && !forwarding {
    for action in self.stop_subscription_object_sends(stream, 0UL) {
      actions.push(action)
    }
  }
  let request_id = subscription.request_id
  Ok(
    self.finish_actions(
      actions +
      [
        Notify(
          SubscriptionUpdateApplied(
            request_id,
            update_id,
            stream,
            subscription.parameters,
          ),
        ),
      ] +
      self.drain_joining_fetches(subscription.request_id, stream),
    ),
  )
}

///|
/// Reject a peer update and terminate the subscription with UPDATE_FAILED.
pub fn ClientSession::reject_subscription_update(
  self : ClientSession,
  stream : Int64,
  update_id : UInt64,
  error : @protocol.RequestErrorMessage,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let subscription = match self.published_subscriptions.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  if subscription.pending_failure is Some(_) {
    return Err(SubscriptionNotAccepted)
  }
  if subscription.pending_updates.is_empty() ||
    subscription.pending_updates[0].request_id != update_id {
    return Err(RequestUpdatePending)
  }
  let error_bytes = match @protocol.encode_request_error(error) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  subscription.pending_updates = []
  match self.inbound_subscriptions.get(stream) {
    Some(inbound) => inbound.state = Ended
    None =>
      match self.requests.get(subscription.request_id) {
        Some(request) => request.mark_terminal()
        None => ()
      }
  }
  let failure : @protocol.PublishDoneMessage = {
    code: 8UL,
    stream_count: subscription.opened_stream_count,
    reason: b"",
  }
  subscription.pending_failure = Some(failure)
  let actions : Array[ClientSessionAction] = [Send(stream, error_bytes, false)] +
    self.fail_pending_joining_fetches(
      subscription.request_id,
      request_error_invalid_joining_request_id,
    )
  for action in self.stop_subscription_object_sends(stream, 0UL) {
    actions.push(action)
  }
  for action in self.finish_failed_subscription(stream) {
    actions.push(action)
  }
  Ok(self.finish_actions(actions))
}

///|
fn ClientSession::stop_subscription_object_sends(
  self : ClientSession,
  subscription_stream : Int64,
  error_code : UInt64,
) -> Array[ClientSessionAction] {
  for (_, pending) in self.pending_object_streams {
    if pending.subscription_stream == subscription_stream {
      pending.cancelled_error_code = Some(error_code)
    }
  }
  let streams : Array[Int64] = []
  for (stream, outgoing) in self.outgoing_object_streams {
    if outgoing.subscription_stream == subscription_stream {
      streams.push(stream)
    }
  }
  let actions : Array[ClientSessionAction] = []
  for stream in streams {
    match self.outgoing_object_streams.get(stream) {
      Some(outgoing) if !outgoing.abort_sent => {
        outgoing.abort_sent = true
        actions.push(AbortSend(stream, error_code))
      }
      _ => ()
    }
  }
  actions
}

///|
fn ClientSession::finish_failed_subscription(
  self : ClientSession,
  stream : Int64,
) -> Array[ClientSessionAction] {
  let subscription = match self.published_subscriptions.get(stream) {
    Some(value) => value
    None => return []
  }
  if subscription.open_stream_count != 0UL ||
    self.has_pending_object_streams(stream) {
    return []
  }
  let done : @protocol.PublishDoneMessage = match subscription.pending_failure {
    Some(value) =>
      {
        code: value.code,
        stream_count: subscription.opened_stream_count,
        reason: value.reason,
      }
    None => return []
  }
  subscription.pending_failure = None
  let bytes = match @protocol.encode_publish_done(done) {
    Ok(value) => value
    Err(_) => return []
  }
  self.note_request_local_fin(stream)
  [
    Send(stream, bytes, true),
    Notify(PublishDone(subscription.request_id, done)),
  ]
}
