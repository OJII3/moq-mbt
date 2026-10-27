///|
/// Bound how long a joining request can retain state while its subscription is
/// pending or has an unresolved REQUEST_UPDATE.
let pending_joining_fetch_timeout_ms : UInt64 = 30000UL

///|
let request_error_timeout : UInt64 = 2UL

///|
let request_error_invalid_range : UInt64 = 0x10UL

///|
let request_error_invalid_joining_request_id : UInt64 = 0x32UL

///|
fn new_client_fetch(
  request_id : UInt64,
  track : @protocol.TrackName,
  start : @protocol.FetchLocation,
  end : @protocol.FetchLocation,
  locally_initiated : Bool,
  stream : Int64?,
  parameters : Array[@protocol.MessageParameter],
) -> ClientFetch {
  {
    request_id,
    track,
    start,
    end,
    locally_initiated,
    stream,
    response_buffer: b"",
    state: FetchPending,
    lifetime: RequestLifetime::new(),
    parameters,
    end_location: None,
    end_of_track: false,
    data_stream: None,
    data_finished: false,
    terminal_notified: false,
    pending_update_id: None,
    pending_update_parameters: [],
    joining_start: None,
    joining_relative: false,
    pending_deadline: None,
    outgoing_fetch_type: None,
  }
}

///|
fn ClientSession::terminate_fetch(
  self : ClientSession,
  fetch : ClientFetch,
  actions : Array[SessionAction],
  notification : SessionAction?,
  data_disposition : FetchDataDisposition,
) -> Array[SessionAction] {
  fetch.state = FetchTerminal
  let should_notify = !fetch.terminal_notified
  fetch.terminal_notified = true
  fetch.pending_deadline = None
  self.remove_pending_joining_fetch(fetch.request_id)
  match fetch.data_stream {
    Some(stream) =>
      match data_disposition {
        KeepFetchData => ()
        StopFetchData(code) => {
          actions.push(StopReceiving(stream, code))
          self.clear_fetch_data_stream(fetch, stream, true)
        }
        AbortFetchData(code) => {
          actions.push(AbortSend(stream, code))
          self.clear_fetch_data_stream(fetch, stream, false)
        }
        RemoveFetchData => self.clear_fetch_data_stream(fetch, stream, false)
      }
    None => ()
  }
  match notification {
    Some(action) if should_notify => actions.push(action)
    Some(_) | None => ()
  }
  actions
}

///|
/// Start a FETCH relative to a local SUBSCRIBE request. `groups_back` counts
/// backward from the subscription's Joining Location group. The local API
/// takes SUBSCRIBE IDs; a peer FETCH can also reference this endpoint's local
/// PUBLISH ID when that PUBLISH established the subscription.
pub fn ClientSession::fetch_relative(
  self : ClientSession,
  subscription_request_id : UInt64,
  groups_back : UInt64,
  parameters : Array[@protocol.MessageParameter],
) -> Result[(UInt64, Array[SessionAction]), ClientSessionError] {
  self.send_joining_fetch(
    subscription_request_id, groups_back, true, parameters,
  )
}

///|
/// Start a FETCH from an absolute group through the subscription's Joining
/// Location group.
pub fn ClientSession::fetch_absolute(
  self : ClientSession,
  subscription_request_id : UInt64,
  start_group : UInt64,
  parameters : Array[@protocol.MessageParameter],
) -> Result[(UInt64, Array[SessionAction]), ClientSessionError] {
  self.send_joining_fetch(
    subscription_request_id, start_group, false, parameters,
  )
}

///|
fn ClientSession::send_joining_fetch(
  self : ClientSession,
  subscription_request_id : UInt64,
  joining_start : UInt64,
  relative : Bool,
  parameters : Array[@protocol.MessageParameter],
) -> Result[(UInt64, Array[SessionAction]), ClientSessionError] {
  match self.ensure_new_request_allowed() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if !self.ready {
    return Err(HandshakeNotComplete)
  }
  let subscription = match self.requests.get(subscription_request_id) {
    Some(value) if value.kind == Subscribing &&
      !value.is_terminal() &&
      !value.lifetime.cancelled => value
    _ => return Err(UnknownRequest)
  }
  let current_forward = subscription_forward(subscription.parameters)
  let pending_forward = match subscription.pending_local_update {
    Some(update) => subscription_forward(update.effective_parameters)
    None => false
  }
  if subscription.is_accepted() && !current_forward && !pending_forward {
    return Err(InvalidFetchRange)
  }
  match subscription.joining_location {
    Some(location) if joining_start > location.group =>
      return Err(InvalidFetchRange)
    _ => ()
  }
  if !valid_fetch_group_order(parameters) {
    return Err(InvalidFetchRange)
  }
  let fetch_type = if relative {
    @protocol.RelativeJoiningFetch(subscription_request_id, joining_start)
  } else {
    @protocol.AbsoluteJoiningFetch(subscription_request_id, joining_start)
  }
  match
    @protocol.encode_fetch({
      request_id: self.next_request_id,
      fetch_type,
      parameters,
    }) {
    Ok(_) => ()
    Err(error) => return Err(Encoding(error))
  }
  let request_id = self.allocate_request_id()
  let fetch = new_client_fetch(
    request_id,
    subscription.track,
    { group: 0UL, object: 0UL, },
    { group: 0xffffffffffffffffUL, object: 0UL, },
    true,
    None,
    parameters,
  )
  fetch.joining_start = Some(joining_start)
  fetch.joining_relative = relative
  fetch.outgoing_fetch_type = Some(fetch_type)
  self.fetches[request_id] = fetch
  Ok(
    (
      request_id,
      self.finish_actions([OpenStream(Bidirectional, Request(request_id))]),
    ),
  )
}

///|
fn ClientSession::receive_joining_fetch(
  self : ClientSession,
  stream : Int64,
  request_id : UInt64,
  joining_request_id : UInt64,
  joining_start : UInt64,
  relative : Bool,
  parameters : Array[@protocol.MessageParameter],
) -> Result[Array[SessionAction], ClientSessionError] {
  let (subscription_stream, subscription_pending, publisher_request_id) = match
    self.peer_request_ids.get(joining_request_id) {
    Some(value) if self.inbound_subscriptions.get(value) is Some(inbound) &&
      inbound.request_id == joining_request_id =>
      (value, inbound.state == Pending, inbound.publisher_request_id)
    _ =>
      match self.requests.get(joining_request_id) {
        Some(request) if request.kind == Publishing && !request.is_terminal() =>
          match request.stream {
            Some(value) => (value, !request.is_accepted(), joining_request_id)
            None => {
              let error = joining_fetch_error(
                request_error_invalid_joining_request_id,
              )
              let bytes = match @protocol.encode_request_error(error) {
                Ok(value) => value
                Err(codec_error) => return Err(Encoding(codec_error))
              }
              return Ok([
                SendFin(stream, bytes),
                EmitNotice(FetchRejected(request_id, stream, error)),
              ])
            }
          }
        _ => {
          let error = joining_fetch_error(
            request_error_invalid_joining_request_id,
          )
          let bytes = match @protocol.encode_request_error(error) {
            Ok(value) => value
            Err(codec_error) => return Err(Encoding(codec_error))
          }
          return Ok([
            SendFin(stream, bytes),
            EmitNotice(FetchRejected(request_id, stream, error)),
          ])
        }
      }
  }
  if self.inbound_subscriptions.get(subscription_stream) is Some(inbound) &&
    inbound.state == Ended {
    let error = joining_fetch_error(request_error_invalid_joining_request_id)
    let bytes = match @protocol.encode_request_error(error) {
      Ok(value) => value
      Err(codec_error) => return Err(Encoding(codec_error))
    }
    return Ok([
      SendFin(stream, bytes),
      EmitNotice(FetchRejected(request_id, stream, error)),
    ])
  }
  let publisher = self.requests.get(publisher_request_id).unwrap()
  if self.control_goaway_sent {
    let error = joining_fetch_error(request_error_going_away)
    let bytes = match @protocol.encode_request_error(error) {
      Ok(value) => value
      Err(codec_error) => return Err(Encoding(codec_error))
    }
    return Ok([
      SendFin(stream, bytes),
      EmitNotice(FetchRejected(request_id, stream, error)),
    ])
  }
  let fetch = new_client_fetch(
    request_id,
    publisher.track,
    { group: 0UL, object: 0UL, },
    { group: 0UL, object: 0UL, },
    false,
    Some(stream),
    parameters,
  )
  fetch.joining_start = Some(joining_start)
  fetch.joining_relative = relative
  fetch.pending_deadline = Some(self.now_ms + pending_joining_fetch_timeout_ms)
  self.fetches[request_id] = fetch
  self.fetch_streams[stream] = request_id
  if subscription_pending {
    let waiting = match self.pending_joining_fetches.get(joining_request_id) {
      Some(values) => values
      None => []
    }
    waiting.push(request_id)
    self.pending_joining_fetches[joining_request_id] = waiting
    return Ok([])
  }
  let subscription = self.published_subscriptions
    .get(subscription_stream)
    .unwrap()
  if !subscription.pending_updates.is_empty() {
    let waiting = match self.pending_joining_fetches.get(joining_request_id) {
      Some(values) => values
      None => []
    }
    waiting.push(request_id)
    self.pending_joining_fetches[joining_request_id] = waiting
    return Ok([])
  }
  Ok(self.resolve_joining_fetch(request_id, subscription_stream))
}

///|
fn joining_fetch_error(code : UInt64) -> @protocol.RequestErrorMessage {
  { code, retry_interval: 0UL, reason: b"", redirect: b"", }
}

///|
fn ClientSession::resolve_joining_fetch(
  self : ClientSession,
  request_id : UInt64,
  subscription_stream : Int64,
) -> Array[SessionAction] {
  let fetch = self.fetches.get(request_id).unwrap()
  let subscription = self.published_subscriptions
    .get(subscription_stream)
    .unwrap()
  let stream = fetch.stream.unwrap()
  let fail = fn(code) {
    let error = joining_fetch_error(code)
    let bytes = @protocol.encode_request_error(error).unwrap()
    self.terminate_fetch(
      fetch,
      [SendFin(stream, bytes)],
      Some(EmitNotice(FetchRejected(request_id, stream, error))),
      RemoveFetchData,
    )
  }
  if !subscription_forward(subscription.parameters) {
    return fail(request_error_invalid_range)
  }
  let joining_location = match subscription.joining_location {
    Some(value) => value
    None => return fail(request_error_invalid_range)
  }
  let joining_start = fetch.joining_start.unwrap()
  let start_group = if fetch.joining_relative {
    if joining_start > joining_location.group {
      return fail(request_error_invalid_range)
    }
    joining_location.group - joining_start
  } else {
    joining_start
  }
  if start_group > joining_location.group {
    return fail(request_error_invalid_range)
  }
  let start : @protocol.FetchLocation = { group: start_group, object: 0UL, }
  let end : @protocol.FetchLocation = {
    group: joining_location.group,
    object: if joining_location.object == 0xffffffffffffffffUL {
      0UL
    } else {
      joining_location.object + 1UL
    },
  }
  fetch.start = start
  fetch.end = end
  fetch.pending_deadline = None
  [
    EmitNotice(
      FetchRequested(
        request_id,
        stream,
        fetch.track,
        start,
        end,
        fetch.parameters,
      ),
    ),
  ]
}

///|
fn ClientSession::drain_joining_fetches(
  self : ClientSession,
  joining_request_id : UInt64,
  subscription_stream : Int64,
) -> Array[SessionAction] {
  let waiting = match self.pending_joining_fetches.get(joining_request_id) {
    Some(values) => values
    None => return []
  }
  let remaining : Array[UInt64] = []
  let actions : Array[SessionAction] = []
  for request_id in waiting {
    let fetch = match self.fetches.get(request_id) {
      Some(value) if value.state == FetchPending => value
      _ => continue
    }
    if self.published_subscriptions.get(subscription_stream).unwrap().pending_updates.is_empty() {
      for action in self.resolve_joining_fetch(request_id, subscription_stream) {
        actions.push(action)
      }
    } else {
      remaining.push(request_id)
    }
    ignore(fetch)
  }
  if remaining.is_empty() {
    self.pending_joining_fetches.remove(joining_request_id)
  } else {
    self.pending_joining_fetches[joining_request_id] = remaining
  }
  actions
}

///|
fn ClientSession::fail_pending_joining_fetches(
  self : ClientSession,
  joining_request_id : UInt64,
  code : UInt64,
) -> Array[SessionAction] {
  let waiting = match self.pending_joining_fetches.get(joining_request_id) {
    Some(values) => values
    None => return []
  }
  self.pending_joining_fetches.remove(joining_request_id)
  let actions : Array[SessionAction] = []
  for request_id in waiting {
    match self.fetches.get(request_id) {
      Some(fetch) if fetch.state == FetchPending => {
        let stream = fetch.stream.unwrap()
        let error = joining_fetch_error(code)
        let bytes = @protocol.encode_request_error(error).unwrap()
        for
          action in self.terminate_fetch(
            fetch,
            [SendFin(stream, bytes)],
            Some(EmitNotice(FetchRejected(request_id, stream, error))),
            RemoveFetchData,
          ) {
          actions.push(action)
        }
      }
      _ => ()
    }
  }
  actions
}

///|
fn ClientSession::remove_pending_joining_fetch(
  self : ClientSession,
  request_id : UInt64,
) -> Unit {
  let joining_ids : Array[UInt64] = []
  for (joining_id, _) in self.pending_joining_fetches {
    joining_ids.push(joining_id)
  }
  for joining_id in joining_ids {
    let waiting = self.pending_joining_fetches.get(joining_id).unwrap()
    let remaining : Array[UInt64] = []
    for waiting_id in waiting {
      if waiting_id != request_id {
        remaining.push(waiting_id)
      }
    }
    if remaining.is_empty() {
      self.pending_joining_fetches.remove(joining_id)
    } else if remaining.length() != waiting.length() {
      self.pending_joining_fetches[joining_id] = remaining
    }
  }
}

///|
fn ClientSession::expire_pending_joining_fetches(
  self : ClientSession,
) -> Array[SessionAction] {
  let expired : Array[UInt64] = []
  for (request_id, fetch) in self.fetches {
    match fetch.pending_deadline {
      Some(deadline) if fetch.state == FetchPending && self.now_ms >= deadline =>
        expired.push(request_id)
      _ => ()
    }
  }
  let actions : Array[SessionAction] = []
  for request_id in expired {
    let fetch = self.fetches.get(request_id).unwrap()
    let stream = fetch.stream.unwrap()
    let error = joining_fetch_error(request_error_timeout)
    let bytes = @protocol.encode_request_error(error).unwrap()
    self.remove_pending_joining_fetch(request_id)
    for
      action in self.terminate_fetch(
        fetch,
        [SendFin(stream, bytes)],
        Some(EmitNotice(FetchRejected(request_id, stream, error))),
        RemoveFetchData,
      ) {
      actions.push(action)
    }
  }
  actions
}
