///|
priv struct PendingObjectStream {
  subscription_stream : Int64
  header : @protocol.SubgroupHeader
  mut cancelled_error_code : UInt64?
  mut finish_requested : Bool
  frames : Array[@protocol.SubgroupFrame]
}

///|
priv struct PublishedSubscription {
  mut opened_stream_count : UInt64
  mut open_stream_count : UInt64
  mut parameters : Array[@protocol.MessageParameter]
  mut pending_updates : Array[PendingSubscriptionUpdate]
  mut pending_failure : @protocol.PublishDoneMessage?
  mut resolved_filter : ResolvedSubscriptionFilter?
  request_id : UInt64
  mut joining_location : @protocol.FetchLocation?
}

///|
priv struct ResolvedSubscriptionFilter {
  start_group : UInt64
  start_object : UInt64
  end_group : UInt64?
  impossible : Bool
}

///|
priv struct OutgoingObjectStream {
  subscription_stream : Int64
  header : @protocol.SubgroupHeader
  token : UInt64
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
fn ClientSession::has_pending_object_streams(
  self : ClientSession,
  subscription_stream : Int64,
) -> Bool {
  for (_, pending) in self.pending_object_streams {
    if pending.subscription_stream == subscription_stream {
      return true
    }
  }
  false
}

///|
fn subscription_allows_location(
  filter : ResolvedSubscriptionFilter?,
  group_id : UInt64,
  object_id : UInt64,
) -> Bool {
  match filter {
    None => true
    Some(window) => {
      if window.impossible ||
        group_id < window.start_group ||
        (group_id == window.start_group && object_id < window.start_object) {
        return false
      }
      match window.end_group {
        Some(end_group) if group_id > end_group => false
        _ => true
      }
    }
  }
}

///|
fn resolve_subscription_filter(
  parameters : Array[@protocol.MessageParameter],
  largest : (UInt64, UInt64)?,
) -> ResolvedSubscriptionFilter? {
  for parameter in parameters {
    if parameter.kind == 0x21 {
      match parameter.value {
        @protocol.Filter(@protocol.AbsoluteStart(group_id, object_id)) =>
          return Some({
            start_group: group_id,
            start_object: object_id,
            end_group: None,
            impossible: false,
          })
        @protocol.Filter(
          @protocol.AbsoluteRange(group_id, object_id, end_group)
        ) =>
          return Some({
            start_group: group_id,
            start_object: object_id,
            end_group: Some(end_group),
            impossible: end_group < group_id,
          })
        @protocol.Filter(@protocol.LargestObject) => {
          let (start_group, start_object, impossible) = match largest {
            None => (0UL, 0UL, false)
            Some((group_id, object_id)) if object_id < 0xffffffffffffffffUL =>
              (group_id, object_id + 1UL, false)
            Some((group_id, _)) if group_id < 0xffffffffffffffffUL =>
              (group_id + 1UL, 0UL, false)
            Some(_) => (0UL, 0UL, true)
          }
          return Some({
            start_group,
            start_object,
            end_group: None,
            impossible,
          })
        }
        @protocol.Filter(@protocol.NextGroupStart) => {
          let (start_group, impossible) = match largest {
            None => (0UL, false)
            Some((group_id, _)) if group_id < 0xffffffffffffffffUL =>
              (group_id + 1UL, false)
            Some(_) => (0UL, true)
          }
          return Some({
            start_group,
            start_object: 0UL,
            end_group: None,
            impossible,
          })
        }
        _ => return None
      }
    }
  }
  None
}

///|
fn subscription_filter_parameter(
  parameters : Array[@protocol.MessageParameter],
) -> @protocol.MessageParameter? {
  for parameter in parameters {
    if parameter.kind == 0x21 {
      return Some(parameter)
    }
  }
  None
}

///|
fn subscription_largest_location(
  parameters : Array[@protocol.MessageParameter],
) -> (UInt64, UInt64)? {
  for parameter in parameters {
    if parameter.kind == 0x09 {
      match parameter.value {
        @protocol.Location(group_id, object_id) =>
          return Some((group_id, object_id))
        _ => ()
      }
    }
  }
  None
}

///|
fn subscription_has_relative_filter(
  parameters : Array[@protocol.MessageParameter],
) -> Bool {
  match subscription_filter_parameter(parameters) {
    Some({ value: @protocol.Filter(@protocol.LargestObject), .. }) => true
    Some({ value: @protocol.Filter(@protocol.NextGroupStart), .. }) => true
    _ => false
  }
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
      self.group_streams.remove(outgoing.token)
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
          if !request.object_drain.can_observe_stream() {
            return Err(UnexpectedMessage)
          }
          state.request_id = Some(request_id)
          state.header = Some(header)
          state.decoder = Some(@protocol.SubgroupDecoder::new(header))
          state.buffer = state.buffer[consumed:].to_owned()
          request.object_drain.note_stream_open()
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
    if request.can_receive_object_data() {
      Ok([Notify(ObjectReceived(request_id, request.track, header, frames))])
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
  if !request.can_receive_object_data() {
    return Ok([])
  }
  Ok([Notify(ObjectDatagramReceived(request_id, request.track, datagram))])
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
    finish_requested: false,
    frames: [],
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
  if !subscription_allows_location(
      self.published_subscriptions.get(outgoing.subscription_stream).unwrap().resolved_filter,
      outgoing.header.group_id,
      frame.object_id,
    ) {
    return Err(SubscriptionNotAccepted)
  }
  let bytes = match encode_outgoing_object(outgoing, frame) {
    Ok(value) => value
    Err(error) => return Err(Track(error))
  }
  let publisher_request_id = match
    self.inbound_subscriptions.get(outgoing.subscription_stream) {
    Some(inbound) => inbound.publisher_request_id
    None => self.request_streams.get(outgoing.subscription_stream).unwrap()
  }
  self.note_published_location(
    publisher_request_id,
    outgoing.header.group_id,
    frame.object_id,
  )
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
  let publisher = self.requests.get(publisher_request_id).unwrap()
  if datagram.track_alias != publisher.track_alias.unwrap() {
    return Err(Track(AliasMismatch))
  }
  let subscription = self.published_subscriptions
    .get(subscription_stream)
    .unwrap()
  if !subscription_allows_location(
      subscription.resolved_filter,
      datagram.group_id,
      datagram.object_id,
    ) {
    return Err(SubscriptionNotAccepted)
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
  self.note_published_location(
    publisher_request_id,
    datagram.group_id,
    datagram.object_id,
  )
  Ok(self.finish_actions([SendDatagram(bytes)]))
}

///|
/// Send one Object Datagram to every active forwarding subscription of a
/// published track. The Track Alias is rewritten for each subscription; a
/// subscription that does not forward or whose filter excludes the Object is
/// skipped. Returns `Ok([])` when every subscription is skipped.
pub fn ClientSession::send_object_datagram_for_track(
  self : ClientSession,
  track : @protocol.TrackName,
  datagram : @protocol.ObjectDatagram,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.ensure_open() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let mut publisher_request_id : UInt64? = None
  for (request_id, request) in self.requests {
    if request.kind == Publishing &&
      request.is_accepted() &&
      !request.is_terminal() &&
      request.track == track {
      publisher_request_id = Some(request_id)
    }
  }
  let publisher_request_id = match publisher_request_id {
    Some(value) => value
    None => return Err(UnknownRequest)
  }
  let publisher = self.requests.get(publisher_request_id).unwrap()
  let base_alias = match publisher.track_alias {
    Some(value) => value
    None => return Err(UnknownRequest)
  }
  if !self.datagram_send_enabled {
    return Err(DatagramSendUnavailable)
  }
  let probe = match
    @protocol.encode_object_datagram({ ..datagram, track_alias: base_alias, }) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  if probe.length() > self.max_datagram_length {
    return Err(DatagramTooLarge(probe.length(), self.max_datagram_length))
  }
  let actions : Array[ClientSessionAction] = []
  for (subscription_stream, subscription) in self.published_subscriptions {
    // The local PUBLISH request also has a published_subscriptions entry; it
    // is the publisher's own state, not a peer subscription.
    let inbound = match self.inbound_subscriptions.get(subscription_stream) {
      Some(value) => value
      None => continue
    }
    if inbound.publisher_request_id != publisher_request_id ||
      !self.subscription_is_active(subscription_stream) ||
      !subscription_forward(subscription.parameters) ||
      !subscription_allows_location(
        subscription.resolved_filter,
        datagram.group_id,
        datagram.object_id,
      ) {
      continue
    }
    let track_alias = match
      self.subscription_publisher_alias(subscription_stream) {
      Some(value) => value
      None => base_alias
    }
    let bytes = match
      @protocol.encode_object_datagram({ ..datagram, track_alias, }) {
      Ok(value) => value
      Err(error) => return Err(Encoding(error))
    }
    if bytes.length() > self.max_datagram_length {
      return Err(DatagramTooLarge(bytes.length(), self.max_datagram_length))
    }
    actions.push(SendDatagram(bytes))
  }
  self.note_published_location(
    publisher_request_id,
    datagram.group_id,
    datagram.object_id,
  )
  Ok(self.finish_actions(actions))
}

///|
fn ClientSession::subscription_publisher_alias(
  self : ClientSession,
  subscription_stream : Int64,
) -> UInt64? {
  let publisher_request_id = match
    self.publisher_request_for_subscription(subscription_stream) {
    Some(value) => value
    None => return None
  }
  match self.requests.get(publisher_request_id) {
    Some(request) => request.track_alias
    None => None
  }
}

///|
fn ClientSession::note_published_location(
  self : ClientSession,
  publisher_request_id : UInt64,
  group_id : UInt64,
  object_id : UInt64,
) -> Unit {
  match self.largest_published_locations.get(publisher_request_id) {
    Some((previous_group, previous_object)) if previous_group > group_id ||
      (previous_group == group_id && previous_object >= object_id) => ()
    _ =>
      self.largest_published_locations[publisher_request_id] = (
        group_id, object_id,
      )
  }
  for (stream, subscription) in self.published_subscriptions {
    let matches_publisher = match self.inbound_subscriptions.get(stream) {
      Some(inbound) => inbound.publisher_request_id == publisher_request_id
      None => subscription.request_id == publisher_request_id
    }
    if matches_publisher &&
      subscription.joining_location is None &&
      subscription_forward(subscription.parameters) {
      subscription.joining_location = Some({
        group: group_id,
        object: object_id,
      })
    }
  }
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
          if request.kind != Publishing ||
            !request.is_accepted() ||
            request.is_terminal() {
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
  if subscription.open_stream_count != 0UL ||
    self.has_pending_object_streams(subscription_stream) {
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
    request.mark_terminal()
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
