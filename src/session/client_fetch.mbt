///|
priv enum ClientFetchState {
  FetchPending
  FetchAccepted
  FetchTerminal
} derive(Eq)

///|
priv struct ClientFetch {
  request_id : UInt64
  track : @protocol.TrackName
  mut start : @protocol.FetchLocation
  mut end : @protocol.FetchLocation
  locally_initiated : Bool
  mut stream : Int64?
  mut response_buffer : Bytes
  mut state : ClientFetchState
  lifetime : RequestLifetime
  mut parameters : Array[@protocol.MessageParameter]
  mut end_location : @protocol.FetchLocation?
  mut end_of_track : Bool
  mut data_stream : Int64?
  mut data_finished : Bool
  mut terminal_notified : Bool
  mut pending_update_id : UInt64?
  mut pending_update_parameters : Array[@protocol.MessageParameter]
  mut joining_start : UInt64?
  mut joining_relative : Bool
  mut pending_deadline : UInt64?
  mut outgoing_fetch_type : @protocol.FetchType?
}

///|
priv enum FetchDataDisposition {
  KeepFetchData
  StopFetchData(UInt64)
  AbortFetchData(UInt64)
  RemoveFetchData
}

///|
fn ClientSession::clear_fetch_data_stream(
  self : ClientSession,
  fetch : ClientFetch,
  stream : Int64,
  discard_inbound : Bool,
) -> Unit {
  self.inbound_fetch_streams.remove(stream)
  self.inbound_fetch_request_ids.remove(fetch.request_id)
  self.outgoing_fetch_streams.remove(stream)
  if discard_inbound {
    self.inbound_stream_kinds[stream] = Discarded
  } else {
    self.inbound_stream_kinds.remove(stream)
  }
  if fetch.data_stream == Some(stream) {
    fetch.data_stream = None
  }
}

///|
priv struct InboundFetchDataStream {
  request_id : UInt64
  mut buffer : Bytes
  decoder : @protocol.FetchDataDecoder?
}

///|
priv struct OutgoingFetchDataStream {
  request_id : UInt64
  encoder : @protocol.FetchDataEncoder
  mut finishing : Bool
  mut pending_send_count : UInt64
}

///|
/// Start an independent request for a historical Object range.
pub fn ClientSession::fetch_track(
  self : ClientSession,
  track : @protocol.TrackName,
  start : @protocol.FetchLocation,
  end : @protocol.FetchLocation,
  parameters : Array[@protocol.MessageParameter],
) -> Result[(RequestId, Array[SessionAction]), ClientSessionError] {
  match self.ensure_new_request_allowed() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if !self.ready {
    return Err(HandshakeNotComplete)
  }
  if start.group > end.group ||
    (
      start.group == end.group &&
      end.object != 0UL &&
      start.object >= end.object
    ) {
    return Err(InvalidFetchRange)
  }
  if !valid_fetch_group_order(parameters) {
    return Err(InvalidFetchRange)
  }
  match
    @protocol.encode_fetch({
      request_id: self.next_request_id,
      fetch_type: @protocol.StandaloneFetch(track, start, end),
      parameters,
    }) {
    Ok(_) => ()
    Err(error) => return Err(Encoding(error))
  }
  let request_id = self.allocate_request_id()
  self.fetches[request_id] = new_client_fetch(
    request_id,
    track,
    start,
    end,
    true,
    None,
    parameters,
  )
  self.fetches.get(request_id).unwrap().outgoing_fetch_type = Some(
    @protocol.StandaloneFetch(track, start, end),
  )
  Ok(
    (
      RequestId(request_id),
      self.finish_actions([
        OpenStream(Bidirectional, Request(RequestId(request_id))),
      ]),
    ),
  )
}

///|
/// Accept a peer's standalone FETCH and open its single Object stream.
pub fn ClientSession::accept_fetch(
  self : ClientSession,
  request_id : RequestId,
  response : @protocol.FetchOkMessage,
) -> Result[Array[SessionAction], ClientSessionError] {
  let request_id = request_id.to_uint64()
  let fetch = match self.fetches.get(request_id) {
    Some(value) if !value.locally_initiated && value.state == FetchPending =>
      value
    _ => return Err(UnknownRequest)
  }
  let effective_end = response.end_location
  if effective_end.group < fetch.start.group ||
    (
      effective_end.group == fetch.start.group &&
      effective_end.object != 0UL &&
      effective_end.object < fetch.start.object
    ) ||
    !fetch_location_not_after(effective_end, fetch.end) {
    return Err(InvalidFetchRange)
  }
  let bytes = match @protocol.encode_fetch_ok(response) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  fetch.state = FetchAccepted
  fetch.end_location = Some(effective_end)
  fetch.end_of_track = response.end_of_track == 1
  let stream = fetch.stream.unwrap()
  Ok(
    self.finish_actions([
      SendChunk(stream, bytes),
      EmitNotice(FetchAccepted(RequestId(request_id), StreamId(stream))),
      OpenStream(Unidirectional, FetchData(RequestId(request_id))),
    ]),
  )
}

///|
/// Reject a peer FETCH before sending data.
pub fn ClientSession::reject_fetch(
  self : ClientSession,
  request_id : RequestId,
  error : @protocol.RequestErrorMessage,
) -> Result[Array[SessionAction], ClientSessionError] {
  let request_id = request_id.to_uint64()
  let fetch = match self.fetches.get(request_id) {
    Some(value) if !value.locally_initiated && value.state == FetchPending =>
      value
    _ => return Err(UnknownRequest)
  }
  let bytes = match @protocol.encode_request_error(error) {
    Ok(value) => value
    Err(codec_error) => return Err(Encoding(codec_error))
  }
  ignore(fetch.lifetime.transition(LocalFin, true))
  Ok(
    self.finish_actions(
      [SendFin(fetch.stream.unwrap(), bytes)] +
      self.terminate_fetch(
        fetch,
        [],
        Some(
          EmitNotice(
            FetchRejected(
              RequestId(request_id),
              StreamId(fetch.stream.unwrap()),
              error,
            ),
          ),
        ),
        RemoveFetchData,
      ),
    ),
  )
}

///|
/// Send an Object on an accepted peer FETCH.
pub fn ClientSession::send_fetch_object(
  self : ClientSession,
  request_id : RequestId,
  object : @protocol.FetchObject,
) -> Result[Array[SessionAction], ClientSessionError] {
  let request_id = request_id.to_uint64()
  let fetch = match self.fetches.get(request_id) {
    Some(value) if !value.locally_initiated && value.state == FetchAccepted =>
      value
    _ => return Err(UnknownRequest)
  }
  if !fetch_object_in_range(fetch, object) {
    return Err(InvalidFetchRange)
  }
  let stream = match fetch.data_stream {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  let encoder = match self.outgoing_fetch_streams.get(stream) {
    Some(value) if !value.finishing => value.encoder
    Some(_) => return Err(UnknownStream)
    None => return Err(UnknownStream)
  }
  let bytes = match encoder.encode(@protocol.FetchDataObject(object)) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  self.outgoing_fetch_streams.get(stream).unwrap().pending_send_count += 1UL
  Ok(self.finish_actions([SendChunk(stream, bytes)]))
}

///|
/// Send an explicit unknown or nonexistent range marker on a peer FETCH.
pub fn ClientSession::send_fetch_range_end(
  self : ClientSession,
  request_id : RequestId,
  range_end : @protocol.FetchRangeEnd,
) -> Result[Array[SessionAction], ClientSessionError] {
  let request_id = request_id.to_uint64()
  let fetch = match self.fetches.get(request_id) {
    Some(value) if !value.locally_initiated && value.state == FetchAccepted =>
      value
    _ => return Err(UnknownRequest)
  }
  let location : @protocol.FetchLocation = {
    group: range_end.group_id,
    object: range_end.object_id,
  }
  if !fetch_location_in_range(fetch, location) {
    return Err(InvalidFetchRange)
  }
  let stream = match fetch.data_stream {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  let outgoing = self.outgoing_fetch_streams.get(stream).unwrap()
  if outgoing.finishing {
    return Err(UnknownStream)
  }
  let encoder = outgoing.encoder
  let bytes = match encoder.encode(@protocol.FetchDataRangeEnd(range_end)) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  self.outgoing_fetch_streams.get(stream).unwrap().pending_send_count += 1UL
  Ok(self.finish_actions([SendChunk(stream, bytes)]))
}

///|
/// End the Fetch data stream. The receiver reports success only after this FIN.
pub fn ClientSession::finish_fetch(
  self : ClientSession,
  request_id : RequestId,
) -> Result[Array[SessionAction], ClientSessionError] {
  let request_id = request_id.to_uint64()
  let fetch = match self.fetches.get(request_id) {
    Some(value) if !value.locally_initiated && value.state == FetchAccepted =>
      value
    _ => return Err(UnknownRequest)
  }
  let stream = match fetch.data_stream {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  match self.outgoing_fetch_streams.get(stream) {
    Some(outgoing) if !outgoing.finishing => {
      outgoing.finishing = true
      outgoing.pending_send_count += 1UL
    }
    _ => return Ok([])
  }
  Ok(self.finish_actions([SendFin(stream, b"")]))
}

///|
/// Cancel a local standalone FETCH.
pub fn ClientSession::cancel_fetch(
  self : ClientSession,
  request_id : RequestId,
) -> Result[Array[SessionAction], ClientSessionError] {
  let request_id = request_id.to_uint64()
  let fetch = match self.fetches.get(request_id) {
    Some(value) if value.locally_initiated => value
    _ => return Err(UnknownRequest)
  }
  if fetch.state == FetchTerminal || fetch.lifetime.cancelled {
    return Ok([])
  }
  let effects = fetch.lifetime.transition(Cancel(1UL), fetch.stream is Some(_))
  let actions : Array[SessionAction] = []
  match fetch.stream {
    Some(stream) => {
      if effects.abort_send {
        actions.push(AbortSend(stream, 1UL))
      }
      if effects.stop_receiving {
        actions.push(StopReceiving(stream, 1UL))
      }
    }
    None => ()
  }
  Ok(
    self.finish_actions(
      self.terminate_fetch(
        fetch,
        actions,
        Some(EmitNotice(FetchCancelled(RequestId(request_id), 1UL))),
        StopFetchData(1UL),
      ),
    ),
  )
}

///|
/// Send a permitted REQUEST_UPDATE on an active Fetch request.
pub fn ClientSession::update_fetch(
  self : ClientSession,
  request_id : RequestId,
  parameters : Array[@protocol.MessageParameter],
) -> Result[(RequestId, Array[SessionAction]), ClientSessionError] {
  let request_id = request_id.to_uint64()
  match self.ensure_new_request_allowed() {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let fetch = match self.fetches.get(request_id) {
    Some(value) if value.locally_initiated && value.state == FetchAccepted =>
      value
    _ => return Err(UnknownRequest)
  }
  let stream = fetch.stream.unwrap()
  if fetch.pending_update_id is Some(_) {
    return Err(RequestUpdatePending)
  }
  let update_id = self.next_request_id
  let encoded = match
    @protocol.encode_request_update_for(
      { request_id: update_id, parameters, },
      @protocol.FetchRequestStream,
    ) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  fetch.pending_update_id = Some(update_id)
  fetch.pending_update_parameters = parameters
  ignore(self.allocate_request_id())
  Ok((RequestId(update_id), self.finish_actions([SendChunk(stream, encoded)])))
}

///|
fn ClientSession::complete_inbound_fetch_if_ready(
  self : ClientSession,
  fetch : ClientFetch,
) -> Array[SessionAction] {
  if fetch.state == FetchAccepted &&
    fetch.data_finished &&
    !fetch.terminal_notified {
    return self.terminate_fetch(
      fetch,
      [],
      Some(EmitNotice(FetchFinished(RequestId(fetch.request_id)))),
      KeepFetchData,
    )
  }
  []
}

///|
fn ClientSession::start_inbound_fetch_data(
  self : ClientSession,
  stream : Int64,
  request_id : UInt64,
) -> Result[Array[SessionAction], ClientSessionError] {
  let fetch = match self.fetches.get(request_id) {
    Some(value) if value.locally_initiated => value
    _ => return Err(UnknownRequest)
  }
  if fetch.lifetime.cancelled || fetch.state == FetchTerminal {
    self.inbound_stream_kinds[stream] = Discarded
    return Ok([])
  }
  if fetch.data_finished ||
    self.inbound_fetch_request_ids.get(request_id) is Some(_) ||
    fetch.data_stream is Some(_) {
    return Err(DuplicateRequestId)
  }
  fetch.data_stream = Some(stream)
  let decoder = @protocol.FetchDataDecoder::new(
    ClientSession::fetch_group_order(fetch.parameters),
  )
  self.inbound_fetch_request_ids[request_id] = stream
  self.inbound_fetch_streams[stream] = {
    request_id,
    buffer: b"",
    decoder: Some(decoder),
  }
  Ok([])
}

///|
fn ClientSession::start_outbound_fetch_data(
  self : ClientSession,
  stream : Int64,
  request_id : UInt64,
) -> Result[Array[SessionAction], ClientSessionError] {
  let fetch = match self.fetches.get(request_id) {
    Some(value) if !value.locally_initiated && value.state == FetchAccepted =>
      value
    _ => return Err(UnknownRequest)
  }
  if fetch.data_stream is Some(_) ||
    self.outgoing_fetch_streams.get(stream) is Some(_) {
    return Err(DuplicateStream)
  }
  fetch.data_stream = Some(stream)
  self.outgoing_fetch_streams[stream] = {
    request_id,
    encoder: @protocol.FetchDataEncoder::new(
      ClientSession::fetch_group_order(fetch.parameters),
    ),
    finishing: false,
    pending_send_count: 1UL,
  }
  Ok([SendChunk(stream, @protocol.encode_fetch_header({ request_id, }))])
}

///|
fn ClientSession::finish_inbound_fetch_data(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[SessionAction], ClientSessionError] {
  let inbound = match self.inbound_fetch_streams.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  let fetch = self.fetches.get(inbound.request_id).unwrap()
  if fetch.lifetime.cancelled || fetch.state == FetchTerminal {
    self.clear_fetch_data_stream(fetch, stream, false)
    return Ok([])
  }
  if !inbound.buffer.is_empty() || inbound.decoder is None {
    self.clear_fetch_data_stream(fetch, stream, false)
    return Err(Track(IncompleteObjectStream))
  }
  fetch.data_finished = true
  self.clear_fetch_data_stream(fetch, stream, false)
  Ok(self.complete_inbound_fetch_if_ready(fetch))
}

///|
fn ClientSession::reset_inbound_fetch_data(
  self : ClientSession,
  stream : Int64,
  error_code : UInt64,
) -> Result[Array[SessionAction], ClientSessionError] {
  let inbound = match self.inbound_fetch_streams.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  let fetch = self.fetches.get(inbound.request_id).unwrap()
  Ok(
    self.terminate_fetch(
      fetch,
      [],
      Some(
        EmitNotice(
          FetchReset(
            RequestId(inbound.request_id),
            StreamId(stream),
            error_code,
          ),
        ),
      ),
      RemoveFetchData,
    ),
  )
}

///|
fn ClientSession::receive_fetch_data(
  self : ClientSession,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[SessionAction], ClientSessionError] {
  let data = match self.inbound_fetch_streams.get(stream) {
    Some(value) => value
    None => return Err(UnknownStream)
  }
  let fetch = self.fetches.get(data.request_id).unwrap()
  if fetch.lifetime.cancelled || fetch.state == FetchTerminal {
    return Ok([])
  }
  if bytes.length() > 1024 * 1024 - data.buffer.length() {
    return Err(BufferLimitExceeded)
  }
  data.buffer = data.buffer + bytes
  let actions : Array[SessionAction] = []
  while !data.buffer.is_empty() {
    let decoder = data.decoder.unwrap()
    match decoder.decode_next(data.buffer) {
      @protocol.Incomplete => return Ok(actions)
      @protocol.Invalid(_) => return Err(Track(InvalidControlMessage))
      @protocol.Complete(frame, consumed) => {
        data.buffer = data.buffer[consumed:].to_owned()
        match frame {
          @protocol.FetchDataObject(object) => {
            if !fetch_object_in_range(fetch, object) {
              return Err(InvalidFetchRange)
            }
            actions.push(
              EmitNotice(
                FetchObjectReceived(RequestId(data.request_id), object),
              ),
            )
          }
          @protocol.FetchDataRangeEnd(range_end) => {
            let location : @protocol.FetchLocation = {
              group: range_end.group_id,
              object: range_end.object_id,
            }
            if !fetch_location_in_range(fetch, location) {
              return Err(InvalidFetchRange)
            }
            actions.push(
              EmitNotice(FetchRangeEnded(RequestId(data.request_id), range_end)),
            )
          }
        }
      }
    }
  }
  Ok(actions)
}

///|
fn fetch_object_in_range(
  fetch : ClientFetch,
  object : @protocol.FetchObject,
) -> Bool {
  let location : @protocol.FetchLocation = {
    group: object.group_id,
    object: object.object_id,
  }
  fetch_location_in_range(fetch, location)
}

///|
fn fetch_location_in_range(
  fetch : ClientFetch,
  location : @protocol.FetchLocation,
) -> Bool {
  let after_start = location.group > fetch.start.group ||
    (
      location.group == fetch.start.group &&
      location.object >= fetch.start.object
    )
  after_start &&
  fetch_location_before(location, fetch.end_location.unwrap_or(fetch.end))
}

///|
fn fetch_location_before(
  location : @protocol.FetchLocation,
  end : @protocol.FetchLocation,
) -> Bool {
  location.group < end.group ||
  (
    location.group == end.group &&
    (end.object == 0UL || location.object < end.object)
  )
}

///|
fn fetch_location_not_after(
  end : @protocol.FetchLocation,
  requested_end : @protocol.FetchLocation,
) -> Bool {
  end.group < requested_end.group ||
  (
    end.group == requested_end.group &&
    (
      requested_end.object == 0UL ||
      (end.object != 0UL && end.object <= requested_end.object)
    )
  )
}

///|

///|
fn ClientSession::receive_fetch_response(
  self : ClientSession,
  request_id : UInt64,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[SessionAction], ClientSessionError] {
  let fetch = self.fetches.get(request_id).unwrap()
  if fetch.stream != Some(stream) {
    return Err(UnexpectedMessage)
  }
  if fetch.lifetime.cancelled || fetch.state == FetchTerminal {
    return Ok([])
  }
  if bytes.length() > 65546 - fetch.response_buffer.length() {
    return Err(BufferLimitExceeded)
  }
  fetch.response_buffer = fetch.response_buffer + bytes
  let decoded = @protocol.decode_control_with_context(
    fetch.response_buffer,
    @protocol.FetchRequestStream,
  )
  match decoded {
    @protocol.Incomplete => Ok([])
    @protocol.Invalid(_) => Err(ClientSessionError::UnexpectedMessage)
    @protocol.Complete(message, consumed) => {
      let trailing = fetch.response_buffer[consumed:].to_owned()
      fetch.response_buffer = b""
      let actions : Array[SessionAction] = match message {
        @protocol.FetchOk(response) if fetch.state == FetchPending => {
          let end_location = response.end_location
          if end_location.group < fetch.start.group ||
            (
              end_location.group == fetch.start.group &&
              end_location.object != 0UL &&
              end_location.object < fetch.start.object
            ) {
            return Err(InvalidFetchRange)
          }
          if !fetch_location_not_after(end_location, fetch.end) {
            return Err(InvalidFetchRange)
          }
          fetch.end_location = Some(end_location)
          fetch.end_of_track = response.end_of_track == 1
          fetch.state = FetchAccepted
          let values : Array[SessionAction] = [
            EmitNotice(FetchAccepted(RequestId(request_id), StreamId(stream))),
            EmitNotice(
              FetchResponseAccepted(
                RequestId(request_id),
                StreamId(stream),
                response,
              ),
            ),
          ]
          if fetch.end_of_track {
            values.push(EmitNotice(FetchEndOfTrack(RequestId(request_id))))
          }
          values + self.complete_inbound_fetch_if_ready(fetch)
        }
        @protocol.RequestError(error) if fetch.state == FetchPending =>
          self.terminate_fetch(
            fetch,
            [],
            Some(EmitNotice(FetchFailed(RequestId(request_id), error))),
            StopFetchData(error.code),
          )
        @protocol.RequestUpdate(update) if !fetch.locally_initiated &&
          fetch.state == FetchAccepted &&
          !fetch.lifetime.peer_send_closed => {
          if (update.request_id & 1UL) != 1UL {
            return Err(InvalidRequestId)
          }
          if self.peer_request_ids.get(update.request_id) is Some(_) {
            return Err(DuplicateRequestId)
          }
          self.peer_request_ids[update.request_id] = stream
          self.note_peer_request_id(update.request_id)
          if fetch.pending_update_id is Some(_) {
            return Err(ClientSessionError::UnexpectedMessage)
          }
          fetch.pending_update_id = Some(update.request_id)
          fetch.pending_update_parameters = update.parameters
          if self.control_goaway_sent {
            let error : @protocol.RequestErrorMessage = {
              code: request_error_going_away,
              retry_interval: 0UL,
              reason: b"",
              redirect: b"",
            }
            let error_bytes = match @protocol.encode_request_error(error) {
              Ok(value) => value
              Err(codec_error) => return Err(Encoding(codec_error))
            }
            fetch.pending_update_id = None
            fetch.pending_update_parameters = []
            return Ok([SendChunk(stream, error_bytes)])
          }
          [
            EmitNotice(
              FetchUpdateRequested(
                RequestId(request_id),
                RequestId(update.request_id),
                StreamId(stream),
                update.parameters,
              ),
            ),
          ]
        }
        @protocol.RequestOk(response) if fetch.locally_initiated &&
          fetch.pending_update_id is Some(update_id) => {
          if (update_id & 1UL) != 0UL {
            return Err(ClientSessionError::UnexpectedMessage)
          }
          fetch.pending_update_id = None
          fetch.parameters = merge_subscription_parameters(
            fetch.parameters,
            fetch.pending_update_parameters,
          )
          fetch.pending_update_parameters = []
          [
            EmitNotice(
              FetchUpdateAccepted(
                RequestId(request_id),
                RequestId(update_id),
                response,
              ),
            ),
          ]
        }
        @protocol.RequestError(error) if fetch.locally_initiated &&
          fetch.pending_update_id is Some(_) => {
          let update_id = fetch.pending_update_id.unwrap()
          fetch.pending_update_id = None
          fetch.pending_update_parameters = []
          [
            EmitNotice(
              FetchUpdateRejected(
                RequestId(request_id),
                RequestId(update_id),
                error,
              ),
            ),
          ]
        }
        @protocol.GoAwayRequest(goaway) =>
          self.receive_request_goaway(stream, goaway)
        _ => return Err(ClientSessionError::UnexpectedMessage)
      }
      if trailing.is_empty() {
        Ok(actions)
      } else {
        match self.receive_fetch_response(request_id, stream, trailing) {
          Ok(more) => Ok(actions + more)
          Err(error) => Err(error)
        }
      }
    }
  }
}

///|
pub fn ClientSession::accept_fetch_update(
  self : ClientSession,
  request_id : RequestId,
  update_id : RequestId,
) -> Result[Array[SessionAction], ClientSessionError] {
  let request_id = request_id.to_uint64()
  let update_id = update_id.to_uint64()
  let fetch = match self.fetches.get(request_id) {
    Some(value) if !value.locally_initiated && value.state == FetchAccepted =>
      value
    _ => return Err(UnknownRequest)
  }
  if (update_id & 1UL) == 0UL ||
    fetch.stream is None ||
    fetch.pending_update_id != Some(update_id) {
    return Err(InvalidRequestId)
  }
  fetch.pending_update_id = None
  fetch.parameters = merge_subscription_parameters(
    fetch.parameters,
    fetch.pending_update_parameters,
  )
  fetch.pending_update_parameters = []
  let encoded = match
    @protocol.encode_request_ok_for(
      { parameters: [], properties: b"", },
      @protocol.FetchUpdate,
    ) {
    Ok(value) => value
    Err(error) => return Err(Encoding(error))
  }
  Ok(self.finish_actions([SendChunk(fetch.stream.unwrap(), encoded)]))
}

///|
pub fn ClientSession::reject_fetch_update(
  self : ClientSession,
  request_id : RequestId,
  update_id : RequestId,
  error : @protocol.RequestErrorMessage,
) -> Result[Array[SessionAction], ClientSessionError] {
  let request_id = request_id.to_uint64()
  let update_id = update_id.to_uint64()
  let fetch = match self.fetches.get(request_id) {
    Some(value) if !value.locally_initiated && value.state == FetchAccepted =>
      value
    _ => return Err(UnknownRequest)
  }
  if fetch.pending_update_id != Some(update_id) {
    return Err(UnknownRequest)
  }
  let encoded = match @protocol.encode_request_error(error) {
    Ok(value) => value
    Err(codec_error) => return Err(Encoding(codec_error))
  }
  let actions : Array[SessionAction] = [
    SendChunk(fetch.stream.unwrap(), encoded),
  ]
  fetch.pending_update_id = None
  fetch.pending_update_parameters = []
  Ok(
    self.finish_actions(
      self.terminate_fetch(
        fetch,
        actions,
        Some(EmitNotice(FetchFailed(RequestId(request_id), error))),
        AbortFetchData(error.code),
      ),
    ),
  )
}

///|
fn ClientSession::fetch_group_order(
  parameters : Array[@protocol.MessageParameter],
) -> @protocol.FetchGroupOrder {
  for parameter in parameters {
    if parameter.kind == 0x22 {
      match parameter.value {
        @protocol.Byte(2) => return @protocol.Descending
        _ => return @protocol.Ascending
      }
    }
  }
  @protocol.Ascending
}

///|
fn valid_fetch_group_order(
  parameters : Array[@protocol.MessageParameter],
) -> Bool {
  for parameter in parameters {
    if parameter.kind == 0x22 {
      match parameter.value {
        @protocol.Byte(1) | @protocol.Byte(2) => ()
        _ => return false
      }
    }
  }
  true
}
