///|
/// Inbound and outbound stream classification and lifecycle handling.

///|
fn ClientSession::receive_stream_data(
  self : ClientSession,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[SessionAction], ClientSessionError] {
  match self.inbound_unclassified_buffers.get(stream) {
    Some(previous) => {
      if bytes.length() > 1024 * 1024 - previous.length() {
        return Err(BufferLimitExceeded)
      }
      let combined = previous + bytes
      match @protocol.decode_vi64(combined) {
        @protocol.Incomplete => {
          self.inbound_unclassified_buffers[stream] = combined
          Ok([])
        }
        @protocol.Invalid(_) => Err(UnexpectedMessage)
        @protocol.Complete(stream_type, _) => {
          self.inbound_unclassified_buffers.remove(stream)
          match stream_type {
            0x2f00UL => {
              self.inbound_stream_kinds[stream] = Control
              self.receive_control(stream, combined)
            }
            0x05UL =>
              match @protocol.decode_fetch_header(combined) {
                @protocol.Incomplete => {
                  self.inbound_unclassified_buffers[stream] = combined
                  Ok([])
                }
                @protocol.Invalid(_) => Err(UnexpectedMessage)
                @protocol.Complete(header, consumed) => {
                  self.inbound_stream_kinds[stream] = FetchData
                  let rest = combined[consumed:].to_owned()
                  match
                    self.start_inbound_fetch_data(stream, header.request_id) {
                    Ok(actions) if self.inbound_stream_kinds.get(stream)
                      is Some(Discarded) => Ok(actions)
                    Ok(actions) if rest.is_empty() => Ok(actions)
                    Ok(actions) =>
                      match self.receive_fetch_data(stream, rest) {
                        Ok(more) => Ok(actions + more)
                        Err(error) => Err(error)
                      }
                    Err(error) => Err(error)
                  }
                }
              }
            0x132b3e28UL => {
              self.inbound_stream_kinds[stream] = Padding
              Ok([])
            }
            _ if is_subgroup_stream_type(stream_type) => {
              self.inbound_stream_kinds[stream] = Subgroup
              self.receive_subgroup(stream, combined)
            }
            _ => Err(UnexpectedMessage)
          }
        }
      }
    }
    None =>
      match self.inbound_stream_kinds.get(stream) {
        Some(Control) => self.receive_control(stream, bytes)
        Some(Request) => self.receive_request(stream, bytes)
        Some(Subgroup) => self.receive_subgroup(stream, bytes)
        Some(FetchData) => self.receive_fetch_data(stream, bytes)
        Some(Padding) | Some(Discarded) => Ok([])
        None =>
          match self.request_stream_route(stream) {
            NamespaceSubscriptionResponse(_)
            | NamespacePublicationResponse(_)
            | FetchResponse(_)
            | TrackResponse(_) => self.receive_request(stream, bytes)
            UnknownRequestStream => Err(UnknownStream)
          }
      }
  }
}

///|
fn is_subgroup_stream_type(stream_type : UInt64) -> Bool {
  stream_type >= 0x10UL &&
  stream_type <= 0x7fUL &&
  (stream_type & 0x10UL) != 0UL
}

///|
fn ClientSession::stream_opened(
  self : ClientSession,
  purpose : ClientStreamPurpose,
  stream : Int64,
) -> Result[Array[SessionAction], ClientSessionError] {
  match purpose {
    Control => {
      if !self.started || self.outbound_control_stream is Some(_) {
        return Err(UnexpectedMessage)
      }
      self.outbound_control_stream = Some(stream)
      Ok([SendChunk(stream, self.handshake.outbound_setup())])
    }
    Request(request_id) => {
      if self.request_streams.get(stream) is Some(_) ||
        self.namespace_stream_exists(stream) {
        return Err(DuplicateStream)
      }
      match self.fetches.get(request_id) {
        Some(fetch) if fetch.locally_initiated && fetch.stream is None => {
          fetch.stream = Some(stream)
          self.fetch_streams[stream] = request_id
          if fetch.lifetime.cancelled {
            return Ok([
              AbortSend(stream, fetch.lifetime.cancel_error_code.unwrap_or(1UL)),
              StopReceiving(stream, 1UL),
            ])
          }
          let encoded = @protocol.encode_fetch({
            request_id,
            fetch_type: fetch.outgoing_fetch_type.unwrap_or(
              @protocol.StandaloneFetch(fetch.track, fetch.start, fetch.end),
            ),
            parameters: fetch.parameters,
          })
          let bytes = match encoded {
            Ok(value) => value
            Err(error) => return Err(Encoding(error))
          }
          return Ok([SendChunk(stream, bytes)])
        }
        Some(_) => return Err(DuplicateStream)
        None => ()
      }
      match self.open_namespace_stream(request_id, stream) {
        Some(result) => return result
        None => ()
      }
      let request = match self.requests.get(request_id) {
        Some(value) if value.stream is None => value
        Some(_) => return Err(DuplicateStream)
        None => return Err(UnknownRequest)
      }
      request.stream = Some(stream)
      self.request_streams[stream] = request_id
      if request.is_terminal() {
        if request.lifetime.cancelled {
          let error_code = match request.lifetime.cancel_error_code {
            Some(value) => value
            None => 1UL
          }
          return Ok(self.end_request_stream(stream, Cancel(error_code)))
        }
        return Ok([])
      }
      let encoded = match request.kind {
        Publishing =>
          @protocol.encode_publish({
            request_id,
            track: request.track,
            track_alias: request.track_alias.unwrap(),
            parameters: [],
            properties: request.properties,
          })
        Subscribing =>
          @protocol.encode_subscribe({
            request_id,
            track: request.track,
            parameters: request.parameters,
          })
      }
      let bytes = match encoded {
        Ok(value) => value
        Err(error) => return Err(Encoding(error))
      }
      Ok([SendChunk(stream, bytes)])
    }
    Object(token) => {
      let pending = match self.pending_object_streams.get(token) {
        Some(value) => value
        None => return Err(UnknownStream)
      }
      match pending.cancelled_error_code {
        Some(error_code) => {
          self.pending_object_streams.remove(token)
          match self.published_subscriptions.get(pending.subscription_stream) {
            Some(subscription) =>
              subscription.opened_stream_count = subscription.opened_stream_count +
                1UL
            None => ()
          }
          return Ok(
            [AbortSend(stream, error_code)] +
            self.finish_failed_subscription(pending.subscription_stream),
          )
        }
        None => ()
      }
      if !self.subscription_is_active(pending.subscription_stream) {
        self.pending_object_streams.remove(token)
        match self.published_subscriptions.get(pending.subscription_stream) {
          Some(subscription) =>
            subscription.opened_stream_count = subscription.opened_stream_count +
              1UL
          None => ()
        }
        return Ok(
          [AbortSend(stream, 1UL)] +
          self.finish_failed_subscription(pending.subscription_stream),
        )
      }
      let subscription = match
        self.published_subscriptions.get(pending.subscription_stream) {
        Some(value) => value
        None => return Err(SubscriptionNotAccepted)
      }
      if self.outgoing_object_streams.get(stream) is Some(_) {
        return Err(Track(UnexpectedMessage))
      }
      subscription.opened_stream_count = subscription.opened_stream_count + 1UL
      subscription.open_stream_count = subscription.open_stream_count + 1UL
      self.pending_object_streams.remove(token)
      let outgoing : OutgoingObjectStream = {
        subscription_stream: pending.subscription_stream,
        header: pending.header,
        token,
        previous_object_id: None,
        partial_object: None,
        finishing: pending.finish_requested,
        abort_sent: false,
        pending_send_count: 1UL,
        delivery_deadline_ms: match pending.finish_time_ms {
          Some(time) =>
            self.subgroup_deadline(pending.subscription_stream, time)
          None => None
        },
      }
      let actions : Array[SessionAction] = [
        SendChunk(stream, @protocol.encode_subgroup_header(pending.header)),
      ]
      if pending.finish_requested {
        actions.push(SendFin(stream, b""))
        outgoing.pending_send_count = outgoing.pending_send_count + 1UL
      }
      self.outgoing_object_streams[stream] = outgoing
      self.group_streams[token] = stream
      Ok(actions)
    }
    FetchData(request_id) => self.start_outbound_fetch_data(stream, request_id)
  }
}

///|
fn ClientSession::stream_finished(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[SessionAction], ClientSessionError] {
  if self.peer_control_stream == Some(stream) ||
    self.inbound_stream_kinds.get(stream) is Some(Control) {
    return Err(Handshake(self.handshake.control_closed()))
  }
  match self.fetch_streams.get(stream) {
    Some(request_id) => {
      let fetch = self.fetches.get(request_id).unwrap()
      if fetch.locally_initiated &&
        (fetch.state == FetchPending || !fetch.response_buffer.is_empty()) {
        return Err(UnexpectedMessage)
      }
      ignore(fetch.lifetime.transition(PeerFin, true))
      fetch.response_buffer = b""
      self.inbound_buffers.remove(stream)
      return Ok([])
    }
    None => ()
  }
  match self.inbound_fetch_streams.get(stream) {
    Some(_) => return self.finish_inbound_fetch_data(stream)
    None => ()
  }
  match self.ignored_object_streams.get(stream) {
    Some(ignored) => {
      self.ignored_object_streams.remove(stream)
      self.inbound_stream_kinds.remove(stream)
      if ignored.counted_active {
        let request = self.requests.get(ignored.request_id).unwrap()
        request.object_drain.note_stream_closed()
      }
      return Ok([])
    }
    None => ()
  }
  match self.incoming_object_streams.get(stream) {
    Some(state) => {
      if state.header is None {
        self.incoming_object_streams.remove(stream)
        self.inbound_stream_kinds.remove(stream)
        return Err(Track(IncompleteObjectStream))
      }
      if !state.buffer.is_empty() {
        return Err(Track(IncompleteObjectStream))
      }
      let request_id = match state.request_id {
        Some(value) => value
        None => {
          self.incoming_object_streams.remove(stream)
          self.inbound_stream_kinds.remove(stream)
          return Err(Track(IncompleteObjectStream))
        }
      }
      let group_id = state.header.unwrap().group_id
      let group_finished = state.header.unwrap().end_of_group
      let request = self.requests.get(request_id).unwrap()
      request.object_drain.note_stream_closed()
      self.incoming_object_streams.remove(stream)
      self.inbound_stream_kinds.remove(stream)
      let actions : Array[SessionAction] = []
      if group_finished {
        actions.push(EmitNotice(GroupFinished(request_id, group_id)))
      }
      match self.take_ready_publish_done(request_id) {
        Some(done) => actions.push(EmitNotice(PublishDone(request_id, done)))
        None => ()
      }
      Ok(actions)
    }
    None => {
      match self.finish_request_stream(stream) {
        Some(result) => return result
        None => ()
      }
      if self.inbound_unclassified_buffers.get(stream) is Some(_) {
        self.inbound_unclassified_buffers.remove(stream)
        return Err(UnexpectedMessage)
      }
      self.inbound_buffers.remove(stream)
      self.inbound_stream_kinds.remove(stream)
      Ok([])
    }
  }
}

///|
fn ClientSession::stream_reset(
  self : ClientSession,
  stream : Int64,
  error_code : UInt64,
) -> Result[Array[SessionAction], ClientSessionError] {
  if self.peer_control_stream == Some(stream) ||
    self.inbound_stream_kinds.get(stream) is Some(Control) {
    return Err(Handshake(self.handshake.control_closed()))
  }
  match self.inbound_fetch_streams.get(stream) {
    Some(_) => return self.reset_inbound_fetch_data(stream, error_code)
    None => ()
  }
  match self.ignored_object_streams.get(stream) {
    Some(ignored) => {
      self.ignored_object_streams.remove(stream)
      self.inbound_stream_kinds.remove(stream)
      if ignored.counted_active {
        let request = self.requests.get(ignored.request_id).unwrap()
        request.object_drain.note_stream_closed()
      }
      return Ok([])
    }
    None => ()
  }
  match self.incoming_object_streams.get(stream) {
    Some(state) => {
      let request_id = match state.request_id {
        Some(value) => value
        None => {
          self.incoming_object_streams.remove(stream)
          self.inbound_stream_kinds.remove(stream)
          return Ok([])
        }
      }
      let request = self.requests.get(request_id).unwrap()
      request.object_drain.note_stream_closed()
      self.incoming_object_streams.remove(stream)
      self.inbound_stream_kinds.remove(stream)
      let actions : Array[SessionAction] = [
        EmitNotice(ObjectStreamReset(request_id, stream)),
      ]
      match self.take_ready_publish_done(request_id) {
        Some(done) => actions.push(EmitNotice(PublishDone(request_id, done)))
        None => ()
      }
      Ok(actions)
    }
    None => {
      match self.reset_request_stream(stream, error_code) {
        Some(result) => return result
        None => ()
      }
      self.inbound_buffers.remove(stream)
      self.incoming_object_streams.remove(stream)
      self.inbound_unclassified_buffers.remove(stream)
      self.inbound_stream_kinds.remove(stream)
      Ok([])
    }
  }
}

///|
fn ClientSession::send_complete(
  self : ClientSession,
  stream : Int64,
  completion : SendCompletion,
) -> Result[Array[SessionAction], ClientSessionError] {
  match self.outgoing_object_streams.get(stream) {
    Some(outgoing) if completion == Reset => {
      let subscription_stream = outgoing.subscription_stream
      self.close_outgoing_object_stream(stream)
      Ok(
        [EmitNotice(StreamSendFinished(stream, Reset))] +
        self.finish_failed_subscription(subscription_stream),
      )
    }
    Some(outgoing) => {
      if outgoing.pending_send_count > 0UL {
        outgoing.pending_send_count = outgoing.pending_send_count - 1UL
      }
      Ok([EmitNotice(StreamSendFinished(stream, Sent))])
    }
    None =>
      match self.outgoing_fetch_streams.get(stream) {
        Some(outgoing) if completion == Reset => {
          let fetch = self.fetches.get(outgoing.request_id).unwrap()
          Ok(
            self.terminate_fetch(
              fetch,
              [EmitNotice(StreamSendFinished(stream, Reset))],
              None,
              RemoveFetchData,
            ),
          )
        }
        Some(outgoing) => {
          if outgoing.pending_send_count > 0UL {
            outgoing.pending_send_count = outgoing.pending_send_count - 1UL
          }
          if outgoing.finishing && outgoing.pending_send_count == 0UL {
            let fetch = self.fetches.get(outgoing.request_id).unwrap()
            Ok(
              self.terminate_fetch(
                fetch,
                [EmitNotice(StreamSendFinished(stream, Sent))],
                None,
                RemoveFetchData,
              ),
            )
          } else {
            Ok([EmitNotice(StreamSendFinished(stream, Sent))])
          }
        }
        None =>
          match self.namespace_send_complete(stream, completion) {
            Some(actions) => Ok(actions)
            None =>
              match self.request_streams.get(stream) {
                Some(request_id) if self.requests.get(request_id).unwrap().lifetime.cancelled =>
                  Ok([])
                _ => Ok([EmitNotice(StreamSendFinished(stream, completion))])
              }
          }
      }
  }
}
