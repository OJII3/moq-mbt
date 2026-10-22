///|
/// Inbound and outbound stream classification and lifecycle handling.

///|
fn ClientSession::receive_stream_data(
  self : ClientSession,
  stream : Int64,
  bytes : Bytes,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
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
            0x05UL => Err(UnexpectedMessage)
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
        Some(Padding) | Some(Discarded) => Ok([])
        None =>
          match self.namespace_request_streams.get(stream) {
            Some(_) => self.receive_request(stream, bytes)
            None =>
              match self.request_streams.get(stream) {
                Some(_) => self.receive_request(stream, bytes)
                None => Err(UnknownStream)
              }
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
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match purpose {
    Control => {
      if !self.started || self.outbound_control_stream is Some(_) {
        return Err(UnexpectedMessage)
      }
      self.outbound_control_stream = Some(stream)
      Ok([Send(stream, self.handshake.outbound_setup(), false)])
    }
    Request(request_id) => {
      if self.request_streams.get(stream) is Some(_) ||
        self.namespace_request_streams.get(stream) is Some(_) {
        return Err(DuplicateStream)
      }
      match self.namespace_requests.get(request_id) {
        Some(request) if request.stream is None => {
          request.stream = Some(stream)
          self.namespace_request_streams[stream] = request_id
          if request.locally_initiated {
            self.request_streams[stream] = request_id
          }
          if request.state == NamespaceTerminal {
            if request.lifetime.cancelled {
              let error_code = request.lifetime.cancel_error_code.unwrap()
              return Ok(self.end_namespace_request(stream, Cancel(error_code)))
            }
            return Ok([])
          }
          let encoded = @protocol.encode_publish_namespace({
            request_id,
            track_namespace: request.track_namespace,
            parameters: request.parameters,
          })
          let bytes = match encoded {
            Ok(value) => value
            Err(error) => return Err(Encoding(error))
          }
          request.announced = true
          return Ok([Send(stream, bytes, false)])
        }
        Some(_) => return Err(DuplicateStream)
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
            properties: b"",
          })
        Subscribing =>
          @protocol.encode_subscribe({
            request_id,
            track: request.track,
            parameters: [],
          })
      }
      let bytes = match encoded {
        Ok(value) => value
        Err(error) => return Err(Encoding(error))
      }
      Ok([Send(stream, bytes, false)])
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
      self.outgoing_object_streams[stream] = {
        subscription_stream: pending.subscription_stream,
        header: pending.header,
        previous_object_id: None,
        finishing: false,
        abort_sent: false,
        pending_send_count: 1UL,
      }
      Ok([Send(stream, @protocol.encode_subgroup_header(pending.header), false)])
    }
  }
}

///|
fn ClientSession::stream_finished(
  self : ClientSession,
  stream : Int64,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  if self.peer_control_stream == Some(stream) ||
    self.inbound_stream_kinds.get(stream) is Some(Control) {
    return Err(Handshake(self.handshake.control_closed()))
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
      let actions : Array[ClientSessionAction] = []
      if group_finished {
        actions.push(Notify(GroupFinished(request_id, group_id)))
      }
      match self.take_ready_publish_done(request_id) {
        Some(done) => actions.push(Notify(PublishDone(request_id, done)))
        None => ()
      }
      Ok(actions)
    }
    None => {
      if self.namespace_request_streams.get(stream) is Some(_) {
        let request = self.namespace_requests
          .get(self.namespace_request_streams.get(stream).unwrap())
          .unwrap()
        if (request.locally_initiated && request.state == NamespacePending) ||
          !request.response_buffer.is_empty() ||
          (
            request.locally_initiated &&
            request.pending_local_update_id is Some(_)
          ) ||
          !self.inbound_buffers.get(stream).unwrap_or(b"").is_empty() {
          return Err(UnexpectedMessage)
        }
        ignore(request.lifetime.transition(PeerFin, true))
        request.response_buffer = b""
        self.inbound_buffers.remove(stream)
        return Ok([])
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
        return Ok([])
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
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  if self.peer_control_stream == Some(stream) ||
    self.inbound_stream_kinds.get(stream) is Some(Control) {
    return Err(Handshake(self.handshake.control_closed()))
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
      let actions : Array[ClientSessionAction] = [
        Notify(ObjectStreamReset(request_id, stream)),
      ]
      match self.take_ready_publish_done(request_id) {
        Some(done) => actions.push(Notify(PublishDone(request_id, done)))
        None => ()
      }
      Ok(actions)
    }
    None => {
      if self.namespace_request_streams.get(stream) is Some(_) {
        return Ok(self.end_namespace_request(stream, PeerReset(error_code)))
      }
      if self.inbound_subscriptions.get(stream) is Some(_) ||
        self.request_streams.get(stream) is Some(_) {
        return Ok(self.end_request_stream(stream, PeerReset(error_code)))
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
  canceled : Bool,
) -> Result[Array[ClientSessionAction], ClientSessionError] {
  match self.outgoing_object_streams.get(stream) {
    Some(outgoing) if canceled => {
      let subscription_stream = outgoing.subscription_stream
      self.close_outgoing_object_stream(stream)
      Ok(
        [Notify(SendFinished(stream, true))] +
        self.finish_failed_subscription(subscription_stream),
      )
    }
    Some(outgoing) => {
      if outgoing.pending_send_count > 0UL {
        outgoing.pending_send_count = outgoing.pending_send_count - 1UL
      }
      if outgoing.finishing && outgoing.pending_send_count == 0UL {
        let subscription_stream = outgoing.subscription_stream
        self.close_outgoing_object_stream(stream)
        Ok(
          [
            Notify(SendFinished(stream, false)),
            Notify(ObjectStreamFinished(stream)),
          ] +
          self.finish_failed_subscription(subscription_stream),
        )
      } else {
        Ok([Notify(SendFinished(stream, false))])
      }
    }
    None =>
      if self.namespace_request_streams.get(stream) is Some(request_id) &&
        self.namespace_requests.get(request_id).unwrap().lifetime.cancelled {
        Ok([])
      } else {
        match self.request_streams.get(stream) {
          Some(request_id) if self.requests.get(request_id).unwrap().lifetime.cancelled =>
            Ok([])
          _ => Ok([Notify(SendFinished(stream, canceled))])
        }
      }
  }
}
