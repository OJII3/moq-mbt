///|
fn writer_group_open(
  publisher : @session.ClientSession,
  group_id : UInt64,
) -> (@session.ObjectGroupWriter, UInt64) raise {
  let (group, actions) = publisher.open_object_group(40, group_id).unwrap()
  let token = match actions {
    [@session.OpenStream(@session.Unidirectional, @session.Object(token))] =>
      token
    _ => fail("expected one object stream open")
  }
  (group, token)
}

///|
fn writer_object_ids(
  actions : Array[@session.SessionAction],
) -> (UInt64, Array[UInt64]) raise {
  let mut decoder : @protocol.SubgroupDecoder? = None
  let mut group_id = 0UL
  let ids : Array[UInt64] = []
  for action in actions {
    match action {
      @session.SendChunk(_, bytes) if !bytes.is_empty() =>
        match decoder {
          None =>
            match @protocol.decode_subgroup_header(bytes) {
              @protocol.Complete(header, consumed) => {
                group_id = header.group_id
                let value = @protocol.SubgroupDecoder::new(header)
                decoder = Some(value)
                let mut remaining = bytes[consumed:].to_owned()
                while !remaining.is_empty() {
                  match value.decode_next(remaining) {
                    @protocol.Complete(frame, used) => {
                      ids.push(frame.object_id)
                      remaining = remaining[used:].to_owned()
                    }
                    _ => fail("expected a complete queued Object")
                  }
                }
              }
              _ => fail("expected a subgroup header")
            }
          Some(value) => {
            let mut remaining = bytes
            while !remaining.is_empty() {
              match value.decode_next(remaining) {
                @protocol.Complete(frame, used) => {
                  ids.push(frame.object_id)
                  remaining = remaining[used:].to_owned()
                }
                _ => fail("expected a complete Object")
              }
            }
          }
        }
      _ => ()
    }
  }
  (group_id, ids)
}

///|
test "object group writer queues objects and FIN until the stream opens" {
  let publisher = publisher_started()
  let (group, token) = writer_group_open(publisher, 0UL)
  assert_eq(group.send(b"one"), Ok([]))
  assert_eq(group.send(b"two"), Ok([]))
  assert_eq(group.finish(), Ok([]))
  let actions = publisher
    .receive(@session.StreamOpened(@session.Object(token), 50))
    .unwrap()
  assert_eq(actions.length(), 4)
  let (group_id, ids) = writer_object_ids(actions)
  assert_eq(group_id, 0UL)
  assert_eq(ids, [0UL, 1UL])
  match actions[3] {
    @session.SendFin(50, bytes) => assert_true(bytes.is_empty())
    _ => fail("expected FIN after queued Objects")
  }
}

///|
test "object group writer sends immediately after the stream opens" {
  let publisher = publisher_started()
  let (group, token) = writer_group_open(publisher, 4UL)
  ignore(publisher.receive(@session.StreamOpened(@session.Object(token), 50)))
  match group.send(b"payload") {
    Ok([@session.SendChunk(50, bytes)]) => {
      assert_true(!bytes.is_empty())
      assert_true(group.send(b"next") is Ok(_))
    }
    _ => fail("expected an immediate Object send")
  }
}

///|
test "object group writer rejects an out-of-order object id" {
  let publisher = publisher_started()
  let (group, _token) = writer_group_open(publisher, 0UL)
  assert_eq(
    group.send_frame({
      object_id: 3UL,
      status: @protocol.Normal,
      properties: b"",
      payload: b"skip",
    }),
    Err(@session.Track(@session.ObjectIdNotIncreasing)),
  )
}

///|
test "object group writer rotates to the next group" {
  let publisher = publisher_started()
  let (group, token) = writer_group_open(publisher, 7UL)
  ignore(publisher.receive(@session.StreamOpened(@session.Object(token), 50)))
  ignore(group.send(b"first"))
  let rotate = group.rotate().unwrap()
  let next_token = match rotate {
    [
      @session.SendFin(50, _),
      @session.OpenStream(@session.Unidirectional, @session.Object(token)),
    ] => token
    _ => fail("expected FIN and the next OpenStream")
  }
  ignore(group.send(b"second"))
  let actions = publisher
    .receive(@session.StreamOpened(@session.Object(next_token), 52))
    .unwrap()
  let (group_id, ids) = writer_object_ids(actions)
  assert_eq(group_id, 8UL)
  assert_eq(ids, [0UL])
}

///|
test "object group writer aborts a pending stream on open" {
  let publisher = publisher_started()
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(group.send(b"queued"))
  assert_eq(group.abort(5UL), Ok([]))
  assert_eq(
    publisher.receive(@session.StreamOpened(@session.Object(token), 50)),
    Ok([@session.AbortSend(50, 5UL)]),
  )
}

///|
test "object group writer aborts an open stream without FIN" {
  let publisher = publisher_started()
  let (group, token) = writer_group_open(publisher, 0UL)
  ignore(publisher.receive(@session.StreamOpened(@session.Object(token), 50)))
  ignore(group.send(b"payload"))
  assert_eq(
    group.abort(6UL),
    Ok([
      @session.AbortSend(50, 6UL),
      @session.EmitNotice(@session.StreamSendFinished(50, @session.Reset)),
    ]),
  )
  assert_eq(group.send(b"late"), Err(@session.UnexpectedMessage))
}

///|
test "send_object_datagram_for_track fans out to every subscription" {
  let publisher = publisher_announced()
  let track = publisher_track()
  cancellation_accept_peer_subscription(publisher, 1UL, 40, track, 1UL)
  cancellation_accept_peer_subscription(publisher, 3UL, 42, track, 1UL)
  let (tokens, actions) = publisher
    .send_object_datagram_for_track(
      track,
      update_datagram(1UL, 0UL, b"payload"),
    )
    .unwrap()
  assert_eq(tokens.length(), 2)
  let sent = actions.filter(fn(action) {
    match action {
      @session.SendDatagram(_, _) => true
      _ => false
    }
  })
  assert_eq(sent.length(), 2)
  for action in sent {
    match action {
      @session.SendDatagram(_, bytes) =>
        match @protocol.decode_datagram(bytes) {
          @protocol.Complete(@protocol.Object(datagram), _) => {
            assert_eq(datagram.track_alias, 1UL)
            assert_eq(datagram.object_id, 0UL)
            assert_eq(datagram.payload, b"payload")
          }
          _ => fail("expected an Object Datagram")
        }
      _ => fail("expected only datagrams")
    }
  }
}

///|
test "send_object_datagram_for_track reports an unknown track" {
  let publisher = publisher_started()
  assert_eq(
    publisher.send_object_datagram_for_track(
      { namespace_fields: [b"demo"], name: b"missing", },
      update_datagram(1UL, 0UL, b"payload"),
    ),
    Err(@session.UnknownRequest),
  )
}

///|
test "session queries resolve tracks and aliases for routing" {
  let publisher = publisher_announced()
  let track = publisher_track()
  assert_eq(publisher.track_for_request(0UL), Some(track))
  assert_eq(publisher.track_alias_for_request(0UL), Some(1UL))
  assert_eq(publisher.track_for_request(99UL), None)
  cancellation_accept_peer_subscription(publisher, 1UL, 40, track, 1UL)
  assert_eq(publisher.subscription_track(40), Some(track))
  assert_eq(publisher.subscription_track(99L), None)
}
