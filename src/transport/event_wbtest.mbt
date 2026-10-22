///|
test "datagram send states distinguish interim and final results" {
  assert_true(!Sent.is_final())
  assert_true(!LostSuspect.is_final())
  assert_true(!Unknown(77).is_final())
  assert_true(LostDiscarded.is_final())
  assert_true(Acknowledged.is_final())
  assert_true(AcknowledgedSpurious.is_final())
  assert_true(Canceled.is_final())
  assert_eq(datagram_send_state_from_code(4), Acknowledged)
  assert_eq(datagram_send_state_from_code(5), AcknowledgedSpurious)
  assert_eq(datagram_send_state_from_code(88), Unknown(88))
}

///|
test "datagram send preflight reports disabled, size, and queue limits" {
  let capability : DatagramSendCapability = { enabled: false, max_length: 0, }
  let disabled = try {
    validate_datagram_send(1, capability, 0, 0)
    0
  } catch {
    DatagramSendUnavailable => 1
    _ => 2
  }
  assert_eq(disabled, 1)

  let capability : DatagramSendCapability = { enabled: true, max_length: 5, }
  let oversized = try {
    validate_datagram_send(6, capability, 0, 0)
    0
  } catch {
    DatagramTooLarge(actual, maximum) if actual == 6 && maximum == 5 => 1
    _ => 2
  }
  assert_eq(oversized, 1)

  let queue_full = try {
    validate_datagram_send(1, capability, 0, max_datagram_sends)
    0
  } catch {
    DatagramSendQueueFull => 1
    _ => 2
  }
  assert_eq(queue_full, 1)
}

///|
test "stream send completion releases only its matching queued buffer" {
  let client : Client = {
    raw: None,
    streams: {},
    send_sizes: [(7L, 4), (11L, 6)],
    datagram_send_sizes: {},
    next_datagram_id: 1L,
    pending_send_bytes: 10,
    pending_datagram_bytes: 0,
    pending_datagrams: 0,
  }
  client.record_send_complete(11L)
  assert_eq(client.pending_send_bytes, 4)
  assert_eq(client.send_sizes, [(7L, 4)])

  client.record_send_complete(99L)
  assert_eq(client.pending_send_bytes, 4)
  assert_eq(client.send_sizes, [(7L, 4)])
}

///|
test "interim datagram states retain buffers and final states release them" {
  let client : Client = {
    raw: None,
    streams: {},
    send_sizes: [],
    datagram_send_sizes: {},
    next_datagram_id: 3L,
    pending_send_bytes: 0,
    pending_datagram_bytes: 9,
    pending_datagrams: 2,
  }
  client.datagram_send_sizes[1L] = 5
  client.datagram_send_sizes[2L] = 4

  client.record_datagram_send_state(1L, LostSuspect)
  assert_eq(client.pending_datagram_bytes, 9)
  assert_eq(client.pending_datagrams, 2)

  client.record_datagram_send_state(1L, Acknowledged)
  assert_eq(client.pending_datagram_bytes, 4)
  assert_eq(client.pending_datagrams, 1)
  assert_true(client.datagram_send_sizes.get(1L) is None)
  assert_eq(client.datagram_send_sizes.get(2L), Some(4))

  let unknown_final = try {
    client.record_datagram_send_state(99L, Canceled)
    false
  } catch {
    TransportError(_) => true
    _ => false
  }
  assert_true(unknown_final)
}
