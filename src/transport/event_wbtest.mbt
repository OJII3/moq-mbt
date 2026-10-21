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
