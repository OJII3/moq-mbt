///|
fn padding_datagram(length : Int32) -> Bytes {
  b"\xf0\x13\x2b\x3e\x29" + Bytes::make(length - 5, 0)
}

///|
fn wait_for_datagram_final(
  client : @transport.Client,
  id : Int64,
) -> @transport.DatagramSendState raise {
  let deadline = @env.now() + 10000UL
  while @env.now() < deadline {
    match client.next_event(100) {
      @transport.DatagramSendStateChanged(event_id, state) if event_id == id =>
        if state.is_final() {
          return state
        }
      @transport.QueueOverflow => fail("transport event buffer overflow")
      @transport.Closed(code) => fail("relay closed connection (\{code})")
      @transport.TransportClosed(status) =>
        fail("QUIC failed (MsQuic status \{status})")
      _ => ()
    }
  }
  fail("timed out waiting for datagram send state")
}

///|
/// Exercise the real MsQuic send limit, local queue bound, final callback, and
/// close path against the configured Draft 18 relay.
fn run_datagram_smoke(
  host : String,
  port : Int32,
  path : String,
  insecure : Bool,
) -> Unit raise {
  let (client, _) = setup_client(host, port, path, insecure)
  defer client.close()

  let capability = client.datagram_send_capability()
  guard capability.enabled else {
    fail("relay did not enable QUIC datagram sending")
  }
  guard capability.max_length >= 5 else {
    fail("relay reported an unusable maximum datagram length")
  }
  println("Datagram sending enabled (maximum \{capability.max_length} bytes)")

  let id = client.send_datagram(padding_datagram(capability.max_length))
  let mut rejected = false
  ignore(client.send_datagram(padding_datagram(capability.max_length + 1))) catch {
    @transport.DatagramTooLarge(actual, maximum) if actual ==
      capability.max_length + 1 &&
      maximum == capability.max_length => rejected = true
    error => fail("unexpected oversized datagram result: \{error.to_repr()}")
  }
  guard rejected else { fail("oversized datagram was accepted") }
  println("Maximum-size boundary accepted; one byte over was rejected")

  let final_state = wait_for_datagram_final(client, id)
  println("Datagram send state: \{final_state.to_repr()}")

  let mut queue_full = false
  for _ in 0..<256 {
    ignore(client.send_datagram(padding_datagram(5))) catch {
      @transport.DatagramSendQueueFull => {
        queue_full = true
        break
      }
      error => fail("unexpected datagram queue result: \{error.to_repr()}")
    }
  }
  guard queue_full else { fail("datagram send queue limit was not enforced") }
  println("Datagram send queue limit enforced")

  // Close with final send states still queued; shutdown completion owns any
  // remaining native send buffers.
  client.close()
}
