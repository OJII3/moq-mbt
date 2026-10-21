///|
test "session action driver retains and retries a refused send in order" {
  let first = @session.Send(4L, b"first", false)
  let second = @session.Send(4L, b"second", true)
  let driver = SessionActionDriver::new()
  driver.pending = [first, second]
  let mut refuse_once = true
  let injected_send = fn(
    _stream : Int64,
    _bytes : Bytes,
    _fin : Bool,
  ) raise @transport.TransportError {
    if refuse_once {
      refuse_once = false
      raise @transport.SendQueueFull
    }
  }

  assert_false(driver.send_head(injected_send))
  assert_eq(driver.pending, [first, second])
  assert_true(driver.send_head(injected_send))
  assert_eq(driver.pending, [second])
  assert_true(driver.send_head(injected_send))
  assert_eq(driver.pending, [])
}
