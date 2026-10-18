///|
test "SETUP exchange handles every split and preserves later control bytes" {
  let setup : @protocol.Setup = {
    options: [{ kind: 7, value: Data(b"test-relay"), }],
  }
  let frame = @protocol.encode_setup(setup).unwrap()
  for split = 0; split < frame.length(); split = split + 1 {
    let handshake = @session.ClientHandshake::new(Draft18, { options: [], }).unwrap()
    assert_eq(handshake.version(), Draft18)
    assert_eq(handshake.outbound_setup(), b"\xaf\x00\x00\x00")
    assert_eq(handshake.receive(42, frame[:split].to_owned()), Ok(Pending))
    assert_eq(
      handshake.receive(42, frame[split:].to_owned() + b"\x10\x00\x00"),
      Ok(Ready(setup, b"\x10\x00\x00")),
    )
    assert_eq(handshake.receive(42, b""), Err(AlreadyCompleted))
  }
}

///|
test "SETUP accepts a non-minimal type encoding" {
  let handshake = @session.ClientHandshake::new(Draft18, { options: [], }).unwrap()
  assert_eq(
    handshake.receive(1, b"\xc0\x2f\x00\x00\x00"),
    Ok(Ready({ options: [], }, b"")),
  )
}

///|
test "peer control stream identity is stable" {
  let handshake = @session.ClientHandshake::new(Draft18, { options: [], }).unwrap()
  assert_eq(handshake.receive(1, b"\xaf"), Ok(Pending))
  assert_eq(handshake.receive(2, b"\x00\x00\x00"), Err(DuplicateControlStream))
  assert_eq(handshake.control_closed().close_code(), 3)
}

///|
test "server cannot send client-only URI options" {
  for
    (kind, error) in [
      (1UL, @session.HandshakeError::InvalidServerPath),
      (5UL, @session.HandshakeError::InvalidServerAuthority),
    ] {
    let handshake = @session.ClientHandshake::new(Draft18, { options: [], }).unwrap()
    let frame = @protocol.encode_setup({
      options: [{ kind, value: Data(b"unexpected"), }],
    }).unwrap()
    assert_eq(handshake.receive(1, frame), Err(error))
  }
  assert_eq(@session.HandshakeError::InvalidServerPath.close_code(), 0x8)
  assert_eq(@session.HandshakeError::InvalidServerAuthority.close_code(), 0x19)
}

///|
test "malformed SETUP and premature control close are reported" {
  let handshake = @session.ClientHandshake::new(Draft18, { options: [], }).unwrap()
  assert_eq(
    handshake.receive(1, b"\xaf\x00\x00\x01\x01"),
    Err(InvalidSetup(MalformedOptions)),
  )
  assert_eq(handshake.control_closed(), ControlStreamClosed)
}

///|
test "local SETUP is snapshotted before caller mutation" {
  let setup : @protocol.Setup = { options: [], }
  let handshake = @session.ClientHandshake::new(Draft18, setup).unwrap()
  setup.options.push({ kind: 7, value: Data(b"changed"), })
  assert_eq(handshake.outbound_setup(), b"\xaf\x00\x00\x00")
}

///|
test "maximum SETUP frame preserves bytes beyond the receive buffer limit" {
  let setup : @protocol.Setup = {
    options: [{ kind: 9, value: Data(Bytes::make(65531, 0)), }],
  }
  let frame = @protocol.encode_setup(setup).unwrap()
  let nonminimal = b"\xff\x00\x00\x00\x00\x00\x00\x2f\x00" +
    frame[2:].to_owned()
  assert_eq(nonminimal.length(), 65546)
  let handshake = @session.ClientHandshake::new(Draft18, { options: [], }).unwrap()
  assert_eq(handshake.receive(1, nonminimal[:100].to_owned()), Ok(Pending))
  assert_eq(
    handshake.receive(1, nonminimal[100:].to_owned() + b"next message"),
    Ok(Ready(setup, b"next message")),
  )
}
