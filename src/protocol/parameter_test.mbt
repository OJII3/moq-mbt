///|
test "TrackName string helpers round-trip the common single-field shape" {
  let track = @protocol.TrackName::from_utf8_field("kaina", "input/hand_pose")
  assert_eq(track.namespace_fields, [b"kaina"])
  assert_eq(track.name, b"input/hand_pose")
  assert_eq(track.to_display_string(), "kaina/input/hand_pose")
}

///|
test "TrackName string helpers preserve multiple namespace fields" {
  let track = @protocol.TrackName::from_utf8_fields(["demo", "camera"], "video")
  assert_eq(track.namespace_fields, [b"demo", b"camera"])
  assert_eq(track.to_display_string(), "demo/camera/video")
}

///|
test "FORWARD parameter defaults to enabled and can be disabled" {
  assert_true(@protocol.forward_requested([]))
  assert_true(@protocol.forward_requested([@protocol.forward_parameter(true)]))
  assert_false(
    @protocol.forward_requested([@protocol.forward_parameter(false)]),
  )
}

///|
test "FORWARD parameter encodes as the Draft 18 parameter kind" {
  let parameter = @protocol.forward_parameter(false)
  assert_eq(parameter.kind, 0x10UL)
  assert_eq(parameter.value, @protocol.Byte(0))
}
