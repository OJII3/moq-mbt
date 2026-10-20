///|
/// One Object carried by a Draft 18 OBJECT_DATAGRAM.
pub(all) struct ObjectDatagram {
  track_alias : UInt64
  group_id : UInt64
  object_id : UInt64
  publisher_priority : Byte?
  end_of_group : Bool
  properties : Bytes
  status : ObjectStatus
  payload : Bytes
} derive(Debug, Eq)

///|
pub extend ObjectDatagram with Debug::{to_repr}

///|
pub extend ObjectDatagram with Eq::{equal, not_equal}

///|
/// Encode one Draft 18 OBJECT_DATAGRAM.
pub fn encode_object_datagram(
  datagram : ObjectDatagram,
) -> Result[Bytes, CodecError] {
  let has_properties = !datagram.properties.is_empty()
  let has_status = datagram.status != Normal || datagram.payload.is_empty()
  if datagram.status != Normal &&
    (!datagram.payload.is_empty() || has_properties || datagram.end_of_group) {
    return Err(InvalidDatagram)
  }
  let bytes : Array[Byte] = []
  let mut kind = 0UL
  if has_properties {
    kind = kind | 0x01UL
  }
  if datagram.end_of_group {
    kind = kind | 0x02UL
  }
  if datagram.object_id == 0UL {
    kind = kind | 0x04UL
  }
  if datagram.publisher_priority is None {
    kind = kind | 0x08UL
  }
  if has_status {
    kind = kind | 0x20UL
  }
  append_vi64s(bytes, kind)
  append_vi64s(bytes, datagram.track_alias)
  append_vi64s(bytes, datagram.group_id)
  if datagram.object_id != 0UL {
    append_vi64s(bytes, datagram.object_id)
  }
  match datagram.publisher_priority {
    Some(priority) => bytes.push(priority)
    None => ()
  }
  if has_properties {
    append_vi64s(bytes, datagram.properties.length().to_uint64())
    for byte in datagram.properties {
      bytes.push(byte)
    }
  }
  if has_status {
    let status = match datagram.status {
      Normal => 0UL
      EndOfGroup => 3UL
      EndOfTrack => 4UL
    }
    append_vi64s(bytes, status)
  } else {
    for byte in datagram.payload {
      bytes.push(byte)
    }
  }
  Ok(Bytes::from_array(bytes))
}

///|
/// Decode one complete Draft 18 OBJECT_DATAGRAM.
pub fn decode_object_datagram(input : Bytes) -> DecodeResult[ObjectDatagram] {
  let (kind, after_kind) = match read_vi64(input, 0, input.length()) {
    Some(value) => value
    None => return Incomplete
  }
  if (kind & 0xd0UL) != 0UL ||
    ((kind & 0x20UL) != 0UL && (kind & 0x02UL) != 0UL) {
    return Invalid(InvalidDatagram)
  }
  let (track_alias, after_alias) = match
    read_vi64(input, after_kind, input.length()) {
    Some(value) => value
    None => return Incomplete
  }
  let (group_id, after_group) = match
    read_vi64(input, after_alias, input.length()) {
    Some(value) => value
    None => return Incomplete
  }
  let mut next = after_group
  let object_id = if (kind & 0x04UL) != 0UL {
    0UL
  } else {
    let (value, after_value) = match read_vi64(input, next, input.length()) {
      Some(value) => value
      None => return Incomplete
    }
    next = after_value
    value
  }
  let publisher_priority = if (kind & 0x08UL) != 0UL {
    None
  } else {
    if next >= input.length() {
      return Incomplete
    }
    let value = Some(input[next])
    next = next + 1
    value
  }
  let properties = if (kind & 0x01UL) != 0UL {
    let (length, after_length) = match read_vi64(input, next, input.length()) {
      Some(value) => value
      None => return Incomplete
    }
    if length == 0UL || length > (input.length() - after_length).to_uint64() {
      return Invalid(InvalidDatagram)
    }
    let end = after_length + length.to_int()
    next = end
    input[after_length:end].to_owned()
  } else {
    b""
  }
  let status = if (kind & 0x20UL) != 0UL {
    let (value, after_status) = match read_vi64(input, next, input.length()) {
      Some(value) => value
      None => return Incomplete
    }
    if after_status != input.length() {
      return Invalid(InvalidDatagram)
    }
    match value {
      0UL => Normal
      3UL => EndOfGroup
      4UL => EndOfTrack
      _ => return Invalid(InvalidDatagram)
    }
  } else {
    if next == input.length() {
      return Invalid(InvalidDatagram)
    }
    Normal
  }
  if status != Normal && !properties.is_empty() {
    return Invalid(InvalidDatagram)
  }
  let payload = if (kind & 0x20UL) != 0UL {
    b""
  } else {
    input[next:].to_owned()
  }
  Complete(
    {
      track_alias,
      group_id,
      object_id,
      publisher_priority,
      end_of_group: (kind & 0x02UL) != 0UL,
      properties,
      status,
      payload,
    },
    input.length(),
  )
}
