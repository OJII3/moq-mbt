///|
/// How the Subgroup ID is represented by a Subgroup stream header.
pub(all) enum SubgroupIdMode {
  Zero
  FirstObject
  Explicit(UInt64)
} derive(Debug, Eq)

///|
/// Status values defined by Draft 18 for subscription Objects.
pub(all) enum ObjectStatus {
  Normal
  EndOfGroup
  EndOfTrack
} derive(Debug, Eq)

///|
/// The decoded Subgroup stream header. A nil publisher priority means the
/// stream inherits the priority negotiated by the subscription.
pub(all) struct SubgroupHeader {
  track_alias : UInt64
  group_id : UInt64
  subgroup_id_mode : SubgroupIdMode
  publisher_priority : Byte?
  end_of_group : Bool
  first_object : Bool
  has_properties : Bool
} derive(Debug, Eq)

///|
/// One Object frame on a Subgroup stream. object_id is absolute in memory;
/// the wire format encodes it as an ID or a delta depending on stream position.
pub(all) struct SubgroupFrame {
  object_id : UInt64
  status : ObjectStatus
  properties : Bytes
  payload : Bytes
} derive(Debug, Eq)

///|
/// Encode a Subgroup stream header according to Draft 18.
pub fn encode_subgroup_header(header : SubgroupHeader) -> Bytes {
  let mode = match header.subgroup_id_mode {
    Zero => 0UL
    FirstObject => 1UL
    Explicit(_) => 2UL
  }
  let mut stream_type = 0x10UL | (mode << 1)
  if header.has_properties {
    stream_type = stream_type | 0x01UL
  }
  if header.end_of_group {
    stream_type = stream_type | 0x08UL
  }
  if header.publisher_priority is None {
    stream_type = stream_type | 0x20UL
  }
  if header.first_object {
    stream_type = stream_type | 0x40UL
  }
  let bytes : Array[Byte] = []
  for byte in encode_vi64(stream_type) {
    bytes.push(byte)
  }
  append_vi64s(bytes, header.track_alias)
  append_vi64s(bytes, header.group_id)
  match header.subgroup_id_mode {
    Explicit(subgroup_id) => append_vi64s(bytes, subgroup_id)
    _ => ()
  }
  match header.publisher_priority {
    Some(priority) => bytes.push(priority)
    None => ()
  }
  Bytes::from_array(bytes)
}

///|
/// Decode a Subgroup stream header and report the number of consumed bytes.
pub fn decode_subgroup_header(input : Bytes) -> DecodeResult[SubgroupHeader] {
  let (stream_type, first_next) = match read_vi64(input, 0, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  let subgroup_id_mode = (stream_type & 0x06UL) >> 1
  if stream_type > 0x7fUL ||
    (stream_type & 0x10UL) == 0UL ||
    subgroup_id_mode == 3UL {
    return Invalid(InvalidSubgroup)
  }
  let (track_alias, after_alias) = match
    read_vi64(input, first_next, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  let (group_id, after_group) = match
    read_vi64(input, after_alias, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  let mut next = after_group
  let subgroup_id_mode = if subgroup_id_mode == 0UL {
    Zero
  } else if subgroup_id_mode == 1UL {
    FirstObject
  } else {
    let (subgroup_id, after_subgroup_id) = match
      read_vi64(input, next, input.length()) {
      Some(pair) => pair
      None => return Incomplete
    }
    next = after_subgroup_id
    Explicit(subgroup_id)
  }
  let publisher_priority = if (stream_type & 0x20UL) != 0UL {
    None
  } else {
    if next >= input.length() {
      return Incomplete
    }
    let priority = input[next]
    next += 1
    Some(priority)
  }
  Complete(
    {
      track_alias,
      group_id,
      subgroup_id_mode,
      publisher_priority,
      end_of_group: (stream_type & 0x08UL) != 0UL,
      first_object: (stream_type & 0x40UL) != 0UL,
      has_properties: (stream_type & 0x01UL) != 0UL,
    },
    next,
  )
}

///|
/// Stateful decoder for the Object frames following one Subgroup header.
pub struct SubgroupDecoder {
  header : SubgroupHeader
  priv mut previous_object_id : UInt64?
  priv mut resolved_subgroup_id : UInt64?
}

///|
pub fn SubgroupDecoder::new(header : SubgroupHeader) -> SubgroupDecoder {
  let resolved_subgroup_id = match header.subgroup_id_mode {
    Zero => Some(0UL)
    Explicit(subgroup_id) => Some(subgroup_id)
    FirstObject => None
  }
  { header, previous_object_id: None, resolved_subgroup_id, }
}

///|
/// Return the subgroup ID once it can be determined from the header and first
/// Object. It is nil until the first Object for FirstObject mode.
pub fn SubgroupDecoder::subgroup_id(self : SubgroupDecoder) -> UInt64? {
  self.resolved_subgroup_id
}

///|
/// Decode the next complete Object frame. Incomplete input does not advance
/// the decoder, so callers can append bytes and retry the same frame.
pub fn SubgroupDecoder::decode_next(
  self : SubgroupDecoder,
  input : Bytes,
) -> DecodeResult[SubgroupFrame] {
  let (object_id_delta, after_delta) = match
    read_vi64(input, 0, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  let object_id = match self.previous_object_id {
    None => object_id_delta
    Some(previous) => {
      let max = 0xffffffffffffffffUL
      if previous == max || object_id_delta > max - previous - 1UL {
        return Invalid(InvalidSubgroup)
      }
      previous + object_id_delta + 1UL
    }
  }
  let mut next = after_delta
  let properties = if self.header.has_properties {
    let (properties_length, after_properties_length) = match
      read_vi64(input, next, input.length()) {
      Some(pair) => pair
      None => return Incomplete
    }
    if properties_length >
      (input.length() - after_properties_length).to_uint64() {
      return Incomplete
    }
    let properties_end = after_properties_length + properties_length.to_int()
    let value = input[after_properties_length:properties_end].to_owned()
    next = properties_end
    value
  } else {
    b""
  }
  let (payload_length, after_payload_length) = match
    read_vi64(input, next, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  next = after_payload_length
  let status = if payload_length == 0UL {
    let (status_value, after_status) = match
      read_vi64(input, next, input.length()) {
      Some(pair) => pair
      None => return Incomplete
    }
    next = after_status
    match status_value {
      0UL => Normal
      3UL => EndOfGroup
      4UL => EndOfTrack
      _ => return Invalid(InvalidSubgroup)
    }
  } else {
    Normal
  }
  if status != Normal && !properties.is_empty() {
    return Invalid(InvalidSubgroup)
  }
  if payload_length > (input.length() - next).to_uint64() {
    return Incomplete
  }
  let end = next + payload_length.to_int()
  let frame : SubgroupFrame = {
    object_id,
    status,
    properties,
    payload: input[next:end].to_owned(),
  }
  self.previous_object_id = Some(object_id)
  if self.resolved_subgroup_id is None {
    self.resolved_subgroup_id = Some(object_id)
  }
  Complete(frame, end)
}

///|
/// Encode one or more Objects with a Subgroup header. Object IDs are absolute
/// in `frames` and encoded as Draft 18 deltas on the wire.
pub fn encode_subgroup_stream(
  header : SubgroupHeader,
  frames : Array[SubgroupFrame],
) -> Result[Bytes, CodecError] {
  let bytes : Array[Byte] = []
  for byte in encode_subgroup_header(header) {
    bytes.push(byte)
  }
  let mut previous_object_id : UInt64? = None
  for frame in frames {
    if !header.has_properties && !frame.properties.is_empty() {
      return Err(InvalidSubgroup)
    }
    if frame.status != Normal &&
      (!frame.payload.is_empty() || !frame.properties.is_empty()) {
      return Err(InvalidSubgroup)
    }
    let object_id_delta = match previous_object_id {
      None => frame.object_id
      Some(previous) => {
        if frame.object_id <= previous {
          return Err(InvalidSubgroup)
        }
        frame.object_id - previous - 1UL
      }
    }
    append_vi64s(bytes, object_id_delta)
    if header.has_properties {
      append_vi64s(bytes, frame.properties.length().to_uint64())
      for byte in frame.properties {
        bytes.push(byte)
      }
    }
    append_vi64s(bytes, frame.payload.length().to_uint64())
    if frame.payload.is_empty() {
      let status_value = match frame.status {
        Normal => 0UL
        EndOfGroup => 3UL
        EndOfTrack => 4UL
      }
      append_vi64s(bytes, status_value)
    }
    for byte in frame.payload {
      bytes.push(byte)
    }
    previous_object_id = Some(frame.object_id)
  }
  Ok(Bytes::from_array(bytes))
}

///|
pub extend SubgroupIdMode with Debug::{to_repr}

///|
pub extend SubgroupIdMode with Eq::{equal, not_equal}

///|
pub extend ObjectStatus with Debug::{to_repr}

///|
pub extend ObjectStatus with Eq::{equal, not_equal}

///|
pub extend SubgroupHeader with Debug::{to_repr}

///|
pub extend SubgroupHeader with Eq::{equal, not_equal}

///|
pub extend SubgroupFrame with Debug::{to_repr}

///|
pub extend SubgroupFrame with Eq::{equal, not_equal}
