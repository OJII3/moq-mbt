///|
/// A normal Object delivered on a Subgroup stream.
pub(all) struct SubgroupObject {
  track_alias : UInt64
  group_id : UInt64
  object_id : UInt64
  end_of_group : Bool
  payload : Bytes
} derive(Debug, Eq)

///|
/// Encode one reliable Subgroup stream carrying a single Object. The stream
/// uses subgroup ID 0, inherited priority, and marks this as the first Object.
pub fn encode_one_object(
  track_alias : UInt64,
  group_id : UInt64,
  object_id : UInt64,
  payload : Bytes,
  end_of_group~ : Bool = true,
) -> Bytes {
  let message_type = if end_of_group { 0x78UL } else { 0x70UL }
  let object = encode_vi64(object_id) +
    encode_vi64(payload.length().to_uint64()) +
    (if payload.is_empty() { encode_vi64(0) } else { b"" }) +
    payload
  encode_vi64(message_type) +
  encode_vi64(track_alias) +
  encode_vi64(group_id) +
  object
}

///|
/// Decode the first Object from a Subgroup stream. The parser accepts stream
/// data split at any byte and reports its consumed length for following objects.
pub fn decode_subgroup_object(input : Bytes) -> DecodeResult[SubgroupObject] {
  let (message_type, first_next) = match read_vi64(input, 0, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  let subgroup_id_mode = (message_type & 0x06UL) >> 1
  if (message_type & 0x80UL) != 0UL ||
    (message_type & 0x10UL) == 0UL ||
    subgroup_id_mode == 3UL ||
    (message_type & 0x40UL) == 0UL {
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
  let mut object_header = after_group
  if subgroup_id_mode == 2UL {
    let (_, after_subgroup_id) = match
      read_vi64(input, object_header, input.length()) {
      Some(pair) => pair
      None => return Incomplete
    }
    object_header = after_subgroup_id
  }
  if (message_type & 0x20UL) == 0UL {
    if object_header >= input.length() {
      return Incomplete
    }
    object_header += 1
  }
  if (message_type & 0x01UL) != 0UL {
    let (properties_length, after_properties_length) = match
      read_vi64(input, object_header, input.length()) {
      Some(pair) => pair
      None => return Incomplete
    }
    if properties_length >
      (input.length() - after_properties_length).to_uint64() {
      return Incomplete
    }
    object_header = after_properties_length + properties_length.to_int()
  }
  let (object_id, after_object_id) = match
    read_vi64(input, object_header, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  let (payload_length, after_length) = match
    read_vi64(input, after_object_id, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  let mut payload_offset = after_length
  if payload_length == 0UL {
    let (status, after_status) = match
      read_vi64(input, payload_offset, input.length()) {
      Some(pair) => pair
      None => return Incomplete
    }
    if status != 0UL {
      return Invalid(InvalidSubgroup)
    }
    payload_offset = after_status
  }
  if payload_length > (input.length() - payload_offset).to_uint64() {
    return Incomplete
  }
  let end = payload_offset + payload_length.to_int()
  Complete(
    {
      track_alias,
      group_id,
      object_id,
      end_of_group: (message_type & 0x08UL) != 0UL,
      payload: input[payload_offset:end].to_owned(),
    },
    end,
  )
}

///|
pub extend SubgroupObject with Debug::{to_repr}

///|
pub extend SubgroupObject with Eq::{equal, not_equal}
