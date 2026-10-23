///|
/// FETCH data streams begin with a request-scoped header instead of a
/// Track Alias routed Subgroup header.
pub(all) struct FetchHeader {
  request_id : UInt64
} derive(Debug, Eq)

///|
pub fn encode_fetch_header(header : FetchHeader) -> Bytes {
  let bytes : Array[Byte] = []
  append_vi64s(bytes, 0x05UL)
  append_vi64s(bytes, header.request_id)
  Bytes::from_array(bytes)
}

///|
pub fn decode_fetch_header(input : Bytes) -> DecodeResult[FetchHeader] {
  let (kind, after_kind) = match read_vi64(input, 0, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  if kind != 0x05UL {
    return Invalid(UnexpectedMessageType(kind))
  }
  let (request_id, end) = match read_vi64(input, after_kind, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  Complete({ request_id, }, end)
}

///|
pub(all) enum FetchGroupOrder {
  Ascending
  Descending
} derive(Debug, Eq)

///|
/// Object contents carried by a FETCH data stream. `subgroup_id` is nil for
/// datagram-forwarded Objects; publisher priority is resolved from stream state.
pub(all) struct FetchObject {
  group_id : UInt64
  subgroup_id : UInt64?
  object_id : UInt64
  publisher_priority : Byte
  datagram_forwarding : Bool
  properties : Bytes
  payload : Bytes
} derive(Debug, Eq)

///|
pub(all) enum FetchRangeKind {
  EndOfNonExistentRange
  EndOfUnknownRange
} derive(Debug, Eq)

///|
pub(all) struct FetchRangeEnd {
  group_id : UInt64
  object_id : UInt64
  kind : FetchRangeKind
} derive(Debug, Eq)

///|
pub(all) enum FetchDataFrame {
  FetchDataObject(FetchObject)
  FetchDataRangeEnd(FetchRangeEnd)
} derive(Debug, Eq)

///|
/// Stateful parser for the sequence following a FETCH_HEADER. Each call
/// consumes at most one complete frame; incomplete input leaves all state
/// unchanged so the same bytes can be retried after more data arrives.
pub struct FetchDataDecoder {
  group_order : FetchGroupOrder
  priv mut previous_group_id : UInt64?
  priv mut previous_object_id : UInt64?
  priv mut previous_subgroup_id : UInt64?
  priv mut previous_priority : Byte?
}

///|
pub fn FetchDataDecoder::new(group_order : FetchGroupOrder) -> FetchDataDecoder {
  {
    group_order,
    previous_group_id: None,
    previous_object_id: None,
    previous_subgroup_id: None,
    previous_priority: None,
  }
}

///|
pub fn FetchDataDecoder::decode_next(
  self : FetchDataDecoder,
  input : Bytes,
) -> DecodeResult[FetchDataFrame] {
  let (flags, after_flags) = match read_vi64(input, 0, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  let range_kind = if flags == 0x8cUL {
    Some(EndOfNonExistentRange)
  } else if flags == 0x10cUL {
    Some(EndOfUnknownRange)
  } else {
    None
  }
  if flags > 0x7fUL && range_kind is None {
    return Invalid(InvalidFetchObject)
  }
  let has_group_delta = (flags & 0x08UL) != 0UL
  let has_object_delta = (flags & 0x04UL) != 0UL
  let subgroup_mode = flags & 0x03UL
  let is_range_end = range_kind is Some(_)
  if is_range_end && (!has_group_delta || !has_object_delta) {
    return Invalid(InvalidFetchObject)
  }
  if self.previous_group_id is None && (!has_group_delta || !has_object_delta) {
    return Invalid(InvalidFetchObject)
  }
  if self.previous_group_id is None &&
    !is_range_end &&
    (
      (
        (flags & 0x40UL) == 0UL &&
        (subgroup_mode == 1UL || subgroup_mode == 2UL)
      ) ||
      (flags & 0x10UL) == 0UL
    ) {
    return Invalid(InvalidFetchObject)
  }
  let mut next = after_flags
  let group_value = if has_group_delta {
    let (value, end) = match read_vi64(input, next, input.length()) {
      Some(pair) => pair
      None => return Incomplete
    }
    next = end
    Some(value)
  } else {
    None
  }
  let explicit_subgroup = if !is_range_end &&
    (flags & 0x40UL) == 0UL &&
    subgroup_mode == 3UL {
    let (value, end) = match read_vi64(input, next, input.length()) {
      Some(pair) => pair
      None => return Incomplete
    }
    next = end
    Some(value)
  } else {
    None
  }
  let object_delta = if has_object_delta {
    let (value, end) = match read_vi64(input, next, input.length()) {
      Some(pair) => pair
      None => return Incomplete
    }
    next = end
    Some(value)
  } else {
    None
  }
  let priority = if !is_range_end && (flags & 0x10UL) != 0UL {
    if next >= input.length() {
      return Incomplete
    }
    let value = input[next]
    next += 1
    Some(value)
  } else {
    None
  }
  let properties = if !is_range_end && (flags & 0x20UL) != 0UL {
    match read_bytes_at(input, next, input.length()) {
      Some((value, end)) => {
        next = end
        value
      }
      None => return Incomplete
    }
  } else {
    b""
  }
  let group_id = if is_range_end {
    group_value.unwrap()
  } else {
    match (self.previous_group_id, group_value) {
      (None, Some(value)) => value
      (Some(_), None) => self.previous_group_id.unwrap()
      (Some(previous), Some(delta)) =>
        match self.group_order {
          Ascending => {
            if delta >= 0xffffffffffffffffUL - previous {
              return Invalid(InvalidFetchObject)
            }
            previous + delta + 1UL
          }
          Descending => {
            if delta >= previous {
              return Invalid(InvalidFetchObject)
            }
            previous - delta - 1UL
          }
        }
      _ => return Invalid(InvalidFetchObject)
    }
  }
  let object_id = match (self.previous_object_id, object_delta) {
    (None, Some(value)) => value
    (Some(_), Some(delta)) if has_group_delta => delta
    (Some(previous), Some(delta)) => {
      if delta > 0xffffffffffffffffUL - previous {
        return Invalid(InvalidFetchObject)
      }
      previous + delta
    }
    (Some(previous), None) => {
      if previous == 0xffffffffffffffffUL {
        return Invalid(InvalidFetchObject)
      }
      previous + 1UL
    }
    _ => return Invalid(InvalidFetchObject)
  }
  if !is_range_end &&
    self.previous_group_id == Some(group_id) &&
    self.previous_object_id is Some(previous) &&
    object_id <= previous {
    return Invalid(InvalidFetchObject)
  }
  if is_range_end {
    let range = FetchDataRangeEnd({
      group_id,
      object_id,
      kind: range_kind.unwrap(),
    })
    self.previous_group_id = Some(group_id)
    self.previous_object_id = Some(object_id)
    return Complete(range, next)
  }
  let datagram_forwarding = (flags & 0x40UL) != 0UL
  let subgroup_id = if datagram_forwarding {
    None
  } else {
    match subgroup_mode {
      0UL => Some(0UL)
      1UL => self.previous_subgroup_id
      2UL =>
        match self.previous_subgroup_id {
          Some(previous) if previous < 0xffffffffffffffffUL =>
            Some(previous + 1UL)
          _ => None
        }
      _ => explicit_subgroup
    }
  }
  if !datagram_forwarding && subgroup_id is None {
    return Invalid(InvalidFetchObject)
  }
  let publisher_priority = if (flags & 0x10UL) != 0UL {
    priority
  } else {
    self.previous_priority
  }
  let publisher_priority = match publisher_priority {
    Some(value) => value
    None => return Invalid(InvalidFetchObject)
  }
  let (payload_length, after_payload_length) = match
    read_vi64(input, next, input.length()) {
    Some(pair) => pair
    None => return Incomplete
  }
  next = after_payload_length
  if payload_length > (input.length() - next).to_uint64() {
    return Incomplete
  }
  let end = next + payload_length.to_int()
  let frame = FetchDataObject({
    group_id,
    subgroup_id,
    object_id,
    publisher_priority,
    datagram_forwarding,
    properties,
    payload: input[next:end].to_owned(),
  })
  self.previous_group_id = Some(group_id)
  self.previous_object_id = Some(object_id)
  if !datagram_forwarding {
    self.previous_subgroup_id = subgroup_id
  }
  self.previous_priority = Some(publisher_priority)
  Complete(frame, end)
}

///|
/// A graceful FIN is valid only at a frame boundary.
pub fn FetchDataDecoder::finish(
  self : FetchDataDecoder,
  trailing : Bytes,
) -> Result[Unit, CodecError] {
  let check : FetchDataDecoder = {
    group_order: self.group_order,
    previous_group_id: self.previous_group_id,
    previous_object_id: self.previous_object_id,
    previous_subgroup_id: self.previous_subgroup_id,
    previous_priority: self.previous_priority,
  }
  let mut offset = 0
  while offset < trailing.length() {
    match check.decode_next(trailing[offset:].to_owned()) {
      Incomplete => return Err(InvalidFetchObject)
      Invalid(error) => return Err(error)
      Complete(_, consumed) => offset += consumed
    }
  }
  Ok(())
}

///|
/// Stateful encoder counterpart to FetchDataDecoder.
pub struct FetchDataEncoder {
  group_order : FetchGroupOrder
  priv mut previous_group_id : UInt64?
  priv mut previous_object_id : UInt64?
  priv mut previous_subgroup_id : UInt64?
  priv mut previous_priority : Byte?
}

///|
pub fn FetchDataEncoder::new(group_order : FetchGroupOrder) -> FetchDataEncoder {
  {
    group_order,
    previous_group_id: None,
    previous_object_id: None,
    previous_subgroup_id: None,
    previous_priority: None,
  }
}

///|
pub fn FetchDataEncoder::encode(
  self : FetchDataEncoder,
  frame : FetchDataFrame,
) -> Result[Bytes, CodecError] {
  let bytes : Array[Byte] = []
  let mut flags = 0UL
  let group_id = match frame {
    FetchDataObject(object) => object.group_id
    FetchDataRangeEnd(range) => range.group_id
  }
  let object_id = match frame {
    FetchDataObject(object) => object.object_id
    FetchDataRangeEnd(range) => range.object_id
  }
  let is_range_end = match frame {
    FetchDataRangeEnd(_) => true
    _ => false
  }
  let group_delta = if is_range_end {
    group_id
  } else {
    match self.previous_group_id {
      None => group_id
      Some(previous) =>
        if self.group_order == Ascending {
          if group_id <= previous {
            if group_id != previous {
              return Err(InvalidFetchObject)
            }
            0UL
          } else {
            group_id - previous - 1UL
          }
        } else if group_id >= previous {
          if group_id != previous {
            return Err(InvalidFetchObject)
          }
          0UL
        } else {
          previous - group_id - 1UL
        }
    }
  }
  let group_changed = self.previous_group_id is None ||
    group_id != self.previous_group_id.unwrap()
  if group_changed || is_range_end {
    flags = flags | 0x08UL
  }
  match frame {
    FetchDataRangeEnd(range) => {
      flags = if range.kind == EndOfNonExistentRange { 0x8cUL } else { 0x10cUL }
      // Range end fields are always explicit, even if the group is unchanged.
      let fields : Array[Byte] = []
      append_vi64s(fields, group_delta)
      append_vi64s(fields, object_id)
      append_vi64s(bytes, flags)
      for byte in fields {
        bytes.push(byte)
      }
    }
    FetchDataObject(object) => {
      let subgroup_mode = if object.datagram_forwarding {
        0UL
      } else {
        match (self.previous_subgroup_id, object.subgroup_id) {
          (_, Some(0UL)) => 0UL
          (Some(previous), Some(current)) if previous == current => 1UL
          (Some(previous), Some(current)) if previous < 0xffffffffffffffffUL &&
            current == previous + 1UL => 2UL
          (_, Some(_)) => 3UL
          _ => return Err(InvalidFetchObject)
        }
      }
      flags = flags | subgroup_mode
      if object.datagram_forwarding {
        flags = flags | 0x40UL
      }
      let object_delta = match self.previous_object_id {
        None => {
          flags = flags | 0x04UL
          object.object_id
        }
        Some(_) if group_changed => {
          flags = flags | 0x04UL
          object.object_id
        }
        Some(previous) => {
          if object.object_id <= previous {
            return Err(InvalidFetchObject)
          }
          object.object_id - previous
        }
      }
      if !group_changed && self.previous_object_id is None {
        return Err(InvalidFetchObject)
      }
      if group_changed || self.previous_group_id is None {
        flags = flags | 0x08UL
      }
      let include_priority = self.previous_priority is None ||
        Some(object.publisher_priority) != self.previous_priority
      if include_priority {
        flags = flags | 0x10UL
      }
      if !object.properties.is_empty() {
        flags = flags | 0x20UL
      }
      // Flags precede all optional fields; stage the remaining encoding first.
      let fields : Array[Byte] = []
      if group_changed || self.previous_group_id is None {
        append_vi64s(fields, group_delta)
      }
      if subgroup_mode == 3UL && !object.datagram_forwarding {
        append_vi64s(fields, object.subgroup_id.unwrap())
      }
      if (flags & 0x04UL) != 0UL {
        append_vi64s(fields, object_delta)
      } else if object_delta != 1UL {
        flags = flags | 0x04UL
        append_vi64s(fields, object_delta)
      }
      if include_priority {
        fields.push(object.publisher_priority)
      }
      if !object.properties.is_empty() {
        append_vi64s(fields, object.properties.length().to_uint64())
        for byte in object.properties {
          fields.push(byte)
        }
      }
      append_vi64s(fields, object.payload.length().to_uint64())
      for byte in object.payload {
        fields.push(byte)
      }
      append_vi64s(bytes, flags)
      for byte in fields {
        bytes.push(byte)
      }
      self.previous_group_id = Some(group_id)
      self.previous_object_id = Some(object_id)
      if !object.datagram_forwarding {
        self.previous_subgroup_id = object.subgroup_id
      }
      self.previous_priority = Some(object.publisher_priority)
      return Ok(Bytes::from_array(bytes))
    }
  }
  if is_range_end {
    self.previous_group_id = Some(group_id)
    self.previous_object_id = Some(object_id)
    return Ok(Bytes::from_array(bytes))
  }
  Err(InvalidFetchObject)
}

///|
pub extend FetchHeader with Debug::{to_repr}

///|
pub extend FetchHeader with Eq::{equal, not_equal}

///|
pub extend FetchGroupOrder with Debug::{to_repr}

///|
pub extend FetchGroupOrder with Eq::{equal, not_equal}

///|
pub extend FetchObject with Debug::{to_repr}

///|
pub extend FetchObject with Eq::{equal, not_equal}

///|
pub extend FetchRangeKind with Debug::{to_repr}

///|
pub extend FetchRangeKind with Eq::{equal, not_equal}

///|
pub extend FetchRangeEnd with Debug::{to_repr}

///|
pub extend FetchRangeEnd with Eq::{equal, not_equal}

///|
pub extend FetchDataFrame with Debug::{to_repr}

///|
pub extend FetchDataFrame with Eq::{equal, not_equal}
