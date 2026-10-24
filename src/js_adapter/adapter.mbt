///|
pub type JsReadableStream = @streams.ReadableStream

///|
/// Convert protocol bytes to a JavaScript Uint8Array view.
/// The returned view aliases the immutable MoonBit Bytes storage.
pub fn bytes_to_uint8array(bytes : Bytes) -> @arraybuffer.Uint8Array {
  @arraybuffer.Uint8Array::from_bytes(bytes)
}

///|
/// Convert a received Uint8Array view to Bytes without copying.
/// Copy the bytes before retaining them if the JavaScript side may reuse or
/// mutate the underlying buffer.
pub fn uint8array_to_bytes(view : @arraybuffer.Uint8Array) -> Bytes {
  view.to_bytes()
}

///|
/// WebTransport uses the WebTransport protocol negotiation extension for the
/// application protocol. Draft 18 identifies MOQT with "moqt-18".
pub let moqt_draft18_protocol : String = "moqt-18"
