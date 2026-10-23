# WebTransport adapter

`webtransport_connect` accepts a `moqt://` URI, preserves its authority, path,
and query, and opens the corresponding `https://` URL. It offers the WebTransport
subprotocol `moqt-18`, as required by MOQT Draft 18. Once `ready` fulfills, the
server-selected `protocol` must equal `moqt-18`; an empty or different selection
returns `Err(WebTransportFailure)`. The relay must advertise and select this protocol through
WebTransport's `WT-Available-Protocols` negotiation.

WebTransport is available only in secure contexts. If the API is missing, the
module still imports; `webtransport_connect` returns the constructor error. The
binding also requires the `protocols` constructor option and the `protocol`
property. As of 2026-10-23, those negotiation features are supported by Chrome
143+, Edge 143+, Firefox 155+, and Safari 26.4+. Browser support changes over
time; check the browser's WebTransport support before connecting.

By default, certificate verification uses the browser's Web PKI. For an
explicitly trusted local or ephemeral certificate, call
`webtransport_connect_with_certificate_hashes` with SHA-256 certificate hashes
as `Uint8Array` values. This opts into the WebTransport certificate-hash
mechanism and its browser restrictions; it does not disable TLS verification.

`ready()` resolves to `Ok(protocol)` or `Err(WebTransportFailure)`, including
typed error name, message, source, and stream error code. `wait_ready(timeout_ms)`
returns `Ok(false)` and closes a connection that did not become ready before the
deadline. Calling `close` while connecting cancels the connection. The returned
`closed` promise resolves once with `LocalClose`, `PeerClose`, or `Failed`.
`close` is idempotent, accepts only unsigned 32-bit codes, and rejects reasons
whose UTF-8 encoding exceeds the WebTransport API's 1024-byte limit.

## Streams

`open_unidirectional()` and `open_bidirectional()` resolve to opaque `Int64`
references. Peer streams use the same reference space. Consume
`next_stream_event()` for `StreamStarted`, `Data`, `StreamFinished`,
`StreamReset`, `SendStopped`, `StreamQueueOverflow`, and `ConnectionClosed`.
`StreamStarted` is queued before that stream's first data or finish event.
Each incoming stream has its own reader loop, so a stalled stream does not
block other streams.

Call `write_stream(reference, bytes, fin=...)` to send in call order. It waits
for `writer.ready` before each write and closes the send side after a final
write. A successful result means the browser accepted the write; WebTransport
does not expose QUIC acknowledgement for this operation. Call
`cancel_stream_read(reference, error_code)` to stop the receive side or
`abort_stream_write(reference, error_code)` to reset the send side.

The adapter bounds active streams and pending stream opens (16 each), queued
writes (256 and 1 MiB), and queued stream events (256 and 1 MiB of data).
Exceeding an event queue limit stops the affected stream and emits
`StreamQueueOverflow`. Event data is copied from the exact Uint8Array view
range into MoonBit-owned bytes.

## Datagrams

`datagram_capability()` reports whether the browser exposes WebTransport
datagrams and its current maximum payload size. `send_datagram(bytes)` writes
those bytes directly; the adapter adds no MoQ or HTTP Datagram framing. The
send queue is limited to 64 datagrams and 1 MiB. A successful result means the
local Web Streams write completed. Datagrams are best effort: this does not
confirm QUIC acknowledgement, peer receipt, ordering, or application delivery.
Unsupported datagrams, oversized payloads, a full send queue, connection close,
and browser write rejection return distinct errors.

`next_datagram()` yields one received payload per event, preserving datagram
boundaries and copying the exact Uint8Array view range. The receive queue is
limited to 256 datagrams and 1 MiB; reaching either limit emits
`DatagramReceiveQueueFull` and stops the local datagram reader. Connection close
releases the reader and writer and resolves waiting reads with `DatagramsClosed`.
