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
