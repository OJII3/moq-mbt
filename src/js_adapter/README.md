# WebTransport adapter

`webtransport_connect` accepts an `https://` URL or a `moqt://` URI. For `moqt:`
it preserves the authority, path, and query while opening the corresponding
`https://` URL. It offers the WebTransport
subprotocol `moqt-18`, as required by MOQT Draft 18. Once `ready` fulfills, the
server-selected `protocol` must equal `moqt-18`; an empty or different selection
returns `Err(WebTransportFailure)`. The relay must advertise and select this protocol through
WebTransport's `WT-Available-Protocols` negotiation.

WebTransport is available only in secure contexts. If the API is missing, the
module still imports; `webtransport_connect` returns a clear constructor error.
The binding also requires the `protocols` constructor option and the `protocol`
property. As of 2026-10-23, those negotiation features are supported by Chrome
143+, Edge 143+, Firefox 155+, and Safari 26.4+. Browser support changes over
time; check the browser's WebTransport support before connecting.

By default, certificate verification uses the browser's Web PKI. For an
explicitly trusted local or ephemeral certificate, pass
the SHA-256 hashes as `certificate_hashes=hashes` to `webtransport_connect`.
This opts into the WebTransport certificate-hash mechanism and its browser
restrictions; it does not disable TLS verification.

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
`next_stream_event()` for `StreamStarted`, `LocalStreamOpened`, `Data`, `StreamFinished`,
`StreamReset`, `SendStopped`, `StreamQueueOverflow`, and `ConnectionClosed`.
`StreamStarted` is queued before that stream's first data or finish event.
`StreamStarted` identifies a peer-opened stream; `LocalStreamOpened` reports a
stream already returned by a local open call.
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

## Client session driver

`WebTransportSessionDriver::from_setup(connection, setup)` creates a Draft 18
`ClientSession` and removes Native-only PATH and AUTHORITY parameters from its
SETUP options; the WebTransport URL supplies those values. Pass
`desired_subscriptions=desired` to restore subscriptions after reconnecting.
Call `start()` after connection creation, then use `dispatch(actions)` for
operations on `driver.session()` and `next_notice()` to receive session
notices. The adapter implements `SessionTransport` from the shared `driver`
package; action queueing, per-stream ordering, and retry live there, with
in-flight limits (8 opens / 128 sends / 32 datagrams) passed as
`SessionDriverConfig`. These limits count transport-accepted operations whose
completion callback has not run. The adapter also bounds pending actions to
1024, pending payload bytes to 16 MiB, and each send chunk to 64 KiB. The
driver starts the session after WebTransport
negotiation succeeds and continues to poll its monotonic clock while active.
I/O promises feed completion or failure events back into the session without
pausing the stream and datagram readers.

`SendCompleted` represents the backend's completion point and reports `Sent`
or `Reset`. In this adapter it means the browser accepted each write and, for
FIN, completed `writer.close()`. Successful final close emits
`StreamDataCommitted` separately: the [WebTransport close algorithm](https://www.w3.org/TR/webtransport/#webtransport-send-stream)
waits for the QUIC all-data-committed state. Ordinary writes only indicate
local acceptance. This does not acknowledge peer application processing.

A queued FIN can still be aborted before the underlying close begins. Once
close is in flight, the [Streams standard](https://streams.spec.whatwg.org/#writable-stream-finish-in-flight-close)
allows it to finish despite a concurrent abort; the browser API cannot promise
a RESET at that point. Policies requiring abort throughout the period before
QUIC commitment therefore require a backend with that capability.

Object Datagrams are tracked by token. `send_object_datagram` returns a token
and emits `DatagramQueued(token)` once the session has queued it for transport.
A local write failure produces `DatagramLocalWriteFailed(token, status)`; the
browser API exposes no network acknowledgement, so a queued datagram is not
proof of peer delivery.

## Browser client facade

`browser_connect(url, setup)` creates a connection and a Draft 18 session
driver. Call `start()`, use `session()` to make MoQ requests, pass each
operation's returned actions to `dispatch()`, and await `next_notice()` for
results. `dispatch()` returns `Dispatched`, `QueueFull`, `TooLarge`, or
`DriverFailed`; retain and retry the same actions after `QueueFull`. A batch
that is too large can be dispatched in order as smaller batches. An individual
action larger than the byte limit cannot be retried: abort its Subgroup and
use `ObjectWriter::write` with smaller chunks on a new Subgroup. Choose chunk
sizes within the driver's byte limit before calling the session API.
Call `close(code, reason)` when the application is done; `closed()`
reports whether the local peer, remote peer, or transport failure ended the
connection. To restore subscriptions after reconnecting, pass the previous
session's `desired_subscriptions()` as
`browser_connect(url, setup, desired_subscriptions=desired)`. The facade
does not access `window` or `document` during import. Certificate hashes and
restored subscriptions can be supplied together as
`browser_connect(url, setup, certificate_hashes=hashes,
desired_subscriptions=desired)`.

See [`examples/browser`](../examples/browser) for a runnable publish /
subscribe page.
