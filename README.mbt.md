# moq-mbt

MoQ (Media over QUIC Transport) implementation in [MoonBit](https://www.moonbitlang.com/).

## Overview

`moq-mbt` aims to provide a lightweight, cross-platform implementation of the Media over QUIC (MoQ) Transport protocol written in MoonBit.

## Key Goals & Features

- **Multi-Target Support**:
  - **Native**: Powered by [`libmsquic`](https://github.com/microsoft/msquic) for high-performance native QUIC networking.
  - **Browser**: JavaScript target using the browser's WebTransport API.
- **Specification Compliance**:
  - Targeting **draft-ietf-moq-transport-18** (Draft 18).
  - Aiming for interoperability with external MoQ relay servers and client implementations.
- **Reference Architecture**:
  - Actively referencing the architecture and implementation of [openmoq/moq5](https://github.com/openmoq/moq5).

## Status

This project is currently under early and active development. The current
Draft 18 implementation supports SETUP, multi-track requests, subscription
updates and cancellation, GOAWAY draining, Namespace publication and
subscription, Standalone and Joining FETCH, reliable Subgroup streams, and
Object Datagrams:

- `protocol`: vi64 integers, SETUP and request framing, Subgroup objects.
- `session`: incremental handshake, track and Namespace request lifecycles, Standalone and Joining FETCH, and multiple Object streams.
- `transport`: native MsQuic client using the `moqt-18` ALPN.
- `cmd/setup_probe`: a client that exchanges SETUP with an external relay.
- `cmd/track_probe`: publisher, subscriber, lifecycle, Namespace, Standalone FETCH, and Joining FETCH probes for Draft 18 relays.

The JavaScript WebTransport adapter is available in `src/js_adapter`. It is
built only for the `js` target and uses `mizchi/js` for JavaScript interop
without adding those dependencies to `protocol` or `session`.

## Development

The Nix devShell includes MoonBit, Node.js tooling, Chromium, MsQuic, OpenSSL,
and the pinned moq-rs relay. Node.js is used by development scripts; the
JavaScript runtime target is the browser. Run the local Draft 18
interoperability checks with:

```sh
nix develop -c bash scripts/check-interop.sh
nix develop -c bash scripts/check-moq-rs-dual.sh
```

The check starts the pinned moq-rs relay on `127.0.0.1:4443`. It verifies
certificate rejection, Draft 18 SETUP, and multi-Object/Group publish and
subscribe. The dual check connects raw QUIC and a Playwright Chromium
WebTransport client to the same relay, exchanges Draft 18 SETUP, and runs
Native → Browser and Browser → Native publish/subscribe E2E. It checks Objects,
Properties, FIN, and `PUBLISH_DONE.stream_count`, and verifies rejection of an
incorrect certificate pin. See [interop notes](docs/interop.md) for the pinned
browser versions, tested scenarios, and limits. This validates Browser
WebTransport ↔ moq-rs ↔ Native raw QUIC; Native WebTransport is a separate M6
implementation.

## References

- [Architecture notes](docs/architecture.md)
- [MoQ Transport Draft 18](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/18/)
- [openmoq/moq5](https://github.com/openmoq/moq5)
- [libmsquic (Microsoft MsQuic)](https://github.com/microsoft/msquic)
