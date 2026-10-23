# moq-mbt

MoQ (Media over QUIC Transport) implementation in [MoonBit](https://www.moonbitlang.com/).

## Overview

`moq-mbt` aims to provide a lightweight, cross-platform implementation of the Media over QUIC (MoQ) Transport protocol written in MoonBit.

## Key Goals & Features

- **Multi-Target Support**:
  - **Native**: Powered by [`libmsquic`](https://github.com/microsoft/msquic) for high-performance native QUIC networking.
  - **JavaScript / Web**: Designed to run in web browsers and JavaScript environments leveraging WebTransport.
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

The JavaScript WebTransport adapter is not implemented yet.
The JavaScript package boundary is prepared in
[`ojii3/moq-mbt/js_adapter`](docs/js-adapter.md); it is built only for the `js`
target and uses `mizchi/js` for JavaScript interop without adding those
dependencies to `protocol` or `session`.

## Development

The Nix devShell includes MoonBit, Node.js, MsQuic, OpenSSL and a pinned moq5 relay.
Run the local Draft 18 interoperability check with:

```sh
nix develop -c bash scripts/check-interop.sh
```

The check starts pinned moq5 and moq-rs relays on `127.0.0.1:4443`. It verifies
SETUP, multi-Object/Group delivery, `PUBLISH_DONE`, moq5 standalone FETCH of
retained Objects, Namespace discovery and withdrawal, subscription updates,
unsubscription with an independent track continuing, Joining FETCH availability,
and Object Datagram fields. Refusal and relay-originated
GOAWAY cases use deterministic Sans-I/O session tests because the relay CLI
cannot trigger them. The pinned moq5 revision closes the Namespace subscriber
session after Namespace subscription cancellation, so independent track
continuation is verified in Sans-I/O rather than claimed as relay behavior.
The pinned relay rejects the current Joining FETCH probe with `INVALID_RANGE`;
joining range and no-overlap behavior remain covered by Sans-I/O tests.
See [interop notes](docs/interop.md) for details and connecting to another relay.

## References

- [Architecture notes](docs/architecture.md)
- [MoQ Transport Draft 18](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/18/)
- [openmoq/moq5](https://github.com/openmoq/moq5)
- [libmsquic (Microsoft MsQuic)](https://github.com/microsoft/msquic)
