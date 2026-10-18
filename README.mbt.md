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
Draft 18 implementation supports SETUP and one publisher/subscriber pair for a
single track, with one object delivered on a reliable Subgroup stream:

- `protocol`: vi64 integers, SETUP and request framing, Subgroup objects.
- `session`: incremental client handshake and single-track publish/subscribe.
- `transport`: native MsQuic client using the `moqt-18` ALPN.
- `cmd/setup_probe`: a client that exchanges SETUP with an external relay.
- `cmd/track_probe`: publisher and subscriber probes for one track and object.

The JavaScript WebTransport adapter is not implemented yet.

## Development

The Nix devShell includes MoonBit, Node.js, MsQuic, OpenSSL and a pinned moq5 relay.
Run the local Draft 18 interoperability check with:

```sh
nix develop -c bash scripts/check-setup-interop.sh
```

The check starts a relay on `127.0.0.1:4443`, exchanges SETUP, publishes a
single track, subscribes to it, checks delivery of one object and the relay's
SETUP exchange acknowledgements, then cleans up the relay and temporary
certificates.
See [interop notes](docs/interop.md) for details and connecting to another relay.

## References

- [Architecture notes](docs/architecture.md)
- [MoQ Transport Draft 18](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/18/)
- [openmoq/moq5](https://github.com/openmoq/moq5)
- [libmsquic (Microsoft MsQuic)](https://github.com/microsoft/msquic)
