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

This project is currently under early and active development.

The first implementation milestone is the Draft 18 SETUP exchange:

- `protocol`: vi64 integers, SETUP framing and options.
- `session`: incremental, transport-independent client handshake.
- `transport`: native MsQuic client using the `moqt-18` ALPN.
- `cmd/setup_probe`: a client that exchanges SETUP with an external relay.

Publish/subscribe and object delivery are the next milestone. The JavaScript
WebTransport adapter is not implemented yet.

## Development

The Nix devShell includes MoonBit, Node.js, MsQuic, OpenSSL and a pinned moq5 relay.
Run the local Draft 18 interoperability check with:

```sh
nix develop -c bash scripts/check-setup-interop.sh
```

The check starts a relay on `127.0.0.1:4443`, exchanges SETUP, checks the relay's
acceptance trace, and cleans up the relay and temporary certificates.
See [interop notes](docs/interop.md) for details and connecting to another relay.

## References

- [Architecture notes](docs/architecture.md)
- [MoQ Transport Draft 18](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/18/)
- [openmoq/moq5](https://github.com/openmoq/moq5)
- [libmsquic (Microsoft MsQuic)](https://github.com/microsoft/msquic)
