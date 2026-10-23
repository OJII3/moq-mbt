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

The JavaScript WebTransport adapter is available in `src/js_adapter`. It is
built only for the `js` target and uses `mizchi/js` for JavaScript interop
without adding those dependencies to `protocol` or `session`.

## Development

The Nix devShell includes MoonBit, Python Playwright with Chromium, MsQuic,
OpenSSL, and the pinned moq-rs relay. Run the interoperability checks with:

```sh
nix develop -c bash scripts/check-interop.sh
nix develop -c bash scripts/check-moq-rs-dual.sh
```

Outside the devShell, install Playwright and Chromium in a Python virtual environment:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install playwright
.venv/bin/python -m playwright install --with-deps chromium
PYTHON=.venv/bin/python bash scripts/check-moq-rs-dual.sh
```

The checks start the pinned moq-rs relay on
`127.0.0.1:4443` and verify certificate rejection, Draft 18 SETUP, and
multi-Object/Group publish and subscribe. See [interop notes](docs/interop.md)
for the tested scenarios and limits.

## References

- [Architecture notes](docs/architecture.md)
- [MoQ Transport Draft 18](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/18/)
- [openmoq/moq5](https://github.com/openmoq/moq5)
- [libmsquic (Microsoft MsQuic)](https://github.com/microsoft/msquic)
