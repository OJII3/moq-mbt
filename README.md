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

## References

- [MoQ Transport Draft 18](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/18/)
- [openmoq/moq5](https://github.com/openmoq/moq5)
- [libmsquic (Microsoft MsQuic)](https://github.com/microsoft/msquic)
