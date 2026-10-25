# moq-mbt

Media over QUIC (MoQ) Transport implementation in [MoonBit](https://www.moonbitlang.com/).

## Overview

`moq-mbt` is a cross-platform implementation of the MoQ Transport protocol targeting **draft-ietf-moq-transport-18** (Draft 18). It provides both native QUIC (via MsQuic) and browser WebTransport adapters.

## Features

- **Specification**: Targeting [draft-ietf-moq-transport-18](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/18/)
  - SETUP handshake (`moqt-18` ALPN / WebTransport subprotocol)
  - Multi-track Publish and Subscribe lifecycles
  - Namespace publication and subscription
  - Standalone FETCH and Joining FETCH
  - Subgroup streams and Object Datagrams
  - `GOAWAY` handling and graceful session draining
- **Multi-Target Support**:
  - **Native**: Built with [`libmsquic`](https://github.com/microsoft/msquic) for raw QUIC networking.
  - **Browser**: Built for JavaScript / WebAssembly environments via the browser's WebTransport API.
- **Sans-I/O Architecture**: The protocol encoding/decoding and session state machine are decoupled from network I/O, timers, and platform runtimes.
- **Interoperability**: Native QUIC ↔ moqx ↔ Browser WebTransport interoperability is covered by the pinned relay probes.

## Packages

```text
src/
├── protocol/       # Wire format, vi64 integer codec, control and data messages
├── session/        # Sans-I/O session state machine (tracks, requests, streams)
├── transport/      # Native MsQuic QUIC transport adapter
├── js_adapter/     # Browser WebTransport adapter
├── cmd/
│   ├── setup_probe          # CLI: SETUP handshake verification
│   ├── track_probe          # CLI: Publish, Subscribe, Fetch, and Namespace probe
│   └── web_interop_probe    # WebTransport probe for browser
└── examples/
    └── browser/             # Browser publish and subscribe sample
```

| Package | Description | Target |
| :--- | :--- | :--- |
| `OJII3/moq-mbt/protocol` | Framing, message serialization, and deserialization | Native, JS |
| `OJII3/moq-mbt/session` | Session state machine and request lifecycles | Native, JS |
| `OJII3/moq-mbt/transport` | Native QUIC client using `libmsquic` | Native |
| `OJII3/moq-mbt/js_adapter` | WebTransport client adapter and browser session facade | JS (Browser) |

## Quick Start

### Prerequisites

Using Nix:

```sh
nix develop
```

Without Nix:
- [MoonBit CLI](https://www.moonbitlang.com/download/) (`moon`)
- `libmsquic` (for native target)
- Node.js 20+ and Chromium (for browser target)

### CLI Probes (Native)

#### Build

```sh
moon build --target native src/cmd/setup_probe
moon build --target native src/cmd/track_probe
```

#### Handshake Probe (`setup_probe`)

Verify connection and SETUP handshake with a relay:

```sh
moon run src/cmd/setup_probe -- <HOST> <PORT> <PATH> [--insecure]

# Example
moon run src/cmd/setup_probe -- 127.0.0.1 4443 /interop --insecure
```

#### Track Operations (`track_probe`)

```sh
# Publish tracks (HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP)
moon run src/cmd/track_probe -- publish 127.0.0.1 4443 /interop my-namespace track0 2 5 --insecure

# Subscribe to tracks
moon run src/cmd/track_probe -- subscribe 127.0.0.1 4443 /interop my-namespace track0 2 5 --insecure

# Other commands
moon run src/cmd/track_probe -- lifecycle 127.0.0.1 4443 /interop --insecure
moon run src/cmd/track_probe -- namespace 127.0.0.1 4443 /interop --insecure
moon run src/cmd/track_probe -- fetch 127.0.0.1 4443 /interop my-namespace track0 0 2 --insecure
moon run src/cmd/track_probe -- joining-fetch 127.0.0.1 4443 /interop --insecure
moon run src/cmd/track_probe -- datagram-smoke 127.0.0.1 4443 /interop --insecure
```

### Browser Sample

A runnable publish/subscribe web sample is available in [`src/examples/browser`](src/examples/browser).

1. Build the web client:
   ```sh
   moon build --release --target js src/cmd/web_interop_probe
   ```

2. Start a local HTTP server:
   ```sh
   python3 -m http.server 8000
   ```

3. Open `http://localhost:8000/src/examples/browser/` in a browser supporting WebTransport.

## Usage

### MoonBit Dependency

Add `OJII3/moq-mbt` to `moon.mod`:

```json
{
  "import": {
    "OJII3/moq-mbt": "0.1.0"
  }
}
```

### Browser Client Example

```moonbit
let setup : @session.Setup = ...
let client = @js_adapter.browser_connect("https://example.com/moq", setup)
client.start()

// Dispatch session actions
let actions = client.session().subscribe(...)
client.dispatch(actions)

// Receive session notices
let notice = client.next_notice()
```

See [`src/js_adapter/README.md`](src/js_adapter/README.md) for buffer limits, datagram capability, and certificate pinning details.

## Testing

```sh
# Unit tests
moon test

# Protocol and relay interoperability checks (requires Nix devShell)
bash scripts/check-interop.sh

# Native ↔ Browser dual interop check
bash scripts/check-moqx-dual.sh
```

## Documentation

- [Architecture notes](docs/architecture.md): Session state machine and transport boundary
- [Interop notes](docs/interop.md): Relay compatibility, tested scenarios, and limits
- [JavaScript adapter](docs/js-adapter.md): WebTransport details, buffer ownership, and FFI policy
- [Browser sample guide](src/examples/browser/README.md): Running the browser demo

## References

- [MoQ Transport Draft 18](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/18/)
- [openmoq/moq5](https://github.com/openmoq/moq5)
- [openmoq/moqx](https://github.com/openmoq/moqx)
- [libmsquic](https://github.com/microsoft/msquic)
