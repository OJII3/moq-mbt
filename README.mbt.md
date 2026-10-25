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
- **Interoperability**: Native QUIC ↔ moqtail ↔ Browser WebTransport interoperability is covered by the pinned relay probes.

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

Start the local `moqtail` relay in one terminal:

```sh
nix develop -c scripts/serve-moqtail.sh
```

Keep it running. The script prints the relay URL and its base64 SHA-256
certificate hash.

In another terminal, start the browser sample:

```sh
cd src/examples/browser
npm install
npm run dev
```

Open the URL printed by Vite. In two tabs, enter the relay URL and certificate
hash printed by the relay script, using the same namespace and track in both.
Choose **Publish** in one tab and **Subscribe** in the other, then select
**Connect** in each. When the publisher shows `published` and the subscriber
shows `ready`, select **Start transfer** in both tabs. The subscriber should
finish with `passed`; select **Close publisher** after the publisher shows
`publishing-done`. Press Ctrl+C in the relay terminal to stop and remove it.

See the [browser sample guide](src/examples/browser/README.md) for more details.

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

### Browser Client API

In MoonBit code built for JavaScript, create the SETUP value and pass it with
the relay URL to `browser_connect`:

```moonbit
let setup : @protocol.Setup = { options: [] }
let client_result = @js_adapter.browser_connect(
  "https://relay.example/interop",
  setup,
)
```

`browser_connect` returns a `Result`; handle its error before using the client.
Call `start()` to begin the session, then consume `next_notice()` until you
receive `HandshakeComplete` before making requests through `session()`. Pass
each request's returned actions to `dispatch()`, and continue reading notices
to observe its result. `next_notice()` resolves to `None` when the session ends.
Call `close(code, reason)` when the application is done. For an explicitly
trusted certificate hash, use `browser_connect_with_certificate_hashes`.

See [`src/js_adapter/README.md`](src/js_adapter/README.md) for the complete
adapter API, certificate pinning, browser requirements, buffer limits, and
datagram behavior. The [browser sample guide](src/examples/browser/README.md)
shows the API in a runnable publish/subscribe flow.

## Testing

```sh
# Unit tests
moon test

# Protocol and relay interoperability checks (requires Nix devShell)
bash scripts/check-interop.sh

# Native ↔ Browser dual interop check
bash scripts/check-moqtail-dual.sh
```

## Documentation

- [Architecture notes](docs/architecture.md): Session state machine and transport boundary
- [Interop notes](docs/interop.md): Relay compatibility, tested scenarios, and limits
- [JavaScript adapter](docs/js-adapter.md): WebTransport details, buffer ownership, and FFI policy
- [Browser sample guide](src/examples/browser/README.md): Running the browser demo

## References

- [MoQ Transport Draft 18](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/18/)
- [openmoq/moq5](https://github.com/openmoq/moq5)
- [moqtail/moqtail](https://github.com/moqtail/moqtail)
- [libmsquic](https://github.com/microsoft/msquic)
