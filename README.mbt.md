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
- **Interoperability**: Native QUIC ↔ aiomoqt relay ↔ Browser WebTransport interoperability is covered by the pinned relay probes.

## Packages

| Package | Description | Target |
| :--- | :--- | :--- |
| `OJII3/moq-mbt/protocol` | Framing, message serialization, and deserialization | Native, JS |
| `OJII3/moq-mbt/session` | Session state machine and request lifecycles | Native, JS |
| `OJII3/moq-mbt/transport` | Native QUIC client using `libmsquic` | Native |
| `OJII3/moq-mbt/test_support` | Scripted transport for tests using `SessionDriver` | Native, JS |
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
- Docker (for the interop relay scripts)

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

# Send a 2 MiB Object payload through the incremental writer on both sides
moon run src/cmd/track_probe -- publish 127.0.0.1 4443 /interop my-namespace large 1 1 --object-bytes=2097152 --insecure
moon run src/cmd/track_probe -- subscribe 127.0.0.1 4443 /interop my-namespace large 1 1 --object-bytes=2097152 --insecure

# Other commands
moon run src/cmd/track_probe -- lifecycle 127.0.0.1 4443 /interop --insecure
moon run src/cmd/track_probe -- namespace 127.0.0.1 4443 /interop --insecure
moon run src/cmd/track_probe -- fetch 127.0.0.1 4443 /interop my-namespace track0 0 0 1 2 2 --insecure
moon run src/cmd/track_probe -- late-join 127.0.0.1 4443 /interop my-namespace track0 --insecure
moon run src/cmd/track_probe -- datagram-smoke 127.0.0.1 4443 /interop --insecure
```

`fetch` takes `START_GROUP START_OBJECT END_GROUP END_OBJECT OBJECTS_PER_GROUP`.
An end object of `0` selects the end of the end group. `late-join` fetches
group 0 and then joins the live track for group 1; its publisher counterpart
uses `--wait-for-fetch --fetch-after-groups=1`.

#### MoQ Interop Runner client mode

`track_probe` also provides the Draft 18 native-QUIC test-client interface for
[`moq-interop-runner`](https://github.com/englishm/moq-interop-runner). It
supports `setup-only`, `announce-only`, `publish-namespace-done`,
`subscribe-namespace-lifecycle`,
`publish-namespace-multi-publisher-retention`, `subscribe-error`,
`rendezvous-timeout`, `announce-subscribe`, `subscribe-before-announce`, and
`data-subgroup-basic`. The client reports the test points it implements with
`--list`; the media fixtures are outside that set.

```sh
moon run src/cmd/track_probe -- --list
RELAY_URL=moqt://127.0.0.1:4443/interop \
  TLS_DISABLE_VERIFY=1 \
  moon run src/cmd/track_probe -- --test setup-only
```

The Runner may pass the same settings as `--relay`, `--test`, and
`--tls-disable-verify`. Results are written to stdout in TAP version 14 format,
with YAML diagnostics for duration, session roles, negotiated version,
transport, the QUIC Initial Destination Connection ID, and failure messages.

Build the Runner-compatible Docker image from the repository root:

```sh
docker build -f docker/interop-client.Dockerfile -t moq-mbt-interop-client:latest .
docker run --rm moq-mbt-interop-client:latest --list
docker run --rm \
  -e RELAY_URL=moqt://relay:4443/interop \
  -e TESTCASE=setup-only \
  -e TLS_DISABLE_VERIFY=1 \
  moq-mbt-interop-client:latest
```

The image is published to `ghcr.io/ojii3/moq-mbt-interop-client` on pushes to
`main` (`:latest`) and `v*` tags.

The relay hostname must be reachable from the container's Docker network.

### Browser Sample

Start the local interop relay in one terminal:

```sh
nix develop -c scripts/serve-interop-relay.sh
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
The WebTransport URL is `https://127.0.0.1:4443/`.
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
    "OJII3/moq-mbt": "0.4.4"
  }
}
```

### Browser Client API

In MoonBit code built for JavaScript, create the SETUP value and pass it with
the relay URL to `BrowserClient::connect`:

```moonbit
let setup : @protocol.Setup = { options: [] }
let client_result = @js_adapter.BrowserClient::connect(
  "https://relay.example/interop",
  setup,
)
```

`BrowserClient::connect` returns a `Result`; handle its error before using the client.
Call `start()`, then consume `next_notice()` until `HandshakeComplete` before
making requests through `session()`, and call `close(code, reason)` when the
application is done.

See [`src/js_adapter/README.md`](src/js_adapter/README.md) for the complete
adapter API, certificate pinning, browser requirements, buffer limits, and
datagram behavior. The [browser sample guide](src/examples/browser/README.md)
shows the API in a runnable publish/subscribe flow.

## Testing

```sh
# Unit tests
moon test

# Protocol and relay interoperability checks
# (requires Nix devShell and Docker)
bash scripts/check-interop.sh

# Native ↔ Browser dual interop check
bash scripts/check-interop-dual.sh

# Native memory leak check with Valgrind (Linux)
bash scripts/check-native-leaks.sh
```

The interop scripts run a pinned aiomoqt Draft 18 relay in Docker. They cover
multi-Group/Object transfer, a 2 MiB Object through the incremental writer,
same-track fan-out to three Native subscribers, Native/Browser large payloads
and Object Datagrams, standalone FETCH with object offsets, late join, and all
ten moq-interop-runner client scenarios. See
[interop notes](docs/interop.md).

## Documentation

- [Architecture notes](docs/architecture.md): Session state machine and transport boundary
- [Interop notes](docs/interop.md): Pinned relay, tested scenarios, and scope
- [JavaScript adapter](docs/js-adapter.md): WebTransport details, buffer ownership, and FFI policy
- [Browser sample guide](src/examples/browser/README.md): Running the browser demo

## References

- [MoQ Transport Draft 18](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/18/)
- [openmoq/moq5](https://github.com/openmoq/moq5)
- [aiomoqt](https://github.com/gmarzot/aiomoqt)
- [libmsquic](https://github.com/microsoft/msquic)
