# JavaScript adapter boundary

The JavaScript-only adapter lives in `src/js_adapter` and is published as
`OJII3/moq-mbt/js_adapter`. Its package supports only the `js` target. The
native MsQuic transport and command packages remain native-only, while
`protocol` and `session` stay independent of JavaScript packages.

The adapter pins the `mizchi/js` modules at `0.13.0` in `moon.mod`:

- `mizchi/js_core` supplies Promise and core JavaScript interop types.
- `mizchi/js_web/streams` supplies Web Streams.
- `mizchi/js_builtin/arraybuffer` supplies ArrayBuffer and Uint8Array types.

On MoonBit's JavaScript backend, `Bytes` and `Uint8Array` share the same
representation. The adapter conversions therefore create aliases, not copies.
A Uint8Array view carries its own `byteOffset` and `byteLength`; converting the
view preserves that extent. Do not replace the view with its full backing
ArrayBuffer. Treat received views as borrowed while the JavaScript producer may
reuse or mutate their storage. Copy bytes before retaining them across async
work or handing them to code that assumes immutable ownership.

`mizchi/js` 0.13.0 documents compatibility with Moon 0.1.20260915 and moonc
v0.10.13 or newer; this project validates it with Moon 0.1.20261012 and moonc
v0.10.15. The dependency remains confined to `js_adapter` and does not enter
the `protocol` or `session` package interfaces.
