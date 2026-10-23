# Browser publish / subscribe sample

This page runs the `web_interop_probe` JavaScript build in a browser. It uses
the JS adapter to connect over WebTransport and exchange two Groups with two
Objects each. Open the page in two tabs with the same URL, namespace, and track;
choose Publish in one tab and Subscribe in the other.

Build the sample from the repository root:

```sh
moon build --release --target js src/cmd/web_interop_probe
python3 -m http.server 8000
```

Open <http://localhost:8000/src/examples/browser/>. `localhost` is a secure
context for browser WebTransport. The relay URL must use HTTPS. For a local
relay with a self-signed certificate, enter its SHA-256 certificate hash in
base64; the adapter passes that hash through the browser's certificate pinning
option. Leave the field empty for a certificate trusted by the browser.

The adapter keeps browser APIs out of module initialization. The library facade
offers `@js_adapter.browser_connect(url, setup)`, `start()`, `session()`,
`dispatch()`, `next_notice()`, and explicit `close()` operations. See
[`src/js_adapter/README.md`](../../js_adapter/README.md) for the adapter API.
