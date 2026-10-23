import base64
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import sys
from threading import Thread
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright


def encode_vi64(value):
    width = 1
    while width < 9 and value >= 2 ** (7 * width):
        width += 1
    encoded = bytearray(width)
    for index in range(width - 1, 0, -1):
        encoded[index] = value & 0xFF
        value >>= 8
    encoded[0] = 0xFF if width == 9 else ((0xFF << (9 - width)) & 0xFF) | value
    return encoded


def decode_vi64(data, offset):
    if offset >= len(data):
        raise ValueError("truncated MoQ varint")
    first = data[offset]
    leading_ones = 0
    while leading_ones < 8 and first & (0x80 >> leading_ones):
        leading_ones += 1
    width = leading_ones + 1
    if offset + width > len(data):
        raise ValueError("truncated MoQ varint")
    value = first & (0xFF >> width)
    for index in range(1, width):
        value = (value << 8) | data[offset + index]
    return value, offset + width


def encode_client_setup(url):
    parsed = urlparse(url)
    options = [(1, parsed.path.encode()), (5, parsed.netloc.encode()), (7, b"moq-mbt/0.1.0")]
    body = bytearray()
    previous_kind = 0
    for kind, value in options:
        body.extend(encode_vi64(kind - previous_kind))
        body.extend(encode_vi64(len(value)))
        body.extend(value)
        previous_kind = kind
    if len(body) > 0xFFFF:
        raise ValueError("MoQ SETUP is too large")
    return bytes([0xAF, 0x00, len(body) >> 8, len(body) & 0xFF]) + body


def decode_server_setup(data):
    message_type, offset = decode_vi64(data, 0)
    if message_type != 0x2F00:
        raise ValueError(f"expected MoQ SETUP, received {message_type}")
    if offset + 2 > len(data):
        raise ValueError("truncated MoQ SETUP length")
    length = (data[offset] << 8) | data[offset + 1]
    offset += 2
    end = offset + length
    if end > len(data):
        raise ValueError("truncated MoQ SETUP payload")

    previous_kind = 0
    options = 0
    while offset < end:
        delta, offset = decode_vi64(data[:end], offset)
        kind = previous_kind + delta
        value_length, offset = decode_vi64(data[:end], offset)
        if kind & 1:
            offset += value_length
            if offset > end:
                raise ValueError("truncated MoQ SETUP parameter")
        if kind in (1, 5):
            raise ValueError(f"server SETUP must not contain option {kind}")
        previous_kind = kind
        options += 1
    return options


def main():
    if len(sys.argv) != 3:
        raise ValueError("usage: check-moq-rs-webtransport.py <url> <sha256-fingerprint-base64>")
    url, fingerprint_base64 = sys.argv[1:]
    fingerprint = base64.b64decode(fingerprint_base64, validate=True)
    if len(fingerprint) != 32:
        raise ValueError("expected a 32-byte SHA-256 certificate fingerprint")

    setup = list(encode_client_setup(url))
    class OriginHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = b"<!doctype html><title>WebTransport check</title>"
            self.send_response(200)
            self.send_header("content-type", "text/html")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    origin = ThreadingHTTPServer(("127.0.0.1", 0), OriginHandler)
    Thread(target=origin.serve_forever, daemon=True).start()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.goto(f"http://127.0.0.1:{origin.server_port}")
        result = page.evaluate(
            """async ({url, fingerprint, setup}) => {
              const transport = new WebTransport(url, {
                protocols: ["moqt-18"],
                serverCertificateHashes: [{algorithm: "sha-256", value: new Uint8Array(fingerprint)}],
              });
              await Promise.race([transport.ready, new Promise((_, reject) => setTimeout(
                () => reject(new Error("timed out waiting for WebTransport.ready")), 10000))]);
              const control = await transport.createUnidirectionalStream();
              const incoming = transport.incomingUnidirectionalStreams.getReader();
              await control.getWriter().write(new Uint8Array(setup));
              const {value: stream, done} = await Promise.race([incoming.read(), new Promise((_, reject) =>
                setTimeout(() => reject(new Error("timed out waiting for server control stream")), 10000))]);
              if (done) throw new Error("server closed before sending its control stream");
              const reader = stream.getReader();
              const chunks = [];
              let length = 0;
              while (length < 4) {
                const part = await Promise.race([reader.read(), new Promise((_, reject) =>
                  setTimeout(() => reject(new Error("timed out waiting for server MoQ SETUP")), 10000))]);
                if (part.done) throw new Error("server closed before completing SETUP");
                chunks.push(part.value);
                length += part.value.length;
              }
              let received = new Uint8Array(length);
              let cursor = 0;
              for (const chunk of chunks) { received.set(chunk, cursor); cursor += chunk.length; }
              const frameLength = (received[2] << 8) | received[3];
              while (received.length < 4 + frameLength) {
                const part = await Promise.race([reader.read(), new Promise((_, reject) =>
                  setTimeout(() => reject(new Error("timed out waiting for server MoQ SETUP")), 10000))]);
                if (part.done) throw new Error("server closed before completing SETUP");
                const combined = new Uint8Array(received.length + part.value.length);
                combined.set(received); combined.set(part.value, received.length); received = combined;
              }
              const protocol = transport.protocol;
              await transport.close();
              return {protocol, setup: [...received.slice(0, 4 + frameLength)]};
            }""",
            {"url": url, "fingerprint": list(fingerprint), "setup": setup},
        )
        options = decode_server_setup(bytes(result["setup"]))
        if result["protocol"] != "moqt-18":
            raise ValueError(f"unexpected WebTransport protocol: {result['protocol']!r}")
        print(f"Playwright WebTransport connected; negotiated {result['protocol']}; received MoQ SETUP ({options} options)")

        wrong_fingerprint = bytearray(fingerprint)
        wrong_fingerprint[0] ^= 0xFF
        rejected = page.evaluate(
            """async ({url, fingerprint}) => {
              try {
                const transport = new WebTransport(url, {
                  protocols: ["moqt-18"],
                  serverCertificateHashes: [{algorithm: "sha-256", value: new Uint8Array(fingerprint)}],
                });
                await Promise.race([transport.ready, new Promise((_, reject) => setTimeout(
                  () => reject(new Error("timed out waiting for WebTransport.ready")), 10000))]);
                await transport.close();
                return false;
              } catch { return true; }
            }""",
            {"url": url, "fingerprint": list(wrong_fingerprint)},
        )
        browser.close()
        if not rejected:
            raise ValueError("incorrect certificate hash unexpectedly connected")
        print("WebTransport rejected an incorrect certificate hash")
    origin.shutdown()
    origin.server_close()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Playwright WebTransport check failed: {error}", file=sys.stderr)
        raise SystemExit(1)
