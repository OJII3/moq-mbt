const http = require("node:http");
const { chromium } = require(process.env.PLAYWRIGHT_CORE_PATH);

const [url, fingerprintBase64] = process.argv.slice(2);
if (!url || !fingerprintBase64) {
  throw new Error("usage: check-moq-rs-webtransport.cjs <url> <sha256-fingerprint-base64>");
}

const fingerprint = Buffer.from(fingerprintBase64, "base64");
if (fingerprint.length !== 32) {
  throw new Error("expected a 32-byte SHA-256 certificate fingerprint");
}

function encodeVi64(value) {
  let width = 1;
  while (width < 9 && value >= 2 ** (7 * width)) width++;
  const bytes = Buffer.alloc(width);
  for (let index = width - 1; index > 0; index--) {
    bytes[index] = Number(value & 0xffn);
    value >>= 8n;
  }
  bytes[0] = width === 9
    ? 0xff
    : ((0xff << (9 - width)) & 0xff) | Number(value);
  return bytes;
}

function decodeVi64(bytes, offset) {
  if (offset >= bytes.length) throw new Error("truncated MoQ varint");
  const first = bytes[offset];
  let leadingOnes = 0;
  while (leadingOnes < 8 && (first & (0x80 >> leadingOnes)) !== 0) {
    leadingOnes++;
  }
  const width = leadingOnes + 1;
  if (offset + width > bytes.length) throw new Error("truncated MoQ varint");
  let value = BigInt(first & (0xff >> width));
  for (let index = 1; index < width; index++) {
    value = (value << 8n) | BigInt(bytes[offset + index]);
  }
  return { value, offset: offset + width };
}

function encodeClientSetup(url) {
  const parsed = new URL(url);
  const options = [
    [1n, Buffer.from(parsed.pathname)],
    [5n, Buffer.from(parsed.host)],
    [7n, Buffer.from("moq-mbt/0.1.0")],
  ];
  const payload = [];
  let previousKind = 0n;
  for (const [kind, value] of options) {
    payload.push(encodeVi64(kind - previousKind), encodeVi64(BigInt(value.length)), value);
    previousKind = kind;
  }
  const body = Buffer.concat(payload);
  if (body.length > 0xffff) throw new Error("MoQ SETUP is too large");
  return Buffer.concat([
    Buffer.from([0xaf, 0x00, body.length >> 8, body.length & 0xff]),
    body,
  ]);
}

function decodeServerSetup(bytes) {
  let { value: type, offset } = decodeVi64(bytes, 0);
  if (type !== 0x2f00n) throw new Error(`expected MoQ SETUP, received ${type}`);
  if (offset + 2 > bytes.length) throw new Error("truncated MoQ SETUP length");
  const length = (bytes[offset] << 8) | bytes[offset + 1];
  offset += 2;
  const end = offset + length;
  if (end > bytes.length) throw new Error("truncated MoQ SETUP payload");

  let previousKind = 0n;
  let options = 0;
  while (offset < end) {
    const delta = decodeVi64(bytes.subarray(0, end), offset);
    const kind = previousKind + delta.value;
    const encodedValue = decodeVi64(bytes.subarray(0, end), delta.offset);
    offset = encodedValue.offset;
    if ((kind & 1n) !== 0n) {
      const valueEnd = offset + Number(encodedValue.value);
      if (valueEnd > end) throw new Error("truncated MoQ SETUP parameter");
      offset = valueEnd;
    }
    if (kind === 1n || kind === 5n) {
      throw new Error(`server SETUP must not contain option ${kind}`);
    }
    previousKind = kind;
    options++;
  }
  return options;
}

async function main() {
  const originServer = http.createServer((_request, response) => {
    response.writeHead(200, { "content-type": "text/html" });
    response.end("<!doctype html><title>WebTransport check</title>");
  });
  await new Promise((resolve) => originServer.listen(0, "127.0.0.1", resolve));

  let browser;
  try {
    browser = await chromium.launch();
    const page = await browser.newPage();
    await page.goto(`http://127.0.0.1:${originServer.address().port}`);
    const result = await page.evaluate(async ({ url, fingerprint, setup }) => {
      const transport = new WebTransport(url, {
        protocols: ["moqt-18"],
        serverCertificateHashes: [{
          algorithm: "sha-256",
          value: new Uint8Array(fingerprint),
        }],
      });
      await Promise.race([
        transport.ready,
        new Promise((_, reject) => setTimeout(
          () => reject(new Error("timed out waiting for WebTransport.ready")),
          10000,
        )),
      ]);
      const protocol = transport.protocol;
      const controlStream = await transport.createUnidirectionalStream();
      const incoming = transport.incomingUnidirectionalStreams.getReader();
      await controlStream.getWriter().write(new Uint8Array(setup));
      const setupPromise = (async () => {
        const next = await incoming.read();
        if (next.done) throw new Error("server closed before sending SETUP");
        const reader = next.value.getReader();
        const chunks = [];
        let length = 0;
        while (true) {
          const part = await reader.read();
          if (part.done) throw new Error("server closed before completing SETUP");
          chunks.push(part.value);
          length += part.value.byteLength;
          if (length >= 4) {
            const received = new Uint8Array(length);
            let cursor = 0;
            for (const chunk of chunks) {
              received.set(chunk, cursor);
              cursor += chunk.byteLength;
            }
            const frameLength = (received[2] << 8) | received[3];
            if (received.length >= 4 + frameLength) {
              return [...received];
            }
          }
        }
      })();
      const serverSetup = await Promise.race([
        setupPromise,
        new Promise((_, reject) => setTimeout(
          () => reject(new Error("timed out waiting for server MoQ SETUP")),
          10000,
        )),
      ]);
      await transport.close();
      return { protocol, serverSetup };
    }, { url, fingerprint: [...fingerprint], setup: [...encodeClientSetup(new URL(url))] });
    result.options = decodeServerSetup(Buffer.from(result.serverSetup));

    if (result.protocol !== "moqt-18") {
      throw new Error(`unexpected WebTransport protocol: ${JSON.stringify(result.protocol)}`);
    }
    console.log(
      `Playwright WebTransport connected; negotiated ${result.protocol}; ` +
      `received MoQ SETUP (${result.options} options)`,
    );
  } finally {
    if (browser) await browser.close();
    await new Promise((resolve) => originServer.close(resolve));
  }
}

main().catch((error) => {
  console.error(`Playwright WebTransport check failed: ${error.message}`);
  process.exitCode = 1;
});
