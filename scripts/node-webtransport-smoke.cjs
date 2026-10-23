#!/usr/bin/env node
const [url, fingerprintBase64] = process.argv.slice(2);
if (!url || !fingerprintBase64) {
  throw new Error("usage: node scripts/node-webtransport-smoke.cjs <url> <sha256-fingerprint-base64>");
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
  return Buffer.concat([Buffer.from([0xaf, 0x00, body.length >> 8, body.length & 0xff]), body]);
}

async function withTimeout(promise, timeoutMs, message) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function readServerSetup(stream) {
  const reader = stream.getReader();
  const chunks = [];
  let length = 0;
  try {
    while (true) {
      const { value, done } = await withTimeout(
        reader.read(), 10000, "timed out waiting for server MoQ SETUP",
      );
      if (done) throw new Error("server closed before sending a complete SETUP");
      chunks.push(Buffer.from(value));
      length += value.byteLength;
      if (length < 4) continue;
      const received = Buffer.concat(chunks, length);
      if (received[0] !== 0xaf || received[1] !== 0x00) {
        throw new Error(`expected Draft 18 SETUP type 0x2f00, received 0x${received.subarray(0, 2).toString("hex")}`);
      }
      const bodyLength = (received[2] << 8) | received[3];
      if (length >= 4 + bodyLength) return received.subarray(0, 4 + bodyLength);
    }
  } finally {
    reader.releaseLock();
  }
}

async function main() {
  const { WebTransport, quicheLoaded } = await import("@fails-components/webtransport");
  await quicheLoaded;
  const transport = new WebTransport(url, {
    protocols: ["moqt-18"],
    serverCertificateHashes: [{ algorithm: "sha-256", value: new Uint8Array(fingerprint) }],
    quicConnectTimeout: 10000,
    webTransportConnectTimeout: 10000,
  });
  transport.closed.catch(() => {});
  try {
    await withTimeout(transport.ready, 10000, "timed out waiting for WebTransport.ready");
    const controlStream = await transport.createUnidirectionalStream();
    const incoming = transport.incomingUnidirectionalStreams.getReader();
    await controlStream.getWriter().write(encodeClientSetup(url));
    const { value: serverStream, done } = await withTimeout(
      incoming.read(), 10000, "timed out waiting for server control stream",
    );
    if (done) throw new Error("server closed before sending its control stream");
    const setup = await readServerSetup(serverStream);
    console.log(`Node WebTransport connected; received Draft 18 MoQ SETUP (${setup.length} bytes)`);
  } finally {
    transport.close();
  }
}

main().catch((error) => {
  console.error(`Node WebTransport smoke failed: ${error.message}`);
  process.exitCode = 1;
});
