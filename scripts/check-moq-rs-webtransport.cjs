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
    const result = await page.evaluate(async ({ url, fingerprint }) => {
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
      await transport.close();
      return protocol;
    }, { url, fingerprint: [...fingerprint] });

    if (result !== "moqt-18") {
      throw new Error(`unexpected WebTransport protocol: ${JSON.stringify(result)}`);
    }
    console.log(`Playwright WebTransport connected; negotiated ${result}`);
  } finally {
    if (browser) await browser.close();
    await new Promise((resolve) => originServer.close(resolve));
  }
}

main().catch((error) => {
  console.error(`Playwright WebTransport check failed: ${error.message}`);
  process.exitCode = 1;
});
