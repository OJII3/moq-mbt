const { spawn } = require("node:child_process");
const { createServer } = require("node:http");
const { chromium } = require(process.env.PLAYWRIGHT_CORE_PATH);

const [mode, url, fingerprintBase64, nativePort, bundlePath] = process.argv.slice(2);
if (!mode || !["subscribe", "publish"].includes(mode) || !url || !fingerprintBase64 || !nativePort || !bundlePath) {
  throw new Error(
    "usage: check-moqtail-browser-e2e.cjs <subscribe|publish> <url> <sha256-fingerprint-base64> <native-port> <bundle-path>",
  );
}

const fingerprint = [...Buffer.from(fingerprintBase64, "base64")];
if (fingerprint.length !== 32) throw new Error("expected a 32-byte SHA-256 certificate fingerprint");

function withTimeout(promise, milliseconds, message) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(message)), milliseconds);
    }),
  ]).finally(() => clearTimeout(timer));
}

async function runNative(role, mode) {
  const trackName = mode === "publish" ? "browser-publisher" : "multi-object";
  const args = [
    role, "127.0.0.1", nativePort, "/interop", "interop", trackName, "2", "2", "--insecure",
  ];
  if (role === "publish") args.push("--hold-open");
  const child = spawn("_build/native/release/build/cmd/track_probe/track_probe.exe", args, {
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  let resolveReady;
  let rejectReady;
  let resolveSubscribed;
  let rejectSubscribed;
  let resolveSuccess;
  let rejectSuccess;
  let stopping = false;
  const readyMarker = role === "publish" ? "PUBLISH request accepted" : "SUBSCRIBE request sent";
  const subscribedMarker = "SUBSCRIBE_OK received";
  const successMarker = role === "publish" ? "PUBLISH_DONE send completed" : "PUBLISH_DONE received";
  const ready = new Promise((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
  const subscribed = new Promise((resolve, reject) => { resolveSubscribed = resolve; rejectSubscribed = reject; });
  const success = new Promise((resolve, reject) => { resolveSuccess = resolve; rejectSuccess = reject; });
  ready.catch(() => {});
  subscribed.catch(() => {});
  success.catch(() => {});
  const onOutput = (data, stream) => {
    output += data;
    stream.write(data);
    if (output.includes(readyMarker)) resolveReady();
    if (output.includes(subscribedMarker)) resolveSubscribed();
    if (output.includes(successMarker)) resolveSuccess();
  };
  child.stdout.setEncoding("utf8").on("data", data => onOutput(data, process.stdout));
  child.stderr.setEncoding("utf8").on("data", data => onOutput(data, process.stderr));
  const done = new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (!output.includes(readyMarker)) {
        rejectReady(new Error(`Native ${role} exited before readiness:\n${output}`));
      }
      if (role === "subscribe" && !stopping && !output.includes(subscribedMarker)) {
        rejectSubscribed(new Error(`Native ${role} exited before subscription acceptance:\n${output}`));
      }
      if (role === "subscribe" && !stopping && !output.includes(successMarker)) {
        rejectSuccess(new Error(`Native ${role} exited before peer delivery:\n${output}`));
      }
      if (code === 0 || (stopping && signal === "SIGTERM")) resolve();
      else reject(new Error(`Native ${role} exited (${signal ?? code}):\n${output}`));
    });
  });
  done.catch(() => {});
  return {
    child,
    done,
    ready,
    subscribed,
    success,
    output: () => output,
    start: () => {},
    stop: () => {
      stopping = true;
      if (child.exitCode === null) child.kill("SIGTERM");
      return done;
    },
  };
}

async function main() {
  const originServer = createServer((_request, response) => {
    response.writeHead(200, { "content-type": "text/html" });
    response.end("<!doctype html><title>moqtail browser interop</title>");
  });
  await new Promise(resolve => originServer.listen(0, "127.0.0.1", resolve));
  let browser;
  const nativePeers = [];
  let nativeSubscriber;
  try {
    browser = await chromium.launch();
    const page = await browser.newPage();
    page.on("console", message => console.log(`browser: ${message.text()}`));
    page.on("pageerror", error => console.error(`browser error: ${error.message}`));
    await page.addInitScript(({ uri, certificateHash, mode, track }) => {
      globalThis.__moqE2EConfig = { uri, certificateHash, mode, track };
      globalThis.__moqE2EResult = { status: "loading" };
      globalThis.__moqE2EStartPromise = new Promise(resolve => {
        globalThis.__moqE2EStart = resolve;
      });
      globalThis.__moqE2EFinishPromise = new Promise(resolve => {
        globalThis.__moqE2EFinish = resolve;
      });
    }, {
      uri: url.replace(/^https:/, "moqt:"),
      certificateHash: fingerprint,
      mode,
      track: mode === "publish" ? "browser-publisher" : "multi-object",
    });
    await page.goto(`http://127.0.0.1:${originServer.address().port}`);
    await page.addScriptTag({ path: bundlePath });

    const readyStatus = mode === "subscribe" ? "ready" : "published";
    try {
      await page.waitForFunction(
        expected => globalThis.__moqE2EResult?.status === expected ||
          globalThis.__moqE2EResult?.status?.startsWith("failed:"),
        readyStatus,
        { timeout: 20000 },
      );
    } catch (error) {
      const current = await page.evaluate(() => globalThis.__moqE2EResult?.status);
      throw new Error(`timed out waiting for browser ${mode} to become ready (status: ${current}): ${error.message}`);
    }
    let status = await page.evaluate(() => globalThis.__moqE2EResult?.status);
    if (status !== readyStatus) throw new Error(`Browser ${mode} failed before Native peer start: ${status}`);

    if (mode === "subscribe") {
      const nativePublisher = await runNative("publish", mode);
      nativePeers.push(nativePublisher);
      await withTimeout(nativePublisher.ready, 20000, "timed out waiting for Native PUBLISH acceptance");
      nativeSubscriber = await runNative("subscribe", mode);
      nativePeers.push(nativeSubscriber);
      await withTimeout(nativeSubscriber.ready, 20000, "timed out waiting for Native SUBSCRIBE request");
      await page.evaluate(() => globalThis.__moqE2EStart());
      try {
        await page.waitForFunction(
          () => globalThis.__moqE2EResult?.status === "subscribed" ||
            globalThis.__moqE2EResult?.status?.startsWith("failed:"),
          undefined,
          { timeout: 20000 },
        );
      } catch (error) {
        const current = await page.evaluate(() => globalThis.__moqE2EResult?.status);
        throw new Error(`timed out waiting for browser SUBSCRIBE to be established (status: ${current}): ${error.message}`);
      }
      status = await page.evaluate(() => globalThis.__moqE2EResult?.status);
      if (status !== "subscribed") throw new Error(`Browser subscribe failed: ${status}`);
    } else {
      nativeSubscriber = await runNative("subscribe", mode);
      nativePeers.push(nativeSubscriber);
      await withTimeout(nativeSubscriber.ready, 20000, "timed out waiting for Native SUBSCRIBE request");
      await withTimeout(
        nativeSubscriber.subscribed,
        20000,
        "timed out waiting for Native SUBSCRIBE acceptance",
      );
      await page.evaluate(() => globalThis.__moqE2EStart());
    }
    try {
      if (mode === "subscribe") {
        await page.waitForFunction(
          () => globalThis.__moqE2EResult?.status === "passed" ||
            globalThis.__moqE2EResult?.status?.startsWith("failed:"),
          undefined,
          { timeout: 45000 },
        );
      } else {
        await page.waitForFunction(
          () => globalThis.__moqE2EResult?.status === "publishing-done" ||
            globalThis.__moqE2EResult?.status?.startsWith("failed:"),
          undefined,
          { timeout: 45000 },
        );
        status = await page.evaluate(() => globalThis.__moqE2EResult?.status);
        if (status !== "publishing-done") throw new Error(`Browser publisher failed: ${status}`);
        await withTimeout(nativeSubscriber.success, 15000, "timed out waiting for Native PUBLISH_DONE");
        await withTimeout(nativeSubscriber.stop(), 5000, "timed out stopping Native subscriber");
        await page.evaluate(() => globalThis.__moqE2EFinish());
        await page.waitForFunction(
          () => globalThis.__moqE2EResult?.status === "passed" ||
            globalThis.__moqE2EResult?.status?.startsWith("failed:"),
          undefined,
          { timeout: 5000 },
        );
      }
    } catch (error) {
      const current = await page.evaluate(() => globalThis.__moqE2EResult?.status);
      const nativeOutput = nativePeers.map(peer => peer.output()).join("\n");
      throw new Error(`browser ${mode} E2E stopped (status: ${current}): ${error.message}\nNative peer output:\n${nativeOutput}`);
    }
    status = await page.evaluate(() => globalThis.__moqE2EResult?.status);
    if (status !== "passed") {
      const nativeOutput = nativePeers.map(peer => peer.output()).join("\n");
      throw new Error(`Browser ${mode} failed: ${status}\nNative peer output:\n${nativeOutput}`);
    }
    for (const peer of nativePeers) {
      if (peer.child.exitCode === null) await withTimeout(peer.stop(), 5000, "timed out stopping Native peer");
      else await peer.done;
    }
    console.log(
      mode === "subscribe"
        ? "Native publisher → browser subscriber passed: 4 Objects, 2 Groups, Properties, FIN, PUBLISH_DONE(2)"
        : "Browser publisher → Native subscriber passed: 4 Objects, 2 Groups, Properties, FIN, PUBLISH_DONE(2)",
    );
  } finally {
    for (const peer of nativePeers) {
      if (peer.child.exitCode === null) {
        peer.child.kill("SIGTERM");
        const exited = await Promise.race([
          peer.done.then(() => true, () => true),
          new Promise(resolve => setTimeout(() => resolve(false), 3000)),
        ]);
        if (!exited) {
          peer.child.kill("SIGKILL");
          await withTimeout(peer.done, 5000, "Native child did not exit after SIGKILL")
          .catch(() => {});
        }
      }
    }
    if (browser) await browser.close();
    await new Promise(resolve => originServer.close(resolve));
  }
}

main().catch(error => {
  console.error(`moqtail browser E2E failed: ${error.message}`);
  process.exitCode = 1;
});
