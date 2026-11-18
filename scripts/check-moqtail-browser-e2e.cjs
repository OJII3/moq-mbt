const { spawn } = require("node:child_process");
const { createServer } = require("node:http");
const { chromium } = require(process.env.PLAYWRIGHT_CORE_PATH);

const [mode, url, fingerprintBase64, nativePort, bundlePath] = process.argv.slice(2);
if (!mode || !["subscribe", "publish"].includes(mode) || !url || !fingerprintBase64 || !nativePort || !bundlePath) {
  throw new Error(
    "usage: check-moqtail-browser-e2e.cjs <subscribe|publish> <url> <sha256-fingerprint-base64> <native-port> <bundle-path>",
  );
}

const groups = Number(process.env.MOQTAIL_BROWSER_GROUPS ?? "2");
const objectsPerGroup = Number(process.env.MOQTAIL_BROWSER_OBJECTS ?? "2");
const objectBytes = Number(process.env.MOQTAIL_BROWSER_OBJECT_BYTES ?? "0");
const datagrams = process.env.MOQTAIL_BROWSER_DATAGRAMS === "1";
if (!Number.isInteger(groups) || groups <= 0) {
  throw new Error("MOQTAIL_BROWSER_GROUPS must be a positive integer");
}
if (!Number.isInteger(objectsPerGroup) || objectsPerGroup <= 0) {
  throw new Error("MOQTAIL_BROWSER_OBJECTS must be a positive integer");
}
if (!Number.isInteger(objectBytes) || objectBytes < 0) {
  throw new Error("MOQTAIL_BROWSER_OBJECT_BYTES must be a non-negative integer");
}
if (datagrams && mode === "publish") {
  throw new Error("the browser publisher does not send Object Datagrams");
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

async function runNative(role, mode, options = {}) {
  const trackName = mode === "publish" ? "browser-publisher" : "multi-object";
  const args = [
    role, "127.0.0.1", nativePort, "/interop", "interop", trackName,
    String(groups), String(objectsPerGroup), "--insecure",
  ];
  if (objectBytes > 0) args.push(`--object-bytes=${objectBytes}`);
  if (datagrams) args.push("--datagrams");
  if (role === "publish") {
    args.push("--hold-open");
    if (options.waitForStart) args.push("--wait-for-start");
  }
  const child = spawn("_build/native/release/build/cmd/track_probe/track_probe.exe", args, {
    stdio: [options.waitForStart ? "pipe" : "ignore", "pipe", "pipe"],
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
    start: () => {
      if (!child.stdin) {
        throw new Error(`Native ${role} did not expose stdin for --wait-for-start`);
      }
      child.stdin.write("\n");
    },
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
    await page.addInitScript(({ uri, certificateHash, mode, track, groups, objectsPerGroup, objectBytes, datagrams }) => {
      globalThis.__moqE2EConfig = { uri, certificateHash, mode, track, groups, objectsPerGroup, objectBytes, datagrams };
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
      groups,
      objectsPerGroup,
      objectBytes,
      datagrams,
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
      const nativePublisher = await runNative("publish", mode, { waitForStart: true });
      nativePeers.push(nativePublisher);
      await withTimeout(nativePublisher.ready, 20000, "timed out waiting for Native PUBLISH acceptance");
      const nativeSubscribers = [];
      for (let index = 0; index < 2; index++) {
        const subscriber = await runNative("subscribe", mode);
        nativePeers.push(subscriber);
        await withTimeout(
          subscriber.ready,
          20000,
          `timed out waiting for Native SUBSCRIBE request ${index + 1}`,
        );
        await withTimeout(
          subscriber.subscribed,
          20000,
          `timed out waiting for Native SUBSCRIBE acceptance ${index + 1}`,
        );
        nativeSubscribers.push(subscriber);
      }
      nativeSubscriber = nativeSubscribers[0];
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
      nativePublisher.start();
      try {
        await page.waitForFunction(
          () => globalThis.__moqE2EResult?.status === "passed" ||
            globalThis.__moqE2EResult?.status?.startsWith("failed:"),
          undefined,
          { timeout: 45000 },
        );
      } catch (error) {
        const current = await page.evaluate(() => globalThis.__moqE2EResult?.status);
        throw new Error(`timed out waiting for browser SUBSCRIBE delivery (status: ${current}): ${error.message}`);
      }
      status = await page.evaluate(() => globalThis.__moqE2EResult?.status);
      if (status !== "passed") throw new Error(`Browser subscribe failed: ${status}`);
      for (let index = 0; index < nativeSubscribers.length; index++) {
        await withTimeout(
          nativeSubscribers[index].success,
          20000,
          `timed out waiting for Native subscriber ${index + 1} PUBLISH_DONE`,
        );
      }
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
    if (mode === "publish") {
      try {
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
      } catch (error) {
        const current = await page.evaluate(() => globalThis.__moqE2EResult?.status);
        const nativeOutput = nativePeers.map(peer => peer.output()).join("\n");
        throw new Error(`browser ${mode} E2E stopped (status: ${current}): ${error.message}\nNative peer output:\n${nativeOutput}`);
      }
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
    const objects = groups * objectsPerGroup;
    console.log(
      mode === "subscribe"
        ? `Native publisher → 2 Native subscribers + browser subscriber passed: ${objects} Objects, ${groups} Groups, Properties, FIN, PUBLISH_DONE(${groups})${datagrams ? ", Object Datagrams" : ""}`
        : `Browser publisher → Native subscriber passed: ${objects} Objects, ${groups} Groups, Properties, FIN, PUBLISH_DONE(${groups})`,
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
