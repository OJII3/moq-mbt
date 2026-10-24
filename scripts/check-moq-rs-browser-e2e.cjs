const { spawn } = require("node:child_process");
const { createServer } = require("node:http");
const { chromium } = require(process.env.PLAYWRIGHT_CORE_PATH);

const [mode, url, fingerprintBase64, nativePort, bundlePath] = process.argv.slice(2);
if (!mode || !["subscribe", "publish"].includes(mode) || !url || !fingerprintBase64 || !nativePort || !bundlePath) {
  throw new Error(
    "usage: check-moq-rs-browser-e2e.cjs <subscribe|publish> <url> <sha256-fingerprint-base64> <native-port> <bundle-path>",
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

async function runNative(role) {
  const args = [
    role, "127.0.0.1", nativePort, "/interop", "interop", "multi-object", "2", "2", "--insecure",
  ];
  if (role === "publish") args.push("--hold-open", "--wait-for-start");
  const child = spawn("_build/native/release/build/cmd/track_probe/track_probe.exe", args, {
    stdio: ["pipe", "pipe", "pipe"],
  });
  let output = "";
  let resolveReady;
  let rejectReady;
  let resolveSuccess;
  let rejectSuccess;
  let stopping = false;
  const readyMarker = role === "publish" ? "PUBLISH request accepted" : "SUBSCRIBE_OK received";
  const successMarker = role === "publish" ? "PUBLISH_DONE send completed" : "PUBLISH_DONE received";
  const ready = new Promise((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
  const success = new Promise((resolve, reject) => { resolveSuccess = resolve; rejectSuccess = reject; });
  const onOutput = (data, stream) => {
    output += data;
    stream.write(data);
    if (output.includes(readyMarker)) resolveReady();
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
    success,
    start: () => child.stdin.end("\n"),
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
    response.end("<!doctype html><title>moq-rs browser interop</title>");
  });
  await new Promise(resolve => originServer.listen(0, "127.0.0.1", resolve));
  let browser;
  let native;
  try {
    browser = await chromium.launch();
    const page = await browser.newPage();
    page.on("console", message => console.log(`browser: ${message.text()}`));
    page.on("pageerror", error => console.error(`browser error: ${error.message}`));
    await page.addInitScript(({ uri, certificateHash, mode }) => {
      globalThis.__moqE2EConfig = { uri, certificateHash, mode };
      globalThis.__moqE2EResult = { status: "loading" };
      globalThis.__moqE2EStartPromise = new Promise(resolve => {
        globalThis.__moqE2EStart = resolve;
      });
      globalThis.__moqE2EFinishPromise = new Promise(resolve => {
        globalThis.__moqE2EFinish = resolve;
      });
    }, { uri: url.replace(/^https:/, "moqt:"), certificateHash: fingerprint, mode });
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
      native = await runNative("publish");
      await Promise.race([
        native.ready,
        new Promise((_, reject) => setTimeout(() => reject(new Error("timed out waiting for Native PUBLISH acceptance")), 20000)),
      ]);
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
      if (status !== "subscribed") throw new Error(`Browser subscribe failed before Native publisher start: ${status}`);
      native.start();
    } else {
      native = await runNative("subscribe");
      await Promise.race([
        native.ready,
        new Promise((_, reject) => setTimeout(() => reject(new Error("timed out waiting for Native SUBSCRIBE_OK")), 20000)),
      ]);
      await page.evaluate(() => globalThis.__moqE2EStart());
    }
    try {
      if (mode === "subscribe") {
        await Promise.race([
          page.waitForFunction(
            () => globalThis.__moqE2EResult?.status === "passed" ||
              globalThis.__moqE2EResult?.status?.startsWith("failed:"),
            undefined,
            { timeout: 45000 },
          ),
          native.done,
        ]);
      } else {
        await page.waitForFunction(
          () => globalThis.__moqE2EResult?.status === "publishing-done" ||
            globalThis.__moqE2EResult?.status?.startsWith("failed:"),
          undefined,
          { timeout: 45000 },
        );
        status = await page.evaluate(() => globalThis.__moqE2EResult?.status);
        if (status !== "publishing-done") throw new Error(`Browser publisher failed: ${status}`);
        await withTimeout(native.success, 15000, "timed out waiting for Native PUBLISH_DONE");
        await withTimeout(native.stop(), 5000, "timed out stopping Native subscriber");
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
      throw new Error(`browser ${mode} E2E stopped (status: ${current}): ${error.message}`);
    }
    status = await page.evaluate(() => globalThis.__moqE2EResult?.status);
    if (status !== "passed") throw new Error(`Browser ${mode} failed: ${status}`);
    if (mode === "subscribe") await native.stop();
    else await native.done;
    console.log(
      mode === "subscribe"
        ? "Native publisher → browser subscriber passed: 4 Objects, 2 Groups, Properties, FIN, PUBLISH_DONE(2)"
        : "Browser publisher → Native subscriber passed: 4 Objects, 2 Groups, Properties, FIN, PUBLISH_DONE(2)",
    );
  } finally {
    if (native && native.child.exitCode === null) {
      native.child.kill("SIGTERM");
      const exited = await Promise.race([
        native.done.then(() => true, () => true),
        new Promise(resolve => setTimeout(() => resolve(false), 3000)),
      ]);
      if (!exited) {
        native.child.kill("SIGKILL");
        await withTimeout(native.done, 5000, "Native child did not exit after SIGKILL")
          .catch(() => {});
      }
    }
    if (browser) await browser.close();
    await new Promise(resolve => originServer.close(resolve));
  }
}

main().catch(error => {
  console.error(`moq-rs browser E2E failed: ${error.message}`);
  process.exitCode = 1;
});
