const { spawn } = require("node:child_process");
const { createServer } = require("node:http");
const { chromium } = require(process.env.PLAYWRIGHT_CORE_PATH);

const [url, fingerprintBase64, nativePort, bundlePath] = process.argv.slice(2);
if (!url || !fingerprintBase64 || !nativePort || !bundlePath) {
  throw new Error(
    "usage: check-browser-leaks.cjs <url> <sha256-fingerprint-base64> <native-port> <bundle-path>",
  );
}

const fingerprint = [...Buffer.from(fingerprintBase64, "base64")];
if (fingerprint.length !== 32) {
  throw new Error("expected a 32-byte SHA-256 certificate fingerprint");
}

function withTimeout(promise, milliseconds, message) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(message)), milliseconds);
    }),
  ]).finally(() => clearTimeout(timer));
}

async function runNative(role, trackName, options = {}) {
  const groups = options.groups ?? 2;
  const objectsPerGroup = options.objectsPerGroup ?? 2;
  const objectBytes = options.objectBytes ?? 0;
  const datagrams = options.datagrams ?? false;
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
  const onOutput = data => {
    output += data;
    if (output.includes(readyMarker)) resolveReady();
    if (output.includes(subscribedMarker)) resolveSubscribed();
    if (output.includes(successMarker)) resolveSuccess();
  };
  child.stdout.setEncoding("utf8").on("data", onOutput);
  child.stderr.setEncoding("utf8").on("data", onOutput);
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

async function stopNativePeer(peer) {
  if (!peer) return;
  if (peer.child.exitCode === null) {
    peer.child.kill("SIGTERM");
    const exited = await Promise.race([
      peer.done.then(() => true, () => true),
      new Promise(resolve => setTimeout(() => resolve(false), 3000)),
    ]);
    if (!exited) {
      peer.child.kill("SIGKILL");
      await withTimeout(peer.done, 5000, "Native child did not exit after SIGKILL").catch(() => {});
    }
  } else {
    await peer.done;
  }
}

async function collectGarbageAndMeasure(cdp, page) {
  let liveRefs = 0;
  let heap = null;
  for (let attempt = 0; attempt < 6; attempt++) {
    await new Promise(resolve => setTimeout(resolve, attempt === 0 ? 50 : 40));
    await cdp.send("HeapProfiler.collectGarbage");
    await new Promise(resolve => setTimeout(resolve, 20));
    await cdp.send("HeapProfiler.collectGarbage");
    heap = await cdp.send("Runtime.getHeapUsage");
    liveRefs = await page.evaluate(() => {
      const refs = globalThis.__moqWeakRefs ?? [];
      return refs.filter(ref => ref.deref() !== undefined).length;
    });
    if (liveRefs === 0) break;
  }
  const trackedRefs = await page.evaluate(() => (globalThis.__moqWeakRefs ?? []).length);
  return { heap, liveRefs, trackedRefs };
}

function assertAllReclaimed(label, measurement) {
  if (measurement.trackedRefs === 0) {
    throw new Error(`${label}: no WeakRefs were tracked`);
  }
  if (measurement.liveRefs !== 0) {
    throw new Error(
      `${label}: ${measurement.liveRefs}/${measurement.trackedRefs} browser client/driver references stayed alive after GC`,
    );
  }
}

function formatKiB(bytes) {
  return `${(bytes / 1024).toFixed(1)} KiB`;
}

async function runBrowserSubscribeScenario(page, scenarioMode, options) {
  const {
    moqUri,
    certHash,
    trackName,
    groups,
    objectsPerGroup,
    objectBytes,
    datagrams,
  } = options;
  let signalResolver = {};
  const readyPromise = new Promise(resolve => { signalResolver.onReady = resolve; });
  const subscribedPromise = new Promise(resolve => { signalResolver.onSubscribed = resolve; });
  await page.evaluate(() => {
    globalThis.__moqCycleSignal = name => globalThis.__moqNodeSignal?.(name);
  });
  globalThis.__moqActiveSignalHandler = name => {
    signalResolver[name]?.();
  };

  const nativePublisher = await runNative("publish", trackName, {
    groups,
    objectsPerGroup,
    objectBytes,
    datagrams,
    waitForStart: true,
  });
  try {
    await withTimeout(nativePublisher.ready, 20000, "timed out waiting for Native PUBLISH acceptance");
    const scenarioPromise = page.evaluate(cfg => {
      let resolveStart;
      const startPromise = new Promise(resolve => { resolveStart = resolve; });
      globalThis.__moqCurrentStart = () => {
        resolveStart();
        globalThis.__moqCurrentStart = null;
      };
      return globalThis.__moqRunLeakScenario({
        ...cfg,
        startPromise,
        onReady: () => globalThis.__moqCycleSignal("onReady"),
        onSubscribed: () => globalThis.__moqCycleSignal("onSubscribed"),
      });
    }, {
      mode: scenarioMode,
      uri: moqUri,
      certificateHash: certHash,
      namespace: "interop",
      track: trackName,
      groups,
      objectsPerGroup,
      objectBytes,
      datagrams,
    });

    await withTimeout(readyPromise, 20000, "timed out waiting for browser handshake");
    await page.evaluate(() => globalThis.__moqCurrentStart?.());
    await withTimeout(subscribedPromise, 20000, "timed out waiting for browser SUBSCRIBE_OK");
    nativePublisher.start();
    const result = await withTimeout(scenarioPromise, 45000, "timed out waiting for browser subscribe scenario");
    if (!result.ok) {
      throw new Error(`browser ${scenarioMode} failed: ${result.error}`);
    }
    return result.streamStats;
  } finally {
    globalThis.__moqActiveSignalHandler = null;
    await stopNativePeer(nativePublisher);
  }
}

async function runBrowserPublishScenario(page, options) {
  const {
    moqUri,
    certHash,
    trackName,
    groups,
    objectsPerGroup,
    objectBytes,
  } = options;
  let signalResolver = {};
  const publishedPromise = new Promise(resolve => { signalResolver.onPublished = resolve; });
  const publishingDonePromise = new Promise(resolve => { signalResolver.onPublishingDone = resolve; });
  globalThis.__moqActiveSignalHandler = name => {
    signalResolver[name]?.();
  };

  let nativeSubscriber = null;
  try {
    const scenarioPromise = page.evaluate(cfg => {
      let resolveStart;
      let resolveFinish;
      const startPromise = new Promise(resolve => { resolveStart = resolve; });
      const finishPromise = new Promise(resolve => { resolveFinish = resolve; });
      globalThis.__moqCurrentStart = () => {
        resolveStart();
        globalThis.__moqCurrentStart = null;
      };
      globalThis.__moqCurrentFinish = () => {
        resolveFinish();
        globalThis.__moqCurrentFinish = null;
      };
      return globalThis.__moqRunLeakScenario({
        ...cfg,
        startPromise,
        finishPromise,
        onPublished: () => globalThis.__moqCycleSignal("onPublished"),
        onPublishingDone: () => globalThis.__moqCycleSignal("onPublishingDone"),
      });
    }, {
      mode: "publish",
      uri: moqUri,
      certificateHash: certHash,
      namespace: "interop",
      track: trackName,
      groups,
      objectsPerGroup,
      objectBytes,
    });

    await withTimeout(publishedPromise, 20000, "timed out waiting for browser PUBLISH_OK");
    nativeSubscriber = await runNative("subscribe", trackName, {
      groups,
      objectsPerGroup,
      objectBytes,
    });
    await withTimeout(nativeSubscriber.ready, 20000, "timed out waiting for Native SUBSCRIBE request");
    await withTimeout(nativeSubscriber.subscribed, 20000, "timed out waiting for Native SUBSCRIBE_OK");
    await page.evaluate(() => globalThis.__moqCurrentStart?.());
    await withTimeout(publishingDonePromise, 45000, "timed out waiting for browser PUBLISH_DONE");
    await withTimeout(nativeSubscriber.success, 15000, "timed out waiting for Native subscriber PUBLISH_DONE");
    await withTimeout(nativeSubscriber.stop(), 5000, "timed out stopping Native subscriber");
    await page.evaluate(() => globalThis.__moqCurrentFinish?.());
    const result = await withTimeout(scenarioPromise, 20000, "timed out waiting for browser publish close");
    if (!result.ok) {
      throw new Error(`browser publish failed: ${result.error}`);
    }
    return result.streamStats;
  } finally {
    globalThis.__moqActiveSignalHandler = null;
    await stopNativePeer(nativeSubscriber);
  }
}

async function main() {
  const originServer = createServer((_request, response) => {
    response.writeHead(200, { "content-type": "text/html" });
    response.end("<!doctype html><title>browser memory leak probe</title>");
  });
  await new Promise(resolve => originServer.listen(0, "127.0.0.1", resolve));
  const moqUri = url.replace(/^https:/, "moqt:");
  const pageErrors = [];
  let browser;
  try {
    browser = await chromium.launch();
    const page = await browser.newPage();
    const cdp = await page.context().newCDPSession(page);
    await cdp.send("HeapProfiler.enable");
    page.on("pageerror", error => {
      pageErrors.push(error.message);
      console.error(`browser pageerror: ${error.message}`);
    });
    await page.exposeFunction("__moqNodeSignal", name => {
      globalThis.__moqActiveSignalHandler?.(name);
    });
    await page.addInitScript(() => {
      globalThis.__moqE2EConfig = { mode: "leak-runner" };
      globalThis.__moqE2EResult = { status: "loading" };
    });
    await page.goto(`http://127.0.0.1:${originServer.address().port}`);
    await page.addScriptTag({ path: bundlePath });
    await page.waitForFunction(
      () => typeof globalThis.__moqRunLeakScenario === "function",
      undefined,
      { timeout: 10000 },
    );

    const maxCycleHeapGrowthBytes = 1024 * 1024;
    const maxBackingGrowthBytes = 64 * 1024;

    console.log("Checking browser memory: repeated subscribe cycles (streams + datagrams)");
    const subscribeCycles = 6;
    const subscribeWarmup = 2;
    let subscribeBaseline = null;
    let subscribeLast = null;
    for (let cycle = 1; cycle <= subscribeCycles; cycle++) {
      await runBrowserSubscribeScenario(page, "subscribe", {
        moqUri,
        certHash: fingerprint,
        trackName: `leak-sub-${cycle}`,
        groups: 2,
        objectsPerGroup: 2,
        objectBytes: 32768,
        datagrams: true,
      });
      const measured = await collectGarbageAndMeasure(cdp, page);
      assertAllReclaimed(`subscribe cycle ${cycle}`, measured);
      if (cycle === subscribeWarmup) subscribeBaseline = measured.heap;
      subscribeLast = measured.heap;
    }
    const subHeapDelta = subscribeLast.usedSize - subscribeBaseline.usedSize;
    const subBackingDelta = subscribeLast.backingStorageSize - subscribeBaseline.backingStorageSize;
    console.log(
      `  reclaimed all ${subscribeCycles} BrowserClient instances; JS heap delta after warmup: ${formatKiB(subHeapDelta)}, backing delta: ${formatKiB(subBackingDelta)}`,
    );
    if (subHeapDelta > maxCycleHeapGrowthBytes || subBackingDelta > maxBackingGrowthBytes) {
      throw new Error(
        `browser subscribe heap grew excessively across cycles (used=${subHeapDelta}, backing=${subBackingDelta})`,
      );
    }

    console.log("Checking browser memory: repeated publish cycles");
    const publishCycles = 4;
    const publishWarmup = 1;
    let publishBaseline = null;
    let publishLast = null;
    for (let cycle = 1; cycle <= publishCycles; cycle++) {
      await runBrowserPublishScenario(page, {
        moqUri,
        certHash: fingerprint,
        trackName: `leak-pub-${cycle}`,
        groups: 2,
        objectsPerGroup: 2,
        objectBytes: 32768,
      });
      const measured = await collectGarbageAndMeasure(cdp, page);
      assertAllReclaimed(`publish cycle ${cycle}`, measured);
      if (cycle === publishWarmup) publishBaseline = measured.heap;
      publishLast = measured.heap;
    }
    const pubHeapDelta = publishLast.usedSize - publishBaseline.usedSize;
    const pubBackingDelta = publishLast.backingStorageSize - publishBaseline.backingStorageSize;
    console.log(
      `  reclaimed all ${publishCycles} BrowserClient instances; JS heap delta after warmup: ${formatKiB(pubHeapDelta)}, backing delta: ${formatKiB(pubBackingDelta)}`,
    );
    if (pubHeapDelta > maxCycleHeapGrowthBytes || pubBackingDelta > maxBackingGrowthBytes) {
      throw new Error(
        `browser publish heap grew excessively across cycles (used=${pubHeapDelta}, backing=${pubBackingDelta})`,
      );
    }

    console.log("Checking browser memory: multi-group stream cleanup within one session");
    const subMultiStats = await runBrowserSubscribeScenario(page, "subscribe", {
      moqUri,
      certHash: fingerprint,
      trackName: "leak-sub-multigroup",
      groups: 16,
      objectsPerGroup: 2,
      objectBytes: 16384,
      datagrams: false,
    });
    if (
      subMultiStats.activeIncoming > 1 ||
      subMultiStats.activeOutgoing > 2 ||
      subMultiStats.eventQueueCountOverflows !== 0 ||
      subMultiStats.eventQueueByteOverflows !== 0 ||
      subMultiStats.activeStreamLimitOverflows !== 0
    ) {
      throw new Error(`multi-group subscribe retained streams or overflowed: ${JSON.stringify(subMultiStats)}`);
    }
    const subMultiMeasured = await collectGarbageAndMeasure(cdp, page);
    assertAllReclaimed("multi-group subscribe", subMultiMeasured);

    const pubMultiStats = await runBrowserPublishScenario(page, {
      moqUri,
      certHash: fingerprint,
      trackName: "leak-pub-multigroup",
      groups: 16,
      objectsPerGroup: 2,
      objectBytes: 16384,
    });
    if (
      pubMultiStats.activeIncoming > 1 ||
      pubMultiStats.activeOutgoing > 2 ||
      pubMultiStats.eventQueueCountOverflows !== 0 ||
      pubMultiStats.eventQueueByteOverflows !== 0 ||
      pubMultiStats.activeStreamLimitOverflows !== 0
    ) {
      throw new Error(`multi-group publish retained streams or overflowed: ${JSON.stringify(pubMultiStats)}`);
    }
    const pubMultiMeasured = await collectGarbageAndMeasure(cdp, page);
    assertAllReclaimed("multi-group publish", pubMultiMeasured);
    console.log(
      `  16-group streams cleaned up (sub in=${subMultiStats.activeIncoming}/out=${subMultiStats.activeOutgoing}, pub in=${pubMultiStats.activeIncoming}/out=${pubMultiStats.activeOutgoing}); all instances reclaimed`,
    );

    console.log("Checking browser memory: driver subscribe, bad-hash failure, and early close");
    await runBrowserSubscribeScenario(page, "driver-subscribe", {
      moqUri,
      certHash: fingerprint,
      trackName: "leak-driver-sub",
      groups: 2,
      objectsPerGroup: 2,
      objectBytes: 1024,
      datagrams: false,
    });
    assertAllReclaimed("driver-subscribe", await collectGarbageAndMeasure(cdp, page));

    const wrongHash = [...fingerprint];
    wrongHash[0] ^= 0xff;
    const badHashResult = await page.evaluate(
      cfg => globalThis.__moqRunLeakScenario(cfg),
      { mode: "bad-hash", uri: moqUri, certificateHash: wrongHash },
    );
    if (!badHashResult.ok) {
      throw new Error(`bad-hash scenario failed: ${badHashResult.error}`);
    }
    assertAllReclaimed("bad-hash", await collectGarbageAndMeasure(cdp, page));

    const earlyCloseResult = await page.evaluate(
      cfg => globalThis.__moqRunLeakScenario(cfg),
      { mode: "early-close", uri: moqUri, certificateHash: fingerprint },
    );
    if (!earlyCloseResult.ok) {
      throw new Error(`early-close scenario failed: ${earlyCloseResult.error}`);
    }
    const finalMeasured = await collectGarbageAndMeasure(cdp, page);
    assertAllReclaimed("early-close", finalMeasured);
    console.log(
      `  reclaimed all ${finalMeasured.trackedRefs} tracked browser objects across all scenarios`,
    );

    if (pageErrors.length > 0) {
      throw new Error(`browser emitted ${pageErrors.length} pageerror(s): ${pageErrors.join("; ")}`);
    }
    console.log("Browser memory checks passed: all sessions reclaimed and heap growth bounded");
  } finally {
    if (browser) await browser.close();
    await new Promise(resolve => originServer.close(resolve));
  }
}

main().catch(error => {
  console.error(`browser leak check failed: ${error.message}`);
  process.exitCode = 1;
});
