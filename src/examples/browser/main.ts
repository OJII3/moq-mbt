const form = document.querySelector<HTMLFormElement>("#connection-form")!;
const status = document.querySelector<HTMLElement>("#status")!;
const startButton = document.querySelector<HTMLButtonElement>("#start")!;
const finishButton = document.querySelector<HTMLButtonElement>("#finish")!;
let startTransfer: (() => void) | undefined;
let finishPublisher: (() => void) | undefined;

form.addEventListener("submit", async event => {
  event.preventDefault();
  form.querySelector<HTMLButtonElement>('button[type="submit"]')!.disabled = true;
  const certificateHash = document.querySelector<HTMLInputElement>("#certificate-hash")!.value.trim();
  globalThis.__moqE2EConfig = {
    uri: document.querySelector<HTMLInputElement>("#url")!.value,
    certificateHash: certificateHash ? [...Uint8Array.from(atob(certificateHash), c => c.charCodeAt(0))] : [],
    mode: document.querySelector<HTMLSelectElement>("#mode")!.value,
    namespace: document.querySelector<HTMLInputElement>("#namespace")!.value,
    track: document.querySelector<HTMLInputElement>("#track")!.value,
  };
  globalThis.__moqE2EResult = { status: "loading" };
  globalThis.__moqE2EStartPromise = new Promise(resolve => { startTransfer = resolve; });
  globalThis.__moqE2EFinishPromise = new Promise(resolve => { finishPublisher = resolve; });
  try {
    await import("mbt:ojii3/moq-mbt/cmd/web_interop_probe");
  } catch (error) {
    status.textContent = `Could not load the sample bundle: ${String(error)}`;
  }
});

startButton.addEventListener("click", () => {
  startButton.disabled = true;
  startTransfer?.();
});
finishButton.addEventListener("click", () => {
  finishButton.disabled = true;
  finishPublisher?.();
});

const statusTimer = setInterval(() => {
  const current = globalThis.__moqE2EResult?.status ?? "Not connected";
  status.textContent = current;
  startButton.disabled = !(current === "ready" || current === "published" || current === "subscribed");
  finishButton.disabled = current !== "publishing-done";
  if (current === "passed" || current.startsWith("failed:")) clearInterval(statusTimer);
}, 200);
