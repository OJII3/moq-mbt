import base64
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import subprocess
import sys
import threading
import time

from playwright.sync_api import sync_playwright


class NativeProcess:
    def __init__(self, role, native_port):
        args = [
            "_build/native/release/build/cmd/track_probe/track_probe.exe",
            role,
            "127.0.0.1",
            native_port,
            "/interop",
            "interop",
            "multi-object",
            "2",
            "2",
            "--insecure",
        ]
        if role == "publish":
            args.append("--hold-open")
        self.process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self.output = []
        self.ready = threading.Event()
        self.success = threading.Event()
        self.done = threading.Event()
        self.stopping = False
        ready_marker = "PUBLISH request accepted" if role == "publish" else "SUBSCRIBE_OK received"
        success_marker = "PUBLISH_DONE send completed" if role == "publish" else "PUBLISH_DONE received"

        def read_output():
            for line in self.process.stdout:
                print(line, end="", flush=True)
                self.output.append(line)
                current_output = "".join(self.output)
                if ready_marker in current_output:
                    self.ready.set()
                if success_marker in current_output:
                    self.success.set()
            code = self.process.wait()
            self.done.set()
            current_output = "".join(self.output)
            if not self.ready.is_set():
                self.error = RuntimeError(f"Native {role} exited before readiness:\n{current_output}")
            elif role == "subscribe" and not self.stopping and not self.success.is_set():
                self.error = RuntimeError(f"Native {role} exited before peer delivery:\n{current_output}")
            elif code != 0 and not (self.stopping and code < 0):
                self.error = RuntimeError(f"Native {role} exited ({code}):\n{current_output}")

        self.error = None
        self.reader = threading.Thread(target=read_output, daemon=True)
        self.reader.start()

    def wait_for(self, event, timeout, message):
        deadline = time.monotonic() + timeout
        while not event.is_set():
            if self.done.is_set():
                if self.error:
                    raise self.error
                raise RuntimeError(f"Native process exited before {message}")
            if time.monotonic() >= deadline:
                raise TimeoutError(message)
            time.sleep(0.05)
        if self.error:
            raise self.error

    def stop(self):
        self.stopping = True
        if self.process.poll() is None:
            self.process.terminate()
        self.process.wait(timeout=5)
        self.reader.join(timeout=1)


def wait_for_status(page, native, acceptable, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = page.evaluate("globalThis.__moqE2EResult?.status")
        if status and status.startswith("failed:"):
            raise RuntimeError(f"Browser failed: {status}")
        if status in acceptable:
            return status
        if native and native.done.is_set():
            if native.error:
                raise native.error
            raise RuntimeError("Native peer exited before browser completion")
        time.sleep(0.1)
    status = page.evaluate("globalThis.__moqE2EResult?.status")
    raise TimeoutError(f"timed out waiting for browser state (status: {status})")


def main():
    if len(sys.argv) != 6:
        raise ValueError("usage: check-moq-rs-browser-e2e.py <subscribe|publish> <url> <fingerprint-base64> <native-port> <bundle-path>")
    mode, url, fingerprint_base64, native_port, bundle_path = sys.argv[1:]
    if mode not in ("subscribe", "publish"):
        raise ValueError(f"invalid mode: {mode}")
    fingerprint = base64.b64decode(fingerprint_base64, validate=True)
    if len(fingerprint) != 32:
        raise ValueError("expected a 32-byte SHA-256 certificate fingerprint")

    class OriginHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = b"<!doctype html><title>moq-rs browser interop</title>"
            self.send_response(200)
            self.send_header("content-type", "text/html")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    origin = ThreadingHTTPServer(("127.0.0.1", 0), OriginHandler)
    Thread(target=origin.serve_forever, daemon=True).start()
    native = None
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.on("console", lambda message: print(f"browser: {message.text}"))
        page.on("pageerror", lambda error: print(f"browser error: {error.message}", file=sys.stderr))
        config = json.dumps(
            {"uri": url.replace("https:", "moqt:", 1), "certificateHash": list(fingerprint), "mode": mode}
        )
        page.add_init_script(
            f"""globalThis.__moqE2EConfig = {config};
            globalThis.__moqE2EResult = {{status: "loading"}};
            globalThis.__moqE2EStartPromise = new Promise(resolve => {{ globalThis.__moqE2EStart = resolve; }});
            globalThis.__moqE2EFinishPromise = new Promise(resolve => {{ globalThis.__moqE2EFinish = resolve; }});"""
        )
        page.goto(f"http://127.0.0.1:{origin.server_port}")
        page.add_script_tag(path=bundle_path)
        ready_status = "ready" if mode == "subscribe" else "published"
        wait_for_status(page, None, {ready_status}, 20)

        if mode == "subscribe":
            native = NativeProcess("publish", native_port)
        else:
            native = NativeProcess("subscribe", native_port)
        native.wait_for(native.ready, 20, f"Native {native.process.args[1]} readiness")
        page.evaluate("globalThis.__moqE2EStart()")

        try:
            if mode == "subscribe":
                wait_for_status(page, native, {"passed"}, 45)
            else:
                wait_for_status(page, native, {"publishing-done"}, 45)
                native.wait_for(native.success, 10, "Native PUBLISH_DONE")
                native.stop()
                page.evaluate("globalThis.__moqE2EFinish()")
                wait_for_status(page, None, {"passed"}, 5)
            if mode == "subscribe":
                native.stop()
            elif native.process.poll() is None:
                native.process.wait(timeout=5)
        except Exception as error:
            status = page.evaluate("globalThis.__moqE2EResult?.status")
            raise RuntimeError(f"browser {mode} E2E stopped (status: {status}): {error}") from error
        finally:
            if native.process.poll() is None:
                native.stop()
            browser.close()
    origin.shutdown()
    origin.server_close()

    print(
        "Native publisher → browser subscriber passed: 4 Objects, 2 Groups, Properties, FIN, PUBLISH_DONE(2)"
        if mode == "subscribe"
        else "Browser publisher → Native subscriber passed: 4 Objects, 2 Groups, Properties, FIN, PUBLISH_DONE(2)"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"moq-rs browser E2E failed: {error}", file=sys.stderr)
        raise SystemExit(1)
