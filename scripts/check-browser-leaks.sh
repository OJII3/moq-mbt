#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
source scripts/interop-relay.sh

task_tmp=$(mktemp -d)
cleanup() {
  interop_relay_stop
  rm -rf "$task_tmp"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

moon build --release --target js src/cmd/web_interop_probe
moon build --release --target native src/cmd/track_probe
bundle=_build/js/release/build/cmd/web_interop_probe/web_interop_probe.js
browser_timeout=${MOQ_BROWSER_LEAK_TIMEOUT:-180}s

openssl ecparam -name prime256v1 -genkey -noout -out "$task_tmp/key.pem" \
  >"$task_tmp/cert.log" 2>&1
openssl req -x509 -new -key "$task_tmp/key.pem" -sha256 -days 1 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -out "$task_tmp/cert.pem" >>"$task_tmp/cert.log" 2>&1
fingerprint=$(openssl x509 -in "$task_tmp/cert.pem" -outform DER \
  | openssl dgst -sha256 -binary | openssl base64 -A)

port=${MOQ_BROWSER_LEAK_CHECK_PORT:-4447}
interop_relay_start "$task_tmp/cert.pem" "$task_tmp/key.pem" "$port" \
  "$task_tmp/relay.log"

if ! timeout --signal=TERM --kill-after=5s "$browser_timeout" \
  node scripts/check-browser-leaks.cjs \
  "https://127.0.0.1:$port/" "$fingerprint" "$port" "$bundle"; then
  cat "$task_tmp/relay.log" >&2 || true
  exit 1
fi
