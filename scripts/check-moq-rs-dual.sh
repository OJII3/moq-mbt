#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
task_tmp=$(mktemp -d)
relay_pid=
raw_pid=
webtransport_pid=
cleanup() {
  for pid in "$webtransport_pid" "$raw_pid" "$relay_pid"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  rm -rf "$task_tmp"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

port=${MOQ_RS_PORT:-4443}
browser_e2e_bundle=_build/js/release/build/cmd/web_interop_probe/web_interop_probe.js
moon build --release --target js src/cmd/web_interop_probe
moon build --release --target native src/cmd/track_probe
openssl ecparam -name prime256v1 -genkey -noout -out "$task_tmp/key.pem" \
  >"$task_tmp/cert.log" 2>&1
openssl req -x509 -new -key "$task_tmp/key.pem" -sha256 -days 1 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -out "$task_tmp/cert.pem" >>"$task_tmp/cert.log" 2>&1
fingerprint=$(openssl x509 -in "$task_tmp/cert.pem" -outform DER \
  | openssl dgst -sha256 -binary | openssl base64 -A)

relay_log="$task_tmp/relay.log"
moq-relay-ietf --bind "127.0.0.1:$port" \
  --tls-cert "$task_tmp/cert.pem" --tls-key "$task_tmp/key.pem" \
  >"$relay_log" 2>&1 &
relay_pid=$!
for ((attempt = 0; attempt < 100; attempt++)); do
  if grep -Fq "listening on 127.0.0.1:$port" "$relay_log"; then
    break
  fi
  if ! kill -0 "$relay_pid" 2>/dev/null || ((attempt == 99)); then
    cat "$relay_log" >&2
    echo "moq-rs failed to start; check the UDP port and certificate configuration" >&2
    exit 1
  fi
  sleep 0.1
done

echo "Checking raw QUIC and WebTransport concurrently on moq-rs 127.0.0.1:$port"
timeout --signal=TERM --kill-after=5s "${MOQ_RS_SETUP_TIMEOUT:-45}s" \
  moon run --release --target native src/cmd/setup_probe -- \
  127.0.0.1 "$port" /interop --insecure >"$task_tmp/raw.log" 2>&1 &
raw_pid=$!
timeout --signal=TERM --kill-after=5s "${MOQ_RS_SETUP_TIMEOUT:-45}s" \
  node scripts/check-moq-rs-webtransport.cjs \
  "https://127.0.0.1:$port/interop" "$fingerprint" \
  >"$task_tmp/webtransport.log" 2>&1 &
webtransport_pid=$!

status=0
if wait "$raw_pid"; then
  raw_pid=
else
  status=1
fi
if wait "$webtransport_pid"; then
  webtransport_pid=
else
  status=1
fi
cat "$task_tmp/raw.log" "$task_tmp/webtransport.log"
if ((status != 0)); then
  cat "$relay_log" >&2
  echo "raw QUIC log:" >&2
  cat "$task_tmp/raw.log" >&2
  echo "WebTransport log:" >&2
  cat "$task_tmp/webtransport.log" >&2
  exit "$status"
fi

if [[ -n ${MOQ_RS_E2E_MODES:-} ]]; then
  read -r -a e2e_modes <<<"$MOQ_RS_E2E_MODES"
else
  e2e_modes=(subscribe publish)
fi
for mode in "${e2e_modes[@]}"; do
  timeout --signal=TERM --kill-after=5s "${MOQ_RS_BROWSER_TIMEOUT:-120}s" \
    node scripts/check-moq-rs-browser-e2e.cjs \
    "$mode" "https://127.0.0.1:$port/interop" "$fingerprint" "$port" \
    "$browser_e2e_bundle" || {
      echo "Browser E2E ($mode) failed; relay log:" >&2
      cat "$relay_log" >&2
      exit 1
    }
done
