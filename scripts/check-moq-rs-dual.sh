#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
task_tmp=$(mktemp -d)
relay_pid=
raw_pid=
webtransport_pid=
node_pid=
cleanup() {
  for pid in "$node_pid" "$webtransport_pid" "$raw_pid" "$relay_pid"; do
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
moon run --release --target native src/cmd/setup_probe -- \
  127.0.0.1 "$port" /interop --insecure >"$task_tmp/raw.log" 2>&1 &
raw_pid=$!
node scripts/check-moq-rs-webtransport.cjs \
  "https://127.0.0.1:$port/interop" "$fingerprint" \
  >"$task_tmp/webtransport.log" 2>&1 &
webtransport_pid=$!
npm run smoke:node-webtransport -- \
  "https://127.0.0.1:$port/interop" "$fingerprint" \
  >"$task_tmp/node.log" 2>&1 &
node_pid=$!

status=0
if wait "$raw_pid"; then
  raw_pid=
else
  status=$?
fi
if wait "$webtransport_pid"; then
  webtransport_pid=
else
  status=$?
fi
if wait "$node_pid"; then
  node_pid=
else
  status=$?
fi
cat "$task_tmp/raw.log" "$task_tmp/webtransport.log" "$task_tmp/node.log"
if ((status != 0)); then
  cat "$relay_log" >&2
  exit "$status"
fi
