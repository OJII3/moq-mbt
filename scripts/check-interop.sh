#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
task_tmp=$(mktemp -d)
relay_pid=
publisher_pid=
cleanup() {
  if [[ -n "$publisher_pid" ]]; then
    kill "$publisher_pid" 2>/dev/null || true
    wait "$publisher_pid" 2>/dev/null || true
  fi
  if [[ -n "$relay_pid" ]]; then
    kill "$relay_pid" 2>/dev/null || true
    wait "$relay_pid" 2>/dev/null || true
  fi
  rm -rf "$task_tmp"
}
trap cleanup EXIT

moon run --release --target native --build-only src/cmd/setup_probe
moon run --release --target native --build-only src/cmd/track_probe
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -keyout "$task_tmp/key.pem" -out "$task_tmp/cert.pem" >"$task_tmp/cert.log" 2>&1

relay_log="$task_tmp/moq-rs-relay.log"
moq-relay-ietf --bind 127.0.0.1:4443 \
  --tls-cert "$task_tmp/cert.pem" --tls-key "$task_tmp/key.pem" --dev \
  >"$relay_log" 2>&1 &
relay_pid=$!
for ((attempt = 0; attempt < 100; attempt++)); do
  if grep -q 'listening on 127.0.0.1:4443' "$relay_log"; then
    break
  fi
  if ! kill -0 "$relay_pid" 2>/dev/null || ((attempt == 99)); then
    cat "$relay_log" >&2
    echo "moq-rs failed to start; check the listener port and certificate configuration" >&2
    exit 1
  fi
  sleep 0.1
done

echo "Checking Draft 18 interoperability with moq-rs"

# An untrusted self-signed certificate must fail with validation enabled.
if moon run --release --target native src/cmd/setup_probe -- \
  127.0.0.1 4443 / >"$task_tmp/verify.log" 2>&1; then
  echo "moq-rs unexpectedly trusted the self-signed certificate" >&2
  exit 1
fi

# Disable validation only for this temporary, loopback-only test relay.
if ! moon run --release --target native src/cmd/setup_probe -- \
  127.0.0.1 4443 / --insecure >"$task_tmp/probe.log" 2>&1; then
  cat "$task_tmp/probe.log" "$relay_log" >&2
  exit 1
fi
cat "$task_tmp/probe.log"

moon run --release --target native src/cmd/track_probe -- \
  publish 127.0.0.1 4443 /moq interop multi-object 3 2 --insecure \
  >"$task_tmp/publisher.log" 2>&1 &
publisher_pid=$!
if ! moon run --release --target native src/cmd/track_probe -- \
  subscribe 127.0.0.1 4443 /moq interop multi-object 3 2 --insecure \
  >"$task_tmp/subscriber.log" 2>&1; then
  cat "$task_tmp/publisher.log" "$task_tmp/subscriber.log" "$relay_log" >&2
  exit 1
fi

if ! wait "$publisher_pid"; then
  publisher_pid=
  cat "$task_tmp/publisher.log" "$relay_log" >&2
  exit 1
fi
publisher_pid=
cat "$task_tmp/publisher.log" "$task_tmp/subscriber.log"
