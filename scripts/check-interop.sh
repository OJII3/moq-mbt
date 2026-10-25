#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
task_tmp=$(mktemp -d)
relay_pid=
publisher_pid=
subscriber_pid=
cleanup() {
  if [[ -n "$subscriber_pid" ]]; then
    kill "$subscriber_pid" 2>/dev/null || true
    wait "$subscriber_pid" 2>/dev/null || true
  fi
  if [[ -n "$publisher_pid" ]]; then
    kill "$publisher_pid" 2>/dev/null || true
    wait "$publisher_pid" 2>/dev/null || true
  fi
  if [[ -n "$relay_pid" ]]; then
    kill -KILL "$relay_pid" 2>/dev/null || true
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

port=${MOQTAIL_PORT:-4443}
moqtail-relay --host 127.0.0.1 --port "$port" \
  --cert-file "$task_tmp/cert.pem" --key-file "$task_tmp/key.pem" \
  >"$task_tmp/relay.log" 2>&1 &
relay_pid=$!
for ((attempt = 0; attempt < 100; attempt++)); do
  if kill -0 "$relay_pid" 2>/dev/null && ((attempt >= 10)); then
    break
  fi
  if ! kill -0 "$relay_pid" 2>/dev/null || ((attempt == 99)); then
    cat "$task_tmp/relay.log" >&2 || true
    echo "moqtail failed to start; check the listener port and certificate configuration" >&2
    exit 1
  fi
  sleep 0.1
done

echo "Checking Draft 18 interoperability with moqtail"

# An untrusted self-signed certificate must fail with validation enabled.
if moon run --release --target native src/cmd/setup_probe -- \
  127.0.0.1 "$port" /interop >"$task_tmp/verify.log" 2>&1; then
  echo "moqtail unexpectedly trusted the self-signed certificate" >&2
  exit 1
fi

# Disable validation only for this temporary, loopback-only test relay.
if ! moon run --release --target native src/cmd/setup_probe -- \
  127.0.0.1 "$port" /interop --insecure >"$task_tmp/probe.log" 2>&1; then
  cat "$task_tmp/probe.log" >&2
  cat "$task_tmp/relay.log" >&2
  exit 1
fi
cat "$task_tmp/probe.log"

# moqtail rejects subscriptions until a publisher has registered the track.
moon run --release --target native src/cmd/track_probe -- \
  publish 127.0.0.1 "$port" /interop interop multi-object 3 2 --insecure \
  >"$task_tmp/publisher.log" 2>&1 &
publisher_pid=$!
for ((attempt = 0; attempt < 100; attempt++)); do
  if grep -Fq 'PUBLISH request accepted' "$task_tmp/publisher.log"; then
    break
  fi
  if ! kill -0 "$publisher_pid" 2>/dev/null || ((attempt == 99)); then
    cat "$task_tmp/publisher.log" >&2
    cat "$task_tmp/relay.log" >&2
    echo "publisher did not register the track" >&2
    exit 1
  fi
  sleep 0.1
done

moon run --release --target native src/cmd/track_probe -- \
  subscribe 127.0.0.1 "$port" /interop interop multi-object 3 2 --insecure \
  >"$task_tmp/subscriber.log" 2>&1 &
subscriber_pid=$!
if ! wait "$subscriber_pid"; then
  subscriber_pid=
  cat "$task_tmp/publisher.log" "$task_tmp/subscriber.log" >&2
  cat "$task_tmp/relay.log" >&2
  exit 1
fi
subscriber_pid=

if ! wait "$publisher_pid"; then
  publisher_pid=
  cat "$task_tmp/publisher.log" >&2
  cat "$task_tmp/relay.log" >&2
  exit 1
fi
publisher_pid=
cat "$task_tmp/publisher.log" "$task_tmp/subscriber.log"
