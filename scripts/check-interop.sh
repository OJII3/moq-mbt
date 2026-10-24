#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
task_tmp=$(mktemp -d)
relay_id=
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
  if [[ -n "$relay_id" ]]; then
    docker rm -f "$relay_id" >/dev/null 2>&1 || true
  fi
  rm -rf "$task_tmp"
}
trap cleanup EXIT

moon run --release --target native --build-only src/cmd/setup_probe
moon run --release --target native --build-only src/cmd/track_probe
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -keyout "$task_tmp/key.pem" -out "$task_tmp/cert.pem" >"$task_tmp/cert.log" 2>&1

port=${MOQX_PORT:-4443}
relay_config="$task_tmp/moqx.yaml"
scripts/write-moqx-config.sh "$relay_config" "$port"
relay_id=$(scripts/start-moqx-container.sh "$task_tmp" "$port")
for ((attempt = 0; attempt < 100; attempt++)); do
  if [[ $(docker inspect --format '{{.State.Running}}' "$relay_id" 2>/dev/null) == true ]] && ((attempt >= 10)); then
    break
  fi
  if [[ $(docker inspect --format '{{.State.Running}}' "$relay_id" 2>/dev/null) != true ]] || ((attempt == 99)); then
    docker logs "$relay_id" >&2 || true
    echo "moqx failed to start; check the listener port and certificate configuration" >&2
    exit 1
  fi
  sleep 0.1
done

echo "Checking Draft 18 interoperability with moqx"

# An untrusted self-signed certificate must fail with validation enabled.
if moon run --release --target native src/cmd/setup_probe -- \
  127.0.0.1 "$port" /interop >"$task_tmp/verify.log" 2>&1; then
  echo "moqx unexpectedly trusted the self-signed certificate" >&2
  exit 1
fi

# Disable validation only for this temporary, loopback-only test relay.
if ! moon run --release --target native src/cmd/setup_probe -- \
  127.0.0.1 "$port" /interop --insecure >"$task_tmp/probe.log" 2>&1; then
  cat "$task_tmp/probe.log" >&2
  docker logs "$relay_id" >&2
  exit 1
fi
cat "$task_tmp/probe.log"

moon run --release --target native src/cmd/track_probe -- \
  subscribe 127.0.0.1 "$port" /interop interop multi-object 3 2 --insecure \
  >"$task_tmp/subscriber.log" 2>&1 &
subscriber_pid=$!
for ((attempt = 0; attempt < 100; attempt++)); do
  if grep -Fq 'SUBSCRIBE request sent' "$task_tmp/subscriber.log"; then
    break
  fi
  if ! kill -0 "$subscriber_pid" 2>/dev/null || ((attempt == 99)); then
    cat "$task_tmp/subscriber.log" >&2
    docker logs "$relay_id" >&2
    echo "subscriber did not send SUBSCRIBE" >&2
    exit 1
  fi
  sleep 0.1
done

moon run --release --target native src/cmd/track_probe -- \
  publish 127.0.0.1 "$port" /interop interop multi-object 3 2 --insecure \
  >"$task_tmp/publisher.log" 2>&1 &
publisher_pid=$!
if ! wait "$subscriber_pid"; then
  subscriber_pid=
  cat "$task_tmp/publisher.log" "$task_tmp/subscriber.log" >&2
  docker logs "$relay_id" >&2
  exit 1
fi
subscriber_pid=

if ! wait "$publisher_pid"; then
  publisher_pid=
  cat "$task_tmp/publisher.log" >&2
  docker logs "$relay_id" >&2
  exit 1
fi
publisher_pid=
cat "$task_tmp/publisher.log" "$task_tmp/subscriber.log"
