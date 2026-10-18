#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
task_tmp=$(mktemp -d)
relay_pid=
cleanup() {
  if [[ -n "$relay_pid" ]]; then
    kill "$relay_pid" 2>/dev/null || true
    wait "$relay_pid" 2>/dev/null || true
  fi
  rm -rf "$task_tmp"
}
trap cleanup EXIT

moon run --release --target native --build-only src/cmd/setup_probe
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -keyout "$task_tmp/key.pem" -out "$task_tmp/cert.pem" >"$task_tmp/cert.log" 2>&1
cat >"$task_tmp/relay.json" <<EOF
{
  "listener": {
    "host": "127.0.0.1", "port": 4443, "versions": [18], "lanes": 1,
    "cert": "$task_tmp/cert.pem", "key": "$task_tmp/key.pem"
  },
  "auth": { "mode": "allow_all" }
}
EOF
moq5-relay serve --config "$task_tmp/relay.json" >"$task_tmp/relay.log" 2>&1 &
relay_pid=$!
for ((attempt = 0; attempt < 100; attempt++)); do
  if grep -q 'MOQ5 Relay: listening on' "$task_tmp/relay.log"; then
    break
  fi
  if ! kill -0 "$relay_pid" 2>/dev/null || ((attempt == 99)); then
    cat "$task_tmp/relay.log" >&2
    exit 1
  fi
  sleep 0.1
done

# An untrusted self-signed certificate must fail with validation enabled.
if moon run --release --target native src/cmd/setup_probe -- \
  127.0.0.1 4443 / >"$task_tmp/verify.log" 2>&1; then
  echo 'Self-signed certificate was unexpectedly trusted' >&2
  exit 1
fi

# Disable validation only for this temporary, loopback-only test relay.
if ! moon run --release --target native src/cmd/setup_probe -- \
  127.0.0.1 4443 / --insecure >"$task_tmp/probe.log" 2>&1; then
  cat "$task_tmp/probe.log" "$task_tmp/relay.log" >&2
  exit 1
fi

# SETUP can be sent independently by either peer. Check the relay's trace too,
# so receiving its SETUP alone cannot hide a dropped outbound client SETUP.
kill -USR2 "$relay_pid"
for ((attempt = 0; attempt < 100; attempt++)); do
  if grep -q '"kind":"auth_decision","detail":0,' "$task_tmp/relay.log"; then
    break
  fi
  if ! kill -0 "$relay_pid" 2>/dev/null || ((attempt == 99)); then
    cat "$task_tmp/relay.log" >&2
    echo 'Relay did not accept the client SETUP' >&2
    exit 1
  fi
  sleep 0.1
done
cat "$task_tmp/probe.log"
echo 'Relay accepted the client SETUP'
