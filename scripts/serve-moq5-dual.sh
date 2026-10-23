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
trap 'exit 130' INT
trap 'exit 143' TERM

native_port=${MOQ5_NATIVE_PORT:-4443}
webtransport_port=${MOQ5_WEBTRANSPORT_PORT:-4444}
origin=${MOQ5_WEBTRANSPORT_ORIGIN:-http://localhost:8000}
openssl ecparam -name prime256v1 -genkey -noout -out "$task_tmp/key.pem" \
  >"$task_tmp/cert.log" 2>&1
openssl req -x509 -new -key "$task_tmp/key.pem" -sha256 -days 1 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -out "$task_tmp/cert.pem" >>"$task_tmp/cert.log" 2>&1
printf 'WebTransport certificate SHA-256 (base64): '
openssl x509 -in "$task_tmp/cert.pem" -outform DER \
  | openssl dgst -sha256 -binary | openssl base64 -A
printf '\nWebTransport URL: https://localhost:%s/moq\nAllowed Origin: %s\n' \
  "$webtransport_port" "$origin"

cat >"$task_tmp/moq5-relay.json" <<EOF
{
  "listener": {
    "host": "127.0.0.1", "port": $native_port, "versions": [18], "lanes": 1,
    "cert": "$task_tmp/cert.pem", "key": "$task_tmp/key.pem"
  },
  "webtransport": {
    "host": "127.0.0.1", "port": $webtransport_port, "versions": [18], "lanes": 1,
    "path": "/moq", "profile": "current",
    "origin_policy": "allowlist", "allowed_origins": ["$origin"],
    "cert": "$task_tmp/cert.pem", "key": "$task_tmp/key.pem"
  },
  "auth": { "mode": "allow_all" }
}
EOF

moq5-relay serve --config "$task_tmp/moq5-relay.json" \
  >"$task_tmp/relay.log" 2>&1 &
relay_pid=$!
for ((attempt = 0; attempt < 100; attempt++)); do
  if grep -Fq "MOQ5 Relay: listening on 127.0.0.1:$native_port" "$task_tmp/relay.log" \
    && grep -Fq "MOQ5 Relay: WebTransport on 127.0.0.1:$webtransport_port/moq" "$task_tmp/relay.log"; then
    cat "$task_tmp/relay.log"
    wait "$relay_pid"
    exit $?
  fi
  if ! kill -0 "$relay_pid" 2>/dev/null || ((attempt == 99)); then
    cat "$task_tmp/relay.log" >&2
    echo "moq5-relay failed to start; check listener ports and certificate configuration" >&2
    exit 1
  fi
  sleep 0.1
done
