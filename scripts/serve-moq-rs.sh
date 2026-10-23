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

port=${MOQ_RS_PORT:-4443}
openssl ecparam -name prime256v1 -genkey -noout -out "$task_tmp/key.pem" \
  >"$task_tmp/cert.log" 2>&1
openssl req -x509 -new -key "$task_tmp/key.pem" -sha256 -days 1 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -out "$task_tmp/cert.pem" >>"$task_tmp/cert.log" 2>&1

printf 'WebTransport certificate SHA-256 (base64): '
openssl x509 -in "$task_tmp/cert.pem" -outform DER \
  | openssl dgst -sha256 -binary | openssl base64 -A
printf '\nWebTransport URL: https://127.0.0.1:%s/interop\n' "$port"
printf 'WebTransport protocol offer: moqt-18\n'

moq-relay-ietf --bind "127.0.0.1:$port" \
  --tls-cert "$task_tmp/cert.pem" --tls-key "$task_tmp/key.pem" --dev \
  >"$task_tmp/relay.log" 2>&1 &
relay_pid=$!
for ((attempt = 0; attempt < 100; attempt++)); do
  if grep -Fq "listening on 127.0.0.1:$port" "$task_tmp/relay.log"; then
    cat "$task_tmp/relay.log"
    wait "$relay_pid"
    exit $?
  fi
  if ! kill -0 "$relay_pid" 2>/dev/null || ((attempt == 99)); then
    cat "$task_tmp/relay.log" >&2
    echo "moq-relay-ietf failed to start; check the listener port and certificate configuration" >&2
    exit 1
  fi
  sleep 0.1
done
