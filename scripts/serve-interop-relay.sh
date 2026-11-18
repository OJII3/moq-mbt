#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
task_tmp=$(mktemp -d)
relay_pid=
cleanup() {
  if [[ -n "$relay_pid" ]]; then
    kill -KILL "$relay_pid" 2>/dev/null || true
    wait "$relay_pid" 2>/dev/null || true
  fi
  rm -rf "$task_tmp"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

port=${MOQTAIL_PORT:-4443}
openssl ecparam -name prime256v1 -genkey -noout -out "$task_tmp/key.pem" \
  >"$task_tmp/cert.log" 2>&1
openssl req -x509 -new -key "$task_tmp/key.pem" -sha256 -days 1 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -out "$task_tmp/cert.pem" >>"$task_tmp/cert.log" 2>&1

fingerprint=$(openssl x509 -in "$task_tmp/cert.pem" -outform DER \
  | openssl dgst -sha256 -binary | openssl base64 -A)
printf 'WebTransport certificate SHA-256 (base64): %s\n' "$fingerprint"
printf '\nWebTransport URL: https://127.0.0.1:%s/interop\n' "$port"
printf 'WebTransport protocol offer: moqt-18\n'

moqtail-relay --host 127.0.0.1 --port "$port" \
  --cert-file "$task_tmp/cert.pem" --key-file "$task_tmp/key.pem" \
  >"$task_tmp/relay.log" 2>&1 &
relay_pid=$!
for ((attempt = 0; attempt < 100; attempt++)); do
  if kill -0 "$relay_pid" 2>/dev/null && ((attempt >= 10)); then
    cat "$task_tmp/relay.log"
    break
  fi
  if ! kill -0 "$relay_pid" 2>/dev/null || ((attempt == 99)); then
    cat "$task_tmp/relay.log" >&2 || true
    echo "moqtail failed to start; check the listener port and certificate configuration" >&2
    exit 1
  fi
  sleep 0.1
done

node scripts/check-moqtail-webtransport.cjs "https://127.0.0.1:$port/interop" "$fingerprint"
tail -f "$task_tmp/relay.log"
