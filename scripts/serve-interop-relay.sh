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

port=${MOQ_INTEROP_PORT:-4443}
openssl ecparam -name prime256v1 -genkey -noout -out "$task_tmp/key.pem" \
  >"$task_tmp/cert.log" 2>&1
openssl req -x509 -new -key "$task_tmp/key.pem" -sha256 -days 1 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -out "$task_tmp/cert.pem" >>"$task_tmp/cert.log" 2>&1

fingerprint=$(openssl x509 -in "$task_tmp/cert.pem" -outform DER \
  | openssl dgst -sha256 -binary | openssl base64 -A)
printf 'WebTransport certificate SHA-256 (base64): %s\n' "$fingerprint"
printf '\nWebTransport URL: https://127.0.0.1:%s/\n' "$port"
printf 'WebTransport protocol offer: moqt-18\n'

interop_relay_start "$task_tmp/cert.pem" "$task_tmp/key.pem" "$port" \
  "$task_tmp/relay.log"

node scripts/check-interop-webtransport.cjs \
  "https://127.0.0.1:$port/" "$fingerprint"
tail -f "$task_tmp/relay.log"
