#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
task_tmp=$(mktemp -d)
relay_id=
cleanup() {
  if [[ -n "$relay_id" ]]; then
    docker rm -f "$relay_id" >/dev/null 2>&1 || true
  fi
  rm -rf "$task_tmp"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

port=${MOQX_PORT:-4443}
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

relay_config="$task_tmp/moqx.yaml"
scripts/write-moqx-config.sh "$relay_config" "$port" "$task_tmp/cert.pem" "$task_tmp/key.pem"
relay_id=$(scripts/start-moqx-container.sh "$task_tmp" "$port")
for ((attempt = 0; attempt < 100; attempt++)); do
  if [[ $(docker inspect --format '{{.State.Running}}' "$relay_id" 2>/dev/null) == true ]] && ((attempt >= 10)); then
    docker logs "$relay_id"
    break
  fi
  if [[ $(docker inspect --format '{{.State.Running}}' "$relay_id" 2>/dev/null) != true ]] || ((attempt == 99)); then
    docker logs "$relay_id" >&2 || true
    echo "moqx failed to start; check the listener port and certificate configuration" >&2
    exit 1
  fi
  sleep 0.1
done

node scripts/check-moqx-webtransport.cjs "https://127.0.0.1:$port/interop" "$fingerprint"
docker logs -f "$relay_id"
