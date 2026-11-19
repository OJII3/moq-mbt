#!/usr/bin/env bash
# Run the native probes under Valgrind and fail on leaked heap blocks.
set -euo pipefail

cd "$(dirname "$0")/.."
source scripts/interop-relay.sh

if ! command -v valgrind >/dev/null 2>&1; then
  echo "valgrind is required; run this script inside the Nix devShell" >&2
  exit 1
fi

task_tmp=$(mktemp -d)
cleanup() {
  interop_relay_stop
  rm -rf "$task_tmp"
}
trap cleanup EXIT

moon build --release --target native src/cmd/setup_probe
moon build --release --target native src/cmd/track_probe
setup_probe=_build/native/release/build/cmd/setup_probe/setup_probe.exe
track_probe=_build/native/release/build/cmd/track_probe/track_probe.exe

openssl ecparam -name prime256v1 -genkey -noout -out "$task_tmp/key.pem" \
  >"$task_tmp/cert.log" 2>&1
openssl req -x509 -new -key "$task_tmp/key.pem" -sha256 -days 1 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -out "$task_tmp/cert.pem" >>"$task_tmp/cert.log" 2>&1

port=${MOQ_LEAK_CHECK_PORT:-4446}
interop_relay_start "$task_tmp/cert.pem" "$task_tmp/key.pem" "$port" \
  "$task_tmp/relay.log"

check_scenario() {
  local name=$1
  shift
  echo "Checking native memory: $name"
  if ! valgrind \
    --leak-check=full \
    --show-leak-kinds=definite,possible \
    --errors-for-leak-kinds=definite,possible \
    --error-exitcode=99 \
    --num-callers=25 \
    "$@" >"$task_tmp/valgrind.log" 2>&1; then
    cat "$task_tmp/valgrind.log" >&2
    cat "$task_tmp/relay.log" >&2
    echo "native memory check failed: $name" >&2
    exit 1
  fi
  grep -E 'in use at exit|ERROR SUMMARY' "$task_tmp/valgrind.log" |
    sed -E 's/^==[0-9]+== *(.*)$/  \1/' || true
}

check_scenario "connection setup and close" \
  "$setup_probe" 127.0.0.1 "$port" /interop --insecure
check_scenario "request lifecycle" \
  "$track_probe" lifecycle 127.0.0.1 "$port" /interop --insecure
check_scenario "datagram send buffers" \
  "$track_probe" datagram-smoke 127.0.0.1 "$port" /interop --insecure
check_scenario "namespace lifecycle" \
  "$track_probe" namespace 127.0.0.1 "$port" /interop --insecure

echo "Native memory checks passed: no definite or possible leaks"
