#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
task_tmp=$(mktemp -d)
relay_pid=
raw_pid=
webtransport_pid=
fetch_publisher_pid=
scenario_subscriber_pid=
scenario_publisher_pid=
cleanup() {
  for pid in "$scenario_subscriber_pid" "$scenario_publisher_pid" "$fetch_publisher_pid" "$webtransport_pid" "$raw_pid"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
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
browser_e2e_bundle=_build/js/release/build/cmd/web_interop_probe/web_interop_probe.js
moon build --release --target js src/cmd/web_interop_probe
moon build --release --target native src/cmd/track_probe
openssl ecparam -name prime256v1 -genkey -noout -out "$task_tmp/key.pem" \
  >"$task_tmp/cert.log" 2>&1
openssl req -x509 -new -key "$task_tmp/key.pem" -sha256 -days 1 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -out "$task_tmp/cert.pem" >>"$task_tmp/cert.log" 2>&1
fingerprint=$(openssl x509 -in "$task_tmp/cert.pem" -outform DER \
  | openssl dgst -sha256 -binary | openssl base64 -A)

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
    echo "moqtail failed to start; check the UDP port and certificate configuration" >&2
    exit 1
  fi
  sleep 0.1
done

echo "Checking raw QUIC and WebTransport concurrently on moqtail 127.0.0.1:$port"
timeout --signal=TERM --kill-after=5s "${MOQTAIL_SETUP_TIMEOUT:-45}s" \
  moon run --release --target native src/cmd/setup_probe -- \
  127.0.0.1 "$port" /interop --insecure >"$task_tmp/raw.log" 2>&1 &
raw_pid=$!
timeout --signal=TERM --kill-after=5s "${MOQTAIL_SETUP_TIMEOUT:-45}s" \
  node scripts/check-moqtail-webtransport.cjs \
  "https://127.0.0.1:$port/interop" "$fingerprint" \
  >"$task_tmp/webtransport.log" 2>&1 &
webtransport_pid=$!

status=0
if wait "$raw_pid"; then
  raw_pid=
else
  status=1
fi
if wait "$webtransport_pid"; then
  webtransport_pid=
else
  status=1
fi
cat "$task_tmp/raw.log" "$task_tmp/webtransport.log"
if ((status != 0)); then
  cat "$task_tmp/relay.log" >&2
  echo "raw QUIC log:" >&2
  cat "$task_tmp/raw.log" >&2
  echo "WebTransport log:" >&2
  cat "$task_tmp/webtransport.log" >&2
  exit "$status"
fi


if [[ -n ${MOQTAIL_E2E_MODES:-} ]]; then
  read -r -a e2e_modes <<<"$MOQTAIL_E2E_MODES"
else
  e2e_modes=(subscribe publish)
fi
for mode in "${e2e_modes[@]}"; do
  timeout --signal=TERM --kill-after=5s "${MOQTAIL_BROWSER_TIMEOUT:-120}s" \
    node scripts/check-moqtail-browser-e2e.cjs \
    "$mode" "https://127.0.0.1:$port/interop" "$fingerprint" "$port" \
    "$browser_e2e_bundle" || {
      echo "Browser E2E ($mode) failed; relay log:" >&2
      cat "$task_tmp/relay.log" >&2
      exit 1
    }
done

run_probe() {
  local name=$1
  shift
  timeout --signal=TERM --kill-after=5s "${MOQTAIL_SCENARIO_TIMEOUT:-60}s" \
    moon run --release --target native src/cmd/track_probe -- \
    "$@" >"$task_tmp/$name.log" 2>&1 || {
      cat "$task_tmp/$name.log" >&2
      cat "$task_tmp/relay.log" >&2
      echo "moqtail $name scenario failed" >&2
      exit 1
    }
  cat "$task_tmp/$name.log"
}

if [[ ${MOQTAIL_RUN_FETCH:-0} == 1 ]]; then
  fetch_group_count=2
  timeout --signal=TERM --kill-after=5s "${MOQTAIL_SCENARIO_TIMEOUT:-60}s" \
    moon run --release --target native src/cmd/track_probe -- \
    publish 127.0.0.1 "$port" /interop interop fetch-live "$fetch_group_count" 2 \
    --insecure --hold-open --wait-for-fetch >"$task_tmp/fetch-publisher.log" 2>&1 &
  fetch_publisher_pid=$!
  for ((attempt = 0; attempt < 100; attempt++)); do
    if grep -Fq 'PUBLISH request accepted' "$task_tmp/fetch-publisher.log"; then break; fi
    if ! kill -0 "$fetch_publisher_pid" 2>/dev/null || ((attempt == 99)); then
      tail -n 30 "$task_tmp/fetch-publisher.log" >&2
      cat "$task_tmp/relay.log" >&2
      echo "FETCH publisher did not register the track" >&2
      exit 1
    fi
    sleep 0.1
  done
  echo "FETCH publisher is active; requesting groups 0 and 1"
  if ! timeout --signal=TERM --kill-after=5s "${MOQTAIL_SCENARIO_TIMEOUT:-60}s" \
    _build/native/release/build/cmd/track_probe/track_probe.exe \
    fetch 127.0.0.1 "$port" /interop interop fetch-live 0 1 --insecure \
    >"$task_tmp/fetch.log" 2>&1; then
    cat "$task_tmp/fetch.log" >&2
    tail -n 50 "$task_tmp/relay.log" >&2
    tail -n 30 "$task_tmp/fetch-publisher.log" >&2
    echo "moqtail standalone FETCH scenario failed" >&2
    exit 1
  fi
  cat "$task_tmp/fetch.log"
  kill "$fetch_publisher_pid" 2>/dev/null || true
  wait "$fetch_publisher_pid" 2>/dev/null || true
  fetch_publisher_pid=
else
  echo "Skipping standalone FETCH; set MOQTAIL_RUN_FETCH=1 to probe the pinned relay"
fi
run_probe lifecycle lifecycle 127.0.0.1 "$port" /interop --insecure
if [[ ${MOQTAIL_RUN_NAMESPACE:-0} == 1 ]]; then
  run_probe namespace namespace 127.0.0.1 "$port" /interop --insecure
else
  echo "Skipping Namespace withdrawal; set MOQTAIL_RUN_NAMESPACE=1 to probe the pinned relay"
fi
if [[ ${MOQTAIL_RUN_JOINING_FETCH:-0} == 1 ]]; then
  run_probe joining-fetch joining-fetch 127.0.0.1 "$port" /interop --insecure
else
  echo "Skipping joining FETCH; set MOQTAIL_RUN_JOINING_FETCH=1 to probe the pinned relay"
fi
run_probe datagram-smoke datagram-smoke 127.0.0.1 "$port" /interop --insecure

mkfifo "$task_tmp/mixed-publisher.stdin"
exec 3<>"$task_tmp/mixed-publisher.stdin"
timeout --signal=TERM --kill-after=5s "${MOQTAIL_SCENARIO_TIMEOUT:-60}s" \
  moon run --release --target native src/cmd/track_probe -- \
  publish 127.0.0.1 "$port" /interop interop datagram-mixed 3 2 --datagrams --insecure --wait-for-start \
  <&3 >"$task_tmp/mixed-publisher.log" 2>&1 &
scenario_publisher_pid=$!
for ((attempt = 0; attempt < 100; attempt++)); do
  if grep -Fq 'PUBLISH request accepted' "$task_tmp/mixed-publisher.log"; then break; fi
  if ! kill -0 "$scenario_publisher_pid" 2>/dev/null || ((attempt == 99)); then
    cat "$task_tmp/mixed-publisher.log" >&2
    cat "$task_tmp/relay.log" >&2
    echo "mixed-mode publisher did not register the track" >&2
    exit 1
  fi
  sleep 0.1
done

timeout --signal=TERM --kill-after=5s "${MOQTAIL_SCENARIO_TIMEOUT:-60}s" \
  moon run --release --target native src/cmd/track_probe -- \
  subscribe 127.0.0.1 "$port" /interop interop datagram-mixed 3 2 --datagrams --insecure \
  >"$task_tmp/mixed-subscriber.log" 2>&1 &
scenario_subscriber_pid=$!
for ((attempt = 0; attempt < 100; attempt++)); do
  if grep -Fq 'SUBSCRIBE_OK received' "$task_tmp/mixed-subscriber.log"; then break; fi
  if ! kill -0 "$scenario_subscriber_pid" 2>/dev/null || ((attempt == 99)); then
    cat "$task_tmp/mixed-publisher.log" "$task_tmp/mixed-subscriber.log" >&2
    cat "$task_tmp/relay.log" >&2
    echo "mixed-mode subscriber did not complete SUBSCRIBE" >&2
    exit 1
  fi
  sleep 0.1
done
printf '\n' >&3
exec 3>&-
if ! wait "$scenario_subscriber_pid"; then
  scenario_subscriber_pid=
  cat "$task_tmp/mixed-publisher.log" "$task_tmp/mixed-subscriber.log" >&2
  cat "$task_tmp/relay.log" >&2
  echo "Datagram and Subgroup mixed-mode scenario failed" >&2
  exit 1
fi
scenario_subscriber_pid=
if ! wait "$scenario_publisher_pid"; then
  scenario_publisher_pid=
  cat "$task_tmp/mixed-publisher.log" >&2
  cat "$task_tmp/relay.log" >&2
  echo "Datagram and Subgroup mixed-mode publisher failed" >&2
  exit 1
fi
scenario_publisher_pid=
cat "$task_tmp/mixed-publisher.log" "$task_tmp/mixed-subscriber.log"
