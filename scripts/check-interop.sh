#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
source scripts/interop-relay.sh
task_tmp=$(mktemp -d)
publisher_pid=
subscriber_pid=
extra_pids=()
cleanup() {
  for pid in ${extra_pids[@]+"${extra_pids[@]}"}; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ -n "$subscriber_pid" ]]; then
    kill "$subscriber_pid" 2>/dev/null || true
    wait "$subscriber_pid" 2>/dev/null || true
  fi
  if [[ -n "$publisher_pid" ]]; then
    kill "$publisher_pid" 2>/dev/null || true
    wait "$publisher_pid" 2>/dev/null || true
  fi
  interop_relay_stop
  rm -rf "$task_tmp"
}
trap cleanup EXIT

moon run --release --target native --build-only src/cmd/setup_probe
moon run --release --target native --build-only src/cmd/track_probe
runner_client=_build/native/release/build/cmd/track_probe/track_probe.exe
scenario_timeout=${MOQ_INTEROP_SCENARIO_TIMEOUT:-90}s
openssl ecparam -name prime256v1 -genkey -noout -out "$task_tmp/key.pem" \
  >"$task_tmp/cert.log" 2>&1
openssl req -x509 -new -key "$task_tmp/key.pem" -sha256 -days 1 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -out "$task_tmp/cert.pem" >>"$task_tmp/cert.log" 2>&1

port=${MOQ_INTEROP_PORT:-4443}
interop_relay_start "$task_tmp/cert.pem" "$task_tmp/key.pem" "$port" \
  "$task_tmp/relay.log"

echo "Checking Draft 18 interoperability with the pinned aiomoqt relay"

# An untrusted self-signed certificate must fail with validation enabled.
if moon run --release --target native src/cmd/setup_probe -- \
  127.0.0.1 "$port" /interop >"$task_tmp/verify.log" 2>&1; then
  echo "the relay unexpectedly trusted the self-signed certificate" >&2
  exit 1
fi

# Disable validation only for this temporary test relay; its probe connects over
# loopback.
if ! moon run --release --target native src/cmd/setup_probe -- \
  127.0.0.1 "$port" /interop --insecure >"$task_tmp/probe.log" 2>&1; then
  cat "$task_tmp/probe.log" >&2
  cat "$task_tmp/relay.log" >&2
  exit 1
fi
cat "$task_tmp/probe.log"

# The relay rejects subscriptions until a publisher has registered the track.
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

wait_for_log() {
  local pattern=$1
  local file=$2
  local pid=$3
  local message=$4
  for ((attempt = 0; attempt < 150; attempt++)); do
    if grep -Fq "$pattern" "$file"; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null || ((attempt == 149)); then
      cat "$file" >&2 || true
      cat "$task_tmp/relay.log" >&2 || true
      echo "$message" >&2
      return 1
    fi
    sleep 0.1
  done
}

run_scaled_publish_subscribe() {
  local name=$1
  local groups=$2
  local objects=$3
  shift 3
  local publish_flags=("$@")
  local subscribe_flags=()
  local flag
  for flag in "$@"; do
    case "$flag" in
      --hold-open|--wait-for-start|--wait-for-fetch) ;;
      *) subscribe_flags+=("$flag") ;;
    esac
  done
  local publisher_log="$task_tmp/$name-publisher.log"
  local subscriber_log="$task_tmp/$name-subscriber.log"
  timeout --signal=TERM --kill-after=5s "$scenario_timeout" \
    "$runner_client" publish 127.0.0.1 "$port" /interop interop \
    "$name" "$groups" "$objects" "${publish_flags[@]}" \
    >"$publisher_log" 2>&1 &
  publisher_pid=$!
  wait_for_log 'PUBLISH request accepted' "$publisher_log" "$publisher_pid" \
    "$name publisher did not register the track"
  timeout --signal=TERM --kill-after=5s "$scenario_timeout" \
    "$runner_client" subscribe 127.0.0.1 "$port" /interop interop \
    "$name" "$groups" "$objects" "${subscribe_flags[@]}" \
    >"$subscriber_log" 2>&1 &
  subscriber_pid=$!
  if ! wait "$subscriber_pid"; then
    subscriber_pid=
    cat "$publisher_log" "$subscriber_log" >&2
    cat "$task_tmp/relay.log" >&2
    echo "$name subscriber failed" >&2
    exit 1
  fi
  subscriber_pid=
  if ! wait "$publisher_pid"; then
    publisher_pid=
    cat "$publisher_log" >&2
    cat "$task_tmp/relay.log" >&2
    echo "$name publisher failed" >&2
    exit 1
  fi
  publisher_pid=
  grep -F 'Verified' "$subscriber_log" | tail -n 1
}

run_publish_fanout() {
  local name=$1
  local groups=$2
  local objects=$3
  local subscribers=$4
  local fifo="$task_tmp/$name.stdin"
  local publisher_log="$task_tmp/$name-publisher.log"
  mkfifo "$fifo"
  exec 3<>"$fifo"
  timeout --signal=TERM --kill-after=5s "$scenario_timeout" \
    "$runner_client" publish 127.0.0.1 "$port" /interop interop \
    "$name" "$groups" "$objects" --insecure --wait-for-start \
    <&3 >"$publisher_log" 2>&1 &
  publisher_pid=$!
  wait_for_log 'PUBLISH request accepted' "$publisher_log" "$publisher_pid" \
    "$name publisher did not register the track"
  local sub_pids=()
  local index
  for ((index = 1; index <= subscribers; index++)); do
    timeout --signal=TERM --kill-after=5s "$scenario_timeout" \
      "$runner_client" subscribe 127.0.0.1 "$port" /interop interop \
      "$name" "$groups" "$objects" --insecure \
      >"$task_tmp/$name-subscriber-$index.log" 2>&1 &
    sub_pids+=("$!")
    extra_pids+=("$!")
  done
  for ((index = 0; index < subscribers; index++)); do
    wait_for_log 'SUBSCRIBE_OK received' \
      "$task_tmp/$name-subscriber-$((index + 1)).log" "${sub_pids[$index]}" \
      "$name subscriber $((index + 1)) did not subscribe"
  done
  printf '\n' >&3
  exec 3>&-
  local status=0
  for pid in "${sub_pids[@]}"; do
    if ! wait "$pid"; then
      status=1
    fi
  done
  if ! wait "$publisher_pid"; then
    status=1
  fi
  publisher_pid=
  extra_pids=()
  if ((status != 0)); then
    cat "$publisher_log" >&2
    for ((index = 1; index <= subscribers; index++)); do
      cat "$task_tmp/$name-subscriber-$index.log" >&2
    done
    cat "$task_tmp/relay.log" >&2
    echo "$name fan-out failed" >&2
    exit 1
  fi
  for ((index = 1; index <= subscribers; index++)); do
    grep -F 'Verified' "$task_tmp/$name-subscriber-$index.log" | tail -n 1
  done
}

# Scale and fan-out scenarios share the relay but use distinct track names.
run_scaled_publish_subscribe large-object 1 4 --object-bytes=2097152 --insecure
run_publish_fanout fanout 3 2 3
run_scaled_publish_subscribe bulk-objects 32 8 --insecure

# Run every moq-interop-runner client scenario. The relay pin is chosen so
# namespace retention and RENDEZVOUS_TIMEOUT are exercised, not skipped.
runner_tests=(
  setup-only
  announce-only
  publish-namespace-done
  subscribe-namespace-lifecycle
  publish-namespace-multi-publisher-retention
  subscribe-error
  rendezvous-timeout
  announce-subscribe
  subscribe-before-announce
  data-subgroup-basic
)
for test_case in "${runner_tests[@]}"; do
  if ! RELAY_URL="moqt://127.0.0.1:$port/interop" TLS_DISABLE_VERIFY=1 \
    "$runner_client" --test "$test_case" \
    >"$task_tmp/runner-$test_case.log" 2>&1; then
    cat "$task_tmp/runner-$test_case.log" >&2
    cat "$task_tmp/relay.log" >&2
    echo "moq-interop-runner client test failed: $test_case" >&2
    exit 1
  fi
done
echo "moq-interop-runner client scenarios passed"
