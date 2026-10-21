#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
task_tmp=$(mktemp -d)
relay_pid=
publisher_pid=
cleanup() {
  if [[ -n "$publisher_pid" ]]; then
    kill "$publisher_pid" 2>/dev/null || true
    wait "$publisher_pid" 2>/dev/null || true
  fi
  if [[ -n "$relay_pid" ]]; then
    kill "$relay_pid" 2>/dev/null || true
    wait "$relay_pid" 2>/dev/null || true
  fi
  rm -rf "$task_tmp"
}
trap cleanup EXIT

moon run --release --target native --build-only src/cmd/setup_probe
moon run --release --target native --build-only src/cmd/track_probe
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -keyout "$task_tmp/key.pem" -out "$task_tmp/cert.pem" >"$task_tmp/cert.log" 2>&1
cat >"$task_tmp/moq5-relay.json" <<EOF
{
  "listener": {
    "host": "127.0.0.1", "port": 4443, "versions": [18], "lanes": 1,
    "cert": "$task_tmp/cert.pem", "key": "$task_tmp/key.pem"
  },
  "auth": { "mode": "allow_all" }
}
EOF

start_relay() {
  local relay=$1
  local relay_log="$task_tmp/$relay-relay.log"
  case "$relay" in
    moq5)
      moq5-relay serve --config "$task_tmp/moq5-relay.json" >"$relay_log" 2>&1 &
      ;;
    moq-rs)
      moq-relay-ietf --bind 127.0.0.1:4443 \
        --tls-cert "$task_tmp/cert.pem" --tls-key "$task_tmp/key.pem" --dev \
        >"$relay_log" 2>&1 &
      ;;
    *)
      echo "Unknown relay: $relay" >&2
      return 2
      ;;
  esac
  relay_pid=$!

  for ((attempt = 0; attempt < 100; attempt++)); do
    if [[ "$relay" == moq5 ]] && grep -q 'MOQ5 Relay: listening on' "$relay_log"; then
      return
    fi
    if [[ "$relay" == moq-rs ]] && grep -q 'listening on 127.0.0.1:4443' "$relay_log"; then
      return
    fi
    if ! kill -0 "$relay_pid" 2>/dev/null || ((attempt == 99)); then
      cat "$relay_log" >&2
      echo "$relay failed to start" >&2
      return 1
    fi
    sleep 0.1
  done
}

stop_relay() {
  if [[ -n "$relay_pid" ]]; then
    kill "$relay_pid" 2>/dev/null || true
    wait "$relay_pid" 2>/dev/null || true
    relay_pid=
  fi
}

run_interop() {
  local relay=$1
  local prefix="$task_tmp/$relay"
  echo "Checking Draft 18 interoperability with $relay"
  start_relay "$relay"

  # An untrusted self-signed certificate must fail with validation enabled.
  if moon run --release --target native src/cmd/setup_probe -- \
    127.0.0.1 4443 / >"$prefix-verify.log" 2>&1; then
    echo "$relay unexpectedly trusted the self-signed certificate" >&2
    return 1
  fi

  # Disable validation only for these temporary, loopback-only test relays.
  if ! moon run --release --target native src/cmd/setup_probe -- \
    127.0.0.1 4443 / --insecure >"$prefix-probe.log" 2>&1; then
    cat "$prefix-probe.log" "$prefix-relay.log" >&2
    return 1
  fi

  if ! grep -q 'Client SETUP acknowledged' "$prefix-probe.log"; then
    cat "$prefix-probe.log" "$prefix-relay.log" >&2
    echo "$relay did not acknowledge client SETUP" >&2
    return 1
  fi
  cat "$prefix-probe.log"

  local datagram_flags=()
  if [[ "$relay" == moq5 ]]; then
    if ! moon run --release --target native src/cmd/track_probe -- \
      datagram-smoke 127.0.0.1 4443 /moq --insecure >"$prefix-datagram.log" 2>&1; then
      cat "$prefix-datagram.log" "$prefix-relay.log" >&2
      return 1
    fi
    if ! grep -Fq 'Maximum-size boundary accepted; one byte over was rejected' "$prefix-datagram.log" || \
      ! grep -Fq 'Datagram send queue limit enforced' "$prefix-datagram.log"; then
      cat "$prefix-datagram.log" "$prefix-relay.log" >&2
      echo "$relay did not pass the datagram boundary and queue checks" >&2
      return 1
    fi
    datagram_flags=(--datagrams)
    cat "$prefix-datagram.log"
  fi

  moon run --release --target native src/cmd/track_probe -- \
    publish 127.0.0.1 4443 /moq interop multi-object 3 2 "${datagram_flags[@]}" --insecure \
    >"$prefix-publisher.log" 2>&1 &
  publisher_pid=$!
  for ((attempt = 0; attempt < 100; attempt++)); do
    if grep -q 'PUBLISH request sent' "$prefix-publisher.log"; then
      break
    fi
    if ! kill -0 "$publisher_pid" 2>/dev/null || ((attempt == 99)); then
      cat "$prefix-publisher.log" "$prefix-relay.log" >&2
      echo "$relay publisher did not send the PUBLISH request" >&2
      return 1
    fi
    sleep 0.1
  done

  if ! moon run --release --target native src/cmd/track_probe -- \
    subscribe 127.0.0.1 4443 /moq interop multi-object 3 2 "${datagram_flags[@]}" --insecure \
    >"$prefix-subscriber.log" 2>&1; then
    cat "$prefix-publisher.log" "$prefix-subscriber.log" "$prefix-relay.log" >&2
    return 1
  fi

  local expected_objects=6
  local received_objects
  received_objects=$(awk '/^Received Object \(group / { count++ } END { print count+0 }' "$prefix-subscriber.log")
  if [[ "$received_objects" != "$expected_objects" ]] || \
    ! grep -Fq 'Verified 6 Objects across 3 groups; PUBLISH_DONE received' "$prefix-subscriber.log"; then
    cat "$prefix-publisher.log" "$prefix-subscriber.log" "$prefix-relay.log" >&2
    echo "$relay subscriber did not verify all $expected_objects Objects and PUBLISH_DONE" >&2
    return 1
  fi
  if [[ "$relay" == moq5 ]] && ! grep -Fq 'Received Object Datagram' "$prefix-subscriber.log"; then
    cat "$prefix-publisher.log" "$prefix-subscriber.log" "$prefix-relay.log" >&2
    echo "$relay subscriber did not receive the Object Datagram alongside stream Objects" >&2
    return 1
  fi
  for group in 0 1 2; do
    for object in 0 1; do
      if ! grep -Fq "Received Object (group $group, object $object," "$prefix-subscriber.log"; then
        cat "$prefix-publisher.log" "$prefix-subscriber.log" "$prefix-relay.log" >&2
        echo "$relay subscriber did not receive group $group object $object" >&2
        return 1
      fi
    done
  done
  if ! wait "$publisher_pid"; then
    publisher_pid=
    cat "$prefix-publisher.log" "$prefix-relay.log" >&2
    return 1
  fi
  publisher_pid=
  if ! grep -Fq 'PUBLISH_DONE send completed' "$prefix-publisher.log"; then
    cat "$prefix-publisher.log" "$prefix-subscriber.log" "$prefix-relay.log" >&2
    echo "$relay publisher did not complete the PUBLISH_DONE send" >&2
    return 1
  fi

  cat "$prefix-publisher.log" "$prefix-subscriber.log"
  stop_relay
}

run_interop moq5
run_interop moq-rs
