#!/usr/bin/env bash
# Start and stop the Draft 18 relay used by the interop scripts.
#
# The relay is aiomoqt, pinned by image digest. Its namespace table retains a
# namespace while any publisher still advertises it, serves object-offset and
# joining FETCH ranges, and applies RENDEZVOUS_TIMEOUT. The stream ring is
# raised so a single Object can exceed 1 MiB without being written atomically
# into a smaller ring.

: "${MOQ_INTEROP_RELAY_IMAGE:=ghcr.io/gmarzot/aiomoqt@sha256:06ebc32bd63eaf4e72cddda2fafa6c190197064506665f710c7ea67b265085a2}"
: "${MOQ_INTEROP_PORT:=4443}"
: "${MOQ_INTEROP_RELAY_STREAM_RING_BYTES:=8388608}"

interop_relay_start() {
  local cert=$1
  local key=$2
  local port=$3
  local log=$4
  local name="moq-interop-relay-$$-$RANDOM"

  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required to run the interop relay" >&2
    return 1
  fi
  if ! docker info >/dev/null 2>&1; then
    echo "the Docker daemon is not reachable" >&2
    return 1
  fi
  if ! docker image inspect "$MOQ_INTEROP_RELAY_IMAGE" >/dev/null 2>&1; then
    if ! docker pull "$MOQ_INTEROP_RELAY_IMAGE" >/dev/null; then
      echo "failed to pull $MOQ_INTEROP_RELAY_IMAGE" >&2
      return 1
    fi
  fi

  docker run --rm --name "$name" \
    -e MOQT_ROLE=relay \
    -e MOQT_PORT=4443 \
    -e MOQT_DUAL=1 \
    -e MOQT_BIND=0.0.0.0 \
    -e DRAFT=18 \
    -p "127.0.0.1:$port:4443/udp" \
    -v "$cert:/certs/cert.pem:ro" \
    -v "$key:/certs/priv.key:ro" \
    "$MOQ_INTEROP_RELAY_IMAGE" \
    --stream-ring-cap "$MOQ_INTEROP_RELAY_STREAM_RING_BYTES" \
    >"$log" 2>&1 &
  local pid=$!

  for ((attempt = 0; attempt < 200; attempt++)); do
    if ! kill -0 "$pid" 2>/dev/null; then
      cat "$log" >&2 || true
      echo "the interop relay exited while starting" >&2
      return 1
    fi
    if grep -Fq 'Listening on' "$log"; then
      INTEROP_RELAY_CONTAINER=$name
      INTEROP_RELAY_PID=$pid
      return 0
    fi
    sleep 0.1
  done
  cat "$log" >&2 || true
  echo "the interop relay did not start" >&2
  docker stop "$name" >/dev/null 2>&1 || true
  wait "$pid" 2>/dev/null || true
  return 1
}

interop_relay_stop() {
  if [[ -n ${INTEROP_RELAY_CONTAINER:-} ]]; then
    docker stop --time 1 "$INTEROP_RELAY_CONTAINER" >/dev/null 2>&1 || true
    INTEROP_RELAY_CONTAINER=
  fi
  if [[ -n ${INTEROP_RELAY_PID:-} ]]; then
    wait "$INTEROP_RELAY_PID" 2>/dev/null || true
    INTEROP_RELAY_PID=
  fi
}
