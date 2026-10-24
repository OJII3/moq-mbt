#!/usr/bin/env bash
set -euo pipefail

if (($# != 2)); then
  echo "usage: start-moqx-container.sh <config-directory> <port>" >&2
  exit 2
fi

config_dir=$1
port=$2
container_name="moqx-mbt-$(date +%s)-$$"
docker_args=(
  --detach
  --name "$container_name"
  --publish "127.0.0.1:$port:$port/udp"
  --volume "$config_dir:/interop:ro"
)
if [[ -n ${MOQX_LOGGING:-} ]]; then
  docker_args+=(--env "MOQX_LOGGING=$MOQX_LOGGING")
fi
if [[ -n ${MOQX_LOCAL_FWD:-} ]]; then
  docker_args+=(--env "MOQX_LOCAL_FWD=$MOQX_LOCAL_FWD")
fi

docker run "${docker_args[@]}" \
  "${MOQX_IMAGE:?use the pinned image from nix develop}" \
  --config=/interop/moqx.yaml --logtostderr >/dev/null
printf '%s\n' "$container_name"
