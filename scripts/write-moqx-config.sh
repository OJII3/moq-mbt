#!/usr/bin/env bash
set -euo pipefail

if (($# != 2)); then
  echo "usage: write-moqx-config.sh <config-path> <port>" >&2
  exit 2
fi

config_path=$1
port=$2
cat >"$config_path" <<EOF
listeners:
  - name: interop
    udp:
      socket:
        address: 0.0.0.0
        port: $port
    tls:
      cert_file: "/interop/cert.pem"
      key_file: "/interop/key.pem"
      insecure: false
    endpoint: "/interop"
    moqt_versions: [18]

services:
  interop:
    match:
      - authority: {any: true}
        path: {prefix: "/"}
    cache:
      enabled: true
      max_tracks: 100
      max_groups_per_track: 3
EOF
