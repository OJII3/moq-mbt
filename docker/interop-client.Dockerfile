FROM ghcr.io/nixos/nix:2.35.4 AS build

WORKDIR /src
COPY . .

RUN nix --extra-experimental-features 'nix-command flakes' develop .#interop-client \
    --command moon update

RUN nix --extra-experimental-features 'nix-command flakes' develop .#interop-client \
    --command bash -euc '\
      moon build --target native --release src/cmd/track_probe && \
      install -Dm755 _build/native/release/build/cmd/track_probe/track_probe.exe /tmp/moq-test-client && \
      client_store_path="$(nix-store --add /tmp/moq-test-client)" && \
      nix-store -qR "$client_store_path" > /tmp/closure && \
      ldd /tmp/moq-test-client | grep -o "/nix/store/[^ ]*" | sed "s#\(/nix/store/[^/]*\).*#\1#" | sort -u > /tmp/runtime-roots && \
      while IFS= read -r store_path; do nix-store -qR "$store_path" >> /tmp/closure; done < /tmp/runtime-roots && \
      cacert_store_path="${SSL_CERT_FILE%%/etc/ssl/certs/ca-bundle.crt}" && \
      nix-store -qR "$cacert_store_path" >> /tmp/closure && \
      sort -u -o /tmp/closure /tmp/closure && \
      while IFS= read -r store_path; do \
        mkdir -p "/rootfs$(dirname "$store_path")"; \
        cp -a "$store_path" "/rootfs$(dirname "$store_path")/"; \
      done < /tmp/closure && \
      mkdir -p /rootfs/usr/local/bin /rootfs/etc/ssl/certs /rootfs/tmp && \
      ln -s "$client_store_path" /rootfs/usr/local/bin/moq-test-client && \
      cp "$SSL_CERT_FILE" /rootfs/etc/ssl/certs/ca-certificates.crt'

FROM scratch
COPY --from=build /rootfs /

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENTRYPOINT ["/usr/local/bin/moq-test-client"]
