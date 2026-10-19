{ lib, rustPlatform, pkg-config, cmake, perl, src }:

rustPlatform.buildRustPackage {
  pname = "moq-rs-relay";
  version = "0.7.25-unstable-2026-10-15";

  inherit src;

  cargoHash = "sha256-47DnsOPJ2wuTeOQ/AptZ463Cdjpgq71fUilAJ3hOaCw=";
  cargoBuildFlags = [ "-p" "moq-relay-ietf" "--bin" "moq-relay-ietf" ];
  doCheck = false;

  nativeBuildInputs = [ pkg-config cmake perl ];

  meta = {
    description = "Cloudflare moq-rs Draft 18 relay for interoperability checks";
    homepage = "https://github.com/cloudflare/moq-rs";
    mainProgram = "moq-relay-ietf";
    platforms = lib.platforms.unix;
  };
}
