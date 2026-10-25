{
  description = "Development environment for moq-mbt";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    moonbit-overlay.url = "github:moonbit-community/moonbit-overlay";
    moonbit-overlay.inputs.nixpkgs.follows = "nixpkgs";
    moqtail-src = {
      url = "github:moqtail/moqtail/4cdcb42de6a887754779d74a06c0bf4a8716e5cb";
      flake = false;
    };
  };

  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-darwin"
      ];

      perSystem = { pkgs, system, self', ... }:
      let
        moqtailRelay = pkgs.rustPlatform.buildRustPackage {
          pname = "moqtail-relay";
          version = "0.15.0";
          src = inputs.moqtail-src;
          cargoLock.lockFile = "${inputs.moqtail-src}/Cargo.lock";
          cargoBuildFlags = [ "--package" "relay" ];
          doCheck = false;
          postInstall = ''
            ln -s $out/bin/relay $out/bin/moqtail-relay
          '';
          meta.mainProgram = "moqtail-relay";
        };
        playwrightBrowsers = pkgs.playwright-driver.browsers.override {
          withChromium = true;
          withFirefox = false;
          withWebkit = false;
          withFfmpeg = false;
        };
      in {
        _module.args.pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [ inputs.moonbit-overlay.overlays.default ];
        };

        packages.moqtail-relay = moqtailRelay;

        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.libmsquic
            pkgs.moonbit-bin.latest
            moqtailRelay
            pkgs.nodejs
            pkgs.playwright-driver
            playwrightBrowsers
            pkgs.openssl
          ];
          shellHook = ''
            export PLAYWRIGHT_CORE_PATH="${pkgs.playwright-driver}"
            export PLAYWRIGHT_BROWSERS_PATH="${playwrightBrowsers}"
          '';
        };
      };
    };
}
