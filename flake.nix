{
  description = "Development environment for moq-mbt";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    moonbit-overlay.url = "github:moonbit-community/moonbit-overlay";
    moonbit-overlay.inputs.nixpkgs.follows = "nixpkgs";
    moqx-src = {
      url = "github:openmoq/moqx/f3df41db408a2edbd580a01b97d0e56505ae080e";
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

        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.docker
            pkgs.libmsquic
            pkgs.moonbit-bin.latest
            pkgs.nodejs
            pkgs.playwright-driver
            playwrightBrowsers
            pkgs.openssl
          ];
          shellHook = ''
            export MOQX_IMAGE="ghcr.io/openmoq/moqx@sha256:8f0c393c2c4fed0e260c876ab7fcb93a68f97000e3e693d2850ebf0a627eb096"
            export PLAYWRIGHT_CORE_PATH="${pkgs.playwright-driver}"
            export PLAYWRIGHT_BROWSERS_PATH="${playwrightBrowsers}"
          '';
        };
      };
    };
}
