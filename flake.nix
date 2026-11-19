{
  description = "Development environment for moq-mbt";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    moonbit-overlay.url = "github:moonbit-community/moonbit-overlay";
    moonbit-overlay.inputs.nixpkgs.follows = "nixpkgs";
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
            pkgs.libmsquic
            pkgs.moonbit-bin.nightly
            pkgs.z3
            pkgs.docker-client
            pkgs.nodejs
            pkgs.playwright-driver
            playwrightBrowsers
            pkgs.openssl
          ] ++ pkgs.lib.optional pkgs.stdenv.hostPlatform.isLinux pkgs.valgrind;
          shellHook = ''
            export PLAYWRIGHT_CORE_PATH="${pkgs.playwright-driver}"
            export PLAYWRIGHT_BROWSERS_PATH="${playwrightBrowsers}"
          '';
        };

        devShells.interop-client = pkgs.mkShell {
          packages = [
            pkgs.libmsquic
            pkgs.moonbit-bin.nightly
            pkgs.cacert
          ];
          shellHook = ''
            export SSL_CERT_FILE="${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
          '';
        };
      };
    };
}
