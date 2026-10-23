{
  description = "Development environment for moq-mbt";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    moonbit-overlay.url = "github:moonbit-community/moonbit-overlay";
    moonbit-overlay.inputs.nixpkgs.follows = "nixpkgs";
    moq-rs-src = {
      url = "github:cloudflare/moq-rs/8779758892d921bcfccbb6e4d9319c149db28c91";
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

        packages.wtquic = pkgs.callPackage ./nix/wtquic.nix { };
        packages.moq5-relay = pkgs.callPackage ./nix/moq5-relay.nix {
          wtquic = self'.packages.wtquic;
        };
        packages.moq-rs-relay = pkgs.callPackage ./nix/moq-rs-relay.nix {
          src = inputs.moq-rs-src;
        };

        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.libmsquic
            pkgs.moonbit-bin.latest
            pkgs.nodejs
            pkgs.playwright-driver
            playwrightBrowsers
            self'.packages.moq5-relay
            self'.packages.moq-rs-relay
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
