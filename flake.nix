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

      perSystem = { pkgs, system, self', ... }: {
        _module.args.pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [ inputs.moonbit-overlay.overlays.default ];
        };

        packages.moq5-relay = pkgs.callPackage ./nix/moq5-relay.nix { };

        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.libmsquic
            pkgs.moonbit-bin.latest
            pkgs.nodejs
            self'.packages.moq5-relay
            pkgs.openssl
          ];
        };
      };
    };
}
