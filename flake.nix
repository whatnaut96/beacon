{
  description = "Flake for beacon cross-compilation and development";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        pkgsAarch64Linux = pkgs.pkgsCross.aarch64-multiplatform;
        pkgsWindows = pkgs.pkgsCross.mingwW64;

        mkBeacon = p: p.rustPlatform.buildRustPackage {
          pname = "beacon";
          version = "0.1.0";
          src = ./.;

          cargoLock.lockFile = ./Cargo.lock;
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            pkgs.rustc
            pkgs.cargo
            pkgs.rust-analyzer
            pkgs.rustfmt
            pkgs.clippy
          ];
        };

        packages = {
          default = mkBeacon pkgs; # Native x86_64-linux
          aarch64-linux = mkBeacon pkgsAarch64Linux; # ARM64 Linux
          x86_64-windows = mkBeacon pkgsWindows; # Windows x86_64
        };
      }
    );
}
