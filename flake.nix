{
  description = "Flake for development workflows.";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    rainix.url = "github:rainprotocol/rainix";
  };

  outputs =
    { flake-utils, rainix, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = rainix.pkgs.${system};
      in
      rec {
        packages =
          let
            rainixPkgs = rainix.packages.${system};
          in
          rainixPkgs
          // {
            prepSolArtifacts = rainix.mkTask.${system} {
              name = "prep-sol-artifacts";
              additionalBuildInputs = rainix.sol-build-inputs.${system};
              body = ''
                set -euxo pipefail
                (cd lib/ethgild && forge build)
              '';
            };

            smoke-test-image = pkgs.writeShellApplication {
              name = "smoke-test-image";
              runtimeInputs = [
                pkgs.nushell
                pkgs.coreutils
                pkgs.docker-client
              ];
              text = ''
                exec nu ${./scripts/smoke-test-image.nu} "$@"
              '';
            };
          };

        devShell = pkgs.mkShell {
          inherit (rainix.devShells.${system}.default) shellHook;
          inherit (rainix.devShells.${system}.default) nativeBuildInputs;

          buildInputs =
            with pkgs;
            [
              bacon
              sqlx-cli
              cargo-expand
              cargo-chef
              packages.prepSolArtifacts
            ]
            ++ rainix.devShells.${system}.default.buildInputs;

          DATABASE_URL = "sqlite:./issuance.db";
        };
      }
    );
}
