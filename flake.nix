{
  description = "Flake for development workflows.";

  inputs = {
    rainix.url =
      "github:rainprotocol/rainix?rev=560ee6ec35b72a2e6c669745b4af33997b2979fb";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { flake-utils, rainix, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let pkgs = rainix.pkgs.${system};
      in rec {
        packages = let rainixPkgs = rainix.packages.${system};
        in rainixPkgs // {
          prepSolArtifacts = rainix.mkTask.${system} {
            name = "prep-sol-artifacts";
            additionalBuildInputs = rainix.sol-build-inputs.${system};
            body = ''
              set -euxo pipefail
              (cd lib/ethgild && forge build)
            '';
          };
        };

        devShell = pkgs.mkShell {
          inherit (rainix.devShells.${system}.default) shellHook;
          inherit (rainix.devShells.${system}.default) nativeBuildInputs;
          buildInputs = with pkgs;
            [ bacon sqlx-cli cargo-expand packages.prepSolArtifacts ]
            ++ rainix.devShells.${system}.default.buildInputs;
        };
      });
}
