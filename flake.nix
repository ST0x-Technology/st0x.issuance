{
  description = "Flake for development workflows.";

  inputs = {
    rainix.url =
      "github:rainprotocol/rainix?rev=ce6ac81ed9e0249014cc852329d887f8ac787e55";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { flake-utils, rainix, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let pkgs = rainix.pkgs.${system};
      in {
        packages = rainix.packages.${system};

        devShell = pkgs.mkShell {
          inherit (rainix.devShells.${system}.default) shellHook;
          inherit (rainix.devShells.${system}.default) nativeBuildInputs;
          buildInputs = with pkgs;
            [ bacon sqlx-cli cargo-expand ]
            ++ rainix.devShells.${system}.default.buildInputs;
        };
      });
}
