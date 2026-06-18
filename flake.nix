{
  description = "Flake for development workflows.";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    rainix.url = "github:rainprotocol/rainix";
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      flake-utils,
      rainix,
      crane,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = rainix.pkgs.${system};

        craneLib = (crane.mkLib pkgs).overrideToolchain rainix.rust-toolchain.${system};

        crateSrc = craneLib.cleanCargoSource ./.;

        # Vendoring reads the whole workspace lock, so the main crate's two git
        # deps (event-sorcery/sqlite-es + fireblocks-sdk) need pinned hashes
        # even though the dto crate never pulls them in.
        cargoVendorDir = craneLib.vendorCargoDeps {
          src = crateSrc;
          outputHashes = {
            "git+https://github.com/ST0X-Technology/event-sorcery.git?tag=0.1.2#8f5c81f3472ac4ca84bbcebbddaa0b3b01f2cfea" =
              "sha256-d0bl1jVmPeu9UPl4cNjY+cAaaLEDmLxw1BQhGrH5eV8=";
            "git+https://github.com/0xgleb/fireblocks-sdk-rs.git?branch=fix/confirming-not-terminal#18227211082342818efaf6a1b58c89c65a6f17cd" =
              "sha256-KThUI0Cvh1JELem7SUQ1K3WqMccFeYfS3BqfLXwk2AE=";
          };
        };

        # The dto crate is a pure-Rust wire-types helper: no sqlx, no Rain
        # `sol!` ABIs, no workspace git deps in its tree. Scoping every cargo
        # invocation to it with `-p` keeps the main crate's database, ABI, and
        # Fireblocks dependencies from being compiled and keeps their build-time
        # requirements out of the build, so we never need the live DB,
        # ST0X_*_ABI env, or sqlite-es migrations the main crate's nix build
        # would require. (The vendored sources for those git deps are still
        # fetched — see the outputHashes above — but cargo only compiles the dto
        # crate.)
        crateArgs = {
          src = crateSrc;
          inherit cargoVendorDir;
          strictDeps = true;
          # The dto crate is pure Rust with sandbox-safe tests (temp-dir binding
          # export), so run it under `nix flake check` for extra CI coverage
          # rather than only type-checking.
          doCheck = true;
          cargoExtraArgs = "-p st0x-issuance-dto";
        };

        cargoArtifacts = craneLib.buildDepsOnly crateArgs;

        st0x-issuance-dto = craneLib.buildPackage (
          crateArgs
          // {
            pname = "st0x-issuance-dto";
            inherit cargoArtifacts;
            cargoExtraArgs = "-p st0x-issuance-dto";
            meta.description = "st0x issuance API DTO types + TypeScript binding exporter";
          }
        );

        # Reproducible TypeScript bindings: run the dto exporter into $out so the
        # dashboard can `nix build .#st0x-issuance-dto-typescript` for the `.ts`
        # files instead of running cargo. For codegen straight into a checkout,
        # use `nix run .#st0x-issuance-dto -- <dir>`.
        st0x-issuance-dto-typescript = pkgs.runCommand "st0x-issuance-dto-typescript" { } ''
          mkdir -p "$out"
          ${st0x-issuance-dto}/bin/st0x-issuance-dto "$out"
          # Fail the build if the exporter ran clean but emitted nothing, so a
          # silent codegen regression can't pass CI with an empty bindings dir.
          if ! find "$out" -type f -name '*.ts' -print -quit | grep -q .; then
            echo "st0x-issuance-dto exporter produced no .ts files" >&2
            exit 1
          fi
        '';

        # Single source for the issuance derivations exported from both `packages`
        # and `checks`, so the two lists can't drift apart.
        issuanceBuilds = {
          inherit
            st0x-issuance-dto
            st0x-issuance-dto-typescript
            ;
        };
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
          }
          // issuanceBuilds;

        # `nix flake check` (run in both rainix.yaml CI jobs) evaluates the
        # `checks` output, not `packages`; aliasing the crate derivations here is
        # what builds them and runs the TypeScript exporter in CI.
        checks = issuanceBuilds;

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
