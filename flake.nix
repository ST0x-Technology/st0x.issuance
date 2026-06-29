{
  description = "Flake for development workflows.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rainix = {
      url = "github:rainprotocol/rainix?rev=36342d3f1a104adf987793df7f101cf804e62a34";
      inputs = {
        foundry.inputs.nixpkgs.follows = "nixpkgs";
        git-hooks-nix.inputs.nixpkgs.follows = "nixpkgs";
        nixpkgs.follows = "nixpkgs";
        rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
        solc.inputs.nixpkgs.follows = "nixpkgs";
      };
    };
    crane.url = "github:ipetkov/crane";
    ragenix = {
      url = "github:yaxitech/ragenix";
      inputs.crane.follows = "crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    deploy-rs = {
      url = "github:serokell/deploy-rs";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    disko = {
      url = "github:nix-community/disko";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    nixos-anywhere = {
      url = "github:nix-community/nixos-anywhere";
      inputs.nixos-stable.follows = "nixpkgs";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    ethgild = {
      type = "git";
      url = "https://github.com/gildlab/ethgild";
      rev = "59ae2ff9bb86fd61e0e8494622cb4da492678b1a";
      flake = false;
      submodules = true;
    };
  };

  outputs =
    {
      self,
      flake-utils,
      rainix,
      crane,
      ragenix,
      deploy-rs,
      ethgild,
      disko,
      nixos-anywhere,
      ...
    }:
    let
      inherit (import ./keys.nix) keys tailscaleHost tailnetSuffix;
      inherit (rainix.inputs.nixpkgs) lib;
      environments = {
        prod = {
          nodeName = "st0x-issuance";
          volumeName = "st0x-issuance-data";
          hostKey = keys.host-prod;
          tailscaleMagicDnsName = "${tailscaleHost.prod}.${tailnetSuffix}";
        };
        staging = {
          nodeName = "st0x-issuance-staging";
          volumeName = "st0x-issuance-staging-data";
          hostKey = keys.host-staging;
          tailscaleMagicDnsName = "${tailscaleHost.staging}.${tailnetSuffix}";
        };
      };
      envNames = builtins.attrNames environments;
    in
    {
      nixosConfigurations =
        let
          mkNixos =
            { environment, modules }:
            lib.nixosSystem {
              system = "x86_64-linux";
              specialArgs = {
                inherit environment;
                inherit (environments.${environment}) volumeName tailscaleMagicDnsName;
                inherit (self.packages.x86_64-linux) st0x-issuance issuer;
              };
              modules = [ disko.nixosModules.disko ] ++ modules;
            };

          full =
            env:
            mkNixos {
              environment = env;
              modules = [
                ragenix.nixosModules.default
                ./os.nix
              ];
            };

          bootstrap =
            env:
            mkNixos {
              environment = env;
              modules = [ ./bootstrap.nix ];
            };
        in
        builtins.listToAttrs (
          builtins.concatMap (env: [
            {
              name = environments.${env}.nodeName;
              value = full env;
            }
            {
              name = "${environments.${env}.nodeName}-bootstrap";
              value = bootstrap env;
            }
          ]) envNames
        );

      deploy =
        (import ./deploy.nix {
          inherit
            lib
            deploy-rs
            self
            environments
            ;
        }).config;
    }
    // flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import rainix.inputs.nixpkgs {
          inherit system;
          config.allowUnfreePredicate = pkg: builtins.elem (pkgs.lib.getName pkg) [ "terraform" ];
        };

        rustToolchain = rainix.rust-toolchain.${system};
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
        foundryBin = rainix.pkgs.${system}.foundry-bin;

        rustShell = rainix.devShells.${system}.rust-shell;

        ragenixPkg = ragenix.packages.${system}.default;
        nixosAnywherePkg = nixos-anywhere.packages.${system}.default;

        infraPkgs = import ./infra {
          inherit
            pkgs
            ragenix
            system
            ;
          environments = envNames;
        };
        rekeySecrets = ''ragenix --rules ./secret/secrets.nix -i "$identity" -r'';

        deployScripts =
          (import ./deploy.nix {
            inherit
              lib
              deploy-rs
              self
              environments
              ;
          }).mkDeployScripts
            {
              inherit pkgs infraPkgs;
              localSystem = system;
            };

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

        inherit
          (import ./nix/mk-abi.nix {
            inherit pkgs;
            foundry = foundryBin;
            solc = rainix.pkgs.${system}.solc_0_8_25;
          })
          mkAbi
          ;

        inherit
          (import ./nix/abis.nix {
            inherit pkgs mkAbi;
            sources = {
              inherit ethgild;
            };
          })
          abis
          abiEnv
          # abisEnvs
          ;

        # Server (st0x-issuance) and operator CLI (issuer)
        rust = pkgs.callPackage ./rust.nix {
          inherit craneLib abiEnv;
        };

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
            prepSolArtifacts = pkgs.writeShellApplication {
              name = "prep-sol-artifacts";
              text = ''
                ln -sfn ${abis.ethgild}/out abis
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

            inherit (rust)
              st0x-issuance
              issuer
              ;

            bootstrap = pkgs.writeShellApplication {
              name = "bootstrap-nixos";
              runtimeInputs = infraPkgs.buildInputs ++ [
                nixosAnywherePkg
                pkgs.gnused
              ];
              text = ''
                env="''${1:?usage: bootstrap <prod|staging>}"
                shift

                case "$env" in
                  ${builtins.concatStringsSep "\n" (
                    map (env: ''
                      ${env})
                        flake_config="${environments.${env}.nodeName}-bootstrap"
                        host_key_field="host-${env}" ;;'') envNames
                  )}
                  *)
                    echo "ERROR: unknown environment '$env'" >&2
                    exit 1 ;;
                esac

                ${infraPkgs.parseIdentity}
                trap _cleanup_identity EXIT

                export env flake_config host_key_field identity

                ${./scripts/bootstrap.sh} "$@"
              '';
            };

            secret = pkgs.writeShellApplication {
              name = "secret";
              runtimeInputs = [
                ragenixPkg
                pkgs.nushell
                pkgs.coreutils
              ];
              text = ''
                exec nu ${./scripts/secret.nu} "$@"
              '';
            };

            rekey = pkgs.writeShellApplication {
              name = "rekey";
              runtimeInputs = [ ragenixPkg ];
              text = ''
                ${infraPkgs.parseIdentity}
                trap _cleanup_identity EXIT
                ${rekeySecrets}
              '';
            };
          }
          // issuanceBuilds
          // infraPkgs.packages
          // deployScripts;

        # `nix flake check` (run in both rainix.yaml CI jobs) evaluates the
        # `checks` output, not `packages`; aliasing the crate derivations here is
        # what builds them and runs the TypeScript exporter in CI.
        checks = issuanceBuilds;

        devShell = pkgs.mkShell (
          {
            # inherit (rainix.devShells.${system}.default) shellHook;
            inherit (rainix.devShells.${system}.default) nativeBuildInputs;
            shellHook = ''
              ${rainix.devShells.${system}.default.shellHook or ""}
              ln -sfn ${abis.ethgild}/out abis
            '';

            buildInputs =
              with pkgs;
              [
                bacon
                sqlx-cli
                cargo-expand
                cargo-chef
                ragenixPkg
                packages.secret
                packages.rekey
                nixosAnywherePkg
                packages.prepSolArtifacts
                foundryBin
              ]
              ++ rainix.devShells.${system}.default.buildInputs
              ++ infraPkgs.buildInputs
              ++ builtins.attrValues infraPkgs.packages
              ++ builtins.attrValues deployScripts
              ++ rustShell.buildInputs;

            DATABASE_URL = "sqlite:./issuance.db";
          }
          // abiEnv
        );
      }
    );
}
