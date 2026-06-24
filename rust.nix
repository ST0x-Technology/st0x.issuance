{
  pkgs,
  abiEnv,
  craneLib,
  ethgildAbis,
}:

let
  # Full source tree including the ethgild `out/` directory produced above.
  # crane's cleanCargoSource strips non-Rust files, so we supply an explicit
  # filter that also keeps `lib/ethgild/out/**` for the `sol!()` macros.
  baseSrc = pkgs.lib.cleanSourceWith {
    src = pkgs.lib.cleanSource ./.;
    filter =
      path: type:
      # Keep everything crane would normally keep PLUS the compiled ABI files.
      (craneLib.filterCargoSources path type)
      # || (pkgs.lib.hasInfix "/lib/ethgild/out/" path)
      || (pkgs.lib.hasInfix "/migrations" path);
  };

  # Overlay the forge-built ABIs over the cleaned source so the `sol!()`
  # macros in src/bindings.rs can resolve them.
  fullSrc = pkgs.runCommand "st0x-issuance-src" { } ''
    cp -rL --no-preserve=mode ${baseSrc} $out
    chmod -R u+w $out
    rm -rf $out/lib/ethgild/out
    mkdir -p $out/lib/ethgild
    cp -r ${ethgildAbis}/out $out/lib/ethgild/out
  '';

  # Git dependency output hashes — update when Cargo.lock is re-pinned.
  baseVendorDir = craneLib.vendorCargoDeps {
    src = fullSrc;
    outputHashes = {
      "git+https://github.com/ST0X-Technology/event-sorcery.git?tag=0.1.1#d0ad3f66bb5a1da23161d898027bff1abb9dabd6" =
        "sha256-fmBLdcyNoPh+Hktl1vVgeXdKtaxA+a1+xYi5Acxsr6o=";
      "git+https://github.com/0xgleb/fireblocks-sdk-rs.git?branch=fix/confirming-not-terminal#18227211082342818efaf6a1b58c89c65a6f17cd" =
        "sha256-KThUI0Cvh1JELem7SUQ1K3WqMccFeYfS3BqfLXwk2AE=";
    };
  };

  # sqlite-es uses sqlx::migrate!("../../migrations") which resolves inside
  # the vendor dir. Fetch migrations from event-sorcery at the same commit
  # as Cargo.lock specifies for sqlite-es.
  cargoLock = builtins.fromTOML (builtins.readFile ./Cargo.lock);
  sqliteEsPackage = builtins.head (
    builtins.filter (p: p.name or "" == "sqlite-es") cargoLock.package
  );
  sqliteEsRev = builtins.head (builtins.match ".*#([a-f0-9]+)" sqliteEsPackage.source);

  sqliteEsMigrations =
    builtins.fetchGit {
      url = "https://github.com/ST0x-Technology/event-sorcery";
      rev = sqliteEsRev;
    }
    + "/migrations";

  cargoVendorDir = pkgs.runCommand "vendor-with-migrations" { } ''
    cp -rL --no-preserve=mode ${baseVendorDir} $out

    # sqlite-es's ../../migrations resolves from crate root (sqlite-es-0.1.0/),
    # going up two levels to vendor root
    cp -r ${sqliteEsMigrations} "$out/migrations"

    # config.toml tells cargo where to find vendored crates. It contains
    # absolute nix store paths like:
    #   [source.nix-sources-c798c58f...]
    #   directory = "/nix/store/xxx-vendor-cargo-deps/c798c58f..."
    # We must update these to point to our wrapped vendor dir, otherwise
    # cargo will look in the original (immutable, no migrations) location.
    ${pkgs.gnused}/bin/sed -i "s|${baseVendorDir}|$out|g" $out/config.toml
  '';

  depsArgs = {
    pname = "st0x-issuance";
    version = "0.1.0";
    src = fullSrc;
    inherit cargoVendorDir;
    strictDeps = true;
    doCheck = false;

    nativeBuildInputs = [
      pkgs.pkg-config
      pkgs.sqlx-cli
    ];
    buildInputs = [
      pkgs.openssl
      pkgs.sqlite
    ]

    ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin [
      pkgs.apple-sdk_15
    ];

    # use path that matches with Dockerfile
    DATABASE_URL = "sqlite:///tmp/build_db.sqlite";
  };

  commonArgs = depsArgs // abiEnv;

  cargoArtifacts = craneLib.buildDepsOnly commonArgs;

  allBins = craneLib.buildPackage (
    commonArgs
    // {
      inherit cargoArtifacts;
      preBuild = ''
        sqlx database create
        sqlx migrate run
      '';
      cargoExtraArgs = "--bin st0x-issuance --bin issuer";
    }
  );

in
{
  st0x-issuance = allBins;
  issuer = allBins;
}
