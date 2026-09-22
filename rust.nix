{
  pkgs,
  abiEnv,
  craneLib,
}:

let
  # Full source tree including the ethgild `out/` directory produced above.
  # crane's cleanCargoSource strips non-Rust files, so we supply an explicit
  # filter that also keeps migrations and the .sqlx offline query cache —
  # without the latter, SQLX_OFFLINE=true compiles with no cached query
  # metadata and every sqlx::query! macro fails.
  baseSrc = pkgs.lib.cleanSourceWith {
    src = pkgs.lib.cleanSource ./.;
    filter =
      path: type:
      (craneLib.filterCargoSources path type)
      || (pkgs.lib.hasInfix "/migrations" path)
      || (pkgs.lib.hasInfix "/.sqlx" path);
  };

  fullSrc = baseSrc;

  # Git dependency output hashes — update when Cargo.lock is re-pinned.
  baseVendorDir = craneLib.vendorCargoDeps {
    src = fullSrc;
    outputHashes = {
      "git+https://github.com/ST0X-Technology/event-sorcery.git?tag=0.1.2#8f5c81f3472ac4ca84bbcebbddaa0b3b01f2cfea" =
        "sha256-d0bl1jVmPeu9UPl4cNjY+cAaaLEDmLxw1BQhGrH5eV8=";
      "git+https://github.com/ST0x-Technology/st0x.alpaca?rev=fb228247af4d083ac8b5b4d5eb69fb004bc7452a#fb228247af4d083ac8b5b4d5eb69fb004bc7452a" =
        "sha256-j6o2cCIr5unEquVyu18zdMsoHYoFlTjE+HU2OIGCStc=";
      "git+https://github.com/ST0x-Technology/st0x.finance?tag=v0.2.0#49cf157109508516ff6a01868d471c7649234b2f" =
        "sha256-3BxD4dYsrroTe7ZXR95QNouhUgp4/oZBPmNAnY6TYgc=";
      "git+https://github.com/rainlanguage/rain.math.float?rev=e226e5a27125e75208e3e709e1c5eee128bd8b3b#e226e5a27125e75208e3e709e1c5eee128bd8b3b" =
        "sha256-LALVrtIfJDLDo7HSK8eSPF5AqwpK9TWjTxT6rpYbWzs=";
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

    SQLX_OFFLINE = "true";
  };

  commonArgs = depsArgs // abiEnv;

  cargoArtifacts = craneLib.buildDepsOnly commonArgs;

  allBins = craneLib.buildPackage (
    commonArgs
    // {
      inherit cargoArtifacts;
      cargoExtraArgs = "--bin st0x-issuance --bin issuer --bin validate-config";
    }
  );

in
{
  st0x-issuance = allBins;
  issuer = allBins;
}
