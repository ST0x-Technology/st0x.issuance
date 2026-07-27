{ lib }:

# kind = "st0x" -- env-file secrets pipeline: rage-decrypt env file,
#                      install it, chown data dirs, write git-rev,
#                      marker file, restart unit.
# kind = "plain"    -- has a systemd unit but no secrets/config. Marker file
#                      gates ConditionPathExists; deploy step just touches it
#                      and restarts.

let
  profileBase = "/nix/var/nix/profiles/per-service";

  baseFields = name: {
    profilePath = "${profileBase}/${name}";
    markerFile = "/run/st0x/${name}.ready";
  };

  # issuance-kind services carry an encrypted env file (all secret env vars)
  # installed by deploy.nix before the unit restarts.
  # Encrypted file names are per-environment (${name}-{env}.env.age) and
  # computed in deploy.nix where the target environment is known. Decrypted
  # runtime paths are the same on every host since each environment runs on
  # its own machine.
  issuanceFields = name: {
    decryptedEnvPath = "/run/agenix/${name}.env";
    # Temporary while receipt custody migrates off Fireblocks: the migration
    # CLI's forward leg reads the Fireblocks RSA key from this path. The
    # `issuer` operator wrapper (os.nix), the service unit's static
    # environment (nix/upgradeable-services.nix), and the deploy-time
    # validate-config invocation (deploy.nix) all export
    # FIREBLOCKS_SECRET_PATH pointing here. Leaves with src/fireblocks.
    decryptedFireblocksKeyPath = "/run/agenix/fireblocks-secret-issuance.key";
  };

  withPaths =
    name: attrs:
    attrs // baseFields name // (if attrs.kind == "st0x" then issuanceFields name else { });

  byName = builtins.mapAttrs withPaths {
    # `order` controls deploy-rs activation sequence within `profilesOrder`. The
    # system profile always runs first; remaining profiles activate in ascending
    # `order`. Lower numbers go first.
    st0x-issuance = {
      enabled = true;
      order = 10;
      kind = "st0x";
      package = "st0x-issuance";
      bin = "st0x-issuance";
      description = "st0x issuance server";
    };
  };

  enabled = lib.filterAttrs (_: v: v.enabled) byName;
in
{
  inherit byName enabled;
}
