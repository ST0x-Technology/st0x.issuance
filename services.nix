{ lib }:

# kind = "issuance" -- env-file secrets pipeline: rage-decrypt env file +
#                      fireblocks key, install both, chown data dirs,
#                      write git-rev, marker file, restart unit.
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
  # plus an encrypted Fireblocks RSA private key, both installed by deploy.nix
  # before the unit restarts.
  issuanceFields = name: {
    encryptedEnvSecret = "${name}.env.age";
    encryptedFireblocksKey = "fireblocks-secret-issuance.key.age";
    decryptedEnvPath = "/run/agenix/${name}.env";
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
