let
  inherit (import ../keys.nix) roles;
  # services.nix is `{ lib }: { byName, enabled }`. These rules only read each
  # service's `kind` from the `byName` map; `enabled` (the sole consumer of
  # `lib`) is never forced here, so a stub `lib` keeps this rules file
  # evaluable under ragenix's pure, NIX_PATH-free eval.
  services =
    (import ../services.nix {
      lib = {
        filterAttrs = _: attrs: attrs;
      };
    }).byName;

  envNames = [
    "prod"
    "staging"
  ];

  issuanceServiceNames = builtins.filter (name: services.${name}.kind == "st0x") (
    builtins.attrNames services
  );

  # Each environment's env file is encrypted only to that environment's host
  # key — a staging compromise cannot decrypt production credentials.
  envSecretRules = builtins.listToAttrs (
    builtins.concatLists (
      map (
        env:
        map (name: {
          name = "${name}-${env}.env.age";
          value.publicKeys = roles.${env}.service;
        }) issuanceServiceNames
      ) envNames
    )
  );

  # One Fireblocks signing key per environment, scoped to that environment only.
  fireblocksKeyRules = builtins.listToAttrs (
    map (env: {
      name = "fireblocks-secret-issuance-${env}.key.age";
      value.publicKeys = roles.${env}.service;
    }) envNames
  );

in
envSecretRules
// fireblocksKeyRules
// {
  "tailscale-authkey-prod.age".publicKeys = roles.prod.service;
  "tailscale-authkey-staging.age".publicKeys = roles.staging.service;
}
