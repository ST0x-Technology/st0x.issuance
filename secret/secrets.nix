let
  inherit (import ../keys.nix) roles;
  # services.nix is `{ lib }: { byName, enabled }`. These rules only read each
  # service's `kind`/`encryptedEnvSecret`/`encryptedFireblocksKey` from the
  # `byName` map; `enabled` (the sole consumer of `lib`) is never forced here,
  # so a stub `lib` keeps this rules file evaluable under ragenix's pure,
  # NIX_PATH-free eval.
  services =
    (import ../services.nix {
      lib = {
        filterAttrs = _: attrs: attrs;
      };
    }).byName;

  # Deduplicate keys across environments (st0x-op appears in both roles).
  dedup = builtins.foldl' (acc: key: if builtins.elem key acc then acc else acc ++ [ key ]) [ ];
  allServiceKeys = dedup (roles.prod.service ++ roles.staging.service);

  # Service secrets are encrypted to both environments' service roles.
  # Each environment's deploy.nix decrypts with its own host key.
  issuanceServiceNames = builtins.filter (name: services.${name}.kind == "st0x") (
    builtins.attrNames services
  );

  # For each issuance-kind service, create rules for both the env file
  # and the fireblocks key (deduplicated since all services share one key file).
  fireblocksKeyNames = builtins.foldl' (
    acc: name:
    let
      keyName = services.${name}.encryptedFireblocksKey;
    in
    if builtins.elem keyName acc then acc else acc ++ [ keyName ]
  ) [ ] issuanceServiceNames;

  envSecretRules = builtins.listToAttrs (
    map (name: {
      name = services.${name}.encryptedEnvSecret;
      value.publicKeys = allServiceKeys;
    }) issuanceServiceNames
  );

  fireblocksKeyRules = builtins.listToAttrs (
    map (keyName: {
      name = keyName;
      value.publicKeys = allServiceKeys;
    }) fireblocksKeyNames
  );

in
envSecretRules
// fireblocksKeyRules
// {
  "tailscale-authkey-prod.age".publicKeys = roles.prod.service;
  "tailscale-authkey-staging.age".publicKeys = roles.staging.service;
}
