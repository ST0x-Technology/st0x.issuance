{
  lib,
  deploy-rs,
  self,
  environments,
}:

let
  system = "x86_64-linux";
  inherit (deploy-rs.lib.${system}) activate;
  profileBase = "/nix/var/nix/profiles/per-service";

  gitRev = self.rev or self.dirtyRev or "unknown";

  rage = "/run/current-system/sw/bin/rage";
  hostKey = "/etc/ssh/ssh_host_ed25519_key";

  inherit (import ./services.nix { inherit lib; }) enabled;

  enabledNames = builtins.attrNames enabled;

  # Explicit ordering: sort enabled services by each service's declared `order`
  # field so adding a new entry forces choosing its slot rather than inheriting
  # the alphabetical attribute order. Duplicate orders would produce a
  # non-deterministic activation sequence, so assert uniqueness here.
  enabledOrders = map (name: enabled.${name}.order) enabledNames;

  uniqueOrders = builtins.foldl' (
    acc: o: if builtins.elem o acc then acc else acc ++ [ o ]
  ) [ ] enabledOrders;

  orderedServices =
    if (builtins.length enabledOrders) == (builtins.length uniqueOrders) then
      builtins.sort (a: b: enabled.${a}.order < enabled.${b}.order) enabledNames
    else
      throw "services.nix: duplicate `order` values among enabled services: ${builtins.toJSON enabledOrders}";

  # Builds the per-service activation command for issuance-kind services.
  # Activation:
  #   1. Stop the unit and clear the marker so systemd won't see a stale-ready
  #      service during system activation.
  #   2. Decrypt and install the env-file secrets (chmod 0640 group st0x).
  #   3. Chown any existing data files so the st0x user can access them.
  #   4. Record the deployed git revision for ops tooling.
  #   5. Touch the marker (ConditionPathExists in the unit) and restart —
  #      unless the operator hold file (/run/st0x/<name>.hold) exists, in
  #      which case the binary and secrets are installed and validated but the
  #      service is left stopped. The hold protects a custody-migration
  #      window: while it is present, NO deploy — intentional or stray — can
  #      start a service against custody that has not finished moving. /run is
  #      tmpfs, so a reboot clears the hold along with the marker; re-create
  #      it before any deploy if the window is still open.
  #
  # If any step fails, deploy-rs exits non-zero and rolls back automatically.
  mkIssuanceProfile =
    env: name:
    let
      cfg = enabled.${name};
      pkg = self.packages.${system}.${cfg.package};
      envSecretsFile = ./secret + "/${name}-${env}.env.age";
      fireblocksKeyFile = ./secret + "/fireblocks-secret-issuance-${env}.key.age";
      deploymentEnvironment =
        if env == "prod" then
          "production"
        else if env == "staging" then
          "staging"
        else
          throw "Unsupported environment '${env}'";
    in
    activate.custom pkg (
      # Newline-joined under `set -e`, NOT " && ": the multi-line steps below
      # end with newlines, and an "&&"-join would emit a line starting with
      # "&&", which bash rejects at parse time. `set -e` keeps the same
      # fail-fast semantics; pipefail makes the rage | install pipes fail if
      # rage exits non-zero, preventing an empty credential file from being
      # written and the service restarted with empty creds.
      builtins.concatStringsSep "\n" [
        "set -euo pipefail"
        "systemctl stop ${name} || true"
        "rm -f ${cfg.markerFile}"
        "mkdir -p /run/st0x /run/agenix"

        # Decrypt env file — contains all secret env vars
        "${rage} -d -i ${hostKey} ${envSecretsFile} | install -D -m 0640 -o root -g st0x /dev/stdin ${cfg.decryptedEnvPath}"

        # Fireblocks RSA key for the receipt-custody migration CLI's forward
        # leg (FIREBLOCKS_SECRET_PATH). Temporary; leaves with src/fireblocks.
        # Decrypted to a temp file and renamed only on success, so a failed
        # decryption can never leave a partial key at the real path.
        ''
          fireblocks_key_tmp=$(mktemp ${cfg.decryptedFireblocksKeyPath}.XXXXXX)
          if ${rage} -d -i ${hostKey} ${fireblocksKeyFile} > "$fireblocks_key_tmp"; then
            chmod 0440 "$fireblocks_key_tmp"
            chown root:st0x "$fireblocks_key_tmp"
            mv "$fireblocks_key_tmp" ${cfg.decryptedFireblocksKeyPath}
          else
            rm -f "$fireblocks_key_tmp"
            exit 1
          fi
        ''

        # Validate config + secrets before restarting. If validation fails,
        # deploy-rs exits non-zero and rolls back instead of starting bad config.
        "set -a; . ${cfg.decryptedEnvPath}; set +a; DATABASE_URL=sqlite:///mnt/data/issuance.db ENVIRONMENT=${deploymentEnvironment} FIREBLOCKS_SECRET_PATH=${cfg.decryptedFireblocksKeyPath} ${cfg.profilePath}/bin/validate-config"

        # Chown existing data files so st0x user can open them after a fresh deploy
        "(chown st0x:st0x /mnt/data/*.db /mnt/data/*.db-wal /mnt/data/*.db-shm /mnt/data/*.db-journal 2>/dev/null || true)"
        "(chown -R st0x:st0x /mnt/data/logs 2>/dev/null || true)"

        "echo '${gitRev}' > /run/st0x/${name}.git-rev"
        ''
          if [ -f /run/st0x/${name}.hold ]; then
            echo 'hold file /run/st0x/${name}.hold present:' \
              '${name} installed and validated but left stopped'
            rm -f ${cfg.markerFile}
          else
            touch ${cfg.markerFile}
            systemctl restart ${name}
          fi
        ''
      ]
    );

  mkPlainProfile =
    _env: name:
    let
      cfg = enabled.${name};
      pkg = self.packages.${system}.${cfg.package};
      # Marker must exist BEFORE systemctl restart, because the unit's
      # ConditionPathExists is evaluated when systemd processes the start
      # request -- if the marker is absent, systemd silently skips the unit
      # (returning exit 0 from `systemctl restart`) and the service never
      # actually starts. Touch it first, then remove it on restart failure so
      # a broken unit doesn't satisfy the condition on the next system
      # activation.
    in
    activate.custom pkg (
      builtins.concatStringsSep " && " [
        "systemctl stop ${name} || true"
        "mkdir -p /run/st0x"
        "touch ${cfg.markerFile}"
        "systemctl restart ${name} || { rm -f ${cfg.markerFile}; exit 1; }"
      ]
    );

  mkServiceProfile =
    env: name:
    let
      cfg = enabled.${name};
    in
    if cfg.kind == "st0x" then
      mkIssuanceProfile env name
    else if cfg.kind == "plain" then
      mkPlainProfile env name
    else
      throw "services.${name}: unknown kind ${cfg.kind}";

  mkProfile = env: name: {
    path = mkServiceProfile env name;
    profilePath = "${profileBase}/${name}";
  };

  # `hostname` here is an eval-time placeholder that keeps the flake pure.
  # The real SSH target is the droplet IP resolved at deploy time and always
  # passed via `--hostname` by the deploy scripts below.
  mkNode =
    {
      env,
      nixosConfig,
      nodeName,
    }:
    {
      hostname = nodeName;
      sshUser = "root";
      user = "root";

      profilesOrder = [ "system" ] ++ orderedServices;

      profiles = {
        system.path = activate.nixos nixosConfig;
      }
      // builtins.listToAttrs (
        map (name: {
          inherit name;
          value = mkProfile env name;
        }) orderedServices
      );
    };

in
{
  config = {
    nodes = builtins.listToAttrs (
      map (
        env:
        let
          cfg = environments.${env};
        in
        {
          name = cfg.nodeName;
          value = mkNode {
            inherit env;
            inherit (cfg) nodeName;
            nixosConfig = self.nixosConfigurations.${cfg.nodeName};
          };
        }
      ) (builtins.attrNames environments)
    );
  };

  mkDeployScripts =
    {
      pkgs,
      infraPkgs,
      localSystem,
    }:
    let
      deployInputs = infraPkgs.buildInputs ++ [
        deploy-rs.packages.${localSystem}.deploy-rs
        pkgs.openssh
      ];

      # --rollback-succeeded false: when a later profile fails (e.g. the
      # service's validate-config gate), do NOT revoke already-confirmed
      # profiles. Reverting the system profile would roll authorized_keys and
      # the firewall back to the previous generation, locking CI out until a
      # manual local redeploy.
      deployFlags =
        if localSystem == "x86_64-linux" then
          "--debug-logs --skip-checks --rollback-succeeded false"
        else
          "--debug-logs --skip-checks --remote-build --rollback-succeeded false";

      nixFlags = "--accept-flake-config --extra-experimental-features 'nix-command flakes'";

      mkEnvDeployScripts =
        env:
        let
          cfg = environments.${env};
          inherit (cfg) hostKey nodeName;
          envInfraPkgs = infraPkgs.perEnv.${env};

          deployPreamble = ''
            # Ahead of the DEPLOY_HOST branch, so the identity flags are
            # consumed off "$@" whichever way the host is resolved -- otherwise
            # they reach `deploy` (or bind $profile in the prelude below).
            ${infraPkgs.parseIdentity}

            if [ -n "''${DEPLOY_HOST:-}" ]; then
              host_ip="$DEPLOY_HOST"
              echo "Using pre-set DEPLOY_HOST=$host_ip"
              # An op:// identity is materialized for decryption alone, which
              # this branch skips -- so no key file exists and the agent really
              # is the credential.
              if [ -n "''${SSH_IDENTITY_OP:-}" ] && [ -z "$ssh_identity" ]; then
                echo "WARNING: SSH_IDENTITY_OP is set but DEPLOY_HOST skips decryption; relying on the 1Password SSH agent for authentication" >&2
              fi
            else
              # Resolves the droplet IP from the encrypted per-env cache
              # (infra/.remote-${env}.age), decryptable by every key in
              # roles.${env}.ssh -- including the CI deploy key.
              ${envInfraPkgs.resolveHostBody}
            fi

            # Pin the host key from keys.nix so SSH verifies it during
            # the handshake. This is safer than ssh-keyscan (which is
            # unauthenticated and fails silently on some CI runners).
            mkdir -p "$HOME/.ssh"
            ssh-keygen -R "$host_ip" >/dev/null 2>&1 || true
            echo "$host_ip ${hostKey}" >> "$HOME/.ssh/known_hosts"

            ssh_flag=""
            # The bare default key is left unpinned: -i replaces ssh's default
            # IdentityFile list, so pinning it would lock out an operator whose
            # login key is another default. Agent keys are unaffected -- ssh
            # still offers them unless IdentitiesOnly=yes.
            if [ -n "$ssh_identity" ] && [ "$ssh_identity" != "$HOME/.ssh/id_ed25519" ]; then
              export NIX_SSHOPTS="-i $ssh_identity"
              ssh_flag="--ssh-opts=-i $ssh_identity"
            fi
          '';

          mkDeployScript =
            name:
            {
              extraDeployFlags ? "",
              prelude ? "",
              target,
            }:
            pkgs.writeShellApplication {
              inherit name;
              runtimeInputs = deployInputs;
              text = ''
                ${deployPreamble}
                ${prelude}
                deploy ${deployFlags} ${extraDeployFlags} --hostname "$host_ip" \
                  ''${ssh_flag:+"$ssh_flag"} "$@" ${target} \
                  -- ${nixFlags}
              '';
            };

        in
        {
          "${env}DeployNixos" = mkDeployScript "${env}-deploy-nixos" {
            target = ".#${nodeName}.system";
          };

          "${env}DeployNixosBoot" = mkDeployScript "${env}-deploy-nixos-boot" {
            extraDeployFlags = "--boot";
            target = ".#${nodeName}.system";
          };

          "${env}DeployService" = mkDeployScript "${env}-deploy-service" {
            prelude = ''
              profile="''${1:?usage: ${env}-deploy-service <profile>}"
              shift
            '';
            target = ''.#${nodeName}."$profile"'';
          };

          "${env}DeployAll" = mkDeployScript "${env}-deploy-all" { target = ".#${nodeName}"; };
        };

    in
    builtins.foldl' (acc: env: acc // mkEnvDeployScripts env) { } (builtins.attrNames environments);
}
