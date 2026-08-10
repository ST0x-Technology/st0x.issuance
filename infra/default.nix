{
  pkgs,
  ragenix,
  system,
  environments,
}:

let
  buildInputs = [
    pkgs.terraform
    pkgs.rage
    pkgs.jq
    ragenix.packages.${system}.default
  ];

  sshBuildInputs = [ pkgs.rage ];

  tfPlanFile = "infra/tfplan";

  mkEncrypted =
    { file, role }:
    {
      path = file;
      agePath = "${file}.age";
      decrypt = ''
        if [ -f ${file}.age ]; then
          if [ -z "''${identity:-}" ]; then
            echo "ERROR: decrypting ${file}.age requires an identity. Options:" >&2
            echo "  -i <path>                    explicit key" >&2
            echo "  SSH_IDENTITY=<path>          env var" >&2
            echo "  --op op://vault/item         1Password CLI" >&2
            echo "  ~/.ssh/id_ed25519            default key" >&2
            exit 1
          fi
          rage -d -i "$identity" ${file}.age > ${file}
        fi
      '';
      encrypt = ''
        if [ -f ${file} ]; then
          nix eval --raw --file ${../keys.nix} roles.${role} --apply 'builtins.concatStringsSep "\n"' \
            | rage -e -R /dev/stdin -o ${file}.age ${file}
        fi
      '';
    };

  state = mkEncrypted {
    file = "infra/terraform.tfstate";
    role = "infra";
  };
  vars = mkEncrypted {
    file = "infra/terraform.tfvars";
    role = "infra";
  };

  # Per-environment remote IP caches for SSH access
  mkRemote =
    env: sshRole:
    mkEncrypted {
      file = "infra/.remote-${env}";
      role = sshRole;
    };

  remoteFiles = builtins.listToAttrs (
    map (env: {
      name = env;
      value = mkRemote env "${env}.ssh";
    }) environments
  );

  # Callers must invoke _cleanup_identity in their own cleanup/on_exit,
  # or call it explicitly before exec, to remove the temporary key file.
  parseIdentity = ''
    set -eo pipefail

    _identity_tmpfile=""
    # The guard must not be `[ -n ... ] && rm`: as the last command of an EXIT
    # trap, its exit 1 (no tmpfile) would override the script's exit code.
    _cleanup_identity() { if [ -n "$_identity_tmpfile" ]; then rm -f "$_identity_tmpfile"; fi; }

    if [ "''${1:-}" = "--op" ]; then
      if [ -z "''${2:-}" ]; then
        echo "ERROR: --op requires an op:// URI" >&2
        exit 1
      fi
      _op_uri="$2"
      shift 2
      _op_args=()
      if [ "''${1:-}" = "--op-account" ]; then
        if [ -z "''${2:-}" ]; then
          echo "ERROR: --op-account requires an account" >&2
          exit 1
        fi
        _op_args+=(--account "$2")
        shift 2
      fi
      _identity_tmpfile="$(mktemp)"
      chmod 600 "$_identity_tmpfile"
      _op="$(command -v op 2>/dev/null || echo /opt/homebrew/bin/op)"
      if [ ! -x "$_op" ]; then
        echo "ERROR: 1Password CLI (op) not found" >&2
        exit 1
      fi
      "$_op" read "$_op_uri" "''${_op_args[@]}" > "$_identity_tmpfile"
      identity="$_identity_tmpfile"
    elif [ "''${1:-}" = "-i" ]; then
      if [ -z "''${2:-}" ]; then
        echo "ERROR: identity is empty -- pass -i <path> or set a default" >&2
        exit 1
      fi
      identity="$2"
      shift 2
    elif [ -n "''${SSH_IDENTITY:-}" ]; then
      identity="$SSH_IDENTITY"
    elif [ -f "$HOME/.ssh/id_ed25519" ]; then
      identity="$HOME/.ssh/id_ed25519"
    else
      echo "ERROR: no identity found -- pass -i <path>, set SSH_IDENTITY, or use --op" >&2
      exit 1
    fi
  '';

  cleanup = "rm -f ${state.path} ${state.path}.backup ${vars.path}";
  cleanupWithPlan = "${cleanup} ${tfPlanFile}";

  syncRemotes = ''
    if [ -f ${state.path} ]; then
      ${builtins.concatStringsSep "\n" (
        map (
          env:
          let
            rf = remoteFiles.${env};
          in
          ''
            jq -r '.outputs.${env}_droplet_ipv4.value // empty' ${state.path} > ${rf.path} || true
            if [ -s ${rf.path} ]; then
              ${rf.encrypt}
            else
              rm -f ${rf.agePath}
            fi
            rm -f ${rf.path}
          ''
        ) environments
      )}
    fi
  '';

  preamble = ''
    ${parseIdentity}
    on_exit() { ${cleanup}; _cleanup_identity; }
    trap on_exit EXIT
    ${vars.decrypt}
  '';

  preambleWithEncrypt = ''
    ${parseIdentity}
    on_exit() {
      (${syncRemotes}) || true
      (${state.encrypt}) || true
      ${cleanupWithPlan} || true
      _cleanup_identity
    }
    trap on_exit EXIT
    ${vars.decrypt}
  '';

  mkEnv =
    env:
    let
      remoteFile = remoteFiles.${env};
      sshInputs = sshBuildInputs ++ [ pkgs.openssh ];

      resolveHost = ''
        ${parseIdentity}
        ${remoteFile.decrypt}
        host_ip=$(cat ${remoteFile.path})
        rm -f ${remoteFile.path}
        if [ -z "$host_ip" ]; then
          echo "ERROR: could not resolve ${env} host from ${remoteFile.agePath}" >&2
          exit 1
        fi
      '';

      requireDecryptedSecrets = ''
        # shellcheck disable=SC2029
        ssh ''${identity:+-i "$identity"} "root@$host_ip" '
          activate=/nix/var/nix/profiles/per-service/st0x-issuance/deploy-rs-activate
          if [ ! -f /run/agenix/st0x-issuance.env ]; then
            echo "Decrypted secrets missing (tmpfs cleared after reboot); trying to re-run service activation to restore them..." >&2
            if [ ! -x "$activate" ]; then
              echo "ERROR: $activate not found!" >&2
              echo "Run ${env}-deploy-service st0x-issuance to decrypt and install runtime secrets, then retry," >&2
              echo "or alternatively run the full service deployment via deploy.nix instead." >&2
              exit 1
            fi
            "$activate"
          fi
        '
      '';

    in
    {
      inherit resolveHost;

      "${env}Remote" = pkgs.writeShellApplication {
        name = "${env}-remote";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT
          # shellcheck disable=SC2029
          ssh ''${identity:+-i "$identity"} "root@$host_ip" "$@"
        '';
      };

      "${env}ServiceStart" = pkgs.writeShellApplication {
        name = "${env}-service-start";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT
          ${requireDecryptedSecrets}
          echo "Starting st0x-issuance on ${env}..."
          ssh ''${identity:+-i "$identity"} "root@$host_ip" \
            "test ! -e /run/st0x/st0x-issuance.hold || { echo 'ERROR: deployment hold is armed; refusing to start st0x-issuance' >&2; exit 1; }; mkdir -p /run/st0x && touch /run/st0x/st0x-issuance.ready && systemctl start st0x-issuance"
          ssh ''${identity:+-i "$identity"} "root@$host_ip" systemctl is-active st0x-issuance
        '';
      };

      "${env}ServiceStop" = pkgs.writeShellApplication {
        name = "${env}-service-stop";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT
          echo "Stopping st0x-issuance on ${env}..."
          ssh ''${identity:+-i "$identity"} "root@$host_ip" \
            "systemctl stop st0x-issuance && rm -f /run/st0x/st0x-issuance.ready"
          echo "Stopped."
        '';
      };

      "${env}ServiceRestart" = pkgs.writeShellApplication {
        name = "${env}-service-restart";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT
          ${requireDecryptedSecrets}
          echo "Restarting st0x-issuance on ${env}..."
          ssh ''${identity:+-i "$identity"} "root@$host_ip" \
            "test ! -e /run/st0x/st0x-issuance.hold || { echo 'ERROR: deployment hold is armed; refusing to restart st0x-issuance' >&2; exit 1; }; mkdir -p /run/st0x && touch /run/st0x/st0x-issuance.ready && systemctl restart st0x-issuance"
          ssh ''${identity:+-i "$identity"} "root@$host_ip" systemctl is-active st0x-issuance
        '';
      };

      "${env}DbReset" = pkgs.writeShellApplication {
        name = "${env}-db-reset";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT

          stay_stopped=false
          for arg in "$@"; do
            case "$arg" in
              --yes) ;;
              --stopped) stay_stopped=true ;;
              *)
                echo "Unknown flag: $arg" >&2
                echo "Usage: ${env}-db-reset --yes [--stopped]" >&2
                exit 1
                ;;
            esac
          done

          if ! printf '%s\n' "$@" | grep -qx -- '--yes'; then
            echo "Refusing destructive reset without --yes" >&2
            echo "Usage: ${env}-db-reset --yes [--stopped]" >&2
            exit 1
          fi

          ssh_remote() {
            # shellcheck disable=SC2029
            ssh ''${identity:+-i "$identity"} "root@$host_ip" "$@"
          }

          if ! ssh_remote "test ! -e /run/st0x/st0x-issuance.hold"; then
            echo "ERROR: deployment hold is armed; refusing to reset the database" >&2
            exit 1
          fi

          _restart_service() {
            if [ "$stay_stopped" = false ]; then
              echo "Ensuring st0x-issuance is restarted on ${env}..." >&2
              ssh_remote "if test -e /run/st0x/st0x-issuance.hold; then echo 'WARNING: deployment hold is armed; leaving st0x-issuance stopped' >&2; else mkdir -p /run/st0x && touch /run/st0x/st0x-issuance.ready && systemctl start st0x-issuance; fi" || true
            fi
            _cleanup_identity
          }
          trap '_restart_service' EXIT

          db_path="/mnt/data/issuance.db"
          backup_dir="/mnt/data/backups/$(date +%Y%m%d-%H%M%S)"

          echo "Stopping st0x-issuance on ${env}..."
          ssh_remote "systemctl stop st0x-issuance && rm -f /run/st0x/st0x-issuance.ready"

          echo "Backing up database to $backup_dir..."
          ssh_remote "mkdir -p $backup_dir && cp $db_path $db_path-wal $db_path-shm $backup_dir/ 2>/dev/null || true"

          echo "Deleting live database..."
          ssh_remote "rm -f $db_path $db_path-wal $db_path-shm $db_path-journal"

          echo "Recreating empty database owned by st0x:st0x..."
          ssh_remote "install -o st0x -g st0x -m 644 /dev/null $db_path"

          if [ "$stay_stopped" = false ]; then
            echo "Starting st0x-issuance on ${env}..."
            ssh_remote "test ! -e /run/st0x/st0x-issuance.hold || { echo 'ERROR: deployment hold is armed; refusing to start st0x-issuance' >&2; exit 1; }; mkdir -p /run/st0x && touch /run/st0x/st0x-issuance.ready && systemctl start st0x-issuance"
            ssh_remote systemctl is-active st0x-issuance
            trap - EXIT
            _cleanup_identity
          else
            trap - EXIT
            _cleanup_identity
            echo "Bot left stopped (--stopped flag). Start manually with ${env}-service-start."
          fi

          echo "Database reset complete. Backup at: $backup_dir"
        '';
      };
    };

  envResults = builtins.listToAttrs (
    map (env: {
      name = env;
      value = mkEnv env;
    }) environments
  );

  perEnv = builtins.mapAttrs (_: result: { inherit (result) resolveHost; }) envResults;

  envPkgs = builtins.foldl' (
    acc: env: acc // builtins.removeAttrs envResults.${env} [ "resolveHost" ]
  ) { } environments;

in
{
  inherit buildInputs sshBuildInputs parseIdentity;

  inherit perEnv;

  packages = {
    tfInit = pkgs.writeShellApplication {
      name = "tf-init";
      runtimeInputs = buildInputs;
      text = ''
        ${preamble}
        terraform -chdir=infra init "$@"
      '';
    };

    tfPlan = pkgs.writeShellApplication {
      name = "tf-plan";
      runtimeInputs = buildInputs;
      text = ''
        ${preamble}
        ${state.decrypt}
        terraform -chdir=infra plan -out=tfplan "$@"
      '';
    };

    tfApply = pkgs.writeShellApplication {
      name = "tf-apply";
      runtimeInputs = buildInputs;
      text = ''
        ${preambleWithEncrypt}
        ${state.decrypt}
        terraform -chdir=infra apply "$@" tfplan
      '';
    };

    tfDestroy = pkgs.writeShellApplication {
      name = "tf-destroy";
      runtimeInputs = buildInputs;
      text = ''
        ${preambleWithEncrypt}
        ${state.decrypt}
        terraform -chdir=infra destroy "$@"
      '';
    };

    tfEditVars = pkgs.writeShellApplication {
      name = "tf-edit-vars";
      runtimeInputs = buildInputs;
      text = ''
        ${parseIdentity}
        on_exit() { rm -f ${vars.path}; _cleanup_identity; }
        trap on_exit EXIT
        ${vars.decrypt}
        ''${EDITOR:-vi} ${vars.path}
        ${vars.encrypt}
      '';
    };
  }
  // envPkgs;
}
