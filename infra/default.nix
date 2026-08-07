{
  pkgs,
  ragenix,
  system,
  environments,
}:

let
  opIdentity = import ./op-identity.nix { inherit pkgs; };

  buildInputs = [
    pkgs.terraform
    pkgs.rage
    pkgs.jq
    pkgs.coreutils
    ragenix.packages.${system}.default
  ];

  sshBuildInputs = [
    pkgs.rage
    pkgs.coreutils
  ];

  tfPlanFile = "infra/tfplan";

  mkEncrypted =
    { file, role }:
    {
      path = file;
      agePath = "${file}.age";
      decrypt = ''
        if [ -f ${file}.age ]; then
          _require_identity
          # Decrypted to a temp file and renamed only on success, so a failed
          # decryption can never leave a truncated file at the real path --
          # otherwise a zero-byte terraform.tfstate here would get
          # re-encrypted over terraform.tfstate.age by the EXIT trap.
          _decrypt_tmp=$(mktemp ${file}.XXXXXX)
          _decrypt_tmpfiles+=("$_decrypt_tmp")
          if rage -d -i "$identity" ${file}.age > "$_decrypt_tmp"; then
            mv "$_decrypt_tmp" ${file}
          else
            rm -f "$_decrypt_tmp"
            exit 1
          fi
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

  # Callers must invoke _cleanup_identity in their own cleanup/on_exit, or
  # call it explicitly before exec, to remove the ephemeral files.
  #
  # Two variables, because the identity plays two unrelated roles. $identity is
  # the key rage decrypts with; $ssh_identity is the key file ssh/deploy-rs may
  # present with -i. File-path sources (-i, $SSH_IDENTITY, ~/.ssh/id_ed25519)
  # set both, here at parse time: it costs nothing and never prompts. A
  # 1Password source sets only $identity, and only lazily, at the first
  # _require_identity call -- so a command that never decrypts (e.g. a host-IP
  # cache hit in resolveHost) never touches 1Password, and ssh authenticates
  # through the 1Password SSH agent whether the cache was warm or cold.
  parseIdentity = ''
    set -eo pipefail

    _identity_tmpfile=""
    _decrypt_tmpfiles=()
    # The guards must not be `[ -n ... ] && rm`: as the last command of an EXIT
    # trap, an exit 1 (nothing to remove) would override the script's exit code.
    _cleanup_identity() {
      if [ -n "$_identity_tmpfile" ]; then rm -f "$_identity_tmpfile"; fi
      if [ ''${#_decrypt_tmpfiles[@]} -gt 0 ]; then rm -f "''${_decrypt_tmpfiles[@]}"; fi
    }

    _identity_from_op() {
      identity="$(${opIdentity}/bin/op-identity "$@")"
      _identity_tmpfile="$identity"
    }

    # A value starting with `-` is the next flag, not a value: rejecting it
    # here beats an opaque failure inside `op read` or `rage` layers down.
    _require_flag_value() {
      case "''${2:-}" in
        "" | -*)
          echo "ERROR: $1" >&2
          exit 1
          ;;
      esac
    }

    identity=""
    # Exported rather than plain-assigned: the scripts that only decrypt never
    # read it back, and an unread plain assignment fails their shellcheck gate.
    export ssh_identity=""
    _identity_source=""
    _identity_op_args=()

    if [ "''${1:-}" = "--op" ]; then
      _require_flag_value "--op requires an op:// URI" "''${2:-}"
      _op_uri="$2"
      shift 2
      _identity_op_args=("$_op_uri")
      if [ "''${1:-}" = "--op-account" ]; then
        _require_flag_value "--op-account requires an account" "''${2:-}"
        _identity_op_args+=(--account "$2")
        shift 2
      fi
      _identity_source="op"
    elif [ "''${1:-}" = "--op-account" ]; then
      # Only ever a modifier of --op. Left in "$@" it would reach the downstream
      # command (an ssh remote command, a deploy profile name) as an argument.
      echo "ERROR: --op-account requires --op" >&2
      exit 1
    elif [ "''${1:-}" = "-i" ]; then
      _require_flag_value "identity is empty -- pass -i <path> or set a default" "''${2:-}"
      identity="$2"
      ssh_identity="$2"
      shift 2
    elif [ -n "''${SSH_IDENTITY:-}" ]; then
      identity="$SSH_IDENTITY"
      ssh_identity="$SSH_IDENTITY"
    elif [ -n "''${SSH_IDENTITY_OP:-}" ]; then
      _identity_op_args=("$SSH_IDENTITY_OP")
      if [ -n "''${SSH_IDENTITY_OP_ACCOUNT:-}" ]; then
        _identity_op_args+=(--account "$SSH_IDENTITY_OP_ACCOUNT")
      fi
      _identity_source="op"
    elif [ -n "''${HOME:-}" ] && [ -f "$HOME/.ssh/id_ed25519" ]; then
      identity="$HOME/.ssh/id_ed25519"
      ssh_identity="$HOME/.ssh/id_ed25519"
    fi

    # Idempotent: a non-empty $identity short-circuits every call after the
    # first, so decrypting several files in one script (e.g. tfvars then
    # tfstate) only ever prompts once.
    _require_identity() {
      if [ -n "$identity" ]; then
        return 0
      fi
      if [ "$_identity_source" = "op" ]; then
        _identity_from_op "''${_identity_op_args[@]}"
      else
        echo "ERROR: no identity found -- pass -i <path>, set SSH_IDENTITY or SSH_IDENTITY_OP, or use --op" >&2
        echo "  an SSH agent alone cannot decrypt age files: the agent protocol signs, it does not do key agreement" >&2
        exit 1
      fi
    }

    trap _cleanup_identity EXIT
  '';

  # Caches the decrypted droplet IP (never key material) so the frequent
  # {env}Remote / {env}Service* / {env}DbReset commands skip both the age
  # decrypt and identity resolution on a hit -- see resolveHost below, which
  # keys entries by the ciphertext's hash, so any rewrite of
  # infra/.remote-{env}.age (tf-apply and tf-destroy re-encrypt it on exit) is
  # an automatic, exact invalidation.
  hostIpCacheLib = ''
    _host_ip_cache_dir() {
      if [ -n "''${XDG_RUNTIME_DIR:-}" ]; then
        printf '%s/st0x-host-ip\n' "$XDG_RUNTIME_DIR"
      elif [ -n "''${TMPDIR:-}" ]; then
        printf '%s/st0x-host-ip\n' "$TMPDIR"
      else
        printf '%s/.cache/st0x-host-ip\n' "''${HOME:-}"
      fi
    }

    # The umask subshell creates the directory 0700 in one step, leaving no
    # window at the ambient mode; a directory that already existed keeps its own
    # mode, so tighten that separately. -L runs before -O because -O follows
    # symlinks -- a planted link would otherwise hand a foreign directory our
    # writes.
    _ensure_host_ip_cache_dir() {
      local cache_dir
      cache_dir=$(_host_ip_cache_dir)
      if ! (umask 077 && mkdir -p "$cache_dir") 2>/dev/null; then
        echo "WARNING: cannot create cache directory $cache_dir -- continuing without the host-IP cache" >&2
        return 1
      fi
      if [ -L "$cache_dir" ] || [ ! -O "$cache_dir" ]; then
        echo "WARNING: cache directory $cache_dir is a symlink or not owned by the current user -- continuing without the host-IP cache" >&2
        return 1
      fi
      chmod 700 "$cache_dir" 2>/dev/null || true
      printf '%s\n' "$cache_dir"
    }

    # One shape rule for both ends of the cache, so a value the decrypt path
    # accepts can never be silently rejected on read-back (a permanent miss).
    _is_host_ip() {
      case "''${1:-}" in
        "" | *[!0-9.]*) return 1 ;;
        *) return 0 ;;
      esac
    }
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

      # Split from resolveHost so a caller can validate argv between the two:
      # parseIdentity only reads flags; this half decrypts and can prompt.
      resolveHostBody = ''
        ${hostIpCacheLib}

        _host_ip_dir=""
        _host_ip_entry=""
        host_ip=""
        if _host_ip_dir=$(_ensure_host_ip_cache_dir) && [ -f ${remoteFile.agePath} ]; then
          # The hash names the entry, so a failure to read the ciphertext leaves
          # $_host_ip_entry empty: no read, no write-back, command unaffected.
          if _host_ip_age_hash=$(sha256sum ${remoteFile.agePath} 2>/dev/null | cut -d' ' -f1) &&
            [ -n "$_host_ip_age_hash" ]; then
            _host_ip_entry="$_host_ip_dir/${env}-$_host_ip_age_hash"
            # An absent or concurrently evicted entry is a miss, not an error, so
            # the read itself must never be fatal.
            _host_ip_cached=$(cat "$_host_ip_entry" 2>/dev/null || true)
            if _is_host_ip "$_host_ip_cached"; then
              host_ip="$_host_ip_cached"
            fi
          else
            echo "WARNING: cannot hash ${remoteFile.agePath} -- continuing without the host-IP cache" >&2
          fi
        fi

        if [ -z "$host_ip" ]; then
          ${remoteFile.decrypt}
          # The check below names the ciphertext to fix; a `cat` error would not.
          host_ip=$(cat ${remoteFile.path} 2>/dev/null || true)
          rm -f ${remoteFile.path}
          if ! _is_host_ip "$host_ip"; then
            echo "ERROR: could not resolve ${env} host from ${remoteFile.agePath}" >&2
            exit 1
          fi
          if [ -n "$_host_ip_entry" ]; then
            # A cache write must never fail the command -- the host is already
            # resolved. Chained because bash ignores errexit left of `||`: an
            # unchained partial write would still be renamed into place and
            # served later as a truncated IP. The umask covers a temp recreated
            # by the redirect after a concurrent run's eviction glob removed it.
            (
              umask 077
              rm -f "$_host_ip_dir/${env}"-* "$_host_ip_dir/${env}".*
              _host_ip_tmp=$(mktemp "$_host_ip_dir/${env}.XXXXXX") &&
                printf '%s\n' "$host_ip" > "$_host_ip_tmp" &&
                mv "$_host_ip_tmp" "$_host_ip_entry"
            ) 2>/dev/null || echo "WARNING: could not write host-IP cache entry -- continuing" >&2
          fi
        fi
      '';

      resolveHost = ''
        ${parseIdentity}
        ${resolveHostBody}
      '';

      requireDecryptedSecrets = ''
        # shellcheck disable=SC2029
        ssh ''${ssh_identity:+-i "$ssh_identity"} "root@$host_ip" '
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
      inherit resolveHost resolveHostBody;

      "${env}Remote" = pkgs.writeShellApplication {
        name = "${env}-remote";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT
          # shellcheck disable=SC2029
          ssh ''${ssh_identity:+-i "$ssh_identity"} "root@$host_ip" "$@"
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
          ssh ''${ssh_identity:+-i "$ssh_identity"} "root@$host_ip" \
            "mkdir -p /run/st0x && touch /run/st0x/st0x-issuance.ready && systemctl start st0x-issuance"
          ssh ''${ssh_identity:+-i "$ssh_identity"} "root@$host_ip" systemctl is-active st0x-issuance
        '';
      };

      "${env}ServiceStop" = pkgs.writeShellApplication {
        name = "${env}-service-stop";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT
          echo "Stopping st0x-issuance on ${env}..."
          ssh ''${ssh_identity:+-i "$ssh_identity"} "root@$host_ip" \
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
          ssh ''${ssh_identity:+-i "$ssh_identity"} "root@$host_ip" \
            "mkdir -p /run/st0x && touch /run/st0x/st0x-issuance.ready && systemctl restart st0x-issuance"
          ssh ''${ssh_identity:+-i "$ssh_identity"} "root@$host_ip" systemctl is-active st0x-issuance
        '';
      };

      "${env}DbReset" = pkgs.writeShellApplication {
        name = "${env}-db-reset";
        runtimeInputs = sshInputs ++ [ pkgs.gnugrep ];
        text = ''
          ${parseIdentity}
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

          # After the refusal above, so a usage error never decrypts or prompts.
          ${resolveHostBody}

          ssh_remote() {
            # shellcheck disable=SC2029
            ssh ''${ssh_identity:+-i "$ssh_identity"} "root@$host_ip" "$@"
          }

          _restart_service() {
            if [ "$stay_stopped" = false ]; then
              echo "Ensuring st0x-issuance is restarted on ${env}..." >&2
              ssh_remote "mkdir -p /run/st0x && touch /run/st0x/st0x-issuance.ready && systemctl start st0x-issuance" || true
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
            ssh_remote "mkdir -p /run/st0x && touch /run/st0x/st0x-issuance.ready && systemctl start st0x-issuance"
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

  perEnv = builtins.mapAttrs (_: result: {
    inherit (result) resolveHost resolveHostBody;
  }) envResults;

  envPkgs = builtins.foldl' (
    acc: env:
    acc
    // builtins.removeAttrs envResults.${env} [
      "resolveHost"
      "resolveHostBody"
    ]
  ) { } environments;

in
{
  # Deliberately not a member of `packages`: that attrset becomes flake
  # packages, and op-identity's contract is to leave a private key on disk for
  # its caller to delete -- there is no caller behind `nix run`.
  inherit
    buildInputs
    sshBuildInputs
    parseIdentity
    opIdentity
    ;

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
