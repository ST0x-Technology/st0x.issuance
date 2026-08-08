#!/usr/bin/env nu

# Edit an age-encrypted secret with ragenix, rekeying every recipient if the
# file actually changed. Rust/structured-arg rewrite of the old bash wrapper:
# `def main` gives self-documenting `--help`, typed flags, and a clear
# "missing file" error instead of the old positional `$1`/`$2` fall-through
# that crashed on an unbound `$HOME`.

# op-identity prints one line: the path of an ephemeral key file this caller
# now owns and must delete.
def read-op-identity [uri: string, account: string] {
  let args = if ($account | is-not-empty) { [$uri "--account" $account] } else { [$uri] }
  # Not `complete`: it buffers stderr too, so an interactive 1Password prompt
  # would stay invisible until op gave up. Assigning the bare external call
  # captures stdout only, leaves stderr on the terminal, and still raises on a
  # non-zero exit. Not wrapped in try/catch either: op-identity already prints a
  # specific `ERROR:` line for every failure branch, so a wrapper would only
  # restate nushell's "non-zero exit code" on top of it.
  let out = (^op-identity ...$args)
  let path = ($out | lines | get 0)
  { path: $path, tmpfile: $path }
}

# Resolves the age/SSH identity used to decrypt, mirroring the deploy tooling's
# precedence: --op (1Password) > --identity/-i > $SSH_IDENTITY >
# $SSH_IDENTITY_OP (1Password) > ~/.ssh/id_ed25519. Returns a
# `{ path, tmpfile }` record; `tmpfile` is "" for a caller-supplied key that
# must NOT be deleted, and the ephemeral 1Password key to delete once both
# ragenix calls are done otherwise.
def resolve-identity [
  --identity: string
  --op: string
  --op-account: string
] {
  if ($op | is-not-empty) {
    return (read-op-identity $op ($op_account | default ""))
  }

  if ($identity | is-not-empty) {
    if not ($identity | path exists) {
      error make --unspanned { msg: $"identity key not found: ($identity)" }
    }
    return { path: $identity, tmpfile: "" }
  }

  let ssh_identity = ($env.SSH_IDENTITY? | default "")
  if ($ssh_identity | is-not-empty) {
    if not ($ssh_identity | path exists) {
      error make --unspanned { msg: $"SSH_IDENTITY key not found: ($ssh_identity)" }
    }
    return { path: $ssh_identity, tmpfile: "" }
  }

  let op_uri = ($env.SSH_IDENTITY_OP? | default "")
  if ($op_uri | is-not-empty) {
    return (read-op-identity $op_uri ($env.SSH_IDENTITY_OP_ACCOUNT? | default ""))
  }

  # Guard $HOME: under nushell it is simply absent rather than an "unbound
  # variable" crash, but a missing HOME must still fall through to a clear error
  # rather than probing a bogus "/.ssh/id_ed25519".
  let home = ($env.HOME? | default "")
  if ($home | is-not-empty) {
    let default_key = ([$home ".ssh" "id_ed25519"] | path join)
    if ($default_key | path exists) {
      return { path: $default_key, tmpfile: "" }
    }
  }

  error make --unspanned {
    msg: ("no decryption identity available -- pass one of:\n"
      + "  -i <key>               explicit age/SSH private key\n"
      + "  --op op://vault/item   read the key from 1Password\n"
      + "  SSH_IDENTITY=<key>     environment variable\n"
      + "  SSH_IDENTITY_OP=<uri>  read the key from 1Password (env form)\n"
      + "  ~/.ssh/id_ed25519      default key\n"
      + "An SSH agent cannot substitute for any of these: age decryption needs "
      + "the raw private key for key agreement, and the agent protocol only "
      + "signs.")
  }
}

def file-hash [file: string]: nothing -> any {
  if ($file | path exists) { open --raw $file | hash sha256 } else { null }
}

def cleanup-identity [identity: record<path: string, tmpfile: string>] {
  if ($identity.tmpfile | is-not-empty) { rm --force $identity.tmpfile }
}

# Edit an age secret and rekey all recipients if it changed.
def main [
  file: string             # secret file to edit, e.g. secret/terraform.tfvars.age
  --identity (-i): string  # path to an age/SSH private key used to decrypt
  --op: string             # 1Password op:// URI to read the identity from
  --op-account: string     # 1Password account to use with --op
] {
  let identity = (resolve-identity --identity $identity --op $op --op-account $op_account)

  # One outer catch around everything past resolve-identity, so no failure path
  # can leave the 1Password temp key on disk. A signal is not a failure path:
  # nushell cannot trap SIGINT, so a key resolved here is only removed if the
  # catch still gets to run. The packaged `secret` (flake.nix) does not rely on
  # that -- it resolves the key in its bash wrapper, passes it as --identity,
  # and lets a bash EXIT trap own the removal.
  try {
    let before = (file-hash $file)

    # ragenix -e launches $EDITOR, so run it directly to inherit the terminal --
    # capturing it via `complete` would break the interactive editor.
    try {
      ^ragenix --rules ./secret/secrets.nix -i $identity.path -e $file
    } catch {|err|
      error make --unspanned { msg: $"failed to edit ($file): ($err.msg)" }
    }

    let after = (file-hash $file)

    if $before != $after {
      print $"($file) changed -- rekeying all recipients"
      try {
        ^ragenix --rules ./secret/secrets.nix -i $identity.path -r
      } catch {|err|
        error make --unspanned {
          msg: ($"rekey failed: ($err.msg)\n"
            + $"WARNING: ($file) may not be encrypted for every recipient yet -- "
            + "re-run this command to finish rekeying before deploying.")
        }
      }
    } else {
      print $"($file) unchanged -- skipping rekey"
    }
  } catch {|err|
    cleanup-identity $identity
    # Re-raised with the original's help text: nushell keeps the offending path
    # or reason there, and a bare `msg` would drop it.
    error make --unspanned { msg: $err.msg, help: ($err.json | from json).help? }
  }

  cleanup-identity $identity
}
