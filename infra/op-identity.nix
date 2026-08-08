# Resolves a 1Password identity URI to a 0600 temp file containing the raw
# key and prints that file's path -- the whole stdout contract. Always
# ephemeral: one `op read` per call, caller deletes the file when done. The
# shared implementation behind both `parseIdentity` (bash, infra/default.nix)
# and `resolve-identity` (nushell, scripts/secret.nu) so there is one `op
# read` code path, not two.
{ pkgs }:

pkgs.writeShellApplication {
  name = "op-identity";
  runtimeInputs = [ pkgs.coreutils ];
  text = ''
    usage() {
      echo "Usage: op-identity <op://vault/item[/field]> [--account <account>]" >&2
      exit 1
    }

    # `op read` is redirected straight to $fetch_dest, never captured via
    # $( ) -- its stdout must not contaminate this tool's own stdout contract,
    # and any TTY prompt it issues must still reach the terminal.
    fetch_key() {
      fetch_uri="$1"
      fetch_account="$2"
      fetch_dest="$3"
      op_args=("$fetch_uri")
      if [ -n "$fetch_account" ]; then
        op_args+=(--account "$fetch_account")
      fi
      if ! "$op_path" read "''${op_args[@]}" > "$fetch_dest"; then
        rm -f "$fetch_dest"
        echo "ERROR: op read failed for $fetch_uri" >&2
        exit 1
      fi
      # `op read` exits 0 for a field that exists but holds nothing, which is a
      # different operator mistake than a wrong key format.
      if [ ! -s "$fetch_dest" ]; then
        rm -f "$fetch_dest"
        echo "ERROR: op read returned no data for $fetch_uri" >&2
        exit 1
      fi
      # The accepted classes are the non-interactive ones rage's identity
      # parsers take: an OpenSSH or PKCS#1 RSA PEM private key (age crate,
      # src/ssh/identity.rs) or an age identity file, whose reader skips blank
      # and `#` comment lines around the AGE-SECRET-KEY-1 body (age crate,
      # src/identity.rs) -- hence classifying the first substantive line,
      # CRLF-tolerant, rather than line 1. 1Password serves an SSH-key item's
      # "private key" field as PKCS#8
      # (-----BEGIN PRIVATE KEY-----) unless the URI asks for OpenSSH. Never
      # echo the line. Passphrase-encrypted age identity files, which rage also
      # accepts, are deliberately excluded: this tool's contract is a key file
      # the caller can use non-interactively, and one of those would surface a
      # rage passphrase prompt layers down instead.
      fetch_header=""
      while IFS= read -r fetch_line || [ -n "$fetch_line" ]; do
        fetch_line="''${fetch_line%$'\r'}"
        case "$fetch_line" in
          "" | "#"*) continue ;;
        esac
        fetch_header="$fetch_line"
        break
      done < "$fetch_dest"
      case "$fetch_header" in
        "-----BEGIN OPENSSH PRIVATE KEY-----" | "-----BEGIN RSA PRIVATE KEY-----" | AGE-SECRET-KEY-1*) ;;
        *)
          rm -f "$fetch_dest"
          echo "ERROR: $fetch_uri did not return a key rage can read" >&2
          echo "  1Password serves SSH-key items as PKCS#8 by default -- append ?ssh-format=openssh to the op:// URI" >&2
          exit 1
          ;;
      esac
    }

    # Re-raises after removing the temp key, so a signalled run terminates with
    # the conventional 128+signal status instead of surviving into the caller's
    # error path.
    on_signal() {
      rm -f "$tmp"
      trap - "$1"
      kill -s "$1" "$$"
    }

    uri="''${1:-}"
    [ -n "$uri" ] || usage
    shift

    account=""
    if [ "''${1:-}" = "--account" ]; then
      account="''${2:-}"
      [ -n "$account" ] || usage
      shift 2
    fi
    [ "$#" -eq 0 ] || usage

    # Before mktemp, so a missing op needs no temp-file cleanup.
    op_path=$(command -v op || true)
    if [ -z "$op_path" ] && [ -x /opt/homebrew/bin/op ]; then
      op_path=/opt/homebrew/bin/op
    fi
    if [ -z "$op_path" ] || [ ! -x "$op_path" ]; then
      echo "ERROR: 1Password CLI (op) not found" >&2
      exit 1
    fi

    tmp=$(mktemp)
    # Not an EXIT trap: the file is the deliverable, so the trap only covers the
    # window before the caller learns the path and takes over deletion.
    trap 'on_signal INT' INT
    trap 'on_signal TERM' TERM
    trap 'on_signal HUP' HUP
    fetch_key "$uri" "$account" "$tmp"
    trap - INT TERM HUP
    printf '%s\n' "$tmp"
  '';
}
