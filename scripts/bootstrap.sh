#!/usr/bin/env bash
# Bootstrap a NixOS host on a freshly-provisioned droplet.
#
# Expected environment (set by the nix wrapper before invoking this script):
#   env              -- prod | staging
#   flake_config     -- "${nodeName}-bootstrap"
#   host_key_field   -- "host-prod" | "host-staging"
#   identity         -- SSH identity (private key) to use
#
# All extra positional arguments are forwarded verbatim to nixos-anywhere.

set -euo pipefail

: "${env:?env not set by wrapper}"
: "${flake_config:?flake_config not set by wrapper}"
: "${host_key_field:?host_key_field not set by wrapper}"
: "${identity:?identity not set by wrapper}"

# Resolve IP from terraform state.
_tfstate_created_by_us=false

if [ -f "infra/terraform.tfstate.age" ]; then
  rage -d -i "$identity" infra/terraform.tfstate.age >infra/terraform.tfstate
  _tfstate_created_by_us=true
  trap 'rm -f infra/terraform.tfstate' EXIT
elif [ ! -f "infra/terraform.tfstate" ]; then
  echo "ERROR: neither infra/terraform.tfstate.age nor infra/terraform.tfstate found" >&2
  exit 1
fi

host_ip=$(jq -r ".outputs.${env}_droplet_ipv4.value" infra/terraform.tfstate)

if [ "$_tfstate_created_by_us" = true ]; then
  rm -f infra/terraform.tfstate
fi

if [ -z "$host_ip" ] || [ "$host_ip" = "null" ]; then
  echo "ERROR: could not resolve IP from terraform output '${env}_droplet_ipv4'" >&2
  exit 1
fi

ssh_opts=(
  -o StrictHostKeyChecking=no
  -o ConnectTimeout=5
  -i "$identity"
)

# Fetch the host key from the kexec/installer environment BEFORE nixos-anywhere
# reboots into the newly installed system. At this point we are talking to the
# known-good in-RAM installer, so no MITM is possible via the public IP.
echo "Fetching host key from installer environment (pre-install)..."
kexec_key=$(
  ssh "${ssh_opts[@]}" "root@$host_ip" \
    cat /etc/ssh/ssh_host_ed25519_key.pub 2>/dev/null |
    awk '{print $1 " " $2}'
)

valid_key='^ssh-ed25519 [A-Za-z0-9+/=_]+$'
if [ -z "$kexec_key" ] || ! echo "$kexec_key" | grep -qE "$valid_key"; then
  echo "ERROR: could not retrieve a valid host key from the installer environment: '$kexec_key'" >&2
  exit 1
fi

nixos-anywhere --flake ".#$flake_config" \
  --option pure-eval false \
  --ssh-option "IdentityFile=$identity" \
  --target-host "root@$host_ip" "$@"

echo "Waiting for host to come back up..."
retries=0
until ssh "${ssh_opts[@]}" "root@$host_ip" true 2>/dev/null; do
  retries=$((retries + 1))
  if [ "$retries" -ge 60 ]; then
    echo "Host did not come back up after 5 minutes" >&2
    exit 1
  fi
  sleep 5
done

# Retrieve the key now served by the rebooted host and verify it matches what
# the installer reported. A mismatch means someone intercepted the connection.
new_key=$(
  ssh "${ssh_opts[@]}" "root@$host_ip" \
    cat /etc/ssh/ssh_host_ed25519_key.pub |
    awk '{print $1 " " $2}'
)

if [ -z "$new_key" ] || ! echo "$new_key" | grep -qE "$valid_key"; then
  echo "ERROR: SSH host key is empty or malformed: '$new_key'" >&2
  exit 1
fi

if [ "$new_key" != "$kexec_key" ]; then
  echo "ERROR: post-reboot host key does not match the key observed in the installer environment." >&2
  echo "  installer : $kexec_key" >&2
  echo "  post-reboot: $new_key" >&2
  echo "Refusing to rekey secrets – possible MITM." >&2
  exit 1
fi

sed -i \
  "/$host_key_field =/ s|\"ssh-ed25519 [A-Za-z0-9+/=_]*\"|\"$new_key\"|" \
  keys.nix

if ! grep -qF "$new_key" keys.nix; then
  echo "ERROR: failed to update $host_key_field in keys.nix" >&2
  exit 1
fi

echo "Updated $host_key_field in keys.nix, rekeying secrets..."
ragenix --rules ./secret/secrets.nix -i "$identity" -r
