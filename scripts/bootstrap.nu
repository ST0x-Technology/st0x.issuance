#!/usr/bin/env nu

# Bootstrap a NixOS host on a freshly-provisioned droplet.
#
# The flake wrapper (`nix run .#bootstrap`) parses identity, resolves
# prod|staging, exports env vars, and forwards extra args to nixos-anywhere:
#   env, flake_config, host_key_field, identity

const TFSTATE_AGE = "infra/terraform.tfstate.age"
const TFSTATE = "infra/terraform.tfstate"
const KEYS_NIX = "keys.nix"
const HOST_KEY_PATTERN = '^ssh-ed25519 [A-Za-z0-9+/=_]+$'

def require-env [name: string] {
  let value = ($env | get -o $name | default "")
  if ($value | is-empty) {
    error make --unspanned { msg: $"environment variable '($name)' not set by wrapper" }
  }
  $value
}

def validate-environment [deployment_env: string] {
  if $deployment_env not-in [prod staging] {
    error make --unspanned { msg: $"unknown environment '($deployment_env)' — expected prod or staging" }
  }
}

def validate-host-key [label: string, key: string] {
  if ($key | is-empty) {
    error make --unspanned { msg: $"($label): SSH host key is empty" }
  }
  if not ($key =~ $HOST_KEY_PATTERN) {
    error make --unspanned { msg: $"($label): SSH host key is malformed: '($key)'" }
  }
}

def parse-host-key-line [line: string] {
  let fields = ($line | str trim | split row " ")
  if ($fields | length) < 2 {
    return ""
  }
  $"($fields.0) ($fields.1)"
}

def resolve-host-ip [deployment_env: string, identity: string] {
  let output_key = $"($deployment_env)_droplet_ipv4"

  let state_json = if ($TFSTATE_AGE | path exists) {
    let decrypt = (do { ^rage -d -i $identity $TFSTATE_AGE } | complete)
    if $decrypt.exit_code != 0 {
      error make --unspanned {
        msg: $"decrypting ($TFSTATE_AGE) failed (exit ($decrypt.exit_code)): ($decrypt.stderr | str trim)"
      }
    }
    $decrypt.stdout
  } else if ($TFSTATE | path exists) {
    open --raw $TFSTATE
  } else {
    error make --unspanned {
      msg: $"neither ($TFSTATE_AGE) nor ($TFSTATE) found — run tfApply first"
    }
  }

  let state = ($state_json | from json)
  let host_ip = (
    $state
    | get -o outputs
    | get -o $output_key
    | get -o value
    | default ""
  )

  if ($host_ip | is-empty) or $host_ip == "null" {
    error make --unspanned {
      msg: ($"terraform output '($output_key)' is missing or null\n"
        + "confirm tfApply succeeded and state was committed")
    }
  }

  $host_ip
}

def ssh-run [
  host_ip: string
  identity: string
  remote_command: string
  --label: string = "ssh"
  --allow-failure = false
] {
  let ssh_args = [
    "-o" "StrictHostKeyChecking=no"
    "-o" "ConnectTimeout=5"
    "-i" $identity
    $"root@($host_ip)"
    $remote_command
  ]
  let result = (do { ^ssh ...$ssh_args } | complete)

  if ($allow_failure == false) and $result.exit_code != 0 {
    let stderr = ($result.stderr | str trim)
    let stdout = ($result.stdout | str trim)
    mut detail = $"exit ($result.exit_code)"
    if ($stderr | is-not-empty) {
      $detail = $"($detail): ($stderr)"
    } else if ($stdout | is-not-empty) {
      $detail = $"($detail): ($stdout)"
    }

    error make --unspanned {
      msg: ($"($label) to root@($host_ip) failed — ($detail)\n"
        + "if this is the first connect after tfApply, check the DO firewall allows TCP 22 for bootstrap")
    }
  }

  $result
}

def fetch-installer-host-key [host_ip: string, identity: string] {
  print "Fetching host key from source host (pre-install)..."
  let result = (ssh-run $host_ip $identity "cat /etc/ssh/ssh_host_ed25519_key.pub" --label "pre-install host key fetch")
  let key = (parse-host-key-line ($result.stdout | lines | first | default ""))
  validate-host-key "source host" $key
  $key
}

def run-nixos-anywhere [
  flake_config: string
  host_ip: string
  identity: string
  extra_args: list<string>
] {
  print $"Running nixos-anywhere on root@($host_ip) with .#($flake_config)..."
  # --copy-host-keys: NixOS would otherwise generate fresh host keys on install,
  # which breaks the pre/post reboot comparison used for MITM detection.
  let command_args = [
    "--flake" $".#($flake_config)"
    "--option" "pure-eval" "false"
    "--copy-host-keys"
    "--ssh-option" $"IdentityFile=($identity)"
    "--target-host" $"root@($host_ip)"
  ] | append $extra_args

  # Nushell >= 0.98 raises on a non-zero external exit before any manual
  # LAST_EXIT_CODE check could run; try/catch (rather than `complete`)
  # keeps nixos-anywhere's output streaming live.
  try {
    ^nixos-anywhere ...$command_args
  } catch {|err|
    error make --unspanned {
      msg: $"nixos-anywhere failed: ($err.msg)"
    }
  }
}

def wait-for-host [host_ip: string, identity: string] {
  print "Waiting for host to come back up..."
  const max_retries = 60
  mut retries = 0

  while $retries < $max_retries {
    let probe = (
      ssh-run $host_ip $identity "true" --label "post-install ssh probe" --allow-failure=true
    )
    if $probe.exit_code == 0 {
      return
    }
    sleep 5sec
    $retries = $retries + 1
  }

  error make --unspanned {
    msg: $"host root@($host_ip) did not come back up after 5 minutes"
  }
}

def update-keys-nix [host_key_field: string, new_key: string] {
  if not ($KEYS_NIX | path exists) {
    error make --unspanned { msg: $"($KEYS_NIX) not found" }
  }

  let prefix = $"    ($host_key_field) ="
  let replacement = $"\"($new_key)\""
  let pattern = '"ssh-ed25519 [A-Za-z0-9+/=_]*"'
  let mapped = (
    open $KEYS_NIX -r
    | lines
    | each {|line|
      if ($line | str starts-with $prefix) {
        { line: ($line | str replace -r $pattern $replacement), updated: true }
      } else {
        { line: $line, updated: false }
      }
    }
  )

  if not ($mapped | any {|row| $row.updated }) {
    error make --unspanned {
      msg: $"failed to find ($host_key_field) assignment in ($KEYS_NIX)"
    }
  }

  let content = ($mapped | get line | str join (char nl))
  if not ($content | str contains $new_key) {
    error make --unspanned {
      msg: $"keys.nix update did not contain expected host key for ($host_key_field)"
    }
  }

  $content | save --force $KEYS_NIX
}

def rekey-secrets [identity: string] {
  print "Rekeying secrets for updated host recipients..."
  let result = (do { ^ragenix --rules ./secret/secrets.nix -i $identity -r } | complete)
  if $result.exit_code != 0 {
    let stderr = ($result.stderr | str trim)
    error make --unspanned {
      msg: ($"ragenix rekey failed (exit ($result.exit_code))"
        + (if ($stderr | is-not-empty) { $": ($stderr)" } else { "" }))
    }
  }
}

def main [...nixos_anywhere_args: string] {
  let deployment_env = (require-env "env")
  let flake_config = (require-env "flake_config")
  let host_key_field = (require-env "host_key_field")
  let identity = (require-env "identity")

  validate-environment $deployment_env

  let host_ip = (resolve-host-ip $deployment_env $identity)
  print $"Target: root@($host_ip), environment: ($deployment_env)"

  let installer_key = (fetch-installer-host-key $host_ip $identity)

  run-nixos-anywhere $flake_config $host_ip $identity $nixos_anywhere_args

  wait-for-host $host_ip $identity

  let post_reboot_stdout = (
    ssh-run $host_ip $identity "cat /etc/ssh/ssh_host_ed25519_key.pub" --label "post-reboot host key fetch"
    | get stdout
  )
  let post_reboot_key = (parse-host-key-line ($post_reboot_stdout | lines | first | default ""))
  validate-host-key "post-reboot host" $post_reboot_key

  if $post_reboot_key != $installer_key {
    error make --unspanned {
      msg: ($"post-reboot host key does not match the source host — possible MITM\n"
        + $"  pre-install : ($installer_key)\n"
        + $"  post-reboot : ($post_reboot_key)\n"
        + "refusing to update keys.nix or rekey secrets")
    }
  }

  update-keys-nix $host_key_field $post_reboot_key
  print $"Updated ($host_key_field) in ($KEYS_NIX)"
  rekey-secrets $identity
  print "Bootstrap complete."
}
