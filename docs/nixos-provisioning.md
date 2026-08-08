# NixOS provisioning runbook

First-time bring-up for issuance on DigitalOcean + NixOS.

Operator and CI access is plain SSH to the droplet's public IP: key-only auth
(`PasswordAuthentication no`), fail2ban on the host, and the deploy tooling
resolves each environment's IP from the encrypted cache
`infra/.remote-{env}.age` (written by `tf-apply`, encrypted to
`roles.{env}.ssh`).

**Prerequisites:** nix with flakes enabled (everything else is provided by the
flake); the **st0x-op private key** (decrypts terraform state/vars and service
secrets — see `roles` in `keys.nix`); a DigitalOcean API token already lives in
the encrypted `infra/terraform.tfvars.age`.

## Invoking flake tooling

Packages are **camelCase** flake attrs; binaries on `$PATH` inside `nix develop`
are **kebab-case**.

```bash
# from repo root — either:
nix run .#tfEditVars
# or:
nix develop
tf-edit-vars
```

### Identity resolution

`tf*`, `{env}Remote`, `{env}Service*`, `bootstrap`, `rekey`, `secret`, and the
deploy scripts all resolve one decryption identity through the same precedence:
`--op <uri>` (+ `--op-account <acct>`, accepted only alongside `--op`) >
`-i <key>` > `$SSH_IDENTITY` > `$SSH_IDENTITY_OP` (+ optional
`$SSH_IDENTITY_OP_ACCOUNT`) > `~/.ssh/id_ed25519`. Every command in this doc is
shown bare — pass one of the flags, as the first argument, only to override the
default. With an `op://` source the key file is materialized for decryption only
and is never passed to `ssh -i`, so SSH itself authenticates through the
1Password SSH agent. `bootstrap` is the exception: it provisions a fresh
droplet, and both `nixos-anywhere` and its pre/post-install SSH probes need a
concrete key path, so bootstrap does pass the materialized key as `ssh -i` /
`IdentityFile=` for the lifetime of that command.

For every command but `bootstrap`, SSH access therefore requires the 1Password
SSH agent to be serving a key that is present in the server's `authorized_keys`
— the `op://` item used for decryption does not have to be that SSH key, and
cannot be when it holds an age identity.

An SSH agent cannot substitute for any of this: age decryption needs the raw
private key for key agreement with the recipient, and the SSH agent protocol
exposes only a sign operation, no way to derive a shared secret from the key it
holds. `DEPLOY_HOST=<ip>` deploys are the one exception (see "Deploy" below) —
they skip decryption entirely, so `SSH_IDENTITY_OP` has no effect there and an
`op://` source leaves the 1Password SSH agent as the only credential. A file
identity (`-i <key>` or `$SSH_IDENTITY`) is still passed to ssh as `-i`.

**Recommended setup:** put your 1Password identity in the gitignored
`.envrc.local` (sourced by `.envrc`; run `direnv allow` once after creating or
editing it):

```bash
echo "export SSH_IDENTITY_OP='op://<vault>/<item>/private key'" >> .envrc.local
# optional, only if the item is not in your default 1Password account:
echo "export SSH_IDENTITY_OP_ACCOUNT=<account>" >> .envrc.local
direnv allow
```

The bare field URI above is right for an item that stores the key in a **text
field**. For a 1Password **SSH key** item, append `?ssh-format=openssh` —
without it `op read` serves the key as PKCS#8, which `rage` cannot parse (see
the
[`op read` reference](https://developer.1password.com/docs/cli/reference/commands/read)).
The tooling checks the fetched key's format and says so if it is wrong.

**Caching:** no key material is ever cached. Every `--op`/`SSH_IDENTITY_OP`
resolution is a fresh `op read` into a `0600` temp file that is deleted when the
command exits, Ctrl-C included: every command resolves the key in its shell
wrapper, where an EXIT trap owns the removal. That resolution is also lazy — it
only happens the moment a command actually needs to decrypt something, so a
command that never decrypts never calls `op` and is never prompted. What IS
cached is the **decrypted droplet IP** (`{env}Remote`, `{env}Service*`,
`{env}DbReset` and the deploy scripts): `0600` inside a `0700` per-user runtime
directory (`$XDG_RUNTIME_DIR`, else `$TMPDIR`, else `~/.cache`), keyed by the
sha256 of `infra/.remote-{env}.age` itself. `tf-apply` and `tf-destroy` rewrite
that file on exit, so every one of them changes the hash and is an automatic,
exact cache miss — there is nothing to clear by hand. (`rekey` does not: it
re-encrypts the secrets in `secret/secrets.nix`, which never include the
remote-IP caches.)

**Prompt model:** `{env}Remote`-style commands prompt `op` at most once per IP
rotation (cache hits skip identity resolution entirely); `tf*`, `rekey`,
`tf-edit-vars`, `secret` and `bootstrap` prompt once per invocation, since they
always decrypt. SSH authentication for the remote/service/deploy scripts goes
through the 1Password SSH agent whenever the identity came from an `op://`
source (`-i` is then never passed to ssh) — the agent's own approval/biometric
settings are the knob for how often you are prompted for a fresh SSH connection.
`bootstrap` is the exception noted above: it passes the materialized key as `-i`
/ `IdentityFile=`, so it prompts `op` up front and that key is the one offered
first. The agent is not locked out — `-i` only replaces ssh's default identity
files, and agent keys are still offered unless `IdentitiesOnly=yes`. One caveat:
the ambient `op` signed-in account is not part of the IP cache key when
`--account`/`SSH_IDENTITY_OP_ACCOUNT` is omitted, so switching accounts reuses a
cached IP the same as before.

Service secrets encrypt to `roles.{env}.service` (`st0x-op` + `host-{env}`). Use
the **st0x-op private key** as `-i` when creating service secrets unless you
have already bootstrapped and rekeyed for your personal key.

---

## Host key placeholders (`keys.nix`)

Before bootstrap, `host-prod` / `host-staging` must be **valid** ed25519 public
keys. The all-`A` placeholder rage rejects with `Invalid recipient`.

Generate a throwaway pair once (same pubkey can sit in both slots until
bootstrap replaces each with the real droplet host key):

```bash
ssh-keygen -t ed25519 -f /tmp/bootstrap-placeholder -N "" -C "bootstrap-placeholder"
# paste /tmp/bootstrap-placeholder.pub into keys.nix host-prod and host-staging
rm /tmp/bootstrap-placeholder /tmp/bootstrap-placeholder.pub
```

Bootstrap overwrites each `host-{env}` with the real key and runs `ragenix -r`.

---

## Secrets checklist

Per environment (`staging`, then `prod`):

| File                                 | Contents                                  |
| ------------------------------------ | ----------------------------------------- |
| `secret/st0x-issuance-{env}.env.age` | Service env (from `.env.secrets.example`) |

Shared:

| File                         | Contents                     |
| ---------------------------- | ---------------------------- |
| `infra/terraform.tfvars.age` | `do_token` and other TF vars |

```bash
cp infra/terraform.tfvars.example infra/terraform.tfvars
$EDITOR infra/terraform.tfvars
nix run .#tfEditVars

nix run .#rekey
git add secret/*.age infra/terraform.tfvars.age keys.nix
git commit -m "ops: encrypted secrets for provisioning"
```

---

## Provision (Terraform)

Creates **both** prod and staging modules by default.

```bash
nix run .#tfInit
nix run .#tfPlan -- -target=module.staging   # staging only
nix run .#tfApply
git add infra/terraform.tfstate.age && git commit -m "ops: terraform state"
```

`tfApply` applies the plan file saved by `tfPlan`, so the `-target` scoping
carries over — the apply above touches staging only, it does not silently
provision prod. Repeat with `-target=module.prod` when ready for prod.

---

## Bootstrap

Requires TCP 22 on the DigitalOcean cloud firewall (for the Ubuntu image and
`nixos-anywhere`). The Terraform module opens it and it stays open post-deploy —
NixOS accepts operator/CI SSH on the public IP with key-only auth.

```bash
nix run .#bootstrap -- staging
nix run .#bootstrap -- prod
```

**Trust-on-first-use caveat:** the first SSH connection to the fresh droplet is
unauthenticated (`StrictHostKeyChecking=no` — DigitalOcean's API exposes no
host-key channel), so bootstrap trusts the key that answers first and then pins
it: the pre-install and post-reboot keys must match (a MITM during install
aborts before `keys.nix` is updated or secrets are rekeyed), and every later
deploy verifies against the pinned key. The residual risk is an attacker owning
the network path from the very first connection to a minutes-old droplet. The
only out-of-band check is manual and timing-sensitive: cloud-init's
`keys_to_console` module prints the host-key fingerprints to the droplet's
virtual console during **first boot only** (the web console has no scrollback
later), so to close the window, open the DO web console right after droplet
creation and compare fingerprints before running bootstrap. If that moment is
missed, the initial connection remains TOFU.

Updates `host-{env}` in `keys.nix`, rekeys secrets. Commit:

```bash
git add keys.nix secret/*.age
git commit -m "ops: bootstrap host keys + rekey"
```

---

## Deploy

The deploy scripts resolve the droplet IP from `infra/.remote-{env}.age` and pin
the host key from `keys.nix`; `DEPLOY_HOST=<ip>` overrides resolution. With
`DEPLOY_HOST` set, no `.remote-{env}.age` decryption happens, so
`SSH_IDENTITY_OP` is ignored — the 1Password SSH agent alone must cover the
connection.

```bash
nix run .#stagingDeployAll
nix run .#prodDeployAll
```

---

## Database (one-time, per environment)

The service opens SQLite with `create_if_missing = false` **on purpose**: a
missing file means a misconfigured path or an unmounted volume, and silently
starting a fresh database would be worse than failing. Deployment never creates
the database either — provisioning it is a deliberate manual step:

- **Fresh environment** (no history to preserve):

  ```bash
  nix run .#stagingRemote -- \
    "install -o st0x -g st0x -m 644 /dev/null /mnt/data/issuance.db"
  ```

- **Migrating from the legacy Docker deployment** (prod): copy the legacy
  `issuance.db` onto the data volume at `/mnt/data/issuance.db` and chown it
  `st0x:st0x` **before** the first service deploy. Do not create an empty file.

Until this is done the service crash-loops with SQLite error 14 ("unable to open
database file") — that is the fail-fast working as intended.
