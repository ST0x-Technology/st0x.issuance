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
nix run .#tfEditVars -- -i "$SSH_IDENTITY"
# or:
nix develop
tf-edit-vars -i "$SSH_IDENTITY"
```

Identity: `export SSH_IDENTITY=~/.ssh/id_ed25519`, or pass `-i`, or
`--op 'op://vault/item'`.

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
nix run .#tfEditVars -- -i "$SSH_IDENTITY"

nix run .#rekey -- -i "$SSH_IDENTITY"
git add secret/*.age infra/*.age infra/.remote-*.age keys.nix
git commit -m "ops: encrypted secrets for provisioning"
```

`rekey` refreshes service secrets, Terraform files, and remote IP caches from
their current roles in `keys.nix`.

---

## Provision (Terraform)

Creates **both** prod and staging modules by default.

```bash
nix run .#tfInit -- -i "$SSH_IDENTITY"
nix run .#tfPlan -- -i "$SSH_IDENTITY" -target=module.staging   # staging only
nix run .#tfApply -- -i "$SSH_IDENTITY"
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
nix run .#bootstrap -- -i "$SSH_IDENTITY" staging
nix run .#bootstrap -- -i "$SSH_IDENTITY" prod
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
the host key from `keys.nix`; `DEPLOY_HOST=<ip>` overrides resolution.

```bash
nix run .#stagingDeployAll -- -i "$SSH_IDENTITY"
nix run .#prodDeployAll -- -i "$SSH_IDENTITY"
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
