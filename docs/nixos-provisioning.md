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

**Two prerequisites, both outside the deploy scripts.** nginx requests a
Let's Encrypt certificate during system activation, so a first deploy fails
and rolls back unless each is in place. Neither is checked for you:

1. **DNS.** An A record for the environment's FQDN, pointing at its reserved
   IP, at the s01issuer.com registrar (GoDaddy). The names are in
   `nix/ingress.nix`: `issuance.s01issuer.com` for prod,
   `issuance-staging.s01issuer.com` for staging.
2. **Firewall.** `tfApply` for that environment, opening TCP 80 and 443 on the
   DigitalOcean firewall. `nix run .#tfPlan`/`.#tfApply` above; the deploy
   scripts never run Terraform, and CI's deploy workflow does not either.

The http-01 challenge is what needs port 80. If it cannot be reached, the
`acme-<fqdn>.service` unit fails, `switch-to-configuration` exits non-zero, and
deploy-rs rolls the generation back. Re-run the deploy once the record and the
firewall rule exist.

**Flipping `st0x.ingress.behindProxy` needs `DeployAll`, not `DeployNixos`.**
The system profile carries nginx and the firewall; only the service profile
restarts the app, and that unit sets `restartIfChanged = false`, so a
system-only deploy never moves the app into proxy mode. The app binds a
different port in each mode (8000 direct, 8001 proxied), so a half-applied
flip shows up as nginx returning 502 rather than as requests reaching the app
with a spoofable source address. Run `nix run .#<env>DeployAll` and let both
profiles land.

The flip does not strand plaintext callers. While `st0x.ingress.legacyPlaintext`
is on (the default), nginx also serves the same route allowlist over plain HTTP
on 8000, so Alpaca and the liquidity bots keep working on the old URL and can
move to the HTTPS name one at a time. Retiring plaintext is a separate step:
set `legacyPlaintext = false`, deploy, then drop the port-8000 rule in `infra/`.

### Proxy cutover, step by step

Each step leaves the environment in a state that serves every caller, so it
can pause for as long as needed between them. `<fqdn>` is the environment's
name from `nix/ingress.nix`.

1. **Certificate only.** With `behindProxy = false` (the default), run
   `DeployAll`. Check that the certificate was issued and that nginx is
   parked: `curl -sI https://<fqdn>/inkind/issuance` must answer `503` over a
   valid TLS handshake. The app still serves 8000 itself; nothing has changed
   for callers.
2. **Flip.** Set `behindProxy = true` and run `DeployAll` (never
   `DeployNixos`, see above). On the box, `ss -tlnp` must show the app on
   `127.0.0.1:8001` and nginx on `:443` and `:8000`.
3. **Verify the forwarded surface.** Over HTTPS, an allowlisted route with a
   bad key must answer `401` (the request reached the app), an unlisted route
   such as `/admin/stuck` must answer `403` from nginx, and
   `POST /tokenized-assets` must answer `403` while `GET` reaches the app.
   Repeat the same three checks against `http://<ip>:8000`: the plaintext
   listener is now nginx and must behave identically.
4. **Verify client-IP authorization.** From an address outside
   `INTERNAL_IP_RANGES`, `GET /tokenized-assets/<u>/status` with the valid key
   must answer `403` from the app, not `200`. A `200` means the app is reading
   the proxy's loopback address instead of `X-Real-IP`: roll back to step 1
   and check the `BEHIND_PROXY` value on the running unit
   (`systemctl show st0x-issuance -p ExecStart`).
5. **Move callers.** Point the liquidity bots' `base_url` at
   `https://<fqdn>`, then ask Alpaca to switch. Watch the nginx access log
   until nothing arrives on 8000 any more.
6. **Retire plaintext.** Set `legacyPlaintext = false`, run `DeployAll`, drop
   the port-8000 firewall rule in `infra/` and `tfApply`. `ss -tlnp` must no
   longer show `:8000`.

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
