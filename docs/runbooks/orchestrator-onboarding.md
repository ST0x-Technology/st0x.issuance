# Orchestrator onboarding and per-asset cutover (RAI-1221 / RAI-1222)

Two ordered procedures in one document. **Onboarding** (steps 1–6) is the ops
work that must be complete for an asset before its `vault_mode` can flip to
`"orchestrator"`: roles and the Turnkey policy are orchestrator-wide — done
once, before the pilot — while approvals are per asset (RKLB's before the pilot
cutover, each remaining asset's before its own). **Cutover** (steps 7–14) is the
per-asset procedure that actually moves an asset onto the orchestrator, authored
for the RKLB pilot (RAI-1222) and reused verbatim for every later asset
(RAI-1246).

Production runs on the GCP bot VM. The `PRODUCTION_RELEASES_ENABLED` repository
variable is on, and the droplet path is retired: `deploy-prod.yaml` refuses to
run, and turnkey-policy-spec retired the droplet's Turnkey user. Both procedures
are written for GCP (README "GCP release path"). The VM side (the deployment
hold, the release roll and the offline `issuer` verbs) follows the production
stack in S01-Issuer/s01.devops. The droplet's hold file
(`docs/runbooks/deploy-hold.md`) does not apply on GCP.

There is no testnet or staging chain for this: every step below runs against the
production chains and is verified by on-chain reads. The first live end-to-end
orchestrator mint/burn is the RKLB pilot's manual exercise (step 13), which
everything before it must fully precede. The full cutover cycle — migrate,
operate, roll back, resume — is rehearsed twice before prod. The Anvil
end-to-end suite (`tests/receipt_custody.rs`,
`test_receipt_custody_migrates_into_the_orchestrator`) runs it on contracts it
deploys itself. The fork rehearsal (`tests/fork_rehearsal.rs`) runs it on a
local Anvil fork of Base, against the real RKLB vault, receipts and
orchestrator. It is ignored by default; run it, as a gate before the pilot, with
a Base RPC that can serve state at the fork block. Anvil sends no JWT, so do not
use a bare Alchemy URL:

```
FORK_RPC_URL=<Base RPC> FORK_BLOCK=<recent block> \
  cargo test --test fork_rehearsal -- --ignored --nocapture
```

The fork signs nothing with a real key and changes nothing on Base. It does not
exercise Turnkey signing, the ops client or the `issuer` commands below, which
accept only the Turnkey signer. Step 7 gives the full gate command, with
`FORK_EMERGENCY_HOLDER`.

## Prerequisites

- Turnkey is the live signer (RAI-1123 done, Fireblocks retired). On GCP the bot
  authenticates its Turnkey requests through Cloud KMS (`TURNKEY_KMS_API_KEY`),
  and Turnkey signs with the wallet in `TURNKEY_ADDRESS`. Step 4's output names
  that wallet as `bot`.
- The orchestrator is deployed by st0x.deploy (PR #222/#223) and its address is
  known; the permissions scripts have run.
- The liquidity-bot counterpart (RAI-1243) is tracked separately — it gates the
  RAI-1222 cutover, not this procedure. Its release plumbing, once the issuance
  orchestrator stack merges to main: cut a `st0x-issuance-client` /
  `st0x-issuance-dto` release tag from main, change the liquidity repo's
  `st0x-issuance-client` and `st0x-issuance-dto` tag pins to that tag in both
  manifests that pin them (the root `Cargo.toml` and `crates/cli/Cargo.toml`),
  update `Cargo.lock` and the issuance `hash` in `rust.nix`, and deploy the
  liquidity bot from the new pins. Step 7's tag and pin pre-checks verify this
  happened. On GCP every `vX.Y.Z` tag is also a production release of the
  issuance bot: `release-tag.yml` deploys that commit's image and config, after
  the approvers approve the `app-deploy` grant. So cut this tag only where a
  production release of main is safe, never inside a cutover window.

  **The tag name is `vX.Y.Z` and it must be a NEW version.** The release
  workflow triggers only on `v[0-9]+.[0-9]+.[0-9]+`
  (`.github/workflows/release-tag.yml`), so an unprefixed tag silently publishes
  nothing. `v0.3.0` already exists — it is from June 2026 and is unrelated to
  this stack — so any "0.3.0" is both the wrong shape and a backwards version.
  Do not reuse it.

  **Read the current tip rather than trusting any version written here**, and
  sort by VERSION, not lexically — `v0.9.1` sorts after `v0.13.3` as plain text,
  which is exactly how a stale literal gets into this file:

  ```sh
  git fetch --tags origin && git tag -l 'v[0-9]*' --sort=-v:refname | head -1
  ```

  Pick the next version above that tip when the tag is cut, then set it once and
  reuse it everywhere below instead of pasting a literal:

  ```sh
  export RELEASE_TAG=vX.Y.Z    # replace with the tag actually cut; record it in the table
  ```

  Re-tagging an existing version is not a harmless retry. When the commit builds
  a different digest, the release refuses the tag ("a version never moves, cut a
  new version instead"). When it builds the same digest (only the same commit,
  or one that changes nothing but files the build ignores, such as Markdown,
  does), the release accepts the tag and deploys that commit's config like a new
  release. So always cut a new version.

  Record the value in the prod-facts table. A literal pasted into a check is how
  this drifted from reality the first time. `$RELEASE_TAG` names only this pin
  tag. The flip in step 12 gets its own new version, `$FLIP_TAG`.

## Operator access on GCP

The operator calls in this runbook use the S01 ops client against the IAP-gated
`/ops` API (README "Operator Client", SPEC "Operator client"). It runs on your
workstation and signs you in with your S01 Google account. Each tier (`read`,
`debug`, `capital`, `breakglass`) needs its Workspace group. Below, `ops` stands
for the client with `--env production`:

```sh
# On your workstation, in a checkout of this repo, with S01_ISSUANCE_PROD_URL,
# S01_ISSUANCE_PROD_CLIENT_ID and S01_ISSUANCE_PROD_CLIENT_SECRET set.
nix develop   # cargo and foundry; jq, gh, gcloud and bc must also be on PATH
# nix develop opens a new shell: define ops() and run the rest inside it.
ops() { cargo run -q -p st0x-issuance-ops -- --env production "$@"; }
```

The client prints each response as one JSON line, so pipe it into `jq`. It exits
0 on success, 2 on a setup error, 77 when the sign-in, IAP or your group
membership refuses the call, and 1 otherwise. The bot validates and decides
everything: the orchestrator address comes from the bot's own config, never from
the command line, and the bot signs with its own Turnkey wallet.

The offline `issuer` verbs (`move-receipts`, `confirm-custody`) have no `/ops`
route. They run on the production VM, in the bot's image, with the bot's own
environment, `CONFIG` and data disk. Without `--rpc-url`, they use the bot's own
RPC for `--network`, so do not pass one. `move-receipts` reads the bot's config
file (`confirm-custody` reads none). Both refuse a local-key signer. Their one
address argument, `move-receipts --to`, exists only for the wallet-rotation
path. It refuses the configured orchestrator address and any other contract (see
SPEC "Receipt custody").

The `cast` reads take one chain's RPC URL. Below, `$RPC_URL` stands for the RPC
of the chain you check: set it before each chain's reads
(`export RPC_URL=<that chain's RPC>`), with an RPC endpoint that you hold for
that chain. `cast` sends no JWT, so a bare Alchemy URL
(`https://<network>.g.alchemy.com/v2`) answers its reads with HTTP 401: use a
URL that needs no JWT. Each read block first checks `cast chain-id`. An RPC URL
can carry the provider's API key, and `cast` prints the URL in a connection
error, so do not paste that output anywhere. `<chain id>`, and the `--chain-id`
of the `issuer` verbs, come from this table (`Network::chain_id` in
`crates/dto/src/lib.rs`):

| Network     | Chain id |
| ----------- | -------- |
| `base`      | 8453     |
| `ethereum`  | 1        |
| `hyperevm`  | 999      |
| `robinhood` | 4663     |
| `binance`   | 56       |

The `git` and `gh` commands also run on your workstation, in a checkout of this
repo.

### Shell on the production VM

A shell on the VM exists only inside an approved PAM grant. A grant expires
after a fixed time, which the production stack's SSH access file in s01.devops
sets. Request it ahead, so that it is active from step 7 (the VM reads) until
the step 12 health check passes, and from rollback step 2 until rollback step 6
ends. Only a shell can release the hold. A grant belongs to the person who
requested it, and PAM refuses a second grant with the same scope while yours is
active or waiting. So another eligible person (the engineering group or an
approver) requests the next grant. That grant must be approved before yours
ends, and that person runs the VM steps from then on. Then open an interactive
session from your workstation, through IAP, with the `gcloud compute ssh`
command in that SSH access file.

The VM commands in this runbook use placeholders for the host's file paths, unit
names, compose service and roll settings: `<images env file>`, `<compose file>`,
`<config file>`, `<roll script>`, `<health URL variable>`, `<stack unit>`,
`<roll timer>`, `<roll service>` and `<bot service>`. Take their values from the
production stack in S01-Issuer/s01.devops and from the bot VM module that it
calls. The module builds the paths and unit names from the stack's VM name.

The VM holds production credentials. Do not run `docker inspect`,
`docker compose config` or `docker exec ... env`, and do not read the secrets
directory that holds `<config file>`: each of them prints the secrets env. The
one exception is the `sha256sum` of `<config file>` (steps 7 and 12, rollback
steps 2 and 6), which prints only a hash.

In that session, `issuer` stands for the `issuer` binary of the bot's image. It
runs as a one-off container of the bot service, with the same environment,
config file and data disk:

```sh
# On the VM. The compose file reads the image digest from the images env file.
issuer() {
  sudo docker compose --env-file <images env file> \
    -f <compose file> \
    run --rm --entrypoint /bin/issuer <bot service> "$@"
}
```

It runs the release that the VM adopted last (`<images env file>`), with the
config file that the last stack start fetched.

`move-receipts` and `confirm-custody` take the store lock for the whole command
(SPEC "Store lock"). The lock is `/mnt/data/issuance.db.lock`, next to the store
on the data disk. The bot takes the same lock when it starts, and holds it while
it runs. While the bot runs, the two verbs refuse with "another process holds
the store lock". While one of them runs, a bot start refuses with "Refusing to
start: cannot take the store lock" and exits. Docker then restarts the bot
container (`restart: unless-stopped`), so a bot that was started during the
command starts by itself as soon as the command ends. The lock ends with the
command, so only the hold below keeps the bot stopped between commands. Never
delete or move the lock file: a process that creates a new file at that path
does not see a lock on the old one. Before the lock, the two verbs read only the
environment, the signer settings and (for `move-receipts`) the config file, and
they open no database. So step 7 runs `move-receipts` as a probe while the bot
runs.

The bot's log, and the log of each `issuer` run, are in Cloud Logging
(s01.devops gives the log name). The log of the release roll is on the VM:
`sudo journalctl -u <roll service>`.

### Deployment hold on the VM

Three systemd units run the bot (the s01.devops production stack).
`<stack unit>` runs the docker compose stack. `<roll timer>` starts
`<roll service>` at a short interval. When a release has written a new
`images.env` to the config bucket, the roll adopts it and restarts the stack on
it. The hold disables the timer and the stack, so that no release, no reboot and
no timer tick starts the bot. A plain `stop` is not enough: after a reboot, an
enabled unit starts again.

The hold does not survive a Terraform replace of the VM: cloud-init enables and
starts both units again. A manual roll, a stack restart or a secrets rotation on
the VM also starts the bot, because `systemctl start` works on a disabled unit.
Before step 9 (and before rollback step 3), agree with devops that none of these
runs (no Terraform apply on the production stack, no VM replace, no manual roll,
no stack restart, no secrets rotation) until the bot runs again after step 12
(after rollback step 6). Also check on the VM that the roll has no health check.
The bot VM module writes the health URL into `<roll script>` when it creates the
VM, so read the script on the VM, not the repo (this must exit 0):

```sh
sudo grep -q -E "^[[:space:]]*<health URL variable>=''$" <roll script>
```

With a health check, a roll whose start takes longer than its budget goes back
to the previous `images.env`, which carries the config before the flip. If the
check fails, do not start the window: escalate to engineering and devops.

**Arm the hold** (on the VM). Disable the timer first and let a roll in progress
end, so that no roll stops between its `images.env` install and its stack
restart. The block first checks that the three unit names exist, so a mistyped
placeholder stops it:

```sh
systemctl cat <roll timer> <roll service> <stack unit> > /dev/null \
  && sudo systemctl disable --now <roll timer> \
  && while [ "$(systemctl is-active <roll service>)" = activating ]; do
       sleep 5
     done \
  && sudo systemctl disable --now <stack unit> \
  && RUNNING=$(sudo docker ps -q \
       --filter label=com.docker.compose.service=<bot service>) \
  && [ -z "$RUNNING" ] \
  && [ "$(systemctl is-enabled <roll timer>)" = disabled ] \
  && ! systemctl is-active --quiet <roll timer> \
  && [ "$(systemctl is-enabled <stack unit>)" = disabled ] \
  && ! systemctl is-active --quiet <stack unit> \
  && echo "OK: bot stopped, hold armed"
```

Without the `OK` line, a unit name is wrong, a bot container still runs or a
unit is still enabled: do not continue. While the hold is armed, a release still
writes `images.env` to the bucket, but the VM does not adopt it.

The hold stops the whole bot, for every asset, so the bot-down alerts fire
during the window (the s01.devops alert rules list them). Tell the on-call
before the window that these alerts are expected until the bot runs again.

Release the hold only when no `issuer` command runs. While one holds the store
lock, the bot container restarts in a loop, and it starts by itself as soon as
the command ends.

**Release the hold on the release that the VM runs.** Never do this after a
chain moved receipts in step 10 and before the step 12 roll: that start runs the
config before the flip while the orchestrator holds the receipts. This starts
the bot again only if no release waits in the bucket:

```sh
sudo gcloud -q storage cat gs://s01-issuance-config/images.env \
  | sudo cmp -s - <images env file> \
  && sudo systemctl enable --now <stack unit> <roll timer>
```

If `cmp` fails, the bucket read failed, a release ran while the hold was armed,
or someone replaced the bucket's `images.env` on purpose. The timer would adopt
that copy, so nothing was changed: the hold stays armed. Run this block again,
and if the copies still differ, find what wrote the bucket copy and escalate to
engineering.

**Release the hold onto a new release** (step 12, rollback step 6). The release
run writes `images.env`, then waits up to 15 minutes for the VM to adopt it.
During that wait, read the bucket's copy:

```sh
sudo gcloud -q storage cat gs://s01-issuance-config/images.env \
  | grep -E '^(BOT_IMAGE|CONFIG_VERSION|GATED_SERVICE_REPLICAS)='
```

`BOT_IMAGE` and `CONFIG_VERSION` must equal the lines in the last
`--- images.env ---` block that the run printed (it prints one block per write
attempt). `GATED_SERVICE_REPLICAS` must be `1`: it is the number of bot
containers the stack runs, and `0` keeps the bot stopped. If a line differs, do
not roll: keep the hold armed and the asset frozen, and escalate to engineering.
Then adopt the release and start the bot with one roll:

```sh
sudo systemctl start <roll service>
sudo journalctl -u <roll service> --since=-15min --no-pager \
  | grep -E 'rolled .* to|FATAL'
```

The journal must show the `rolled ... to` line with the same `BOT_IMAGE` and
`CONFIG_VERSION`. Record that line with the cutover (step 12 checks it again).
Only then enable the units again:

```sh
sudo systemctl enable --now <roll timer> <stack unit>
```

Without that `rolled` line, the roll did not adopt the release (some failures
print no `FATAL` line). Arm the hold again, keep the asset frozen, and escalate
to engineering. If the run's 15 minutes end before the roll, the run fails, but
the roll still adopts the release: the roll journal is the record. Every
production release, successful or failed, leaves its `app-deploy` grant active
for up to an hour (README "GCP release path"). A release inside that hour fails
at "Await PAM approval" until an approver revokes that grant (see step 12), so
keep an approver ready while the asset is frozen.

## 1. Ship the orchestrator addresses in the config (stays dark)

Add to `config.prod.toml`:

```toml
[orchestrator.addresses]
base = "0x…"   # from st0x.deploy; one entry per network — each chain has
               # its own orchestrator deployment
```

Do **not** add any `[assets.<SYM>]` section — with no `vault_mode` overrides
every asset stays vault-direct, so this deploys dark. Parsing is strict (unknown
keys, unknown network names, and malformed or zero addresses are startup errors,
even while dark), and once any asset resolves to orchestrator mode, startup
requires an entry for **every** configured chain. Every verification below runs
per network against that network's entry, and an asset's cutover (steps 7–14)
runs per chain it is listed on. Verify locally with
`cargo test --lib deploy_config_files_parse_and_stay_dark`, which parses both
deploy configs strictly (`validate-config` without `CONFIG` never reads the
file). Then merge it to main. On GCP, a merge that changes `config.prod.toml`
ships as a config-only release on the live image when nothing but configs and
Markdown changed since the commit production runs. Otherwise it ships with the
next version tag (README "GCP release path"). Each release waits until the
approvers approve the `app-deploy` PAM grant in the GCP console. The ops verbs
read the running bot's own config, and `move-receipts` reads the same file
through the bot's `CONFIG`, so no check ever uses a stray local copy.

## 2. On-chain roles (st0x.deploy coordination)

Confirm with the st0x.deploy owners that the bot's Turnkey wallet is granted
`MINT_ROLE` + `BURN_ROLE` on the orchestrator. This is the only role fact the
issuance bot needs — it never calls the emergency/admin functions. Separately,
the orchestrator itself must hold `DEPOSIT` and `WITHDRAW` on each vault's
authorizer (a deploy-side grant, per vault); step 6's preflight verifies these
alongside the bot's roles. `EMERGENCY_ROLE` / `DEFAULT_ADMIN_ROLE` holders are
st0x.deploy's governance call; record who holds `EMERGENCY_ROLE` on each chain
in the table below, because the rollback (step 14) and the shortfall escalation
(last section) page them. Step 7 checks each chain's grant on-chain.

Verification is step 6's preflight (`hasRole` reads) — no trust in the
deploy-side report is required.

## 3. Turnkey signing policy

Create (or extend) the Turnkey policy for the issuer wallet to allow:

| Allowance                               | Target contract               | Why                                                                                                                                                     |
| --------------------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `mint(...)`                             | orchestrator                  | orchestrator-mode mints                                                                                                                                 |
| `burn(...)`                             | orchestrator                  | orchestrator-mode burns                                                                                                                                 |
| `approve(...)`                          | each vault share token        | the one-time approval (step 5)                                                                                                                          |
| `safeBatchTransferFrom(...)` (ERC-1155) | each vault's receipt contract | receipt migration during cutover (RAI-1222) — the BATCH selector: every Turnkey-signed receipt move submits it, and a policy grants contract + selector |

These allowances replace the retired per-vault Fireblocks whitelist/TAP entries;
note they span three target kinds — the orchestrator (`mint`/`burn`), each vault
share token (`approve`), and each vault's receipt contract (the batch transfer)
— and they are **additions**: do not remove or narrow the policy shapes the
vault-direct path already signs under Turnkey (Fireblocks stays retired). During
the per-asset rollout BOTH mint/burn paths run in production simultaneously,
until RAI-1223 retires vault-direct mode.

The allowances above are P68, P69 and P70 in turnkey-policy-spec. Each allows a
request only from a Turnkey user that holds the `s01-orchestrator` tag. So grant
that tag to the issuance bot's signing user before step 4. The liquidity bot's
MintAuth signing (P67) is gated by the `orchestrator-mint-auth` tag. Grant that
tag to the liquidity bot's production signing user before the step 7 MintAuth
checks, both the allow side and the deny side. Never put both tags on one user:
the orchestrator needs two separate keys for a mint.

Each grant is a root-quorum change in turnkey-policy-spec. A reviewed PR adds
the tag to that user's `tags` in `constants/identities.json`. On merge, CI
submits the change, and the root quorum approves it. That private file holds the
exact user names and IDs, so this runbook names the users by role only. On GCP,
the issuance bot signs as the GCP issuance user, so that user gets
`s01-orchestrator`. After each grant, run `npm run audit-identities` from a
turnkey-policy-spec checkout. It must report no difference from
`constants/identities.json`, and no user may hold both tags.

Record the policy names and the granted tags in the table below. Their Turnkey
IDs stay in turnkey-policy-spec and on RAI-1221.

## 4. Prove the policy by signing (nothing broadcast)

```sh
ops debug verify-orchestrator-signing base RKLB | jq -e .
```

The bot signs one transaction per shape in the table above — never broadcasting
— and refuses with `HTTP 422` (client exit 1) if the policy denies one. → exit
0, and the response lists each shape and names the bot wallet as `bot`. `jq -e`
fails when the call printed nothing, so a refusal cannot pass. The 422 body is
generic: the bot's ERROR log line `Orchestrator signing proof failed` names the
refused shape, so search Cloud Logging for that message. Run it per asset as
each asset approaches cutover (the approve/transfer shapes are token-scoped). A
policy gap surfaces here as a refusal instead of during the pilot's first live
mint. It can pass only after the `s01-orchestrator` grant in step 3.

The optional `--mint-to <ADDRESS>` sets the recipient of the signed mint shape;
the response names it as `mint_to`. Without it, the shape names the bot wallet.
If the signing policy requires a specific mint recipient, pass that address here
and in step 7, or the proof fails. P68 in turnkey-policy-spec shows whether it
does.

## 5. Execute the approval (per asset, staged with the rollout)

```sh
ops capital approve-orchestrator base RKLB | jq -e .
```

One-time unlimited ERC-20 approval, bot wallet → orchestrator, on the asset's
vault share token, signed by Turnkey. Before sending, the bot verifies the
configured address answers as an orchestrator (interface reads plus a healthy
`vaultLogicIsExpected()`), so a typo'd or stale `[orchestrator.addresses]` entry
is refused rather than granted an unlimited allowance. Idempotent: a re-run
answers `already_unlimited` and sends nothing, so batching every asset's
approval early is safe — approvals are inert until the asset's `vault_mode`
flips. `approved` means the receipt succeeded and a re-read allowance is
unlimited. When this step actually submits, the transaction is also live proof
that the policy's `approve` allowance works; the idempotent no-op path proves
nothing new — step 4's signing proof covers `approve` in that case.

The client exits 0 on every one of these answers, so read `outcome`, not the
exit code (SPEC "Role-gated operator API"). On `submitted_unconfirmed`, the
approval was signed and broadcast, but not confirmed within the bot's 18-second
deadline. Look `tx_hash` up on the explorer before doing anything else: once it
lands, a re-run sends nothing. An explorer that does not know the hash proves
nothing, because the transaction can still wait in a node's pool. So also read
the bot wallet's nonce on that chain, with
`cast nonce <bot wallet> --block pending --rpc-url "$RPC_URL"` and again with
`--block latest`. Re-run only when, a few minutes later, the explorer still does
not know the hash and the two nonces are equal: then the approval was dropped.
If it stays pending without mining, it holds the wallet's nonce and later mints
and burns on that network queue behind it until it mines or that nonce is
replaced through Turnkey; re-running the approval only adds another transaction
behind it.

A refusal (404, 409, 422, 500, 502 or 503) prints no `outcome`: the client exits
non-zero and explains the status on stderr, and `jq -e` fails. A 409 means that
a mint or burn on that network holds a signed nonce: nothing was sent, so run it
again after that work settles. A 503 sent nothing. A 502 can come after the
broadcast (the approval reverted or did not take effect), so check the explorer
before a re-run.

Record each executed approval in the table below.

## 6. Final gate: preflight must report ready

```sh
ops read orchestrator-preflight base --asset RKLB | jq -e '.ready'
```

On-chain read-only. Checks `hasRole(MINT_ROLE, bot)`, `hasRole(BURN_ROLE, bot)`,
`vaultLogicIsExpected()`, and per asset `allowance(bot, orchestrator)` plus the
orchestrator's `DEPOSIT`/`WITHDRAW` grants on the vault's authorizer. The two
scopes serve different moments: `--asset <SYM>` is the explicit per-asset
cutover gate — it works while the asset is still vault-direct in config, which
is exactly when the RAI-1222 pre-check runs; omitting `--asset` is the aggregate
sweep over the assets whose configured `vault_mode` already resolves to
orchestrator (a standing health re-check, not a cutover gate). The bot answers
200 with the report even when a check fails, so the gate is the report's `ready`
field: `jq -e '.ready'` exits 0 only when every check passes. The RAI-1222
pre-checks gate on that exit code for the asset being cut over.

## Cutover scope: one symbol, every chain it is listed on

`vault_mode` lives in `[assets.<SYM>]` and is keyed by SYMBOL, not by
`(symbol, network)`. Flipping RKLB flips it on **every** chain RKLB is listed on
— including the ones carrying zero supply. A chain that is missing its approval,
its Turnkey policy scope or its role grants does not stay on the old path; it
breaks. Startup enforces the same thing from the other side: once any asset
resolves to orchestrator mode, an entry is required for every configured chain
(step 1).

**Derive the chain set; never assume it.** The set is the one fact in this
document guaranteed to drift: a listing added after this file was written, or a
chain group enabled later, is covered by the flip while a hardcoded list still
names the old chains. The service answers authoritatively, before the flip, from
the live listing view (`list_enabled_assets` in `src/tokenized_asset/view.rs` —
one row per `(underlying, network)` listing, which `read orchestrator-health`
emits regardless of mode). Make this the first per-chain action and record its
output with the cutover:

```sh
ops read orchestrator-health \
  | jq -er '.assets[] | select(.underlying == "<SYM>") | "\(.network) \(.vault)"'
```

It prints each chain with its vault address, which the reads below use. `jq -e`
fails when nothing prints, so a failed call cannot pass as an empty set.

At the time of writing this prints `base`, `ethereum`, `hyperevm` and
`robinhood` for RKLB, and the per-chain rows in the tables below are a copy of
that output, not a fixed list — add a row for every network the command prints.
Robinhood is easy to forget and is in the set: RKLB has a vault there
(`docs/runbooks/robinhood-vault-registration.md`), and `config.prod.toml`
watches `wtRKLB` under `[wrapped_tokens.robinhood]`, which startup rejects
unless the `CHAIN_ROBINHOOD_*` group is configured — so that chain is running.
BNB Smart Chain (`binance`) has an `[orchestrator.addresses]` entry only because
startup demands one per configured chain group (step 1), not because the symbol
is listed there; it is in the cutover set only if the command above prints it.

So read steps 4–14 with this split. The commands below are written for Base;
where a step is per chain, run it once per chain the symbol is listed on, with
that chain's network (for the offline `issuer` verbs also its `--chain-id`, and
for the `cast` reads its RPC URL), and record each chain's output separately.

| Step                                     | Scope                                                |
| ---------------------------------------- | ---------------------------------------------------- |
| 4 signing proof, 5 approval, 6 preflight | **per chain**                                        |
| 7 pre-checks                             | **per chain**, except the deployment-wide ones       |
| 8 freeze and drain                       | per symbol — one freeze covers every listing         |
| 9 snapshot, 10 move, 11 verify           | **per chain**                                        |
| 12 flip                                  | per symbol — one config edit, one release            |
| 12 post-flip health verification         | **per chain**                                        |
| 13 validation                            | **per chain** that carries real flow                 |
| 14 rollback                              | per symbol to flip, **per chain** to return receipts |

Freeze, unfreeze and status address the `Underlying` aggregate and take no
network argument by design, so they need no repetition. Everything that names a
vault, a receipt contract or an orchestrator address does.

**Zero-supply chains.** A chain the symbol is listed on but that carries no
supply is still in the flip — steps 4–7 and the post-flip health check still run
for it — but there is nothing to move. The symbol's chain set is every network
where it is listed. Confirm it for every network in `[orchestrator.addresses]`
(the loop below names the five that the config has today): the client exits 0
for a listing, and it exits 1 and names the `404` for a chain without one. Any
other error is a failed call, not a missing listing: run it again.

```sh
for NETWORK in base ethereum hyperevm robinhood binance; do
  if ops debug tokenized-asset <SYM> --network "$NETWORK" >/dev/null; then
    echo "$NETWORK: listed"
  else
    echo "$NETWORK: not listed, or the call failed (read the client's error)"
  fi
done
```

An empty chain has nothing to snapshot or move, so skip the step-9 snapshot and
steps 10–11 for it. The step-9 deployment hold is deployment-wide and still
applies, and step 9 scans each empty chain again after the hold, and runs the
quiescence check when no chain goes through step 10. Nothing else is needed: the
cutover records no custody on any chain (step 10), so an empty chain after the
flip behaves like a chain whose receipts moved: its inventory stays empty.

Step 10 refuses an empty chain rather than "verifying" a move on two zero
readings: `move-receipts` refuses before its prompt when inventory tracks no
receipts for the vault ("nothing to move"), and the engine refuses again
(`InventoryEmpty`) when no tracked receipt has a balance
(`src/receipt_inventory/migration.rs`). Neither refusal can tell a legitimately
empty vault from an inventory that was never backfilled — so check each listed
chain before the window, on-chain, not by assumption. Every receipt id the vault
ever issued lies in `1..=highwaterId()`: the vault gives each new deposit the id
`highwaterId + 1`, and `redeposit` refuses an id above it. Read the bot wallet's
balance of every id in that range:

```sh
(
  set -euo pipefail
  [ "$(cast chain-id --rpc-url "$RPC_URL")" = "<chain id>" ] \
    || { echo "FAIL: RPC is not chain <chain id>" >&2; exit 1; }
  RECEIPT=$(cast call <vault> 'receipt()(address)' --rpc-url "$RPC_URL")
  # `cut` drops the `[1e4]`-style suffix cast prints on round numbers.
  HIGHWATER=$(cast call <vault> 'highwaterId()(uint256)' --rpc-url "$RPC_URL" \
    | cut -d' ' -f1)
  [[ "$HIGHWATER" =~ ^[0-9]+$ ]] \
    || { echo "FAIL: highwaterId is '$HIGHWATER'" >&2; exit 1; }
  NONZERO=0
  for ((ID = 1; ID <= HIGHWATER; ID++)); do
    BAL=$(cast call "$RECEIPT" 'balanceOf(address,uint256)(uint256)' \
      <bot wallet> "$ID" --rpc-url "$RPC_URL" | cut -d' ' -f1)
    [ "$BAL" = 0 ] || { echo "$ID: $BAL"; NONZERO=$((NONZERO + 1)); }
  done
  echo "OK: read all $HIGHWATER ids, $NONZERO non-zero"
)
```

Run it in your workstation shell, with `<vault>` from the chain-set response,
`<bot wallet>` as the `bot` field of step 4 or 6, and the RPC URL of the chain
you check. It prints each non-zero id with its amount, and its last line gives
the verdict. Without the `OK` line, a call failed or the RPC serves another
chain, even when no row was printed.

The chain is legitimately empty only when the last line is
`OK: read all N ids, 0 non-zero` AND the vault's share supply is zero:
`cast call <vault> 'totalSupply()(uint256)' --rpc-url "$RPC_URL"` → `0`, on the
same `$RPC_URL` that the loop checked with `cast chain-id`. Every deposit mints
its shares and its receipt in the same amount, so a non-zero supply with no
receipt at the bot wallet means a wrong address or receipts held elsewhere: stop
and escalate to engineering. For an empty chain, record `no receipts` in its
receipt-move, snapshot and verification columns, and skip the step-9 snapshot
and steps 10–11 for it. A non-zero count means the chain has receipts, and it
goes through steps 9–11 like any other chain. If `move-receipts` then refuses
with "nothing to move" or `InventoryEmpty` on such a chain, the inventory is
behind the chain. If no chain has moved yet, leave the window as in the step-10
detour (release the hold, start the bot again, unfreeze), then escalate to
engineering. If any chain has moved, keep the hold armed and escalate.

If the per-`(symbol, network)` override is ever built, this section is what
changes: cutover would then target one chain at a time and the table collapses
to "per chain" throughout.

## 7. Cutover pre-checks (gate on exit codes, not eyeballs)

All of these must hold for the asset being cut over, immediately before its
window. Every check is a command with an expected exit status, except two manual
reviews (the domain and implementation-hash comparison, and the deny-side
Turnkey policy review), the deny-side probes, which expect denied activities,
the live tag lookup, which expects a version line, the config hash check, which
compares two hashes, and the VM probe, which expects one refusal message. Record
each command's output (with any RPC URL masked), and the result of each review,
with the cutover. Repeat the per-chain checks for each chain in the derived set;
only the `PRODUCTION_RELEASES_ENABLED` check, the live tag, main and config hash
checks, the fork rehearsal, the stuck and mode reads, the deny-side Turnkey
review and probes, and the liquidity bot's deployed-revision, tag/pin and
config-address checks are deployment-wide and run once. The allow side of the
Turnkey MintAuth check is per chain: the orchestrator's EIP-712 domain carries
the id of the chain it runs on (see the policy bullet).

- The configured orchestrator on this chain is the deployment
  `config.prod.toml`'s comment claims. Preflight and `approve-orchestrator` pass
  against ANY orchestrator-shaped contract at the pinned address, so the domain
  read and the implementation hash are the only checks that catch a different
  deployment on one chain.

  The orchestrator address is a BeaconProxy on every chain. Its own code is only
  the proxy stub, with the beacon address inside it. A hash of that code stays
  the same on every chain even when one chain's beacon points to different
  logic. The beacons also have different owners (a timelock on Base, another
  timelock on the other chains), so they can diverge. Hash the code the beacon
  points to, not the proxy code:

  ```sh
  (
    set -euo pipefail
    [ "$(cast chain-id --rpc-url "$RPC_URL")" = "<chain id>" ] \
      || { echo "FAIL: RPC is not chain <chain id>" >&2; exit 1; }
    cast call <orchestrator address> \
      'eip712Domain()(bytes1,string,string,uint256,address,bytes32,uint256[])' \
      --rpc-url "$RPC_URL"
    # ERC-1967 beacon slot: keccak256("eip1967.proxy.beacon") - 1.
    SLOT=$(cast storage <orchestrator address> \
      0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50 \
      --rpc-url "$RPC_URL")
    BEACON=$(cast parse-bytes32-address "$SLOT")
    IMPL=$(cast call "$BEACON" 'implementation()(address)' --rpc-url "$RPC_URL")
    cast call "$BEACON" 'owner()(address)' --rpc-url "$RPC_URL"
    CODE=$(cast code "$IMPL" --rpc-url "$RPC_URL")
    [ "$CODE" != 0x ] || { echo "FAIL: no code at $IMPL" >&2; exit 1; }
    echo "beacon $BEACON implementation $IMPL hash $(cast keccak "$CODE")"
  )
  ```

  A failed read stops the block with an error. Without the last line, do not
  compare hashes: the hash of empty code is the same on every chain.

  → name `ST0xOrchestrator`, version `1`, `chainId` equal to this chain's id,
  `verifyingContract` equal to the configured address; and the implementation
  code hash equal on every chain in the set. Record the domain, the beacon
  address, the beacon owner and the implementation hash in this chain's
  prod-facts row. A different implementation hash on one chain is a stop: that
  chain runs different orchestrator logic.
- Step 6's preflight reports ready for this asset:
  `ops read orchestrator-preflight <network> --asset <SYM> | jq -e '.ready'` →
  exit 0.
- Step 4's signing proof passes for this asset (it covers the ERC-1155
  `safeBatchTransferFrom` shape the receipt move, step 10, submits):
  `ops debug verify-orchestrator-signing <network> <SYM>` → exit 0. If step 4
  used `--mint-to <ADDRESS>`, pass the same flag here.
- The `EMERGENCY_ROLE` holder recorded for this chain holds the role on this
  chain's orchestrator. Each chain's orchestrator has its own role state, and
  rollback step 4 pages this holder. Preflight does not read this role:

  ```sh
  [ "$(cast chain-id --rpc-url "$RPC_URL")" = "<chain id>" ] \
    && ROLE=$(cast call <orchestrator address> 'EMERGENCY_ROLE()(bytes32)' \
      --rpc-url "$RPC_URL") \
    && [ "$(cast call <orchestrator address> 'hasRole(bytes32,address)(bool)' \
      "$ROLE" <EMERGENCY_ROLE holder> --rpc-url "$RPC_URL")" = true ]
  ```

  → exit 0.
- `PRODUCTION_RELEASES_ENABLED` is on, so production runs on GCP, which this
  procedure targets (see the top of this document). A failed `gh` call fails the
  check:

  ```sh
  gh variable list --repo ST0x-Technology/st0x.issuance --json name,value \
    | jq -e 'any(.[]; .name == "PRODUCTION_RELEASES_ENABLED"
              and (.value | ascii_downcase) == "true")'
  ```

  → exit 0 (the release workflows compare the value with `'true'`, and GitHub
  expressions ignore case). If it fails, production is not on GCP, and this
  runbook does not apply: stop.
- The version tag that production runs is known (`<live tag>` below). On the VM,
  read the live image digest:
  `sudo sed -n 's/^BOT_IMAGE=.*@//p' <images env file>`. Then find the release
  run that wrote that digest. A run counts even when it failed after it wrote
  `images.env`. Config releases on main keep the image, so the script skips
  them:

  ```sh
  # On your workstation.
  DIGEST=<sha256:... from the VM>
  REPO=ST0x-Technology/st0x.issuance
  for WF in release-tag.yml production-release.yml; do
    gh run list -R "$REPO" --workflow "$WF" --limit 10 \
        --json databaseId,headBranch,event \
        -q '.[] | select(.event != "push" or .headBranch != "main")
               | "\(.databaseId) \(.headBranch)"' \
      | while read -r ID BRANCH; do
          LOG=$(gh run view -R "$REPO" "$ID" --log 2>/dev/null) \
            || { echo "$WF run $ID: LOG NOT READABLE"; continue; }
          grep -q "BOT_IMAGE=.*@$DIGEST" <<<"$LOG" || continue
          VERSION=$(grep -o -m1 'VERSION: v[0-9.]*' <<<"$LOG" | cut -d' ' -f2)
          echo "$WF run $ID: ${VERSION:-$BRANCH}"
        done
  done
  ```

  → at least one line that names a version: that version is `<live tag>`. Record
  it with the cutover. Two versions can name the same image (a tag on a commit
  that changes no built file builds the same digest): then use the highest
  version. A run that is still in progress, was cancelled before any job, or is
  past log retention prints `LOG NOT READABLE`; read its state with
  `gh run view <ID> -R "$REPO" --json status,conclusion,jobs`. A failed
  `gh run list` prints no run line (only an error), so run the block again
  before you read "no output" as "no match". If no line names a version, raise
  `--limit`. If an unexplained `LOG NOT READABLE` line remains, or the digest
  came from a Terraform pin with no release run, stop and escalate to
  engineering: never guess the tag.
- Main has no code since `<live tag>`, because the step 12 tag ships everything
  on main up to the flip:

  ```sh
  REPO=ST0x-Technology/st0x.issuance
  gh api "repos/$REPO/compare/<live tag>...main" > /tmp/compare-live.json \
    && jq -e '.status == "ahead" or .status == "identical"' \
         /tmp/compare-live.json > /dev/null \
    && ! jq -r '.files[].filename' /tmp/compare-live.json \
         | grep -v -x -E 'config\.(prod|staging)\.toml|.*\.md'
  ```

  → exit 0, with no output. A listed file is code that the step 12 tag would
  ship inside the window: release it on its own first, then start step 7 again,
  because that release changes the code production runs. A status other than
  `ahead` or `identical` means `<live tag>` is not on main's line: stop and
  escalate to engineering. From this check until the step 12 release runs on the
  VM, merge nothing else to main. A config merge starts a release that the hold
  blocks, and its open grant then blocks the step 12 release. A code merge would
  ship in the step 12 tag.
- Production runs main's config, so the step 12 tag ships no config change but
  the flip. On the VM, hash the config file that the bot runs (`sha256sum`
  prints only a hash, and this file is the config, not the secrets env):

  ```sh
  sudo sha256sum <config file>
  ```

  On your workstation:

  ```sh
  git fetch -q origin main && git show origin/main:config.prod.toml | sha256sum
  ```

  → the two hashes are equal. If they differ, production does not run main's
  config: a config release was denied, failed or is still running, or production
  runs a rollback to an older tag. Find which, bring production to main's config
  (a revert on main starts its own config release, whose grant must close before
  step 12), and start step 7 again. Step 12 checks this once more before the
  tag.
- A VM shell grant covers the window, and the offline `issuer` runs on the VM
  with this chain's settings. In a VM session ("Shell on the production VM"),
  while the bot runs:

  ```sh
  [ -n "$(sudo docker ps -q \
      --filter label=com.docker.compose.service=<bot service> \
      --filter label=com.docker.compose.oneoff=False)" ] \
    && sudo docker compose --env-file <images env file> \
         -f <compose file> \
         run --rm -T --entrypoint /bin/issuer <bot service> \
         move-receipts <SYM> --to-configured-orchestrator \
         --network <network> --chain-id <chain id> </dev/null
  ```

  → it refuses with "another process holds the store lock": the running bot
  holds the lock. The probe reads the image, the environment, this chain's
  `[orchestrator.addresses]` entry and the Turnkey signer settings first. That
  refusal comes next, before any database or chain access. An image without the
  store lock refuses with "deployment hold is not armed" instead: production
  does not run main's code, so stop. The probe runs without a terminal (`-T`)
  and with no input (`</dev/null`). So it can never confirm a move: at the
  prompt, the empty input declines. If it ends with "aborted by operator", it
  reached the prompt, so the bot does not hold the lock (for example, a bot
  container that restarts in a loop): stop. Any other error is a stop. No output
  means that no bot container runs (or `docker ps` failed): find out why before
  you continue.
- On a chain that goes through step 10, the bot wallet can pay for the first
  batch of the move. `move-receipts` refuses before its prompt unless the wallet
  holds the current gas price times 3,000,000 wei (`verify_gas_readiness`,
  `src/tokenized_asset/cli.rs`). The check asks for twice that, because the gas
  price moves. A move of more than 14 receipts sends more transactions; if one
  runs out of gas, fund the wallet and run the move again (it resumes):

  ```sh
  (
    set -euo pipefail
    [ "$(cast chain-id --rpc-url "$RPC_URL")" = "<chain id>" ] \
      || { echo "FAIL: RPC is not chain <chain id>" >&2; exit 1; }
    PRICE=$(cast gas-price --rpc-url "$RPC_URL")
    BAL=$(cast balance <bot wallet> --rpc-url "$RPC_URL")
    [ "$(echo "$BAL >= 2 * $PRICE * 3000000" | bc)" = 1 ]
  )
  ```

  → exit 0. If it fails, fund the bot wallet on that chain before the window.
- For the RKLB pilot, the fork rehearsal passes on the code that production
  runs. It covers RKLB on Base only, so it runs once, not per chain. On your
  workstation, use a checkout of `<live tag>` (above), with no local changes.
  Set `FORK_EMERGENCY_HOLDER` to the `EMERGENCY_ROLE` holder recorded for Base,
  so the rollback runs as that holder. Set `FORK_BLOCK` to a Base block from the
  day of the window. Anvil sends no JWT, so `FORK_RPC_URL` must not be a bare
  Alchemy URL:

  ```sh
  FORK_RPC_URL=<Base RPC> FORK_BLOCK=<Base block from today> \
    FORK_EMERGENCY_HOLDER=<EMERGENCY_ROLE holder> \
    cargo test --test fork_rehearsal -- --ignored --nocapture
  ```

  → exit 0, and the `rollback holder` line of the record names that holder. A
  record that names the stand-in wallet fails this check. Record the commit and
  the printed `--- fork rehearsal record ---` lines (they include the fork block
  and the holder) with the cutover.
- No stuck mints or redemptions for this asset:

  ```sh
  ops read stuck \
    | jq -e --arg sym <SYM> \
        '[.stuck[] | select(.underlying == $sym or .underlying == null)]
         | length == 0'
  ```

  → exit 0 (`jq -e` fails the check if any entry names the asset, or names no
  asset: a `Failed` redemption whose history the bot could not read has no
  `underlying`).
- The asset still reads vault-direct pre-flip on every chain. The status
  endpoint the liquidity bot polls reads the same config; operators cannot call
  that endpoint on GCP:

  ```sh
  ops read orchestrator-health \
    | jq -e --arg sym <SYM> \
        '[.assets[] | select(.underlying == $sym) | .vault_mode]
         | length > 0 and all(. == "vault_direct")'
  ```

  → exit 0.
- The liquidity bot is deployed with MintAuthV1 delivery live (RAI-1243), each
  fact checked in that repo's checkout at the DEPLOYED revision:
  - The deployed revision contains RAI-1243:
    `git merge-base --is-ancestor <rai-1243-merge-commit> <deployed-rev>` →
    exit 0.
  - The tag exists on the issuance REMOTE before anything is checked against it
    — a pin matching a tag nobody cut would otherwise pass the greps below. The
    remote is what the liquidity bot's git pin resolves against, so a local
    clone proves nothing either way: a tag created locally and never pushed is
    exactly "a tag nobody cut", and a clone made before the tag was cut fails
    although the release exists:

    ```sh
    git ls-remote --exit-code --tags \
      https://github.com/ST0x-Technology/st0x.issuance.git "refs/tags/$RELEASE_TAG"
    ```

    → exit 0.
  - BOTH issuance pins are on `$RELEASE_TAG`, and neither is on a branch, in
    both manifests that pin them — one check per dependency and per file, so a
    single correct pin cannot vouch for another:

    ```sh
    for MANIFEST in Cargo.toml crates/cli/Cargo.toml; do
      grep -q -E "st0x-issuance-client.*tag *= *\"$RELEASE_TAG\"" "$MANIFEST" \
        && grep -q -E "st0x-issuance-dto.*tag *= *\"$RELEASE_TAG\"" "$MANIFEST" \
        && ! grep -q -E 'st0x-issuance-(client|dto).*branch' "$MANIFEST" \
        && echo "OK: $MANIFEST" \
        || echo "FAIL: $MANIFEST"
    done
    ```

    → `OK: Cargo.toml` and `OK: crates/cli/Cargo.toml`. (Double quotes, so
    `$RELEASE_TAG` expands — single quotes would search for the literal variable
    name and fail closed.)
  - Its deployed plaintext config carries the orchestrator section AND the exact
    deployed orchestrator address from step 1 (the section existing with a stale
    address would sign MintAuths for the wrong contract):
    `grep -F '[orchestrator' <deployed config.toml>` → exit 0, and
    `grep -iF '<orchestrator address>' <deployed config.toml>` → exit 0.
  - Its Turnkey policy allows EIP-712 typed-data signing scoped via
    `eth.eip_712` conditions — `primary_type == "MintAuth"` (the EIP-712 struct
    name the orchestrator hashes; `MintAuthV1` is the delivery payload's wire
    label, NOT the struct — a policy conditioned on it would never match a real
    signing request), `domain.verifying_contract == <orchestrator address>`
    written as lowercase `0x` hex (the payload serializes addresses lowercase,
    so a checksummed literal can fail a string comparison; use the same
    lowercase form in the cutover evidence), and a `domain.chain_id` condition
    that admits EVERY chain in the derived set. That last condition is why this
    bullet is per chain: `eip712Domain()` answers each chain's own `chainId`
    (the domain check above), so a policy verified against one chain's id does
    not vouch for signing on the others, and a MintAuth for a mint on a chain
    the policy does not admit is refused — that mint then sits in `Minting`
    holding journaled AP shares, the exact case that "Mint waiting on an
    authorization that never arrived" covers. Turnkey has no separate typed-data
    activity type: EIP-712 signing arrives as
    `ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2` with `PAYLOAD_ENCODING_EIP712`, so that
    activity type necessarily appears in the policy — but because policies are
    default-deny and the `eth.eip_712` conditions are unset for a bare-digest
    (hexadecimal-encoded) request, this grant does NOT permit raw digest
    signing: anything that is not an EIP-712 payload with exactly our domain and
    struct is refused. Two checks, one per side of that claim. Run both while
    the liquidity bot's production signing user holds `orchestrator-mint-auth`
    (step 3): P67 allows only a holder of that tag, so without it Turnkey
    refuses the allow side too, and the deny side proves nothing:
    - **Allow side** — the scoped grant admits our exact payload; executable
      proof against prod Turnkey, needed **once per chain in the set** with that
      chain's id in the payload's `chainId`. In a st0x.liquidity checkout,
      inside its `nix develop` shell (the build needs its ABI paths, and the
      test starts `anvil`):

      ```sh
      ST0X_ORCHESTRATOR_ADDRESS=<orchestrator address> \
        cargo test -p st0x-evm --features turnkey,local-signer \
          turnkey_typed_signing_integration -- --ignored
      ```

      → exit 0 and `1 passed`. The test also reads `TURNKEY_API_PRIVATE_KEY`,
      `TURNKEY_ORG_ID` and `TURNKEY_ADDRESS`, and fails without them. Without
      `-p st0x-evm`, cargo refuses the features. Without both `-p` and
      `--features`, cargo tests only the root package, runs no Turnkey test and
      still exits 0. With `turnkey` alone, the test target does not compile (an
      unused import fails the build under the workspace's `warnings = "deny"`).
      The test only exercises the permitted path — an overly broad policy would
      also pass it, which is why the deny side needs its own check.

      **Open before the pilot:** the test fixes chain 8453 in its payload, so it
      proves Base only, and it authenticates with `TURNKEY_API_PRIVATE_KEY` (a
      stored API key), while the production signing user authenticates through
      Cloud KMS. Before the window, the liquidity owners (RAI-1243) make it take
      the chain id and run as the production signing user, or engineering
      decides what stands in for it on each chain. Record that decision with the
      cutover.
    - **Deny side** — default-deny only holds if no OTHER policy can approve
      signing for the liquidity bot's signing wallet. Two parts:
      1. Review EVERY effective allow policy in the org that names that wallet,
         its private key, or any of their tags — conditional policies included
         (a condition that a bare-digest request can satisfy is a grant,
         "unconditioned" is not the bar), across all policy versions and any
         categorical signing allowances. Confirm the scoped MintAuth policy
         above is the ONLY one whose condition a raw-payload signing activity
         for this wallet can satisfy — checking the whole activity family, not
         just `ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2`: earlier raw-payload versions
         and the batch `SIGN_RAW_PAYLOADS` variants are the same signing
         surface. Record the reviewed policy IDs with the cutover record.
      2. Executable negative probes, each submitted for the liquidity bot's
         signing wallet as its production signing user via Turnkey's API/SDK
         (the same open item as the allow side), and each of which must be
         DENIED by policy (nothing is broadcast even if signed; a signature
         would only prove the policy gap). Do not use the liquidity repo's
         `turnkey_typed_signing_policy_denials_integration` as written: its
         foreign-chain probe signs even under a correct policy, so it does not
         prove the `chain_id` bound, and its signature does not start the tag
         removal below. Record every denied activity ID with the cutover; ANY
         signed result is a cutover blocker — some policy grants more than the
         scoped MintAuth shape:
         - Bare digest: `ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2` with
           `PAYLOAD_ENCODING_HEXADECIMAL`, `HASH_FUNCTION_NO_OP`, any 32-byte
           payload — proves no raw-digest grant exists.
         - Three `PAYLOAD_ENCODING_EIP712` payloads, each the real MintAuth
           payload with exactly ONE fact wrong, so each probe isolates one
           policy condition (a policy that omits a condition passes the allow
           test and the bare-digest probe, yet signs unintended requests — these
           catch that):
           - wrong `primaryType` (rename the struct, e.g. `NotMintAuth`) —
             proves the `primary_type` condition exists;
           - correct struct, `verifyingContract` set to any other address —
             proves the `verifying_contract` condition exists;
           - correct struct and contract, `chainId` set to a chain that P67 does
             not admit (turnkey-policy-spec lists the admitted chains), for
             example 31337 — proves the `chain_id` condition exists and is
             bounded. (A chain that P67 admits signs, so probing with one of
             them proves nothing, even when it is outside the RKLB set.)

         The review in part 1 must confirm every condition of the deployed P67
         against turnkey-policy-spec.

         If a probe signs, remove `orchestrator-mint-auth` before any other
         step: a root user runs `npm run remove-tag <tag ID>` from a
         turnkey-policy-spec checkout, with root API credentials. It removes
         that tag from every user that holds it, and keeps their other tags. The
         change needs the root quorum: enough other roots to meet the quorum
         approve it in the dashboard, and until then the tag still grants. The
         script prints "OK" even while the change waits, so its output is not
         proof. Confirm the removal with `npm run audit-identities`: no
         `pending-activities` line fails, and the expected FAIL lines name the
         removal itself (`tag-holders` reports
         `"orchestrator-mint-auth" held by []`, and `user-tags` reports the
         difference from the file). Those lines stay, and the audit exits 1,
         until the PR below merges. Then open a PR that removes the tag from
         that user in `constants/identities.json`: CI applies that file on every
         deploy, so without the PR the tag comes back. Then find the policy that
         allowed the probe. If a probe waits in `CONSENSUS_NEEDED`, another
         policy matched it: reject that activity at once, then find that policy.

## 8. Freeze and drain

Freeze the asset (`ops capital freeze <SYM>`; `ops read status <SYM>` then reads
`frozen`): `POST /inkind/issuance` rejects frozen assets before Alpaca journals
shares, so nothing gets stranded mid-flow. Wait for this asset's in-flight mints
and redemptions to reach terminal states (re-check step 7's stuck read). The
freeze-plus-drain is also what guarantees no aggregate straddles the mode flip —
an operation's mode anchors once, at `Initiated` / `RedemptionDetected`.

The stuck read does not prove the drain: it hides in-progress work younger than
one hour, and redemptions that the freeze holds. The quiescence gate of
`move-receipts` in step 10 proves it.

## 9. Deploy hold and snapshot

Have the step 12 flip ready first: a reviewed and approved PR to main that
changes only `config.prod.toml` and the dark test, not merged yet. The test
change is code, so the flip ships with a version tag, not as a config-only
release (`production-release.yml` skips a config-only release when any other
file changed). The merge freeze from step 7 still holds, so the tag ships no
other code. Have this preparation in place: an active VM shell grant ("Shell on
the production VM"), the devops agreement and the health check read ("Deployment
hold on the VM"), and the on-call told about the alerts.

Then stop the bot and arm the deployment hold ("Arm the hold" in "Deployment
hold on the VM"). `move-receipts` must not run while the bot runs: the engine's
projection rebuilds and quiescence reads must not race a running service
(`src/receipt_inventory/migration.rs`). The store lock enforces this: the bot
and the two offline verbs take it, so each refuses while the other runs. The
lock ends with the command, so only the hold keeps the bot stopped from the move
to the flip in step 12.

Then snapshot: record this token's per-receipt on-chain balances of BOTH wallets
(run the "Zero-supply chains" loop once as written and once with the
orchestrator address in place of `<bot wallet>`, and keep both outputs) — the
bot wallet and the orchestrator's pre-move balances for the same receipt ids
(the engine deliberately allows a destination with pre-existing balances and
verifies per-identifier GAINS, so the verification in step 11 needs the
before-values). Do not compare the bot wallet with `receipt_inventory_view`: it
projects only `Mint` events, so every burned receipt reads as a discrepancy.
`move-receipts` checks the tracked balances against the chain itself (step 10).

After the hold is armed, run the bot-wallet loop from "Zero-supply chains" again
on each chain marked `no receipts`. Step 8 lets in-flight mints finish, and a
vault-direct mint can put a receipt at the bot wallet after the first scan. The
chain stays `no receipts` only if the loop still ends with
`OK: read all N ids, 0 non-zero`; otherwise snapshot it as above and run steps
10–11 for it like any other chain.

If no chain goes through step 10, the quiescence gate of `move-receipts` never
runs, so run it this way: with the hold armed, run
`issuer confirm-custody <SYM>` once on one chain (rollback step 6 shows the
command) and answer yes at its prompt. A drained asset ends with the
`InventoryEmpty` refusal ("has no tracked receipts"). `MintsInFlight`,
`RedemptionsInFlight` or `BurnReserved` means that work is still open: take the
step-10 detour. If it prints `Confirmed custody of N receipt(s)`, inventory
tracks receipts on that chain although the chain read `no receipts`: stop and
escalate to engineering. Any other refusal: nothing has moved, so leave the
window as in the step-10 detour, then escalate to engineering.

## 10. Move the receipts

```
issuer move-receipts <SYM> \
  --to-configured-orchestrator \
  --network base --chain-id 8453
```

Run it on the VM, as "Shell on the production VM" describes: the bot's
environment supplies `CONFIG`, so the CLI reads the same file as the bot. The
destination is read from the `--network`'s `[orchestrator.addresses]` entry —
never typed — and corroborated as an ERC-1155-receiving contract before anything
is signed. A configured address that is not such a contract is refused before
the prompt. The command prompts with the asset, vault, holder, destination and
its corroborated kind, and the tracked receipt count. Before any chain moves,
run the move once on each chain that goes through step 10 and answer no, and
compare each count with that chain's non-zero count from the step-9 snapshot.
Answer yes only when every chain matches. A mismatch means inventory is behind
the chain: take the detour below, with nothing moved, and escalate to
engineering. A vault tracking more than 14 receipts moves in multiple bounded
transactions, each verified before the next. A re-run after any interruption is
safe: an interrupted move resumes with only the remaining receipts, and a
completed move reports "already migrated" and submits nothing. That is true only
until the service starts again (step 12). After that start, a re-run (with the
hold armed again) refuses before the prompt with "nothing to move", and its
message says this is expected after a cutover: the moved receipts are no longer
in inventory.

The CLI prints each refusal's message, not its name. The names in this runbook
match these messages:

| Name                          | The message contains                                       |
| ----------------------------- | ---------------------------------------------------------- |
| `MintsInFlight`               | "mint(s) are between initiation and a terminal state"      |
| `RedemptionsInFlight`         | "redemption(s) are between detection and a terminal state" |
| `BurnReserved`                | "receipt(s) reserved for an in-flight"                     |
| `CutoverCustodyAtDestination` | "is recorded at the cutover destination"                   |
| `CustodyUnobserved`           | "custody has never been confirmed"                         |
| `InventoryEmpty`              | "has no tracked receipts on chain"                         |
| `InventoryDivergence`         | "diverges: inventory tracks"                               |

If the move refuses with `MintsInFlight`, `RedemptionsInFlight` or
`BurnReserved`, work for the asset is still open. A redemption that arrived
after the freeze is held, and the stuck read does not show it while the asset is
frozen. The config is still vault-direct (the flip is not merged until step 12),
so release the hold on the release that the VM runs ("Deployment hold on the
VM"), unfreeze (`ops capital unfreeze <SYM>`) until the work completes, then go
back to step 8. A failed row that recovery does not retry (a classified failure,
a `JournalRejected` mint, a `Failed` redemption) does not drain on a restart:
resolve it with the failed-row actions in rollback step 3 (re-drive, close, or
escalate). Here the asset is still vault-direct, so nothing burns through the
orchestrator. A `BurnFailed` redemption here is a vault-direct burn: "Shortfall
escalation" does not apply to it, so escalate it to engineering. An `Initiated`
mint that Alpaca never confirms or rejects does not drain either: confirm its
status with Alpaca and escalate to engineering. This detour applies only while
no chain has moved yet. If a refusal comes after any chain moved, keep the hold
armed and escalate to engineering: a vault-direct restart would drop the moved
receipts from inventory while the orchestrator holds them.

Any other refusal comes before the transfer it guards: for example
`InventoryDivergence` ("diverges: inventory tracks"), "certification is expired"
or "owner freeze blocks". If nothing has moved on any chain yet, leave the
window as in this detour, then escalate to engineering. If any transfer was
already submitted, keep the hold armed and escalate.

If the move refuses with `CutoverCustodyAtDestination`, an earlier release
recorded this vault's cutover with custody at the orchestrator. The no-custody
cutover and the rollback in step 14 do not apply to that vault: stop, keep the
hold armed, and escalate to engineering.

If the move refuses with `CustodyUnobserved` on a chain that holds receipts,
custody was never confirmed for that vault: no startup reconciliation has
finished without errors while the vault held receipts (for example, its first
receipt arrived after the last restart). Keep the hold armed and run:

```
issuer confirm-custody <SYM> \
  --network base --chain-id 8453
```

It checks that the bot wallet holds every tracked balance, and only then records
the bot wallet as holder. Then re-run the move. If `confirm-custody` refuses
with a balance mismatch, inventory disagrees with the chain: stop, keep the hold
armed, and escalate to engineering.

The move records no custody. The bot wallet stays the recorded holder, and the
command says so when it completes ("Custody stays recorded at Turnkey wallet
…"). The orchestrator now owns the receipts, and inventory tracks only what the
bot wallet owns. In orchestrator mode the vault's inventory stays empty: the
orchestrator mint, burn and recovery paths do not register or reconcile receipts
in it.

## 11. Verify the move

- For every receipt id, the orchestrator's `balanceOf` GAIN over its step-9
  pre-move balance equals the bot wallet's transferred amount from the same
  snapshot; the bot wallet reads zero. (Final-balance equality with the bot's
  snapshot is only correct when the orchestrator started at zero — the gain
  check is the one the engine itself enforces.) Read the after-values with the
  "Zero-supply chains" loop, with the orchestrator address in place of
  `<bot wallet>`, as in the step 9 snapshot.
- The bot wallet holds no receipt of this vault: run the "Zero-supply chains"
  loop for the bot wallet on every chain that went through step 10. It must end
  with `OK: read all N ids, 0 non-zero`. A listed id is a receipt that inventory
  did not track, so step 10 did not move it: keep the hold armed and escalate to
  engineering.
- `nextBurnReceiptId(token)` sits at or below the lowest transferred id, so
  every transferred receipt is reachable by the burn walk with no manual
  `setBurnIndex` (`BurnIndexLowered` events appear only if the pointer had
  previously advanced past a transferred id — their absence on a fresh
  orchestrator is normal).

  Read the pointer on-chain — the bot is stopped at this point (step 9), and
  `read orchestrator-health` fills `next_burn_receipt_id` only for assets whose
  resolved mode is already orchestrator, which this one is not until step 12:

  ```sh
  [ "$(cast chain-id --rpc-url "$RPC_URL")" = "<chain id>" ] \
    && cast call <orchestrator address> 'nextBurnReceiptId(address)(uint256)' \
      <vault> --rpc-url "$RPC_URL"
  ```

  After the flip, the health endpoint is what shows the same pointer.

  If the pointer sits **above** the lowest transferred id, those receipts are
  stranded: the burn walk starts past them, so their backing is invisible to
  every future burn and a redemption large enough to need them reverts
  `InsufficientReceipts`. Do not proceed. `setBurnIndex` is the remedy and is an
  `EMERGENCY_ROLE` action owned by st0x.deploy — this bot has no CLI for it and
  never calls it (it only ever reads `nextBurnReceiptId`). Page the holder
  recorded below, have the pointer lowered, and re-run this verification before
  the flip.

  **Gas, for later assets.** The pointer starts at zero on a fresh orchestrator,
  so the first burn walks every receipt id from zero. RKLB is small enough for
  that to be irrelevant (highest id 75). It is not irrelevant for the full
  rollout — COIN is at 1643 and MSTR at 768 — where an unbounded first walk is a
  real gas risk. A bounded burn plus a deliberate `setBurnIndex` starting point
  is still an open design item, so treat any asset of that size as blocked on it
  rather than assuming this step scales.
- Inventory still lists the moved receipts at this point. This is expected: the
  bot is stopped, and nothing reads the chain until step 12 starts it.

## 12. Flip, deploy, unfreeze

Set `vault_mode = "orchestrator"` in the asset's `[assets.<SYM>]` table of the
TOML config, in a reviewed commit on main. The flip must live on main: on GCP,
production deploys only main's config (a config-only release) or the config of a
version tag cut from main (README "GCP release path"). A rollback dispatch to a
tag cut before the flip deploys that tag's config, so it undoes the flip. The
orchestrator then holds every receipt, so the next redemption completes its
Alpaca journal and then cannot burn (step 13 checks for this after every
release). `deploy_config_files_parse_and_stay_dark` (`src/config.rs`) refuses
any asset override in both config files, so the same commit changes it to accept
exactly this one. Merge it after step 11 verified the move.

The comment above `[assets.RKLB]` in `config.prod.toml` still describes the
droplet flip (`prodDeployNixos`, "deploy the service"). Follow this step, not
that comment, and let the flip PR rewrite the comment to point here. The checks
below allow that comment change.

Merge the flip PR from step 9 as a squash merge (as Graphite merges), not as a
merge commit or a rebase merge. Below, "the flip commit" is that squash commit
on main: the check finds it by the `(#<PR>)` at the end of its subject, and
reads its first parent as main before the flip. The merge starts a
`production-release.yml` run that skips with "code changed since the commit
production runs": that is expected, because the dark test changed. Wait until
the `build-push` job of `build-oci.yml` succeeds on the flip commit (a
`roll-staging` failure does not matter here). A newer push to main cancels a
waiting `build-oci.yml` run, so if the run was cancelled, re-run it. The step 7
main check cannot run again as written, because the flip itself changes
`src/config.rs`. So check the two parts of the tag apart: nothing but config and
Markdown before the flip, and nothing but the flip in the flip commit:

```sh
# On your workstation. <PR> is the flip PR's number. PRs land on main through
# Graphite as squash commits, so GitHub can show the PR as closed with no merge
# commit: find the commit on main by the "(#<PR>)" in its subject instead.
REPO=ST0x-Technology/st0x.issuance
git fetch -q origin main \
  && MERGE=$(git log origin/main -1 --format=%H --fixed-strings \
       --grep="(#<PR>)") \
  && [ -n "$MERGE" ] \
  && git log -1 --format=%s "$MERGE" | grep -q -E '\(#<PR>\)$' \
  && gh api "repos/$REPO/commits/$MERGE" > /tmp/flip-commit.json \
  && PARENT=$(jq -er '.parents[0].sha' /tmp/flip-commit.json) \
  && gh api "repos/$REPO/compare/<live tag>...$PARENT" \
       > /tmp/compare-flip.json \
  && jq -e '.status == "ahead" or .status == "identical"' \
       /tmp/compare-flip.json > /dev/null \
  && ! jq -r '.files[].filename' /tmp/compare-flip.json \
       | grep -v -x -E 'config\.(prod|staging)\.toml|.*\.md' \
  && [ "$(jq -r '[.files[].filename] | sort | join(" ")' \
         /tmp/flip-commit.json)" = "config.prod.toml src/config.rs" ] \
  && echo "OK: $MERGE"
```

Without the `OK` line, do not cut the tag, and keep the hold armed and the asset
frozen. If `MERGE` is empty, the squash commit is not on main yet: wait, and run
the check again. If a `git fetch` or `gh api` call failed, run the check again.
A listed file is code below the flip commit, and no later commit can take it out
of a tag on that commit: escalate to engineering. If `MERGE` stays empty after
the PR shows as landed, or the check fails for any other reason, escalate to
engineering. Do not start the bot on the release it already runs: the
orchestrator holds the moved receipts.

Then prove that the tag ships no other config change. On the VM, hash the config
file that the bot runs. `sha256sum` prints only a hash, and this file is the
config, not the secrets env:

```sh
sudo sha256sum <config file>
```

On your workstation, in the same shell as the check above:

```sh
git show "$PARENT":config.prod.toml | sha256sum
git diff "$PARENT" "$MERGE" -- config.prod.toml
```

The two hashes must be equal: production runs main's config from before the
flip. Then the diff is the whole config change that the tag ships, and it must
show only the `[assets.<SYM>]` lines and the comment above them. If the hashes
differ, main carries a config change that production does not run yet, and the
tag would ship it: do not cut the tag, keep the hold armed, and escalate to
engineering.

Then pick a new version above the current tip (the first command prints it), set
it, and cut it on the flip commit, not on a later commit of main:

```sh
git fetch --tags origin && git tag -l 'v[0-9]*' --sort=-v:refname | head -1
export FLIP_TAG=vX.Y.Z    # replace with the next version; record it
[[ "$FLIP_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] \
  && git tag "$FLIP_TAG" "$MERGE" \
  && git push origin "$FLIP_TAG" \
  || echo "FAIL: no tag pushed" >&2
```

`release-tag.yml` labels the image that `build-oci.yml` built and attested, and
deploys it with that commit's `config.prod.toml`. The approvers see the config
change in the grant's justification, also printed in the run's "Await PAM
approval" log:

```text
CONFIG issuance-runtime-config: vN -> NEW VERSION (+a/-b lines vs vN)
```

`N` must equal the number that
`sudo sed -n 's/^CONFIG_VERSION=//p' <images env file>` prints on the VM. `a`
and `b` must equal the counts that the release computes the same way (plain git
output, with no color or external diff tool):

```sh
git diff --no-color --no-ext-diff --no-textconv "$PARENT" "$MERGE" \
  -- config.prod.toml | grep -c '^+[^+]'
git diff --no-color --no-ext-diff --no-textconv "$PARENT" "$MERGE" \
  -- config.prod.toml | grep -c '^-[^-]'
```

If not, do not approve, and escalate to engineering.

The justification also has a `BOT_IMAGE ROLL` line, from the live digest to the
new one. That roll is expected: `config.prod.toml` and `src/config.rs` are build
inputs, so the flip commit builds a new image even though no other code changes.

The deploy waits at "Await PAM approval" until the approvers approve the
`app-deploy` grant in the GCP console, so have enough approvers on call
(s01.devops gives the number). Production release runs queue behind each other.
If another production release run is in progress or queued, cancel it in GitHub,
or deny its grant, before this run reaches the approval step. If another run's
grant is still open, this step fails at once: an approver denies that grant if
it waits for approval, or revokes it if it is active, and then re-run the failed
`deploy-production / release` job. A grant that waits for approval stays open
until it expires, and an active grant for up to an hour. If no approval comes
within 60 minutes, re-run the failed job, which reuses its own waiting grant. Do
not push a new tag for a retry.

The hold is still armed, so the VM does not adopt the release on its own. While
the run waits for the VM, release the hold onto the new release ("Deployment
hold on the VM"). That one roll installs the new `images.env`, fetches the
flip's config version, and starts the bot on it. So the bot never starts on the
old config.

Wait for the start: startup rebuilds views, backfills and reconciles before
Rocket binds, which can take several minutes. A connection error, or a 502 or
503 from the load balancer, before the start completes is not a failure: this
read is safe to retry. Then verify that the bot reports orchestrator mode on
every chain of the asset (the status endpoint reads the same config):

```sh
ops read orchestrator-health \
  | jq -e --arg sym <SYM> \
      '[.assets[] | select(.underlying == $sym) | .vault_mode]
       | length > 0 and all(. == "orchestrator")'
```

On this start, the backfiller and startup reconciliation read zero at the bot
wallet for every moved receipt and remove it from inventory. After the start,
the vault's inventory is empty. That is the expected state, not lost receipts:
the orchestrator holds them on-chain (step 11). A `CustodyDisplaced` or
`CustodyUnconfirmed` refusal here is a real problem, not cutover noise. If the
start fails or the health check fails after it, keep the asset frozen and
escalate to engineering. If the health check reads `vault_direct` on any chain,
the bot runs the config before the flip while the orchestrator holds the
receipts: arm the hold again at once, then escalate.

Before you unfreeze, check that the `rolled` line you recorded shows the new
tag's `BOT_IMAGE` and `CONFIG_VERSION`. Then unfreeze
(`ops capital unfreeze <SYM>`; `ops read status <SYM>` then reads `enabled`).
The unfreeze exits 0 on every outcome, so read its `outcome`: it must be
`unfroze`. `remains_frozen` means that another hold, such as a freeze window,
still keeps the asset frozen: leave it frozen until that hold ends or its owner
releases it, and record why.

## 13. Pilot validation (manual, in prod)

RKLB has essentially no organic flow — that is the point — so validation is
actively driven. Manually trigger a small mint through the liquidity bot's
rebalancing path (exercising the real MintAuthV1 delivery) and a small
redemption (send tokens to the redemption wallet), and follow every stage
end-to-end: authorization delivery, Turnkey signing, orchestrator
`Minted`/`Burned` events, Alpaca journals/callbacks, and
`ops read orchestrator-health`.

**Verify the burn against the vault's `Withdraw` events, not the `Burned`
range.** `Burned` carries `firstReceiptId` and `nextBurnReceiptIdAfter`, which
are the burn POINTER before and after — a half-open span, not a list of what was
consumed. `firstReceiptId` may itself have been partially drained by an earlier
burn, and the span can cover ids the burn never touched. Reading it as a receipt
manifest will overstate what happened. The per-receipt truth is the vault's own
`Withdraw` logs from the same transaction: one per receipt, each carrying `id`
and `shares`. Check that their `shares` sum to `Burned.amount` and that every
`id` is a receipt id of this vault that the orchestrator held before the burn
(moved in step 10, or minted through the orchestrator). The vault's inventory
does not track these ids: it stays empty in orchestrator mode (step 12).
`Burned.amount` is the redemption's persisted `alpaca_quantity` — the detected
transfer truncated to 9 decimals, with the remainder kept in the bot wallet as
`dust_retained` — which is exactly what
`handle_record_orchestrator_burn_confirmed` requires `shares_burned` to equal.
Do NOT compare the sum to the raw amount sent to the redemption wallet: a
transfer carrying more than 9 decimals of dust makes that read as a mismatch,
which mid-pilot looks like missing backing. The range is only ever the pointer.

Repeat over a soak window (≥1 week with the asset left in orchestrator mode):
every attempt completes, no unexplained `ops read stuck` entries, and any
transient failure exercises a recovery path observably. Before any release in
the window, and above all before a rollback dispatch to an older version, check
on your workstation that its tag (or commit) carries the flip:

```sh
git fetch --tags origin && git show <tag>:config.prod.toml \
  | grep -A1 -i -E '^\[ *assets\."?<SYM>"? *\]' \
  | grep -q -E '^vault_mode = "orchestrator"'
```

→ exit 0. After every release in the window, run the step 12 health check again.
If it fails, freeze the asset at once, release a version that carries the flip,
and escalate to engineering every redemption detected and every mint initiated
since that release. Their mode is fixed as vault-direct: such a redemption
cannot burn, and such a mint puts its receipt at the bot wallet, out of reach of
orchestrator burns. The freeze stops only new mints, so wait until every
escalated mint is completed or closed. Then, before you unfreeze, run the
bot-wallet loop on each chain: it must end with
`OK: read all N ids, 0 non-zero`. If it lists ids, keep the asset frozen, give
them to engineering, and run the loop again after they are resolved. Record the
go/no-go that gates the full rollout (RAI-1246).

## 14. Rollback (per asset; rehearsed on Anvil)

Before you start, rule out two states that fall outside this rollback. In both,
the returned receipts are read at a wallet that recorded custody does not name,
so the start in rollback step 6 refuses the backfill (`CustodyDisplaced`), and
the service does not start. No command reads recorded custody yet, so tell them
from the release that ran this asset's cutover and from the signing-wallet
history. If either applies, do not start: escalate to engineering (SPEC "Receipt
custody").

- **A cutover recorded by an earlier release.** Custody for the vault names the
  orchestrator (step 10 refuses a re-run on it with
  `CutoverCustodyAtDestination`).
- **A signing-wallet rotation while the asset is in orchestrator mode.** Custody
  still names the old wallet, and the vault's empty inventory leaves nothing to
  migrate or re-confirm. Do not rotate the signing wallet while any asset is in
  orchestrator mode: roll the asset back first.

This rollback also assumes that the step 12 start ran. Before that start,
inventory still lists the moved receipts (step 11), so the checks below do not
apply. If you stop the cutover after step 10 and before that start, keep the
hold armed and escalate to engineering.

The rollback touches only this asset:

1. Freeze the asset and let in-flight work drain (step 8's procedure). The stuck
   read does not prove the drain: it hides in-progress work younger than one
   hour, and redemptions that the freeze holds. Step 3 proves it.
2. Prepare the flip back as a reviewed and approved PR to main that comments out
   the `[assets.<SYM>]` table again and restores the dark test (as in step 12).
   An explicit `vault_mode = "vault_direct"` is still an override: the dark test
   refuses it, and the rollback step 6 check fails. Do not merge the PR until
   rollback step 6. Every start before rollback step 6 must still run the
   orchestrator config: the orchestrator holds the receipts until rollback step
   4, and a redemption detected under the vault-direct config cannot burn. Like
   the flip, the flip back changes the dark test, so it ships with a version
   tag. Run the step 7 live tag, main and config hash checks now, so that
   nothing can stop the rollback halfway. If the main check lists a file, or the
   config hashes differ, escalate to engineering before rollback step 3: the
   rollback step 6 tag must not ship other code or config. Then merge nothing
   else to main until the rollback step 6 release runs on the VM.
3. Have the same preparation as step 9 (an active VM shell grant, the devops
   agreement and the health check read, the on-call told). Then stop the bot and
   arm the hold ("Arm the hold" in "Deployment hold on the VM") — **before** any
   receipt moves, for the same reason step 9 requires it on the way in: the
   returned receipts must reach inventory through one clean start, not through a
   pass that races the withdrawals.

   Then prove that no mint or redemption for the asset is open. No command lists
   all of that work while the asset is frozen (the stuck read hides held
   redemptions and in-progress work younger than one hour), but the quiescence
   gate of `confirm-custody` counts all of it, at any age, held redemptions
   included. Run it once, on one chain (the command is in rollback step 6), and
   answer yes at its prompt. In orchestrator mode the inventory is empty, so:

   - **`InventoryEmpty`**: nothing is open. Keep the hold armed and go to
     rollback step 4.
   - **`Confirmed custody of N receipt(s)`**: inventory tracks receipts in
     orchestrator mode. Keep the hold armed, stop, and escalate to engineering.
   - **`MintsInFlight`, `RedemptionsInFlight` or `BurnReserved`**: work is still
     open. Release the hold on the release that the VM runs (the flip back is
     not merged yet), and drain it as below. Then go back to rollback step 1.
   - **Any other refusal**: nothing has moved. Release the hold on the release
     that the VM runs, keep the asset frozen, and escalate to engineering.

   To drain: a held redemption runs only while the asset is not frozen, and it
   burns through the orchestrator, so unfreeze until it completes. A failed row
   that recovery does not retry (a classified `MintingFailed` or `BurnFailed`, a
   `JournalRejected` mint, a `Failed` redemption) does not drain on a restart.
   The stuck read lists it at any age. A `JournalRejected` mint holds no
   backing: confirm with Alpaca that its journal was rejected, then close it
   with the close call in step 3 of "Mint waiting on an authorization that never
   arrived", with a `reason` that names the rejection (the reason stays in the
   mint's history). Re-drive a `MintingFailed` mint with
   `ops debug reprocess-mint <issuer_request_id>`. If the bot refuses it
   (`HTTP 422`), close it as step 3 of that section describes, including its
   Alpaca reconciliation: the mint's journal completed, so the AP's shares are
   held with no tokens. If the re-drive is accepted but the mint fails again,
   escalate to engineering: it blocks rollback step 4. Resolve a `BurnFailed`
   redemption by the classification at the start of its `detail` in the stuck
   read. For `InsufficientReceipts`, `VaultLogicMismatch` or
   `ReceiptLogicMismatch`, use "Shortfall escalation". For
   `AllowanceInsufficient`, run
   `ops capital approve-orchestrator <network> <SYM>`, then
   `ops debug recover-redemption <id>`. Escalate any other `BurnFailed` to
   engineering. Any other failed row, such as a redemption whose Alpaca call
   failed, is an escalation to engineering, and it blocks rollback step 4. Two
   open mints do not drain by themselves either. A `Minting` row with no `tx_id`
   is an orchestrator mint that waits on its authorization: while the delivery
   is broken it stays open, so resolve it with "Mint waiting on an authorization
   that never arrived". An `Initiated` mint that Alpaca never confirms or
   rejects stays open too: confirm its status with Alpaca and escalate to
   engineering. Both block rollback step 4. Do all of this before rollback step
   4, while the orchestrator still holds the receipts.
4. Page the `EMERGENCY_ROLE` holder (recorded below):
   `withdrawReceipt(token, id, amount, bot_wallet)` for **every receipt the
   orchestrator holds for the token** — the migrated ones AND every receipt
   minted through the orchestrator since the cutover — returns them on-chain.
   Every receipt id the vault ever issued lies in `1..=highwaterId()` (see
   "Zero-supply chains"), so read the orchestrator's balance of every id in that
   range. Do not use the step-9 snapshot for amounts: step 13 requires at least
   one real orchestrator burn, and each burn drains the lowest ids first — the
   migrated ones — so their snapshot amounts are too high after any burn. A
   `withdrawReceipt` for a snapshot amount then reverts.

   ```sh
   (
     set -euo pipefail
     [ "$(cast chain-id --rpc-url "$RPC_URL")" = "<chain id>" ] \
       || { echo "FAIL: RPC is not chain <chain id>" >&2; exit 1; }
     RECEIPT=$(cast call <vault> 'receipt()(address)' --rpc-url "$RPC_URL")
     HIGHWATER=$(cast call <vault> 'highwaterId()(uint256)' \
       --rpc-url "$RPC_URL" | cut -d' ' -f1)
     [[ "$HIGHWATER" =~ ^[0-9]+$ ]] \
       || { echo "FAIL: highwaterId is '$HIGHWATER'" >&2; exit 1; }
     NONZERO=0
     for ((ID = 1; ID <= HIGHWATER; ID++)); do
       BAL=$(cast call "$RECEIPT" 'balanceOf(address,uint256)(uint256)' \
         <orchestrator> "$ID" --rpc-url "$RPC_URL" | cut -d' ' -f1)
       [ "$BAL" = 0 ] || { echo "$ID: $BAL"; NONZERO=$((NONZERO + 1)); }
     done
     echo "OK: read all $HIGHWATER ids, $NONZERO non-zero"
   )
   ```

   As in "Zero-supply chains", run it on your workstation with the RPC URL of
   the chain you check. Without the `OK` line, the read failed.

   Withdraw every id that it lists. Read its amount again just before its
   withdrawal, and use that reading as the `amount`. After each withdrawal,
   verify that the bot wallet's `balanceOf` for that id went up by exactly the
   amount you withdrew. A withdrawal is an ERC-1155 receipt transfer, so the
   vault's transfer checks apply to it (`authorizeReceiptTransfer3`): an owner
   freeze or an expired certification can revert it. Clear that cause first.
5. Run the loop from step 4 again. Its last line must be
   `OK: read all N ids, 0 non-zero`: the orchestrator holds nothing for the
   token. A listed id is a receipt still stranded at the orchestrator — withdraw
   it before you continue. This check is complete because it reads every id the
   vault ever issued. No log search and no custody command is needed: the
   cutover recorded no custody, so there is no custody to restore.
6. With the hold still armed, merge the flip back from step 2 and ship it with a
   new version tag, as step 12 ships the flip (the same `build-oci.yml` wait,
   flip commit check with the flip-back PR's number as `<PR>`, config hash
   check, tag commands with a new value for `$FLIP_TAG`, and PAM approval).
   While the run waits for the VM, release the hold onto the new release, as in
   step 12, so that the bot starts on the flipped back config. On this start the
   backfiller finds each `withdrawReceipt` as a transfer into the bot wallet and
   adds the receipt to inventory at its on-chain balance. Wait for the start, as
   in step 12 (a connection error, a 502 or a 503 before the start completes is
   not a failure: retry). Before you unfreeze, check that the asset is back in
   vault-direct mode on every chain (this must exit 0):

   ```sh
   ops read orchestrator-health \
     | jq -e --arg sym <SYM> \
         '[.assets[] | select(.underlying == $sym) | .vault_mode]
          | length > 0 and all(. == "vault_direct")'
   ```

   If the start fails, or this check fails because a chain still reads
   `orchestrator`, keep the asset frozen, arm the hold again at once, and
   escalate to engineering. Do not run `confirm-custody`, and do not unfreeze.

   Also check that inventory tracks every id step 4 returned at its balance.
   `receipt_inventory_view` cannot show this: it projects only `Mint` events, so
   it never sees the rediscovered receipts. No command reads the inventory yet,
   so use `confirm-custody` as the check. Arm the hold, and run it once per
   chain whose receipts step 4 returned, on the VM (see "Shell on the production
   VM"):

   ```
   issuer confirm-custody <SYM> \
     --network base --chain-id 8453
   ```

   It compares every tracked balance with the bot wallet's on-chain balance and
   refuses on the first mismatch. The count it confirms must equal the number of
   ids step 4 returned on that chain. It restores nothing: custody already names
   the bot wallet, so the record does not change (a chain that never held
   receipts records the bot wallet for the first time).

   It also refuses while a redemption for the asset is open. Step 3 proved that
   nothing was open, and the bot did not run again until this start. So an open
   redemption here was detected on this start, in vault-direct mode, and the
   freeze holds it. It is safe to run: release the hold on the release that the
   VM runs, and unfreeze until it completes. While the asset is unfrozen, new
   mints can start. So freeze again, and wait until the asset's in-flight mints
   and redemptions drain, as in step 8. Then arm the hold and run this check
   again. If it refuses with `MintsInFlight`, `RedemptionsInFlight` or
   `BurnReserved`, work is still open: release the hold on the release that the
   VM runs, let that work drain (unfreeze while a held redemption must run),
   freeze again and repeat this check. The redemption's burn used up returned
   receipts, so the count must now equal the ids that the bot wallet still
   holds: run the step 4 loop with the bot wallet (`<bot wallet>` in
   "Zero-supply chains") in place of `<orchestrator>`, and use its non-zero
   count. If that count is 0, `InventoryEmpty` is the expected result.

   If the count differs from what these rules expect, no runbook step fixes it.
   Lower means the backfill missed a returned receipt; higher means the bot
   wallet holds receipts that step 4 did not return. Keep the hold armed and the
   asset frozen, find the ids by comparing the step 4 list with the bot-wallet
   loop, and escalate to engineering.

   Then release the hold on the release that the VM runs. The returned receipts
   are recorded with source `External`, with no link to the mint that issued
   them. That is safe: only in-flight vault-direct mints use that link (mint
   recovery and the admin close gate), and step 3 and the freeze leave none.

   Before you unfreeze, check that main no longer carries the override, so the
   next release keeps the asset in vault-direct mode:

   ```sh
   # On your workstation.
   git fetch origin main \
     && git show origin/main:config.prod.toml > /tmp/main-config.toml \
     && ! grep -q -i -E '^\[ *assets\."?<SYM>"? *\]' /tmp/main-config.toml
   ```

   → exit 0. If it fails, keep the asset frozen until the flip back merges.
7. Unfreeze, as in step 12: the `outcome` must be `unfroze`.

Redemptions already burned through the orchestrator keep their persisted
`burn_mode`; their recovery and verification follow the persisted mode, so they
stay recoverable after the flip back.

## Record of prod facts

Fill in as the steps execute; this table is the standing record the acceptance
criteria ask for.

| Fact                                                                | Value | Date | Verified by                    |
| ------------------------------------------------------------------- | ----- | ---- | ------------------------------ |
| Orchestrator address                                                |       |      | config.prod.toml + deploy      |
| `MINT_ROLE` grant tx / holder check                                 |       |      | `orchestrator-preflight`       |
| `BURN_ROLE` grant tx / holder check                                 |       |      | `orchestrator-preflight`       |
| `DEPOSIT`/`WITHDRAW` grants (orchestrator on each vault authorizer) |       |      | `orchestrator-preflight`       |
| `EMERGENCY_ROLE` holder, per chain (rollback, escalation)           |       |      | step 7 `hasRole` read          |
| Turnkey policy name(s)                                              |       |      | `verify-orchestrator-signing`  |
| Turnkey tags granted (both orchestrator tags)                       |       |      | `npm run audit-identities`     |
| Issuance release tag (`$RELEASE_TAG`, `vX.Y.Z`)                     |       |      | `git ls-remote --tags …`       |
| Orchestrator `eip712Domain()` (one row per chain)                   |       |      | `cast call`                    |
| Orchestrator beacon, beacon owner, implementation hash (per chain)  |       |      | `cast storage` / `cast keccak` |

Per-asset approvals — one row per chain the asset is listed on, since the flip
covers every listing and an unapproved chain breaks rather than staying behind:

| Asset | Chain     | Approval tx hash | Date | Operator |
| ----- | --------- | ---------------- | ---- | -------- |
| RKLB  | Base      |                  |      |          |
| RKLB  | Ethereum  |                  |      |          |
| RKLB  | HyperEVM  |                  |      |          |
| RKLB  | Robinhood |                  |      |          |

Per-asset cutovers (steps 7–13; RAI-1222 acceptance record). The flip is one
config edit for the symbol; everything touching receipts is per chain, so the
receipt-move, snapshot and verification columns are recorded per row:

| Asset | Chain     | Preflight exit 0 | Final receipt-move tx | Snapshot ref | Step 11 verification | Validation mints/redemptions | Go/no-go |
| ----- | --------- | ---------------- | --------------------- | ------------ | -------------------- | ---------------------------- | -------- |
| RKLB  | Base      |                  |                       |              |                      |                              |          |
| RKLB  | Ethereum  |                  |                       |              |                      |                              |          |
| RKLB  | HyperEVM  |                  |                       |              |                      |                              |          |
| RKLB  | Robinhood |                  |                       |              |                      |                              |          |

| Asset | Flip release tag | Flip commit | Soak end | Overall go/no-go |
| ----- | ---------------- | ----------- | -------- | ---------------- |
| RKLB  |                  |             |          |                  |

Per-cutover evidence (one block per asset; attach the full outputs):

| Record                                                        | RKLB |
| ------------------------------------------------------------- | ---- |
| `<live tag>` and the release run that wrote its digest        |      |
| Chain set (the "Cutover scope" `orchestrator-health` output)  |      |
| Step 7 main check output                                      |      |
| Fork rehearsal record (`--- fork rehearsal record ---` lines) |      |
| Quiescence result (step 9 or step 10)                         |      |
| `rolled ... to` line of the step 12 roll                      |      |
| Turnkey allow-side decision for chains other than Base        |      |
| Flip-back tag and its `rolled` line (rollback only)           |      |

## Mint waiting on an authorization that never arrived

Orchestrator-mode mints cannot proceed without the liquidity bot's `MintAuthV1`
delivery. The exposure is that Alpaca has ALREADY journaled the AP's shares by
then: journal completion sends `Deposit`, which emits `MintingStarted` with no
authorization check (`Mint::handle_deposit`), and the orchestrator submit branch
then defers without recording an event — so the mint sits in `Minting`, not
`JournalConfirmed`, holding real backing and minting nothing until the
authorization lands.

**Detection.** The mint recovery pass raises the alert itself; do not wait for
the stuck read (`ops read stuck`), which does not surface an in-progress mint
until it is an hour old (`STUCK_THRESHOLD`, `src/admin.rs`) — an hour of
silently-held AP shares. The pass runs every five minutes and measures each wait
from the moment the shares were journaled, so the first line appears on the
second or third pass after that point:

| Level   | Wait      | Meaning                                                    |
| ------- | --------- | ---------------------------------------------------------- |
| `WARN`  | 10-30 min | Chase the delivery.                                        |
| `ERROR` | ≥ 30 min  | Escalate: the shares have been committed for half an hour. |

A mint that passes 30 minutes moves from the `WARN` line to the `ERROR` line, so
a mint that leaves the `WARN` line is not resolved.

Each level prints at most ONE line per pass, not one per mint, carrying
`waiting_mints` (how many are in that band) and the OLDEST waiter's
`oldest_issuer_request_id`, `oldest_tokenization_request_id` and
`oldest_waited_seconds`. So the count tells you the scale and the ids give you
somewhere to start; the other waiting mints are not named, and the same line
repeats every pass while the condition holds. Once they are old enough for the
stuck read, the rest are the orchestrator-mode `Minting` rows for the asset with
no `tx_id`:

```sh
ops read stuck > /tmp/stuck.json \
  && jq --arg sym <SYM> \
      '.stuck[] | select(.underlying == $sym and .state == "Minting" and .tx_id == null)' \
      /tmp/stuck.json
```

The `&&` stops on a failed call, so no output means no waiting mint only when
the block exits 0. A mint's `aggregate_id` in this read is its
`issuer_request_id`.

Mints waiting BEFORE their journal confirms are deliberately not alerted: Alpaca
has committed nothing at that point, so there is no exposure.

Escalation:

1. Confirm the mint is actually waiting on delivery and not on something else.
   The stuck read cannot show that directly — the row reads
   `state: "Minting", detail: "Deposit in progress"` and carries no field for
   whether an authorization was recorded — so confirm it from the service log:
   the WARN line below, for that `issuer_request_id`, repeated about once a
   minute while the mint's recovery job is live, is this case:

   ```text
   Orchestrator mint is awaiting its recipient authorization; deferring submission
   ```

   That WARN repeats only while the mint's recovery job is live. The job gives
   up after `MAX_SCHEDULED_RECOVERY_NO_PROGRESS_POLLS` polls with no progress
   (360 polls at one minute each, so about 6 hours), logs the ERROR "Scheduled
   mint recovery abandoned the mint while still incomplete", and is marked
   `Killed`. After that, the reconcile pass does not start a new job for the
   mint, so the per-mint WARN stops, while the summary `ERROR` above keeps
   firing. The WARN comes back when something drives the mint again: a restart
   (the startup re-scan), or `ops debug reprocess-mint <issuer_request_id>`,
   which accepts a `Minting` mint and replaces its `Killed` recovery job. So for
   a long wait, such as one overnight, do not expect a recent WARN: search the
   log back to when the mint started.

   The alert names only the oldest waiter. With `waiting_mints` above one, find
   the others by the age of their wait. For the first hour, the stuck read does
   not list them (see Detection), so use the per-mint WARN lines. After one
   hour, use the stuck filter above. After about 6 hours, that filter is the
   only source, because the WARN lines stop when the jobs are `Killed`.
2. Check the liquidity bot's side before anything else: is its delivery job
   running, and does its deployed config carry the `[orchestrator]` section with
   THIS chain's address? A missing or stale address means it is signing
   `MintAuth`s for the wrong contract, and no amount of redelivery fixes that
   (step 7's cutover pre-checks verify both).

   Then have it redeliver to
   `POST /internal/mints/<tokenization_request_id>/authorization`. Redelivery is
   the designed repair vector: an identical redelivery is idempotent and
   re-drives mint recovery, which covers the case where the first delivery
   recorded but its wake was lost. If the first delivery was recorded, the
   redelivery must be byte-identical: a different nonce is then a conflicting
   authorization (step 3).

   The ops client has no verb for this route: the liquidity bot sends the
   redelivery (a JSON body with `nonce` and `signature`). A refusal comes back
   as a JSON body that names its cause (for example a conflicting
   authorization). A `502` body says only that on-chain validation is
   unavailable. Step 3 needs that cause, so get it from the liquidity bot's
   side.

   An empty `"0x"` signature is valid for a contract recipient authorized
   through the orchestrator's `authorizeMint` callback; it is not a way to skip
   the signature for an EOA, which is refused.
3. If redelivery does not work, read the delivery's response. It tells you which
   case you have, and each case needs a different action.

   - **`409` "A different authorization is already recorded for this mint".**
     The mint already has an authorization, so it no longer waits on delivery:
     the submit path uses the recorded one. Most likely its recovery job gave
     up. Re-drive it with `ops debug reprocess-mint <issuer_request_id>` and
     check that it leaves `Minting`. If it then fails with
     `BadRecipientSignature` or `RecipientCallbackRejected`, the recorded
     authorization can never work (reprocess refuses those): close the mint, as
     below.
   - **`409` "Authorization not acceptable: mint is in state ..."**. The mint no
     longer waits on delivery, so a new nonce does not help. Read its state in
     the stuck read and escalate to engineering.
   - **`409` "The mint was modified concurrently; retry the delivery"**, or a
     **`500`**. A `500` can come after the authorization was recorded. Deliver
     the same payload again: an identical redelivery is idempotent. Then search
     the log for "mint_view does not show this mint holding its nonce": if it is
     there, the duplicate-nonce guard cannot see this claim until the next
     startup, so escalate to engineering for a stack restart (devops).
   - **Any other refusal** (a `409` nonce held by another mint, a `422` invalid
     or consumed nonce, a `502` read failure). Nothing was recorded. Do NOT
     close the mint: a mint in `Minting` still accepts its first authorization
     (`Mint::accepts_mint_authorization`, `src/mint/mod.rs`). Have the liquidity
     bot deliver again with a new nonce, or wait until signing is back if the
     signer or Turnkey is down.

   Closing does NOT reverse the Alpaca journal that already completed.
   `CloseMint` only records the mint as closed. After a close, the AP's shares
   are journaled to our custodian and no tokens back them. A fresh initiation
   journals the backing a second time. So before anyone re-initiates the AP's
   position, reconcile the completed journal by hand with Alpaca and record the
   outcome with the cutover.

   The close is a breakglass call. Set `--reason` to the real cause: it stays in
   the mint's history.

   ```sh
   ops breakglass close-mint <issuer_request_id> \
     --reason "authorization never delivered"
   ```

   The client prints a refusal on stderr as
   `error: HTTP 422 Unprocessable Entity: the request failed.`, then the bot's
   JSON body after `Server said:`, a request id, and a Cloud Logging link that
   searches for the mint id. It exits 1. The pre-close gate's `422`s name their
   cause in that body. A refusal from the aggregate itself (for example a
   missing or wrong `--acknowledged-unresolved-mint-tx-hash`) comes back as the
   generic body `{"error":"Unprocessable Entity","status":422}`. Its cause is in
   the service log, on the `Failed to close mint` line for that mint (its
   `aggregate_id`).

   A mint that already holds a prepared deposit identity is closed in two
   layers, and the order matters. First, the pre-close gate
   (`refuse_unsafe_close`, `src/admin.rs`) runs before `CloseMint` is sent and
   never sees the acknowledgement fields: every persisted signed transaction
   with no terminal outcome must be provably unable to land — a finalized
   reverted receipt, or the signing wallet's finalized nonce past it — otherwise
   the close is refused with `UnresolvedPersistedTx` whatever the call carries.
   Prove the prepared transaction dead on-chain first. Only then does the
   aggregate's requirement apply: the call needs
   `--acknowledged-unresolved-mint-tx-hash` alongside `--reason`, echoing that
   mint's exact prepared hash, or the aggregate answers `422` in turn. That
   acknowledgement states the transaction is UNRESOLVED, not dead — so the close
   keeps holding the mint's `(recipient, nonce)` pair and a later delivery of
   the same nonce is still refused. Pass `--acknowledged-unresolved-mint-nonce`
   only to close a `NonceReplayUnresolved` mint, and only once its absence is
   verified against a chain view outside this bot; that is what releases the
   pair.
4. Record the cause with the cutover. A delivery that never arrives is a
   liquidity-bot or Turnkey-policy failure, not an issuance one, and the fix
   belongs on that side.

## Shortfall escalation (InsufficientReceipts)

An orchestrator burn that reverts `InsufficientReceipts(token, shortfall)` puts
the redemption in a failed, manual-recovery-only state — the bot never
auto-retries through it and never mints to cover a shortfall (see SPEC "Failure
States"). Escalation:

1. Put the receipts the burn needs back in reach. No `EMERGENCY_ROLE` action
   moves receipts INTO the orchestrator: its emergency functions only move
   assets out (`withdrawReceipt`, `withdrawShares`, `sweepERC1155`) or move the
   burn pointer (`setBurnIndex`). So the holder of the missing receipts
   transfers them to the orchestrator with a plain ERC-1155 transfer (if the bot
   wallet holds them, escalate to engineering). If the receipts are already at
   the orchestrator but behind the burn pointer, page the `EMERGENCY_ROLE`
   holder (recorded above) to lower it with `setBurnIndex` (see SPEC "Contract
   Summary" on `EMERGENCY_ROLE`).
2. Once receipts are in place, re-drive each redemption of the token that failed
   this way (each has a `BurnFailed` row in the stuck read) via the existing
   recovery surface — `ops debug recover-redemption <id>`, which issues
   `ResumeBurn`; the failure classification blocks only _automatic_ retries, not
   the manual re-drive.

A pre-submit `VaultLogicMismatch` halt is different — the orchestrator is
version-locked against upgraded vault beacons, no redemption has failed, and the
bot defers until `vaultLogicIsExpected()` reads true again (visible in
`ops read orchestrator-health` and the preflight). When the mismatch instead
races a transaction that was already submitted, the redemption records a
classified `BurningFailed` (`VaultLogicMismatch`/`ReceiptLogicMismatch`) that is
never auto-retried; once the health check reads true again, re-drive it via the
manual `ResumeBurn` admin recovery — same as the shortfall's step 2.
