# Orchestrator onboarding and per-asset cutover (RAI-1221 / RAI-1222)

Two ordered procedures in one document. **Onboarding** (steps 1–6) is the ops
work that must be complete for an asset before its `vault_mode` can flip to
`"orchestrator"`: roles and the Turnkey policy are orchestrator-wide — done
once, before the pilot — while approvals are per asset (RKLB's before the pilot
cutover, each remaining asset's before its own). **Cutover** (steps 7–14) is the
per-asset procedure that actually moves an asset onto the orchestrator, authored
for the RKLB pilot (RAI-1222) and reused verbatim for every later asset
(RAI-1246).

Both procedures run against the DigitalOcean droplet: the issuer host, the
unit's `CONFIG` path, `docs/runbooks/deploy-hold.md` and `prodDeployNixos`. They
apply only while the `PRODUCTION_RELEASES_ENABLED` repository variable is off.
When it is on, production runs on the GCP bot VM (README "GCP release path"),
which none of these steps reach, and no GCP procedure exists yet. Step 7 checks
the variable before the freeze. Do not turn it on while any asset is in
orchestrator mode: the rollback (step 14) would then have no procedure.

There is no testnet or staging chain for this: every step below runs against
prod (Base mainnet) and is verified by on-chain reads. The first live end-to-end
orchestrator mint/burn is the RKLB pilot's manual exercise (step 13), which
everything before it must fully precede. The full cutover cycle — migrate,
operate, roll back, resume — is rehearsed twice before prod. The Anvil
end-to-end suite (`tests/receipt_custody.rs`,
`test_receipt_custody_migrates_into_the_orchestrator`) runs it on contracts it
deploys itself. The fork rehearsal (`tests/fork_rehearsal.rs`) runs it on a
local Anvil fork of Base, against the real RKLB vault, receipts and
orchestrator. It is ignored by default; run it, as a gate before the pilot, with
a Base RPC that can serve state at the fork block:

```
FORK_RPC_URL=<Base RPC> FORK_BLOCK=<recent block> \
  cargo test --test fork_rehearsal -- --ignored --nocapture
```

The fork signs nothing with a real key and changes nothing on Base. It does not
exercise Turnkey signing or the `issuer` commands below, which accept only the
Turnkey signer.

## Prerequisites

- Turnkey is the live signer (RAI-1123 done, Fireblocks retired): the service
  runs with the `TURNKEY_*` env group, and the bot wallet is `TURNKEY_ADDRESS`.
- The orchestrator is deployed by st0x.deploy (PR #222/#223) and its address is
  known; the permissions scripts have run.
- The liquidity-bot counterpart (RAI-1243) is tracked separately — it gates the
  RAI-1222 cutover, not this procedure. Its release plumbing, once the issuance
  orchestrator stack merges to main: cut a `st0x-issuance-client` /
  `st0x-issuance-dto` release tag from main, swap the liquidity repo's
  `Cargo.toml` git-branch pin back to that tag (the pin carries a swap-back
  comment marking the spot), and deploy the liquidity bot from the swapped pin.
  Step 7's cutover pre-check ("pin is on the release tag") verifies this
  happened.

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
  git tag -l 'v[0-9]*' --sort=-v:refname | head -1
  ```

  Pick the next version above that tip when the tag is cut, then set it once and
  reuse it everywhere below instead of pasting a literal:

  ```sh
  export RELEASE_TAG=vX.Y.Z    # replace with the tag actually cut; record it in the table
  ```

  Re-tagging an existing version is not a harmless retry: `release-tag.yml`
  moves that version label onto an attested digest, and production manifests pin
  against the label.

  Record the value in the prod-facts table. A literal pasted into a check is how
  this drifted from reality the first time.

All `issuer` subcommands below run on the issuer host (over SSH) with the
service's own environment; they refuse a local-key signer because every fact
they verify or establish is keyed to the Turnkey bot wallet. The orchestrator
address is never typed — it comes from the TOML config file — the bot wallet
from `TURNKEY_ADDRESS`, and vault/receipt addresses from the listing view and
on-chain resolution. The one address argument of these subcommands,
`move-receipts --to`, exists only for the wallet-rotation path, refuses the
configured orchestrator address and any other contract, and is guarded by the
kind-aware corroboration witness (see SPEC "Receipt custody").

Every call in this runbook that sends `X-API-KEY` runs on the issuer host,
against the service's own listener: the admin calls (`/admin/*`), the asset read
(`GET /tokenized-assets/<SYM>`) and the `/status` reads. The public HTTPS origin
cannot serve them. nginx refuses the admin calls and the asset read
(`nix/ingress.nix`), and it passes `/status` with the caller's address, which
`InternalAuth` checks against `INTERNAL_IP_RANGES` (`src/auth/mod.rs`). On the
issuer host the call comes from loopback, and the key never leaves the host.

The `issuer` wrapper loads the service's environment only for its own process,
and the host has no `jq` or `cast`. So set up one shell on the issuer host and
run every host command of this runbook in it, the `cast` reads included: the RPC
URLs are secrets of the service's environment and stay on the host.

```sh
# On the issuer host, as root.
nix shell nixpkgs#jq nixpkgs#foundry   # run the lines below in this shell
set -a; . /run/agenix/st0x-issuance.env; set +a   # the service's environment
export CONFIG="$(systemctl show st0x-issuance -p Environment --value \
  | tr ' ' '\n' | sed -n 's/^CONFIG=//p')"   # the unit's own config file
test -f "$CONFIG"
export ISSUER_BASE_URL=http://127.0.0.1:8001   # prod runs behindProxy (#443)
export INTERNAL_API_KEY="$ISSUER_API_KEY"   # the service's key; never echo it
```

Open this shell after the step-1 release is deployed, and run the `CONFIG` line
again after every system-profile deploy (step 12, rollback step 6): it reads the
unit's file at that moment, and an older file lacks the newer entries. The
`issuer` commands find each chain's RPC the way the service does, so they take
no `--rpc-url`. The `cast` reads take a URL: Base's is `$RPC_URL` (or
`$CHAIN_BASE_RPC_URL`, where the grouped form is set), and every other chain's
is its `$CHAIN_<NETWORK>_RPC_URL`. A chain that reaches its RPC only through
`ALCHEMY_API_KEY` has no such variable. For that chain, set its own variable in
this shell to `https://<host>.g.alchemy.com/v2/$ALCHEMY_API_KEY`, where `<host>`
is `base-mainnet`, `eth-mainnet`, `hyperliquid-mainnet` or `robinhood-mainnet`.
Never set one chain's URL under another chain's name, and never echo it: the URL
holds the key. The bot wallet is `$TURNKEY_ADDRESS`. The `git`, `gh` and
`nix run .#…Deploy…` commands run on your workstation, in a checkout of this
repo.

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
requires an entry for **every** configured chain. Every `issuer` verification
below runs per `--network` against that network's entry, and an asset's cutover
(steps 7–14) runs per chain it is listed on. Verify locally with
`cargo test --lib deploy_config_files_parse_and_stay_dark`, which parses both
deploy configs strictly (`validate-config` without `CONFIG` never reads the
file). Then merge it and ship it in a release from main. The config file is
baked into the systemd unit (`CONFIG=<nix store path>`, see
`nix/upgradeable-services.nix`). Every `issuer` command below that reads the
orchestrator address passes `--config "$CONFIG"` — the unit's own value, set in
the host shell above — so the CLI provably validates and approves against the
exact file the running service resolves, never a stray local copy.

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

Record the policy name(s) in the table below.

## 4. Prove the policy by signing (nothing broadcast)

```
issuer verify-orchestrator-signing RKLB \
  --config "$CONFIG" \
  --network base --chain-id 8453
```

Signs one transaction per shape in the table above — never broadcasting — and
fails naming the refused shape if the policy denies one. Run it per asset as
each asset approaches cutover (the approve/transfer shapes are token-scoped). A
policy gap surfaces here as a named refusal instead of during the pilot's first
live mint.

## 5. Execute the approval (per asset, staged with the rollout)

```
issuer approve-orchestrator RKLB \
  --config "$CONFIG" \
  --network base --chain-id 8453
```

One-time unlimited ERC-20 approval, bot wallet → orchestrator, on the asset's
vault share token, signed by Turnkey after an explicit confirmation. Before
sending, the command verifies the configured address answers as an orchestrator
(interface reads plus a healthy `vaultLogicIsExpected()`), so a typo'd or stale
`[orchestrator.addresses]` entry is refused rather than granted an unlimited
allowance. Idempotent: a re-run reports "already unlimited" and sends nothing,
so batching every asset's approval early is safe — approvals are inert until the
asset's `vault_mode` flips. Success is re-verified by an on-chain allowance
read. When this step actually submits, the transaction is also live proof that
the policy's `approve` allowance works; the idempotent no-op path proves nothing
new — step 4's signing proof covers `approve` in that case.

If it fails with "submitted, unconfirmed", the approval was signed but not
confirmed within 5 minutes. Look the printed hash up on the explorer before
doing anything else: once it lands, a re-run sends nothing; if the explorer does
not know it, nothing was sent and a re-run is safe. If it stays pending without
mining, it holds the wallet's nonce and later mints and burns on that network
queue behind it until it mines or that nonce is replaced through Turnkey;
re-running the approval only adds another transaction behind it.

Record each executed approval in the table below.

## 6. Final gate: preflight must print READY

```
issuer orchestrator-preflight \
  --config "$CONFIG" \
  --network base --chain-id 8453 \
  --asset RKLB
```

On-chain read-only (locally it runs any pending database migrations and
projection catch-up before the asset lookup). Checks `hasRole(MINT_ROLE, bot)`,
`hasRole(BURN_ROLE, bot)`, `vaultLogicIsExpected()`, and per asset
`allowance(bot, orchestrator)` plus the orchestrator's `DEPOSIT`/`WITHDRAW`
grants on the vault's authorizer. The two scopes serve different moments:
`--asset <SYM>` is the explicit per-asset cutover gate — it works while the
asset is still vault-direct in config, which is exactly when the RAI-1222
pre-check runs; omitting `--asset` is the aggregate sweep over the assets whose
configured `vault_mode` already resolves to orchestrator (a standing health
re-check, not a cutover gate). The gate is the **exit status**: zero only when
every check passes, so the RAI-1222 pre-checks gate on the exit code for the
asset being cut over; `Overall: READY` is the human-readable rendering of the
same verdict.

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
one row per `(underlying, network)` listing, which `/admin/orchestrator-health`
emits regardless of mode). Make this the first per-chain action and record its
output with the cutover:

```sh
curl -fsS -H "X-API-KEY: $INTERNAL_API_KEY" "$ISSUER_BASE_URL/admin/orchestrator-health" \
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
that chain's `--network` and `--chain-id` (and that chain's RPC URL for the
`cast` reads), and record each chain's output separately.

| Step                                     | Scope                                                |
| ---------------------------------------- | ---------------------------------------------------- |
| 4 signing proof, 5 approval, 6 preflight | **per chain**                                        |
| 7 pre-checks                             | **per chain**, except the deployment-wide ones       |
| 8 freeze and drain                       | per symbol — one freeze covers every listing         |
| 9 snapshot, 10 move, 11 verify           | **per chain**                                        |
| 12 flip                                  | per symbol — one config edit, one deploy             |
| 12 post-flip health verification         | **per chain**                                        |
| 13 validation                            | **per chain** that carries real flow                 |
| 14 rollback                              | per symbol to flip, **per chain** to return receipts |

Freeze, unfreeze and status address the `Underlying` aggregate and take no
network argument by design, so they need no repetition. Everything that names a
vault, a receipt contract or an orchestrator address does.

**Zero-supply chains.** A chain the symbol is listed on but that carries no
supply is still in the flip — steps 4–7 and the post-flip health check still run
for it — but there is nothing to move. The symbol's chain set is every network
where it is listed; confirm it on the issuer host, since a `200` is a listing
and a `404` is not:

```sh
for NETWORK in base ethereum hyperevm robinhood binance; do
  printf '%s: ' "$NETWORK"
  curl -sS -o /dev/null -w '%{http_code}\n' \
    -H "X-API-KEY: $INTERNAL_API_KEY" \
    "$ISSUER_BASE_URL/tokenized-assets/<SYM>?network=$NETWORK"
done
```

An empty chain has nothing to snapshot or move, so skip the step-9 snapshot and
steps 10–11 for it. The step-9 deployment hold is deployment-wide and still
applies. Nothing else is needed: the cutover records no custody on any chain
(step 10), so an empty chain after the flip behaves like a chain whose receipts
moved: its inventory stays empty.

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
  for ID in $(seq 1 "$HIGHWATER"); do
    BAL=$(cast call "$RECEIPT" 'balanceOf(address,uint256)(uint256)' \
      <bot wallet> "$ID" --rpc-url "$RPC_URL" | cut -d' ' -f1)
    [ "$BAL" = 0 ] || { echo "$ID: $BAL"; NONZERO=$((NONZERO + 1)); }
  done
  echo "OK: read all $HIGHWATER ids, $NONZERO non-zero"
)
```

Run it in the host shell, with `<vault>` from the chain-set response,
`<bot wallet>` as `$TURNKEY_ADDRESS`, and the RPC URL of the chain you check. It
prints each non-zero id with its amount, and its last line gives the verdict.
Without the `OK` line, a call failed or the RPC serves another chain, even when
no row was printed.

The chain is legitimately empty only when the last line is
`OK: read all N ids, 0 non-zero` AND the vault's share supply is zero:
`cast call <vault> 'totalSupply()(uint256)' --rpc-url "$RPC_URL"` → `0`. Every
deposit mints its shares and its receipt in the same amount, so a non-zero
supply with no receipt at the bot wallet means a wrong address or receipts held
elsewhere: stop and escalate to engineering. For an empty chain, record
`no receipts` in its receipt-move, snapshot and verification columns, and skip
the step-9 snapshot and steps 10–11 for it. A non-zero count means the chain has
receipts, and it goes through steps 9–11 like any other chain. If
`move-receipts` then refuses with "nothing to move" or `InventoryEmpty` on such
a chain, the inventory is behind the chain. If no chain has moved yet, leave the
window as in the step-10 detour (release the hold, start the service with the
service deployment only, unfreeze), then escalate to engineering. If any chain
has moved, keep the hold armed and escalate.

If the per-`(symbol, network)` override is ever built, this section is what
changes: cutover would then target one chain at a time and the table collapses
to "per chain" throughout.

## 7. Cutover pre-checks (gate on exit codes, not eyeballs)

All of these must hold for the asset being cut over, immediately before its
window. Every check is a command with an expected exit status, except two manual
reviews: the domain and implementation-hash comparison, and the deny-side
Turnkey policy review. Record each command's output, and the result of each
review, with the cutover. Repeat the per-chain checks for each chain in the
derived set; only the `PRODUCTION_RELEASES_ENABLED` check, the fork rehearsal
and the liquidity bot's deployed-revision, tag/pin and config-address checks are
deployment-wide and run once. The Turnkey MintAuth policy checks are NOT
deployment-wide: the orchestrator's EIP-712 domain carries the id of the chain
it runs on, so they run per chain too (see the policy bullet):

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
  cast call <orchestrator address> \
    'eip712Domain()(bytes1,string,string,uint256,address,bytes32,uint256[])' \
    --rpc-url "$RPC_URL"
  # ERC-1967 beacon slot: keccak256("eip1967.proxy.beacon") - 1.
  BEACON=$(cast parse-bytes32-address "$(cast storage <orchestrator address> \
    0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50 \
    --rpc-url "$RPC_URL")")
  IMPL=$(cast call "$BEACON" 'implementation()(address)' --rpc-url "$RPC_URL")
  cast call "$BEACON" 'owner()(address)' --rpc-url "$RPC_URL"
  cast keccak "$(cast code "$IMPL" --rpc-url "$RPC_URL")"
  ```

  → name `ST0xOrchestrator`, version `1`, `chainId` equal to this chain's
  `--chain-id`, `verifyingContract` equal to the configured address; and the
  implementation code hash equal on every chain in the set. Record the domain,
  the beacon address, the beacon owner and the implementation hash in this
  chain's prod-facts row. A different implementation hash on one chain is a
  stop: that chain runs different orchestrator logic.
- Step 6's preflight exits zero for this asset:
  `issuer orchestrator-preflight --asset <SYM> …` → exit 0.
- Step 4's signing proof exits zero for this asset (it covers the ERC-1155
  `safeBatchTransferFrom` shape the receipt move, step 10, submits):
  `issuer verify-orchestrator-signing <SYM> …` → exit 0.
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
- `PRODUCTION_RELEASES_ENABLED` is off, so production runs on the droplet that
  this procedure reaches (see the top of this document). An unset variable
  counts as off, and a failed `gh` call fails the check:

  ```sh
  gh variable list --repo ST0x-Technology/st0x.issuance --json name,value \
    | jq -e 'all(.[]; .name != "PRODUCTION_RELEASES_ENABLED"
              or (.value | ascii_downcase) != "true")'
  ```

  → exit 0.
- For the RKLB pilot, the fork rehearsal passes on the code that production
  runs. It covers RKLB on Base only, so it runs once, not per chain. On your
  workstation, use a checkout of the commit that
  `/run/st0x/st0x-issuance.git-rev` names, with no local changes. Set
  `FORK_EMERGENCY_HOLDER` to the `EMERGENCY_ROLE` holder recorded for Base, so
  the rollback runs as that holder. Set `FORK_BLOCK` to a Base block from the
  day of the window:

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
  curl -fsS -H "X-API-KEY: $INTERNAL_API_KEY" "$ISSUER_BASE_URL/admin/stuck" \
    | jq -e --arg sym <SYM> \
        '[.stuck[] | select(.underlying == $sym)] | length == 0'
  ```

  → exit 0 (`jq -e` fails the check if any entry names the asset).
- The status endpoint the liquidity bot polls serves the mode field, still
  reading vault-direct pre-flip:

  ```sh
  curl -fsS -H "X-API-KEY: $INTERNAL_API_KEY" \
    "$ISSUER_BASE_URL/tokenized-assets/<SYM>/status" \
    | jq -e '.vault_mode == "vault_direct"'
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
  - BOTH issuance pins are on `$RELEASE_TAG`, and neither is on a branch — one
    check per dependency, so a single correct pin cannot vouch for the other:
    `grep -E "st0x-issuance-client.*tag *= *\"$RELEASE_TAG\"" Cargo.toml` → exit
    0, `grep -E "st0x-issuance-dto.*tag *= *\"$RELEASE_TAG\"" Cargo.toml` → exit
    0, and `! grep -E 'st0x-issuance-(client|dto).*branch' Cargo.toml` → exit 0.
    (Double quotes, so `$RELEASE_TAG` expands — single quotes would search for
    the literal variable name and fail closed.)
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
    struct is refused. Two checks, one per side of that claim:
    - **Allow side** — the scoped grant admits our exact payload; executable
      proof against prod Turnkey, run **once per chain in the set** with that
      chain's id in the payload's `chainId`:
      `ST0X_ORCHESTRATOR_ADDRESS=<orchestrator address> cargo test
      turnkey_typed_signing_integration -- --ignored`
      → exit 0 on every chain. (The test lives in the liquidity repo; read how
      it takes the chain before running it — a run that hardcodes one chain
      proves only that chain. It also only exercises the permitted path — an
      overly broad policy would also pass it, which is why the deny side needs
      its own check.)
    - **Deny side** — default-deny only holds if no OTHER policy can approve
      signing for this wallet. Two parts:
      1. Review EVERY effective allow policy in the org that names the bot
         wallet, its private key, or any of their tags — conditional policies
         included (a condition that a bare-digest request can satisfy is a
         grant, "unconditioned" is not the bar), across all policy versions and
         any categorical signing allowances. Confirm the scoped MintAuth policy
         above is the ONLY one whose condition a raw-payload signing activity
         for this wallet can satisfy — checking the whole activity family, not
         just `ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2`: earlier raw-payload versions
         and the batch `SIGN_RAW_PAYLOADS` variants are the same signing
         surface. Record the reviewed policy IDs with the cutover record.
      2. Executable negative probes, each submitted for the bot wallet with the
         bot's own API key via Turnkey's API/SDK, and each of which must be
         DENIED by policy (nothing is broadcast even if signed; a signature
         would only prove the policy gap). Record every denied activity ID with
         the cutover; ANY signed result is a cutover blocker — some policy
         grants more than the scoped MintAuth shape:
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
           - correct struct and contract, `chainId` set to a chain OUTSIDE the
             derived set — proves the `chain_id` condition exists and is bounded
             to the set. (A chain inside the set must be admitted, so probing
             with one of them proves nothing.)

## 8. Freeze and drain

Freeze the asset (`issuer freeze <SYM>`): `POST /inkind/issuance` rejects frozen
assets before Alpaca journals shares, so nothing gets stranded mid-flow. Wait
for this asset's in-flight mints and redemptions to reach terminal states
(re-check step 7's `/admin/stuck` check). The freeze-plus-drain is also what
guarantees no aggregate straddles the mode flip — an operation's mode anchors
once, at `Initiated` / `RedemptionDetected`.

`/admin/stuck` does not prove the drain: it hides work that entered its current
state less than one hour ago, and redemptions that the freeze holds. The
quiescence gate of `move-receipts` in step 10 proves it.

## 9. Deploy hold and snapshot

Have the step 12 deploy checkout ready first: the reviewed flip cherry-picked
onto what production runs, and pushed (step 12 says how). A cherry-pick conflict
must not fall inside the window. Then arm the deployment hold and stop the
service per `docs/runbooks/deploy-hold.md` — `move-receipts` refuses without it,
because the engine's projection rebuilds and quiescence reads must not race a
running service. Then snapshot: record this token's per-receipt on-chain
balances of BOTH wallets (run the "Zero-supply chains" loop once as written and
once with the orchestrator address in place of `<bot wallet>`, and keep both
outputs) — the bot wallet and the orchestrator's pre-move balances for the same
receipt ids (the engine deliberately allows a destination with pre-existing
balances and verifies per-identifier GAINS, so the verification in step 11 needs
the before-values). Do not compare the bot wallet with `receipt_inventory_view`:
it projects only `Mint` events, so every burned receipt reads as a discrepancy.
`move-receipts` checks the tracked balances against the chain itself (step 10).

After the hold is armed, run the bot-wallet loop from "Zero-supply chains" again
on each chain marked `no receipts`. Step 8 lets in-flight mints finish, and a
vault-direct mint can put a receipt at the bot wallet after the first scan. The
chain stays `no receipts` only if the loop still ends with
`OK: read all N ids, 0 non-zero`; otherwise snapshot it as above and run steps
10–11 for it like any other chain.

If no chain goes through step 10, the quiescence gate of `move-receipts` never
runs, so run it this way: with the hold armed, run
`issuer confirm-custody
<SYM>` once on one chain (rollback step 6 shows the
command) and answer yes at its prompt. A drained asset ends with the
`InventoryEmpty` refusal ("has no tracked receipts"). `MintsInFlight`,
`RedemptionsInFlight` or `BurnReserved` means that work is still open: take the
step-10 detour.

## 10. Move the receipts

```
issuer move-receipts <SYM> \
  --to-configured-orchestrator \
  --config "$CONFIG" \
  --network base --chain-id 8453
```

The destination is read from the `--network`'s `[orchestrator.addresses]` entry
— never typed — and corroborated as an ERC-1155-receiving contract before
anything is signed. A configured address that is not such a contract is refused
before the prompt. The command prompts with the asset, vault, holder,
destination and its corroborated kind, and the tracked receipt count. Before any
chain moves, run the move once per chain and answer no, and compare each count
with that chain's non-zero count from the step-9 snapshot. Answer yes only when
every chain matches. A mismatch means inventory is behind the chain: take the
detour below, with nothing moved, and escalate to engineering. A vault tracking
more than 14 receipts moves in multiple bounded transactions, each verified
before the next. A re-run after any interruption is safe: an interrupted move
resumes with only the remaining receipts, and a completed move reports "already
migrated" and submits nothing. That is true only until the service starts again
(step 12). After that start, a re-run (with the hold armed again) refuses before
the prompt with "nothing to move", and its message says this is expected after a
cutover: the moved receipts are no longer in inventory.

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
after the freeze is held, and `/admin/stuck` does not show it while the asset is
frozen. The config is still vault-direct, so release the hold, start the service
with the service deployment only, from a checkout of the commit that
`/run/st0x/st0x-issuance.git-rev` names (the flip is not deployed until step
12), unfreeze until the work completes, then go back to step 8. A failed row
that recovery does not retry (a classified failure, a `JournalRejected` mint, a
`Failed` redemption) does not drain on a restart: resolve it as in rollback
step 3. This detour applies only while no chain has moved yet. If a refusal
comes after any chain moved, keep the hold armed and escalate to engineering: a
vault-direct restart would drop the moved receipts from inventory while the
orchestrator holds them.

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
  check is the one the engine itself enforces.)
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

  Read the pointer on-chain — the service is stopped at this point (step 9), and
  `GET /admin/orchestrator-health` fills `next_burn_receipt_id` only for assets
  whose resolved mode is already orchestrator, which this one is not until step
  12:

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
  service is stopped, and nothing reads the chain until step 12 starts it.

## 12. Flip, deploy, unfreeze

Set `vault_mode = "orchestrator"` in the asset's `[assets.<SYM>]` table of the
TOML config, in a reviewed commit on main. The flip must live on main: a release
deploys the `config.prod.toml` of the tag it deploys (`deploy-prod.yaml` checks
out the tag), and tags come from main. A flip that exists only in your checkout
is undone by the next release, and so is a deploy of a tag cut before the flip.
The orchestrator then holds every receipt, so the next redemption completes its
Alpaca journal and then cannot burn (step 13 checks for this after every
deploy). `deploy_config_files_parse_and_stay_dark` (`src/config.rs`) refuses any
asset override in both config files, so the same commit changes it to accept
exactly this one. Merge it after step 11 verified the move.

The system profile and the service deployment below ship your checkout, so build
them from what production runs plus only the flip. Before step 9, check out what
production runs: the live release tag, plus every earlier flip deployed on top
of it (`/run/st0x/st0x-issuance.git-rev` on the issuer host names the deployed
commit). Cherry-pick the reviewed flip commit onto it, and run both deploys from
that checkout. Push that checkout to a branch named for this cutover, and record
the branch and its commit with the cutover: the next cutover builds on it. Code
merged to main since the live tag then waits for its own release, and nothing
can stop the window halfway.

The service reads that file through the unit's `CONFIG` path, and only the
system profile installs a new one: the service deployment in
`docs/runbooks/deploy-hold.md` restarts the unit with the old file. So, with the
hold still armed, deploy the system profile from that checkout first. It updates
the unit and does not start it (`restartIfChanged = false`):

```sh
nix run .#prodDeployNixos -- -i "$SSH_IDENTITY"
```

Then release the hold and run the service deployment (the deploy activation
restarts the unit). Wait for the start: startup rebuilds views, backfills and
reconciles before Rocket binds 127.0.0.1:8001, which can take several minutes.
Confirm the start as `docs/runbooks/deploy-hold.md` describes. A connection
error before the start completes is not a failure: retry. Then verify in the
host shell that `/admin/orchestrator-health` reports orchestrator mode on every
chain of the asset (the status endpoint reads the same config):

```sh
curl -fsS -H "X-API-KEY: $INTERNAL_API_KEY" "$ISSUER_BASE_URL/admin/orchestrator-health" \
  | jq -e --arg sym <SYM> \
      '[.assets[] | select(.underlying == $sym) | .vault_mode]
       | length > 0 and all(. == "orchestrator")'
```

On this start, the backfiller and startup reconciliation read zero at the bot
wallet for every moved receipt and remove it from inventory. After the start,
the vault's inventory is empty. That is the expected state, not lost receipts:
the orchestrator holds them on-chain (step 11). A `CustodyDisplaced` or
`CustodyUnconfirmed` refusal here is a real problem, not cutover noise. If the
start fails or the health check fails after it, keep the asset frozen, follow
"If validation or activation fails" in `docs/runbooks/deploy-hold.md`, and
escalate to engineering.

Before you unfreeze, check that main carries the flip. The deploy above came
from your checkout, so only the merge keeps the flip in the next release. On
your workstation:

```sh
git fetch origin main \
  && git show origin/main:config.prod.toml \
  | grep -A1 -i -E '^\[ *assets\."?<SYM>"? *\]' \
  | grep -q -E '^vault_mode = "orchestrator"'
```

→ exit 0. If it fails, keep the asset frozen until the flip merges. Then
unfreeze.

## 13. Pilot validation (manual, in prod)

RKLB has essentially no organic flow — that is the point — so validation is
actively driven. Manually trigger a small mint through the liquidity bot's
rebalancing path (exercising the real MintAuthV1 delivery) and a small
redemption (send tokens to the redemption wallet), and follow every stage
end-to-end: authorization delivery, Turnkey signing, orchestrator
`Minted`/`Burned` events, Alpaca journals/callbacks, `/admin` health.

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
every attempt completes, no unexplained `/admin/stuck` entries, and any
transient failure exercises a recovery path observably. Before any deploy in the
window, check on your workstation that the tag (or commit) carries the flip:

```sh
git show <tag>:config.prod.toml \
  | grep -A1 -i -E '^\[ *assets\."?<SYM>"? *\]' \
  | grep -q -E '^vault_mode = "orchestrator"'
```

→ exit 0. After every deploy in the window, run the step 12
`/admin/orchestrator-health` check again. If it fails, freeze the asset at once,
deploy a source that carries the flip, and escalate to engineering every
redemption detected and every mint initiated since that deploy. Their mode is
fixed as vault-direct: such a redemption cannot burn, and such a mint puts its
receipt at the bot wallet, out of reach of orchestrator burns. The freeze stops
only new mints, so wait until every escalated mint is completed or closed. Then,
before you unfreeze, run the bot-wallet loop on each chain: it must end with
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

1. Freeze the asset and let in-flight work drain (step 8's procedure).
   `/admin/stuck` does not prove the drain: it hides work that entered its
   current state less than one hour ago, and redemptions that the freeze holds.
   Step 3 proves it.
2. Prepare the flip back as a reviewed commit on main that comments out the
   `[assets.<SYM>]` table again and restores the dark test (as in step 12). An
   explicit `vault_mode = "vault_direct"` is still an override: the dark test
   refuses it, and the step 6 check fails. Do not merge or deploy the commit
   until step 6. Every start before step 6 must still run the orchestrator
   config: the orchestrator holds the receipts until step 4, and a redemption
   detected under the vault-direct config cannot burn. Prepare the step 6 deploy
   checkout now as well: what production runs, with the flip back cherry-picked
   onto it.
3. Arm the deployment hold and stop the service (per
   `docs/runbooks/deploy-hold.md`) — **before** any receipt moves, for the same
   reason step 9 requires it on the way in: the returned receipts must reach
   inventory through one clean start, not through a pass that races the
   withdrawals.

   Then prove that no mint or redemption for the asset is open. No command lists
   that work while the asset is frozen, but the quiescence gate of
   `confirm-custody` counts all of it, at any age, held redemptions included.
   Run it once, on one chain (the command is in rollback step 6), and answer yes
   at its prompt. In orchestrator mode the inventory is empty, so a drained
   asset ends with the `InventoryEmpty` refusal. `MintsInFlight`,
   `RedemptionsInFlight` or `BurnReserved` means that work is still open.
   Release the hold, restart the service with the service deployment only, from
   a checkout of the commit that `/run/st0x/st0x-issuance.git-rev` names (the
   flip back is not deployed yet), and drain it. A held redemption runs only
   while the asset is not frozen, and it burns through the orchestrator, so
   unfreeze until it completes. A failed row that recovery does not retry (a
   classified `MintingFailed` or `BurnFailed`, a `JournalRejected` mint, a
   `Failed` redemption) does not drain on a restart. `/admin/stuck` lists it at
   any age. A `JournalRejected` mint holds no backing: confirm with Alpaca that
   its journal was rejected, then close it with the close call in step 3 of
   "Mint waiting on an authorization that never arrived", with a `reason` that
   names the rejection (the reason stays in the mint's history). Re-drive a
   `MintingFailed` mint with `POST /admin/reprocess/mint/<aggregate_id>`. If
   that route refuses it (`422`), close it as step 3 of that section describes,
   including its Alpaca reconciliation: the mint's journal completed, so the
   AP's shares are held with no tokens. If the re-drive is accepted but the mint
   fails again, escalate to engineering: it blocks step 4. Resolve a
   `BurnFailed` redemption through "Shortfall escalation". Any other failed row,
   such as a redemption whose Alpaca call failed, is an escalation to
   engineering, and it blocks step 4. Do all of this before step 4, while the
   orchestrator still holds the receipts. Then go back to step 1.
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
     for ID in $(seq 1 "$HIGHWATER"); do
       BAL=$(cast call "$RECEIPT" 'balanceOf(address,uint256)(uint256)' \
         <orchestrator> "$ID" --rpc-url "$RPC_URL" | cut -d' ' -f1)
       [ "$BAL" = 0 ] || { echo "$ID: $BAL"; NONZERO=$((NONZERO + 1)); }
     done
     echo "OK: read all $HIGHWATER ids, $NONZERO non-zero"
   )
   ```

   As in "Zero-supply chains", run it in the host shell with the RPC URL of the
   chain you check. Without the `OK` line, the read failed.

   Withdraw every id that it lists. Read its amount again just before its
   withdrawal, and use that reading as the `amount`. After each withdrawal,
   verify that the bot wallet's `balanceOf` for that id went up by exactly the
   amount you withdrew.
5. Run the loop from step 4 again. Its last line must be
   `OK: read all N ids, 0 non-zero`: the orchestrator holds nothing for the
   token. A listed id is a receipt still stranded at the orchestrator — withdraw
   it before you continue. This check is complete because it reads every id the
   vault ever issued. No log search and no custody command is needed: the
   cutover recorded no custody, so there is no custody to restore.
6. With the hold still armed, merge the flip back from step 2. Deploy the system
   profile from a checkout of what production runs (as in step 12: the commit
   that `/run/st0x/st0x-issuance.git-rev` names, with every other asset's flip
   still on it) with the flip back cherry-picked onto it, so the unit reads the
   flipped config (`prodDeployNixos`). Prepare that checkout in step 2, so
   nothing can stop the rollback halfway. Then release the hold, deploy, and let
   the service start. On this start the backfiller finds each `withdrawReceipt`
   as a transfer into the bot wallet and adds the receipt to inventory at its
   on-chain balance. Wait for the start and confirm it, as in step 12 (a
   connection error before the start completes is not a failure: retry). Before
   you unfreeze, check on the issuer host that the asset is back in vault-direct
   mode (this must exit 0):

   ```sh
   curl -fsS -H "X-API-KEY: $INTERNAL_API_KEY" \
     "$ISSUER_BASE_URL/tokenized-assets/<SYM>/status" \
     | jq -e '.vault_mode == "vault_direct"'
   ```

   Also check that inventory tracks every id step 4 returned at its balance.
   `receipt_inventory_view` cannot show this: it projects only `Mint` events, so
   it never sees the rediscovered receipts. No command reads the inventory yet,
   so use `confirm-custody` as the check. Arm the hold, stop the service, and
   run it once per chain whose receipts step 4 returned:

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
   nothing was open, and the service did not run again until this start. So an
   open redemption here was detected on this start, in vault-direct mode, and
   the freeze holds it. It is safe to run: release the hold, deploy, and
   unfreeze until it completes. Then freeze, arm the hold, stop the service, and
   run this check again. Its burn used up returned receipts, so the count must
   now equal the ids that the bot wallet still holds: run the step 4 loop with
   `$TURNKEY_ADDRESS` in place of `<orchestrator>`, and use its non-zero count.
   If that count is 0, `InventoryEmpty` is the expected result.

   If the count differs from what these rules expect, no runbook step fixes it.
   Lower means the backfill missed a returned receipt; higher means the bot
   wallet holds receipts that step 4 did not return. Keep the hold armed and the
   asset frozen, find the ids by comparing the step 4 list with the bot-wallet
   loop, and escalate to engineering.

   Then release the hold and deploy again. The returned receipts are recorded
   with source `External`, with no link to the mint that issued them. That is
   safe: only in-flight vault-direct mints use that link (mint recovery and the
   admin close gate), and step 3 and the freeze leave none.

   Before you unfreeze, check that main no longer carries the override, so the
   next release keeps the asset in vault-direct mode:

   ```sh
   # On your workstation.
   git fetch origin main \
     && git show origin/main:config.prod.toml > /tmp/main-config.toml \
     && ! grep -q -i -E '^\[ *assets\."?<SYM>"? *\]' /tmp/main-config.toml
   ```

   → exit 0. If it fails, keep the asset frozen until the flip back merges.
7. Unfreeze.

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

| Asset | Flip deploy | Deploy branch / commit | Soak end | Overall go/no-go |
| ----- | ----------- | ---------------------- | -------- | ---------------- |
| RKLB  |             |                        |          |                  |

## Mint waiting on an authorization that never arrived

Orchestrator-mode mints cannot proceed without the liquidity bot's `MintAuthV1`
delivery. The exposure is that Alpaca has ALREADY journaled the AP's shares by
then: journal completion sends `Deposit`, which emits `MintingStarted` with no
authorization check (`Mint::handle_deposit`), and the orchestrator submit branch
then defers without recording an event — so the mint sits in `Minting`, not
`JournalConfirmed`, holding real backing and minting nothing until the
authorization lands.

**Detection.** The mint recovery pass raises the alert itself; do not wait for
`/admin/stuck`, which does not surface an in-progress mint until it is an hour
old (`STUCK_THRESHOLD`, `src/admin.rs`) — an hour of silently-held AP shares.
The pass runs every five minutes and measures each wait from the moment the
shares were journaled, so the first line appears on the second or third pass
after that point:

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
repeats every pass while the condition holds. Once they are old enough for
`/admin/stuck`, the rest are the orchestrator-mode `Minting` rows for the asset
with no `tx_id`:

```sh
curl -fsS -H "X-API-KEY: $INTERNAL_API_KEY" "$ISSUER_BASE_URL/admin/stuck" \
  | jq --arg sym <SYM> \
      '.stuck[] | select(.underlying == $sym and .state == "Minting" and .tx_id == null)'
```

Mints waiting BEFORE their journal confirms are deliberately not alerted: Alpaca
has committed nothing at that point, so there is no exposure.

Escalation:

1. Confirm the mint is actually waiting on delivery and not on something else.
   `/admin/stuck` cannot show that directly — the row reads
   `state: "Minting", detail: "Deposit in progress"` and carries no field for
   whether an authorization was recorded — so confirm it from the service log:
   the WARN
   `Orchestrator mint is awaiting its recipient authorization;
   deferring submission`
   for that `issuer_request_id`, repeated on every recovery pass, is this case.
   That WARN repeats only while the mint's recovery job is live. The job gives
   up after `MAX_SCHEDULED_RECOVERY_NO_PROGRESS_POLLS` polls with no progress
   (360 polls at one minute each, so about 6 hours), logs the ERROR "Scheduled
   mint recovery abandoned the mint while still incomplete", and is marked
   `Killed`. After that, the reconcile pass does not start a new job for the
   mint, so the per-mint WARN stops, while the summary `ERROR` above keeps
   firing. The WARN comes back when something drives the mint again: a restart
   (the startup re-scan), or `POST /admin/reprocess/mint/<issuer_request_id>`,
   which accepts a `Minting` mint and replaces its `Killed` recovery job. So for
   a long wait, such as one overnight, do not expect a recent WARN: search the
   log back to when the mint started.

   The alert names only the oldest waiter. With `waiting_mints` above one, find
   the others by the age of their wait. For the first hour, `/admin/stuck` does
   not list them (see Detection), so use the per-mint WARN lines. After one
   hour, use the `/admin/stuck` filter above. After about 6 hours, that filter
   is the only source, because the WARN lines stop when the jobs are `Killed`.
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

   This route carries the same JSON data guard as the close below, so a bodyless
   POST answers `404` rather than anything descriptive:

   ```sh
   curl --fail-with-body -sS -X POST \
     -H "X-API-KEY: $INTERNAL_API_KEY" \
     -H 'Content-Type: application/json' \
     -d '{"nonce":"0x…","signature":"0x…"}' \
     "$ISSUER_BASE_URL/internal/mints/<tokenization_request_id>/authorization"
   ```

   `--fail-with-body`, not `-f`: every refusal comes back as a JSON body that
   names its cause (for example a conflicting authorization), and step 3 needs
   that cause.

   An empty `"0x"` signature is valid for a contract recipient authorized
   through the orchestrator's `authorizeMint` callback; it is not a way to skip
   the signature for an EOA, which is refused.
3. If redelivery does not work, read the delivery's response. It tells you which
   case you have, and the two cases need different actions.

   - **`409` "A different authorization is already recorded for this mint".**
     The mint already has an authorization, so it no longer waits on delivery:
     the submit path uses the recorded one. Most likely its recovery job gave
     up. Re-drive it with `POST /admin/reprocess/mint/<aggregate_id>` and check
     that it leaves `Minting`. If it then fails with `BadRecipientSignature` or
     `RecipientCallbackRejected`, the recorded authorization can never work
     (reprocess refuses those): close the mint, as below.
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

   The route carries a JSON data guard, so a bodyless POST does not match it at
   all and answers `404` — which reads like a wrong URL exactly when it is not.
   Set `reason` to the real cause: it stays in the mint's history.

   ```sh
   curl --fail-with-body -sS -X POST \
     -H "X-API-KEY: $INTERNAL_API_KEY" \
     -H 'Content-Type: application/json' \
     -d '{"reason":"authorization never delivered"}' \
     "$ISSUER_BASE_URL/admin/close/mint/<aggregate_id>"
   ```

   `--fail-with-body`, not `-f`: the pre-close gate's `422`s come back as a JSON
   body naming the cause, and `-f` throws that body away, leaving only
   `curl: (22) ... 422` mid-incident with the AP's shares already journaled. A
   refusal from the aggregate itself (for example a missing or wrong
   `acknowledged_unresolved_mint_tx_hash`) comes back as the generic body
   `{"error":"Unprocessable Entity","status":422}`. Its cause is in the service
   log, on the `Failed to close mint` line for that `aggregate_id`.

   A mint that already holds a prepared deposit identity is closed in two
   layers, and the order matters. First, the pre-close gate
   (`refuse_unsafe_close`, `src/admin.rs`) runs before `CloseMint` is sent and
   never sees the acknowledgement fields: every persisted signed transaction
   with no terminal outcome must be provably unable to land — a finalized
   reverted receipt, or the signing wallet's finalized nonce past it — otherwise
   the close is refused with `UnresolvedPersistedTx` whatever the body carries.
   Prove the prepared transaction dead on-chain first. Only then does the
   aggregate's requirement apply: the body needs
   `acknowledged_unresolved_mint_tx_hash` alongside `reason`, echoing that
   mint's exact prepared hash, or the aggregate answers `422` in turn. That
   acknowledgement states the transaction is UNRESOLVED, not dead — so the close
   keeps holding the mint's `(recipient, nonce)` pair and a later delivery of
   the same nonce is still refused. Supply `acknowledged_unresolved_mint_nonce`
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
   this way (each has a `BurnFailed` row in `/admin/stuck`) via the existing
   admin recovery surface — `POST /admin/recover/redemption/<id>`, which issues
   `ResumeBurn`; the failure classification blocks only _automatic_ retries, not
   the manual re-drive.

A pre-submit `VaultLogicMismatch` halt is different — the orchestrator is
version-locked against upgraded vault beacons, no redemption has failed, and the
bot defers until `vaultLogicIsExpected()` reads true again (visible in
`GET /admin/orchestrator-health` and the preflight). When the mismatch instead
races a transaction that was already submitted, the redemption records a
classified `BurningFailed` (`VaultLogicMismatch`/`ReceiptLogicMismatch`) that is
never auto-retried; once the health check reads true again, re-drive it via the
manual `ResumeBurn` admin recovery — same as the shortfall's step 2.
