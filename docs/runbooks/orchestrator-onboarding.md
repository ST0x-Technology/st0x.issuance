# Orchestrator onboarding and per-asset cutover (RAI-1221 / RAI-1222)

Two ordered procedures in one document. **Onboarding** (steps 1–6) is the ops
work that must be complete for an asset before its `vault_mode` can flip to
`"orchestrator"`: roles and the Turnkey policy are orchestrator-wide — done
once, before the pilot — while approvals are per asset (RKLB's before the pilot
cutover, each remaining asset's before its own). **Cutover** (steps 7–14) is the
per-asset procedure that actually moves an asset onto the orchestrator, authored
for the RKLB pilot (RAI-1222) and reused verbatim for every later asset
(RAI-1246).

There is no testnet or staging chain for this: every step below runs against
prod (Base mainnet) and is verified by on-chain reads. The first live end-to-end
mint/burn through Turnkey is the RKLB pilot's manual exercise (step 13), which
everything before it must fully precede. The full cutover cycle — migrate,
operate, roll back, resume — is rehearsed by the Anvil end-to-end suite
(`tests/receipt_custody.rs`,
`test_receipt_custody_migrates_into_the_orchestrator`), the only pre-prod
environment.

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
on-chain resolution. The one address argument in this document,
`move-receipts --to`, exists only for the wallet-rotation path, refuses the
configured orchestrator address, and is guarded by the kind-aware corroboration
witness (see SPEC "Receipt custody").

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
`cargo run --bin validate-config`, then deploy. The config file is baked into
the systemd unit (`CONFIG=<nix store path>`, see
`nix/upgradeable-services.nix`). Every command below passes `--config "$CONFIG"`
— the unit's own value — so the CLI provably validates and approves against the
exact file the running service resolves, never a stray local copy.

## 2. On-chain roles (st0x.deploy coordination)

Confirm with the st0x.deploy owners that the bot's Turnkey wallet is granted
`MINT_ROLE` + `BURN_ROLE` on the orchestrator. This is the only role fact the
issuance bot needs — it never calls the emergency/admin functions. Separately,
the orchestrator itself must hold `DEPOSIT` and `WITHDRAW` on each vault's
authorizer (a deploy-side grant, per vault); step 6's preflight verifies these
alongside the bot's roles. `EMERGENCY_ROLE` / `DEFAULT_ADMIN_ROLE` holders are
st0x.deploy's governance call; record who holds `EMERGENCY_ROLE` in the table
below, because the shortfall escalation (last section) pages them.

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
  --network base --chain-id 8453 --rpc-url "$RPC_URL"
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
  --network base --chain-id 8453 --rpc-url "$RPC_URL"
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

Record each executed approval in the table below.

## 6. Final gate: preflight must print READY

```
issuer orchestrator-preflight \
  --config "$CONFIG" \
  --network base --chain-id 8453 --rpc-url "$RPC_URL" \
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
curl -fsS -H "X-API-KEY: $INTERNAL_API_KEY" "$ISSUANCE_URL/admin/orchestrator-health" \
  | jq -r '.assets[] | select(.underlying == "<SYM>") | .network'
```

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
that chain's `--network` / `--chain-id` / `--rpc-url`, and record each chain's
output separately.

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
supply is still in the flip — steps 4–6 and the post-flip health check still run
for it — but there is nothing to move. The symbol's chain set is every network
where it is listed; confirm it on the issuer host, since a `200` is a listing
and a `404` is not:

```sh
for NETWORK in base ethereum hyperevm robinhood binance; do
  printf '%s: ' "$NETWORK"
  curl -sS -o /dev/null -w '%{http_code}\n' \
    -H "X-API-KEY: $INTERNAL_API_KEY" \
    "$ISSUANCE_URL/tokenized-assets/<SYM>?network=$NETWORK"
done
```

Step 10 refuses an empty chain rather than "verifying" a move on two zero
readings: `move-receipts` answers `CustodyUnobserved` when the vault never had
custody recorded, or `InventoryEmpty` when custody is recorded at the bot wallet
but no tracked receipt has a balance (`src/receipt_inventory/migration.rs`).
Either refusal exits non-zero mid-window with the hold armed and the service
stopped, and neither can tell a legitimately empty vault from an inventory that
was never backfilled — so check each listed chain before the window, on-chain,
not by assumption. Read the vault's receipt contract and confirm the bot wallet
holds no receipt balance there:

```sh
RECEIPT=$(cast call <vault> 'receipt()(address)' --rpc-url "$RPC_URL")
# Every receipt id ever transferred to the bot wallet on this chain, from the
# ERC-1155 transfer logs (single and batch). Topics are 32-byte padded, so
# grep for the wallet's 40 hex characters without the 0x prefix.
# Scan from the receipt contract's DEPLOYMENT block, not the backfill start
# block: a receipt transferred before the backfill start is invisible to a
# scan that begins there, and the balance check below never sees its id.
cast logs --from-block <receipt contract deployment block> --address "$RECEIPT" \
  'TransferSingle(address,address,address,uint256,uint256)' \
  --rpc-url "$RPC_URL" | grep -i '<bot wallet hex>'
cast logs --from-block <receipt contract deployment block> --address "$RECEIPT" \
  'TransferBatch(address,address,address,uint256[],uint256[])' \
  --rpc-url "$RPC_URL" | grep -i '<bot wallet hex>'
# Then, for every id those logs name:
cast call "$RECEIPT" 'balanceOf(address,uint256)(uint256)' <bot wallet> <id> \
  --rpc-url "$RPC_URL"    # → 0
```

No transfer to the bot wallet at all, or zero for every id the logs name, means
the chain is legitimately empty: record `no receipts` in that chain's
receipt-move, snapshot and verification columns. Steps 10 and 11 have nothing to
move for it, but read the known gap below before you continue. Any non-zero
balance means the inventory is behind the chain — stop, and backfill before
moving anything.

**Known gap: a proven-empty chain gets no custody record.** Step 10 is the only
operation that records custody at the orchestrator (`RecordCustodyMigration`).
When you skip it, the chain still flips, but its recorded custody stays
`Unobserved`, or stays `Held` at the bot wallet if the vault ever held receipts
there. After the first orchestrator mint on that chain, receipt inventory tracks
a receipt that the orchestrator holds, and each state fails in its own way:

- `Unobserved`: startup reconciliation reads `balanceOf(bot_wallet, id) = 0` and
  refuses it on every restart. The WARN
  `Startup reconciliation failed — receipt balances may be stale` shows it, with
  `Refusing to deplete receipt` in its error field when that vault is the first
  failure. A zero reading after an orchestrator burn is refused the same way.
- `Held` at the bot wallet: the same zero reading is ACCEPTED, and inventory
  depletes a receipt that still holds shares. No log line shows this. It is the
  mirror-low direction that `confirm-custody` cannot detect. Orchestrator burn
  readings are refused, and the log shows
  `Refusing balance reading for
  receipt` and
  `Failed to reconcile receipt inventory after an orchestrator
  burn`.

No operator command records custody at the orchestrator for an empty vault yet.
The planned fix extends `move-receipts`: on an empty vault it will prove
on-chain that the bot wallet holds none of the vault's receipts, and then record
the migration without a transfer. Until that ships, a proven-empty chain is a
stop, not a skip: do not flip a symbol whose chain set includes one, and
escalate to engineering. Listings cannot be removed, so a chain cannot be taken
out of the set.

The one way through today is to make the chain non-empty before cutover: one
small vault-direct mint on that chain, through the normal mint path, before the
freeze. This is a real mint, with an Alpaca journal and real backing, so it
needs sign-off. The bot wallet then holds one receipt there, and step 10 moves
it and records custody at the orchestrator, as on any other chain. Re-run the
check above just before step 10, because a redemption can use up that receipt,
and give the chain the full per-chain preparation of steps 4–6.

If the per-`(symbol, network)` override is ever built, this section is what
changes: cutover would then target one chain at a time and the table collapses
to "per chain" throughout.

## 7. Cutover pre-checks (gate on exit codes, not eyeballs)

All of these must hold for the asset being cut over, immediately before its
window. Every check is a command with an expected exit status — record each
command's output with the cutover. Repeat the per-chain checks for each chain in
the derived set; only the `$ISSUANCE_URL` check and the liquidity bot's
deployed-revision, tag/pin and config-address checks are deployment-wide and run
once. The Turnkey MintAuth policy checks are NOT deployment-wide: the
orchestrator's EIP-712 domain carries the id of the chain it runs on, so they
run per chain too (see the policy bullet):

- The configured orchestrator on this chain is the deployment
  `config.prod.toml`'s comment claims. Preflight and `approve-orchestrator` pass
  against ANY orchestrator-shaped contract at the pinned address, so the domain
  read and the implementation hash are the only checks that catch a different
  deployment on one chain.

  The orchestrator address is a BeaconProxy on every chain. Its own code is only
  the proxy stub, with the beacon address inside it. A hash of that code stays
  the same on every chain even when one chain's beacon points to different
  logic. The beacons also have different owners (a Safe on Base, another Safe on
  the other chains), so they can diverge. Hash the code the beacon points to,
  not the proxy code:

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
- `$ISSUANCE_URL` is the canonical HTTPS issuer origin before any check sends
  `X-API-KEY` to it (a plain-`http://` or mistyped origin would leak the key in
  cleartext or to the wrong host): `[[ "$ISSUANCE_URL" == https://* ]]` →
  exit 0.
- No stuck mints or redemptions for this asset:

  ```sh
  curl -fsS -H "X-API-KEY: $INTERNAL_API_KEY" "$ISSUANCE_URL/admin/stuck" \
    | jq -e --arg sym <SYM> \
        '[.stuck[] | select(.underlying == $sym)] | length == 0'
  ```

  → exit 0 (`jq -e` fails the check if any entry names the asset).
- The status endpoint the liquidity bot polls serves the mode field, still
  reading vault-direct pre-flip:

  ```sh
  curl -fsS -H "X-API-KEY: $INTERNAL_API_KEY" \
    "$ISSUANCE_URL/tokenized-assets/<SYM>/status" \
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
    holding journaled AP shares, the exact case the escalation section at the
    end covers. Turnkey has no separate typed-data activity type: EIP-712
    signing arrives as `ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2` with
    `PAYLOAD_ENCODING_EIP712`, so that activity type necessarily appears in the
    policy — but because policies are default-deny and the `eth.eip_712`
    conditions are unset for a bare-digest (hexadecimal-encoded) request, this
    grant does NOT permit raw digest signing: anything that is not an EIP-712
    payload with exactly our domain and struct is refused. Two checks, one per
    side of that claim:
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
(re-check `/admin/stuck` and step 7's preflight). The freeze-plus-drain is also
what guarantees no aggregate straddles the mode flip — an operation's mode
anchors once, at `Initiated` / `RedemptionDetected`.

## 9. Deploy hold and snapshot

Arm the deployment hold and stop the service per `docs/runbooks/deploy-hold.md`
— `move-receipts` refuses without it, because the engine's projection rebuilds
and quiescence reads must not race a running service. Then snapshot: record this
token's per-receipt on-chain balances of BOTH wallets — the bot wallet
(sanity-checked against `receipt_inventory_view`) and the orchestrator's
pre-move balances for the same receipt ids (the engine deliberately allows a
destination with pre-existing balances and verifies per-identifier GAINS, so the
verification in step 11 needs the before-values) — and investigate any
discrepancy before proceeding.

## 10. Move the receipts

```
issuer move-receipts <SYM> \
  --to-configured-orchestrator \
  --config "$CONFIG" \
  --network base --chain-id 8453 --rpc-url "$RPC_URL"
```

The destination is read from the `--network`'s `[orchestrator.addresses]` entry
— never typed — and corroborated as an ERC-1155-receiving contract before
anything is signed. The command prompts with the asset, vault, holder,
destination and its corroborated kind, and the tracked receipt count. A vault
tracking more than 14 receipts moves in multiple bounded transactions, each
verified before the next. A re-run after any interruption is safe: an
interrupted move resumes with only the remaining receipts, and a completed move
reports "already migrated" and submits nothing.

## 11. Verify the move

- For every receipt id, the orchestrator's `balanceOf` GAIN over its step-9
  pre-move balance equals the bot wallet's transferred amount from the same
  snapshot; the bot wallet reads zero. (Final-balance equality with the bot's
  snapshot is only correct when the orchestrator started at zero — the gain
  check is the one the engine itself enforces.)
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
  cast call <orchestrator address> 'nextBurnReceiptId(address)(uint256)' <vault> \
    --rpc-url "$RPC_URL"
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
- The recorded custody history shows the move bot → orchestrator (this is what a
  later rollback derives its origin from).

## 12. Flip, deploy, unfreeze

Set `vault_mode = "orchestrator"` in the asset's `[assets.<SYM>]` table of the
TOML config; release the hold and run the service deployment (the deploy
activation restarts the unit). Verify `/admin/orchestrator-health` and the
status endpoint report orchestrator mode for the asset, and that startup
reconciliation logged the migrated vault as **skipped at INFO** ("custody
recorded at a migrated destination") — a `CustodyDisplaced` ERROR here is a real
problem, not cutover noise. Unfreeze.

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
`id` is one this vault tracks. `Burned.amount` is the redemption's persisted
`alpaca_quantity` — the detected transfer truncated to 9 decimals, with the
remainder kept in the bot wallet as `dust_retained` — which is exactly what
`handle_record_orchestrator_burn_confirmed` requires `shares_burned` to equal.
Do NOT compare the sum to the raw amount sent to the redemption wallet: a
transfer carrying more than 9 decimals of dust makes that read as a mismatch,
which mid-pilot looks like missing backing. The range is only ever the pointer.

Repeat over a soak window (≥1 week with the asset left in orchestrator mode):
every attempt completes, no unexplained `/admin/stuck` entries, and any
transient failure exercises a recovery path observably. Record the go/no-go that
gates the full rollout (RAI-1246).

## 14. Rollback (per asset; rehearsed on Anvil)

Touches only this asset:

1. Freeze the asset and let in-flight work drain (step 8's procedure).
2. Flip its `vault_mode` back to `"vault_direct"` in the TOML config.
3. Arm the deployment hold and stop the service (per
   `docs/runbooks/deploy-hold.md`) — **before** any receipt moves, for the same
   reason step 9 requires it on the way in: projection rebuilds and custody
   reads must not race a running service, and a live reconciliation pass must
   never observe returned balances while persisted custody still names the
   orchestrator.
4. Page the `EMERGENCY_ROLE` holder (recorded below):
   `withdrawReceipt(token, id, amount, bot_wallet)` for **every receipt the
   orchestrator holds for the token** — the migrated ones AND every receipt
   minted through the orchestrator since the cutover — returns them on-chain.
   The step-9 snapshot lists only the migrated ids, so enumerate the rest from
   the vault's `Deposit` logs with `owner == orchestrator` since the flip. Use
   the snapshot ONLY to list the migrated ids, never for amounts: step 13
   requires at least one real orchestrator burn, and each burn drains the lowest
   ids first — the migrated ones — so their snapshot amounts are too high after
   any burn. A `withdrawReceipt` for a snapshot amount then reverts.

   For every id, read the amount just before its withdrawal and use that reading
   as the `amount`:

   ```sh
   RECEIPT=$(cast call <vault> 'receipt()(address)' --rpc-url "$RPC_URL")
   cast call "$RECEIPT" 'balanceOf(address,uint256)(uint256)' \
     <orchestrator> <id> --rpc-url "$RPC_URL"
   ```

   Skip an id that reads zero: a burn drained it and there is nothing to return.
   After each withdrawal, verify that the bot wallet's `balanceOf` for that id
   went up by exactly the amount you withdrew, and that the orchestrator's
   `balanceOf` for that id now reads zero.
5. Before re-recording custody, search the service logs since the flip for all
   three warnings the orchestrator receipt registration emits
   (`src/mint/job.rs`):
   - `Failed to register the orchestrator mint's receipt`
   - `Orchestrator mint carried no decodable Deposit`
   - `Could not re-read the recovered mint's transaction to register its receipt`

   Each hit is a receipt the orchestrator holds that inventory does not know
   about. The check in this step cannot see it (see the failure directions
   below), and nothing registers it for you: no `issuer` subcommand and no admin
   route registers a receipt in receipt inventory, and the backfiller never
   picks up a `Deposit` owned by the orchestrator. **A hit blocks the
   rollback.** Keep the hold armed and the service stopped, and escalate to
   engineering with the `issuer_request_id` and transaction hash from the log
   line. Continue only after engineering confirms the receipt is registered.

   With the hold still armed, re-record custody, **once per chain** whose
   receipts step 4 returned:
   `issuer confirm-custody <SYM> --network base --chain-id 8453
   --rpc-url "$RPC_URL"`.
   **Before** this command runs, recorded custody is EXPECTED to still name the
   orchestrator — the on-chain withdrawal (step 4) does not touch the persisted
   record, so that is not stale data to investigate. **After** it succeeds,
   recorded custody must name the bot wallet: the command verifies on-chain that
   the bot wallet holds every tracked balance and only then records it as
   holder. Reconciliation stays skipped between the two states.

   **If it refuses with a balance mismatch, do not work around it.** The check
   is exact: tracked balances must equal what the bot wallet holds on-chain.
   Orchestrator burns keep the tracked side current — on each confirmed burn the
   manager takes the receipts inventory tracks from `firstReceiptId` through
   `nextBurnReceiptIdAfter` inclusive, re-reads `balanceOf(orchestrator, id)`
   for each, and records those READINGS (so a re-run corrects drift rather than
   subtracting twice), and orchestrator mints register their receipt the same
   way vault-direct mints do. Both are best-effort by design, and **they fail in
   opposite directions; only one of them fails safe**:

   - A failed burn reconciliation leaves the mirror HIGH. This refusal is that
     failure surfacing rather than being acted on. Investigate the gap — compare
     tracked balances against the chain per receipt id, and look for the
     reconciliation warning in the service logs for the burn that diverged —
     before any further step. A rollback completed over a mirror that overstates
     the receipts is how backing silently goes missing.
   - A failed mint registration leaves the mirror LOW, and that one is silent:
     `confirm-custody` builds its comparison list FROM the tracked receipts, so
     a receipt inventory never learned about is never compared, the command
     PASSES, and the receipt is stranded at the orchestrator — nothing
     rediscovers it later, because the backfiller matches only bot-wallet-owned
     deposits. A pass here therefore does not prove the rollback returned
     everything; the log search at the top of this step is what does.

   Before this reconciliation existed, nothing updated the tracked side after an
   orchestrator burn, so this command could never succeed once the pilot's
   validation redemption had run. If you are rolling back a deployment that
   predates it, expect the refusal and reconcile manually.
6. Release the hold, deploy, verify startup reconciliation reads the vault
   normally again, unfreeze.

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
| `EMERGENCY_ROLE` holder (for escalation)                            |       |      | st0x.deploy                    |
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

| Asset | Chain     | Preflight exit 0 | Final receipt-move tx | Snapshot ref | Validation mints/redemptions | Go/no-go |
| ----- | --------- | ---------------- | --------------------- | ------------ | ---------------------------- | -------- |
| RKLB  | Base      |                  |                       |              |                              |          |
| RKLB  | Ethereum  |                  |                       |              |                              |          |
| RKLB  | HyperEVM  |                  |                       |              |                              |          |
| RKLB  | Robinhood |                  |                       |              |                              |          |

| Asset | Flip deploy | Soak end | Overall go/no-go |
| ----- | ----------- | -------- | ---------------- |
| RKLB  |             |          |                  |

## Mint waiting on an authorization that never arrived

Orchestrator-mode mints cannot proceed without the liquidity bot's `MintAuthV1`
delivery. The exposure is that Alpaca has ALREADY journaled the AP's shares by
then: journal completion sends `Deposit`, which emits `MintingStarted` with no
authorization check (`Mint::handle_deposit`), and the orchestrator submit branch
then defers without recording an event — so the mint sits in `Minting`, not
`JournalConfirmed`, holding real backing and minting nothing until the
authorization lands.

**There is no alert for this yet.** Today the only signal is `/admin/stuck`, and
an in-progress mint does not appear there until it is an hour old
(`STUCK_THRESHOLD`, `src/admin.rs`). An hour of silently-held AP shares is the
gap; a WARN-then-ERROR alert on the wait is tracked separately and this section
gets a detection step when it lands. Until then, the trigger is an
orchestrator-mode `Minting` row for the asset with no `tx_id`, past the
threshold — check for it deliberately during the pilot rather than waiting to be
told:

```sh
curl -fsS -H "X-API-KEY: $INTERNAL_API_KEY" "$ISSUANCE_URL/admin/stuck" \
  | jq --arg sym <SYM> \
      '.stuck[] | select(.underlying == $sym and .state == "Minting" and .tx_id == null)'
```

Escalation:

1. Confirm the mint is actually waiting on delivery and not on something else.
   `/admin/stuck` cannot show that directly — the row reads
   `state: "Minting", detail: "Deposit in progress"` and carries no field for
   whether an authorization was recorded — so confirm it from the service log:
   the WARN
   `Orchestrator mint is awaiting its recipient authorization;
   deferring submission`
   for that `issuer_request_id`, repeated on every recovery pass, is this case.
2. Have the liquidity bot redeliver to
   `POST /internal/mints/<tokenization_request_id>/authorization`. Redelivery is
   the designed repair vector: an identical redelivery is idempotent and
   re-drives mint recovery, which covers the case where the first delivery
   recorded but its wake was lost.
3. If redelivery does not work, find out which case you have. The two cases need
   different actions.

   - **No authorization is recorded yet** (for example, the signer or Turnkey is
     unavailable). Do NOT close the mint. It can still be rescued in place: a
     mint in `Minting` accepts its first authorization
     (`Mint::accepts_mint_authorization`, `src/mint/mod.rs`). Wait until signing
     is back, then have the liquidity bot deliver as in step 2.
   - **A conflicting authorization is already recorded** (for example, a wrong
     nonce). A different authorization is refused with
     `ConflictingMintAuthorization`, so the mint cannot be rescued in place.
     Close it, as below.

   Closing does NOT reverse the Alpaca journal that already completed.
   `CloseMint` only records the mint as closed. After a close, the AP's shares
   are journaled to our custodian and no tokens back them. A fresh initiation
   journals the backing a second time. So before anyone re-initiates the AP's
   position, reconcile the completed journal by hand with Alpaca and record the
   outcome with the cutover.

   The route carries a JSON data guard, so a bodyless POST does not match it at
   all and answers `404` — which reads like a wrong URL exactly when it is not:

   ```sh
   curl --fail-with-body -sS -X POST \
     -H "X-API-KEY: $INTERNAL_API_KEY" \
     -H 'Content-Type: application/json' \
     -d '{"reason":"authorization never delivered"}' \
     "$ISSUANCE_URL/admin/close/mint/<aggregate_id>"
   ```

   `--fail-with-body`, not `-f`: every refusal this call can hit comes back as a
   JSON body naming the cause (the pre-close gate's `422`s and the aggregate's
   own error), and `-f` throws that body away, leaving only `curl: (22) ... 422`
   mid-incident with the AP's shares already journaled.

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

1. Page the `EMERGENCY_ROLE` holder (recorded above) — recovery is an on-chain
   emergency action: transfer the missing receipts into the orchestrator (or
   adjust the burn pointer; see SPEC "Contract Summary" on `EMERGENCY_ROLE`),
   owned by st0x.deploy governance.
2. Once receipts are in place, re-drive the redemption via the existing admin
   recovery surface — `POST /admin/recover/redemption/<id>`, which issues
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
