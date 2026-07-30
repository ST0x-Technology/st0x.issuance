# Plan: migrating deposit receipts to the Turnkey wallet

Status: the migration CLI executes the live move end to end. By default it
builds the forward transaction locally, obtains only its signature through a
Fireblocks `RAW` operation, and broadcasts through the configured RPC, bypassing
Fireblocks' transaction engine. If Fireblocks RAW signing is also unavailable,
`--outgoing-wallet-control local-private-key` signs directly with the
emergency-only `CUSTODY_MIGRATION_PRIVATE_KEY` and makes no Fireblocks API call.
Rollback is always signed by Turnkey. Every wallet address is derived — from the
selected retiring-wallet control, the Turnkey configuration, or recorded custody
— never typed.

## What this unblocks

The Turnkey signing backend is merged but undeployed. It cannot ship until the
ERC-1155 deposit receipts held by the Fireblocks signing address are moved to
the Turnkey signing address.

The receipt inventory is keyed to the bot's own signing address: the backfiller
and reconciler both read `balanceOf(bot_wallet, receipt_id)`
(`src/receipt_inventory/backfill.rs:470`,
`src/receipt_inventory/reconcile.rs:148`). Switching backends changes that
address. Receipts left behind are invisible to the bot, which would then be
unable to burn against real backing.

This proposes an automated, idempotent, per-asset migration we can run
ourselves, starting with AMAT alone, rather than a manual transfer synchronised
against a service cutover.

## On-chain authorisation

A receipt transfer clears the vault's authorisation path:

1. `safeTransferFrom` / `safeBatchTransferFrom` on the Receipt contract, both
   inherited from `ERC1155Upgradeable`.
2. `Receipt._update` calls
   `manager.authorizeReceiptTransfer3(operator, from, to, ids, amounts)`.
3. `ReceiptVault.authorizeReceiptTransfer3` enforces only that the caller is the
   Receipt contract, reverting `UnmanagedReceiptTransfer()`. No other
   restriction.
4. `OffchainAssetReceiptVault.authorizeReceiptTransfer3` runs
   `ownerFreezeCheckTransaction(from, to)`, then delegates to
   `authorizer.authorize(operator, TRANSFER_RECEIPT, abi.encode(TransferReceiptStateChange{from, to, ids, amounts, isCertificationExpired: isCertificationExpired()}))`.
5. The authorizer's `TRANSFER_RECEIPT` branch reverts
   `CertificationExpired(from, to)` if certification has expired, absent a
   privileged exemption. Otherwise it executes a bare `return`.

**No role is required.** Not `TRANSFER_RECEIPT`, not a handler role, nothing
granted to either address. Two runtime conditions must hold when the transaction
lands:

- **Certification is not expired**, or the transfer reverts.
- **The owner freeze does not block the pair.** `ownerFreezeCheckTransaction`
  reverts when `block.timestamp <= ownerFrozenUntil` and the sender is absent
  from `alwaysAllowedFroms` and the recipient absent from `alwaysAllowedTos`. An
  unset freeze leaves `ownerFrozenUntil` at zero and blocks nothing.

Both are read-only checkable against the live vaults before scheduling anything.

Sourced from `main` on gildlab/ethgild; confirm against deployed bytecode. The
receiving Turnkey address is an externally owned account, so the ERC-1155
receiver callback does not apply.

### The operator

`Receipt._update` takes the operator from `_consumeOperator()`: a single-use
operator if `withOperator` set one, otherwise `_msgSender()`.

- **Direct transfer:** operator is `_msgSender()`, the Fireblocks address, equal
  to `from`.
- **Manager-initiated transfer:** `managerTransferFrom(sender, ...)` runs under
  `onlyManager` and `withOperator(sender)`, so the operator is the `sender` the
  vault passes, not the holder.

The authorizer checks no roles on the non-expired path, so operator identity
does not affect an ordinary transfer. It matters only on the
certification-expired path, where the privileged exemptions are keyed to it.

### Certification is externally controlled

The bot never calls `certify()` in production; the only certification code here
is the Anvil test harness (`grant_certify_role`, `certify_vault` in
`src/test_utils.rs`). Certification is maintained by whoever holds `CERTIFY` on
the live vault.

The migration window therefore depends on a property we neither own nor monitor.
Certification could lapse between pre-flight check and transaction landing, and
every remaining asset would begin reverting mid-migration. The automation must
read `isCertificationExpired()` immediately before each submission and abort
cleanly rather than retry into a wall.

## Custodian authorisation and emergency control

The default path requires the Fireblocks workspace to permit a `RAW` operation
for the retiring vault account. The CLI builds the exact EIP-1559 receipt
transfer, asks Fireblocks to sign its hash, verifies the returned signature
recovers to the recorded custody holder, and broadcasts the signed envelope
through the configured RPC. It does not use Fireblocks' transaction-building,
node, or broadcast infrastructure.

The contract-whitelisting and TAP material below documents the older
`CONTRACT_CALL` route. `verify-custodians` still resolves that whitelist (and
`--smoke` submits through it), so invoke that optional preflight only when the
legacy route is configured. It is not required by either `migrate-receipts`
forward mode.

The last-resort path is explicit:

Select `--outgoing-wallet-control local-private-key` and provide
`CUSTODY_MIGRATION_PRIVATE_KEY` through the process environment.

Enter it with a non-echoing prompt, never in an argument or inline shell
assignment: those are visible in process listings or shell history. The key is
parsed into a valid secp256k1 signer, redacted from debug output, never logged
or persisted, and its derived address must equal recorded custody before the CLI
signs. This path bypasses Fireblocks entirely; use it only under the team's
emergency key-handling procedure and remove it from the process environment as
soon as the migration command exits.

### Step 1: whitelist each Receipt contract

For every vault in scope, whitelist its Receipt contract as a contract wallet in
the Fireblocks workspace.

The Receipt address is not the vault address. Obtain it per vault by calling the
vault's `receipt()` view function; that is the same address the vault itself
checks against in `ReceiptVault.authorizeReceiptTransfer3`. These addresses
should be read off-chain and confirmed before whitelisting, not transcribed from
any other source.

AMAT's Receipt contract is the only one strictly required for the first run.
Whitelisting the rest can follow once AMAT proves the path.

### Step 2: add a TAP rule permitting the call

A rule of this shape, in Fireblocks policy JSON, allows a given vault account to
make contract calls to whitelisted contract destinations:

```json
{
  "type": "TRANSFER",
  "action": "ALLOW",
  "transactionType": "CONTRACT_CALL",
  "dstAddressType": "WHITELISTED",
  "src": { "ids": [["<VAULT_ACCOUNT_ID>", "VAULT", "*"]] },
  "dst": { "ids": [["<CONTRACT_WALLET_ID>", "UNMANAGED", "CONTRACT"]] },
  "asset": "BASECHAIN_ETH",
  "amount": 0,
  "amountScope": "SINGLE_TX",
  "amountCurrency": "USD",
  "periodSec": 0,
  "operators": { "usersGroups": ["<GROUP_ID>"] }
}
```

Field notes for whoever applies this:

- `src` is the bot's vault account. The deleted integration defaulted
  `FIREBLOCKS_VAULT_ACCOUNT_ID` to `"0"`; confirm the account actually in use
  rather than assuming the default.
- `asset` should be the Fireblocks asset identifier for the chain, which was
  `BASECHAIN_ETH` for Base (chain 8453) in the deleted configuration's default
  `FIREBLOCKS_CHAIN_ASSET_IDS` mapping.
- `amount` is 0 because the call transfers no native value, matching what
  `build_contract_call_request` submitted.
- `dst` names the specific whitelisted Receipt contract's wallet id in the
  workspace (confirm the id format in the workspace if it differs). The
  Fireblocks documentation shows a wildcard `[["*", "UNMANAGED", "CONTRACT"]]`
  here; the narrowed form above is the safer configuration and is worth
  insisting on given this is a one-shot migration.
- `operators` must name the group or users permitted to initiate, which for an
  automated run is the API user the bot authenticates as.

### What this rule does and does not constrain

The rule matches on transaction type and destination contract. There is no
function-selector field in this rule shape, so whitelisting the Receipt contract
grants the vault account the ability to call **any** method on it, including
`setApprovalForAll`, not just the batch transfer we need.

That is a wider grant than the operation requires. Three mitigations worth
considering, in order of preference: scope `dst` to the single Receipt contract
rather than the wildcard; confirm with Fireblocks whether method-level
restriction is available in this workspace tier, which the public rule schema
does not appear to express; and remove the rule and the whitelisting once the
migration completes, since this grant has no ongoing purpose after cutover.

### The active default

The workspace's `RAW` permission is now the default migration route. Confirm it
for the vault account before the window. If that path fails, the explicit local
private-key mode above is the final fallback; it does not silently activate from
the mere presence of a key.

## Which receipts to move

A receipt is an ERC-1155 identifier scoped to one vault, modelled as
`ReceiptId(U256)` (`src/receipt_inventory/mod.rs:62-73`) and tracked in an
inventory aggregate keyed `{chain_id}:{vault}`
(`src/receipt_inventory/vault_key.rs`).

The set to move, per asset, is every identifier in that vault's inventory
holding a non-zero balance at the Fireblocks address. Enumerate it twice:

- **From our state:** the inventory aggregate, maintained by the backfiller and
  reconciler from on-chain transfer events and `balanceOf`.
- **From the chain:** `balanceOfBatch` against the Fireblocks address, which is
  authoritative.

Divergence between the two is a stop condition, not something to reconcile in
flight: it means we do not know what we hold. Our inventory can be stale; the
chain query needs a candidate set that only the inventory supplies cheaply.

One `safeBatchTransferFrom` carries parallel `ids` and `amounts` and moves a
vault's receipts atomically, subject to gas. Batch per vault: one authorisation
event, one outcome, no partially migrated vault.

## How the CLI executes the move

The forward transfer is signed by the retiring custodian: ERC-1155 only lets the
holder move its own balance, and the holder is the Fireblocks wallet. Both
forward modes build the same batch locally and broadcast through the configured
RPC. `fireblocks-raw` obtains the signature through the Fireblocks API;
`local-private-key` signs directly. The rollback is signed by Turnkey.

`issuer migrate-receipts <UNDERLYING> --network base --direction <forward|rollback>
--chain-id 8453 --outgoing-wallet-control <fireblocks-raw|local-private-key>`
always requires Turnkey and decides direction from the inventory's recorded
custody; the stated `--direction` must agree with that resolution, so a re-run
after a recorded forward move refuses instead of silently rolling back. The
default `fireblocks-raw` mode requires `FIREBLOCKS_*`; the emergency mode
requires `CUSTODY_MIGRATION_PRIVATE_KEY` instead:

- **Custody at the retiring wallet** → forward: every gate runs in-binary
  (quiescence, exact inventory/chain agreement, certification and owner-freeze
  re-read, per-identifier post-condition deltas measured as the recipient's
  gain). The retiring wallet derived from the selected control must equal the
  recorded holder before signing. Ownership verification is the check; no
  address is typed.
- **Custody at the Turnkey wallet** → rollback: Turnkey signs the same batch
  back to the recorded migration origin, independently corroborated by the
  selected outgoing-wallet control. Same gates.
- A move already completed (e.g. executed manually in the Fireblocks console, or
  a re-run after a lost terminal) is detected — source empty, recipient holding
  every tracked balance — and recorded instead of transferred again.

Configuration comes from `RPC_URL`, `DATABASE_URL`, and the `TURNKEY_*` group.
The default forward mode additionally uses the `FIREBLOCKS_*` group the old
service already used. The emergency mode instead reads
`CUSTODY_MIGRATION_PRIVATE_KEY` for this command only; unset it immediately
afterward. `--outgoing-wallet-control` is the explicit selector (defaulting to
`fireblocks-raw`) — the CLI never falls back from Fireblocks to a local key
automatically.

Quiescence is enforced in-binary and is deliberately **not** a freeze check: the
`Underlying` freeze means "corporate action in progress" — a different fact with
its own lifecycle — and the migration neither requires declaring one nor ends
one that is real (one is real right now). The migration refuses while any burn
is reserved against the vault, or any redemption or mint **for the migrating
asset** is between initiation and terminal. The in-flight gates are scoped to
the asset because stuck work only ever resumes against its own vault — a
permanently stuck legacy redemption on one asset (awaiting its own recovery
feature) must not hold every other vault's migration hostage. Work that cannot
be attributed to an asset counts against every vault instead of none.

## The service must be stopped first

The issuer must be **completely stopped** before custody moves, and this is not
optional. Reconciliation reads `balanceOf(bot_wallet, receipt_id)` for every
tracked receipt; a service still configured with the outgoing signer after
custody moved would read zero. Custody is tracked on the inventory aggregate
(`CustodyConfirmed` / `CustodyMigrated` events), and a reconciliation pass that
finds the wallet rotated while holding none of the claimed receipts fails at
ERROR without writing — the displacement guard. But that guard only arms once
custody is recorded, and the drain gates only see persisted state: stopping the
service is what actually serialises against it.

Window control is operational: **pause rebalancing on st0x.liquidity** (the only
Authorized Participant is our own liquidity bot, so this stops new redemptions
arriving) and **stop the issuer service** (no mints are processed). The
per-asset rebalancing flag gates new automatic triggers only — already
dispatched jobs run to completion, which is what the drain step waits out — and
the paused config must be confirmed against the **actually deployed** liquidity
revision, never inferred from the repository. If anything moves under the
migration regardless, its exact-balance divergence check refuses rather than
proceeds.

A vault that refuses (stuck work, balance mismatch) stays safely on Fireblocks
while every other vault migrates; it catches up in a later pass. The Fireblocks
whitelisting and TAP rule must therefore stay in place until the **last** vault
has migrated — not merely until cutover day.

## Preflights — run at the start of the window, before the forward move

In execution order these slot into the cutover sequence between its steps 3
(drain, stop, hold armed) and 4 (backup + export) — they are listed first only
because they gate everything after them.

0. **Install the artifact that contains these commands.** The on-host `issuer`
   wrapper executes whatever the deployed per-service profile holds, and the
   subcommands below only exist in this stack's artifact — running them against
   the currently-deployed profile fails with an unknown-subcommand error.

   Deploys run through CI, never from an operator machine (the host is
   x86_64-linux; the workflow builds on its runner and pushes with deploy-rs).
   Arm the hold first so installing does not restart the service, then tag the
   merged main and dispatch the production workflow:

   ```
   ssh <host> touch /run/st0x/st0x-issuance.hold
   git tag v<next> && git push origin v<next>       # on main, post-merge
   gh workflow run deploy-prod.yaml -f tag=v<next>
   ```

   Wait for the workflow run to finish before continuing (it verifies the tag is
   on main, builds, and deploys every profile). Under the hold, the deploy stops
   the service, installs the new binary and secrets, runs `validate-config`, and
   leaves the service stopped. Because the service goes down here, run this at
   the start of the window — after rebalancing is paused and the drain is clean
   — not days ahead. Remove the hold only at the runbook step that starts the
   replacement service.
1. **`issuer confirm-custody <UNDERLYING> --network base --chain-id 8453`**,
   once per vault. Fetches the Fireblocks wallet from the Fireblocks API,
   verifies on-chain that it holds exactly every tracked balance, and records it
   as the inventory's custody holder. This arms the displacement guard for every
   vault — production history predates custody tracking, and an unarmed guard
   treats a zero balance as "spent". Run it for **all** vaults before any
   service starts against a rotated wallet, not just the rehearsal asset.
2. **Optional legacy preflight:**
   `issuer verify-custodians <UNDERLYING>
   --network base --chain-id 8453`.
   Run this only when the Receipt contract is whitelisted. It authenticates
   against Fireblocks, resolves that whitelist, and signs the exact
   rollback-shaped transaction with Turnkey **without broadcasting it**. It also
   reports both wallets' gas and requires Turnkey to hold at least 0.001 ETH
   (1,000,000 gas at 1 gwei). `migrate-receipts` independently repeats the
   Turnkey rollback proof and gas check immediately before every forward move,
   so skipping this legacy preflight removes no forward safety gate. This
   command does not exercise Fireblocks RAW signing; the first forward migration
   does.
3. **Optional legacy smoke:** `--smoke` on `verify-custodians` additionally
   submits a zero-amount transfer of one receipt id through the full Fireblocks
   path — whitelisting, TAP rule, signing, the vault's authorization gates —
   while moving nothing. A zero-amount transfer cannot create the inventory
   divergence a real dust transfer would (the migration refuses whenever tracked
   and on-chain balances disagree, so **never** smoke-test with a non-zero
   amount). This proves only the legacy `CONTRACT_CALL` route; Fireblocks RAW is
   first exercised by the forward migration itself.

## Two passes, not one

The cutover runs twice: once as a rehearsal on a single asset, and once for
real. The rehearsal buys the thing no amount of testing against Anvil can — a
demonstration that the production Fireblocks authorization, Turnkey policies,
and approvals actually work, taken while the exposure is one asset.

**Pass 1 — single-asset rehearsal (AMAT), rollback mandatory.** Run the full
sequence below for AMAT alone, exercise both directions of the flow against the
new custody — one canary **redemption first** (it burns a just-migrated
pre-cutover receipt, which is the actual proof custody moved and reconciled; a
mint only proves the new signer can sign fresh work), then one canary mint — and
then **roll back. Rollback is not a decision point in the rehearsal**: two
service instances never run in parallel, and the rehearsal's purpose is proving
the full round trip including the way back. Operation continues on Fireblocks
until the real pass.

The rehearsal's rollback carries the database forward rather than restoring the
pre-window backup: the replacement service performed real writes (a redemption
and a mint), and only custody and the running binary reverse. The complete cycle
— forward migration, redemption-first canaries, rollback including the canary
mint's newly created receipt, and the resumed original service redeeming that
canary — is exercised end to end against a real vault by
`test_single_asset_rehearsal_operates_reverses_and_resumes` in
`tests/turnkey_cutover.rs`.

One consequence to expect, not fear: the rehearsal writes custody events into
AMAT's inventory stream, and the still-deployed old binary does not know those
event types, so **AMAT receipt tracking and burns are degraded on the old binary
between the rehearsal's rollback and the real cutover**. Every other asset is
untouched (aggregates are per-vault, and startup reconciliation tolerates a
failing vault). Keep AMAT's rebalancing paused for that day.

**Between the passes — optional legacy authorization.** Whitelisting every other
receipt token (RAI-1544) is needed only if retaining the `CONTRACT_CALL`
smoke/fallback route. Fireblocks RAW and local-private-key do not use those
destination entries.

**Pass 2 — the real cutover.** After **regular market close** — not after
extended hours — run the sequence for every asset, no planned rollback. Regular
close is the boundary because Alpaca journaling and the liquidity bot's hedging
both follow the regular session; starting while extended-hours trading is live
means racing flows that cannot be retracted once dispatched.

## Cutover sequence

Applies to both the rehearsal and the real pass; the rehearsal scopes every step
to AMAT and ends with the mandatory rollback.

1. **Pause rebalancing on st0x.liquidity.** The sole AP is our own bot, so this
   stops new redemptions arriving at the redemption wallet. No freeze is
   involved anywhere in this sequence; the live corporate-action freeze stays
   exactly as it is.
2. **Confirm liquidity is quiescent.** No equity or USDC flow in progress;
   anything already dispatched settles first.
3. **Drain and stop the issuer service, and arm the deploy hold.**
   `/admin/stuck` clear, every pending transaction terminal, then stop the
   service completely and create the hold file:

   ```
   systemctl stop st0x-issuance
   touch /run/st0x/st0x-issuance.hold
   ```

   While the hold exists, any deploy — including the intentional one that ships
   the Turnkey artifact in step 7, and any stray CI deploy — installs the binary
   and secrets and runs `validate-config`, but leaves the service stopped. This
   closes the gap where a deploy would otherwise auto-restart a service against
   custody that has not finished moving. `/run` is tmpfs: a reboot clears the
   hold (and the start marker), so re-create it before any deploy if the window
   is still open. The migration independently refuses while any mint or
   redemption is non-terminal, but the drain is what makes that gate pass.
4. **Record pre-cutover state.** Application revision, block height, wallet
   nonces, checkpoints, then on the host:

   The filenames are per-pass on purpose — `VACUUM INTO` refuses an existing
   destination, and the rehearsal's evidence must survive the real pass:
   substitute a date-stamped name per run (e.g.
   `pre-cutover-issuance-20260728T2100.db` and the matching `.json`).

   ```
   sqlite3 /mnt/data/issuance.db \
     "VACUUM INTO '/mnt/data/pre-cutover-issuance.db'"
   sqlite3 -json /mnt/data/pre-cutover-issuance.db \
     "SELECT aggregate_id,
             json_extract(payload, '$.Discovered.receipt_id') AS receipt_id
      FROM events
      WHERE aggregate_type = 'ReceiptInventory'
        AND event_type = 'ReceiptInventoryEvent::Discovered'" \
     > /mnt/data/pre-cutover-receipt-ids.json
   ```

   The `VACUUM INTO` backup is the full-fidelity export; the second query lists
   every receipt identifier the inventory has ever tracked, keyed by
   `{chain_id}:{vault}`. The `cast` readings run from the operator machine (the
   repo dev shell provides `cast`; the host does not) against any Base RPC with
   `--rpc-url`. Resolve the receipt contract once
   (`cast call <vault> 'receipt()(address)'`), then for each identifier record
   both wallets' balances
   (`cast call <receipt-contract> 'balanceOf(address,uint256)(uint256)'
   <wallet> <receipt_id>`)
   — step 6 compares against exactly these readings.
5. **Move custody.** Use the default Fireblocks RAW path first:

   ```text
   issuer migrate-receipts <UNDERLYING> --network base --direction forward \
     --chain-id 8453 --outgoing-wallet-control fireblocks-raw
   ```

   It asks Fireblocks only to sign, broadcasts through `RPC_URL`, and verifies
   per-identifier post-conditions (the recipient's **gain**, not its absolute
   balance) before reporting success. If RAW signing itself is unavailable, use
   the last-resort path with the key supplied through the environment:

   ```bash
   read -rsp 'Custody migration private key: ' \
     CUSTODY_MIGRATION_PRIVATE_KEY && echo
   export CUSTODY_MIGRATION_PRIVATE_KEY
   issuer migrate-receipts <UNDERLYING> --network base --direction forward \
     --chain-id 8453 --outgoing-wallet-control local-private-key
   unset CUSTODY_MIGRATION_PRIVATE_KEY
   ```

   The input is neither echoed nor placed in shell history. Unset it immediately
   after the command exits. The CLI derives its address and refuses unless it is
   exactly the recorded custody holder.
6. **Verify custody against the export.** Re-run the step-4 `cast call`
   readings: the Fireblocks wallet holds none of the migrated receipts; the
   Turnkey wallet's gain matches the step-4 readings exactly.
7. **Start the replacement service** — the pinned Turnkey + multichain artifact,
   Base-only configuration, on the same database. Deploy it (the hold leaves it
   installed and validated but stopped), then release the hold and start:

   ```
   rm /run/st0x/st0x-issuance.hold
   touch /run/st0x/st0x-issuance.ready
   systemctl start st0x-issuance
   ```

   (The artifact was already installed by the step-0 deploy, so no new deploy is
   needed here; re-dispatching the CI workflow after removing the hold would
   also start the unit, but the three commands above are the direct path.)
   Confirm startup resolves the Turnkey address and chain 8453.
8. **Canary redemption, then canary mint.** Redemption first: it burns a
   just-migrated receipt, proving custody and inventory reconciliation. The
   redemption is driven manually from st0x.liquidity while rebalancing stays
   paused.
9. **Rehearsal: roll back now** — stop the service and **re-arm the hold**
   (`systemctl stop st0x-issuance && touch /run/st0x/st0x-issuance.hold`; the
   window is open again, so the stray-deploy gap from step 3 must stay closed),
   then run the same `migrate-receipts` with `--direction rollback` (custody is
   at Turnkey, so it rolls back to the recorded Fireblocks origin, cross-checked
   through the selected outgoing-wallet control), and verify custody returned
   with the step-4 `cast call` readings.

   **Restoring the old Fireblocks service is a CI re-deploy, not a restart**:
   the step-0 deploy replaced the per-service profile, so the old binary is no
   longer on the host — dispatch the deploy workflow with the **previous
   released tag** (`gh workflow run deploy-prod.yaml -f tag=<previous-tag>`),
   which installs it under the re-armed hold, then release the hold and start as
   in step 7. Every asset except the rehearsed one resumes normal operation,
   while the rehearsed asset stays degraded on the old binary (its inventory
   stream now carries custody events the old binary cannot read — see Pass 1
   above), so its rebalancing stays paused until the real cutover. The
   canary-redeemable-after-rollback proof is exercised by the rehearsal e2e with
   the current binary running as the outgoing wallet; it is not a live step on
   the old binary. **Real pass: continue** — production verification, then
   re-enable rebalancing.

## Rolling back

Rollback is the same command: with custody recorded at the Turnkey wallet, the
CLI signs the batch with Turnkey back to the recorded migration origin. The
selected outgoing-wallet control independently derives that origin: the
Fireblocks API in `fireblocks-raw` mode or the emergency key in
`local-private-key` mode. A mismatch is refused before anything is signed. No
address is typed; ownership verification before (Turnkey holds exactly every
tracked balance) and after (the retiring wallet's gain matches) is the check.
The rollback moves **everything tracked**, including receipts the Turnkey
service minted during its window.

The asymmetry is in the custodian: the inbound leg needs a **Turnkey policy
permitting the reverse transfer** (RAI-1545, in place) — and `verify-custodians`
proves it against the real transaction shape before the window, so an aborted
cutover can never strand custody behind an emergency policy change.

Roll back if: custody verification in step 6 does not reconcile, the canary
redemption fails or produces an unexpected on-chain result, or the restarted
service cannot see the migrated receipts. Hard boundary for the real pass: after
the first new Turnkey-backed write, do not restore the old database or restart
the Fireblocks service — stop traffic, preserve evidence, and roll forward with
Turnkey.

## If something goes wrong

Every failure below leaves a recoverable state; custody is only ever at one of
two custodian-controlled wallets, and whichever wallet holds the receipts
determines which service may run.

- **Fireblocks RAW signing is rejected or unavailable.** Nothing moved. Fix the
  RAW policy/API path and retry, or explicitly switch to `local-private-key`
  under the emergency key-handling procedure. There is no automatic fallback. If
  the wait is long enough to warrant resuming service, restoring the
  Fireblocks-era binary is a CI re-deploy of the previous released tag (see
  step 9) — the new artifact is what is installed on the host.
- **The emergency private key does not derive the recorded custody holder.**
  Nothing is signed. Stop and verify the key source; never override the address
  check or type a replacement destination.
- **The transfer reverts on-chain** (certification expired, owner freeze on the
  vault). Nothing moved; the command names the gate. Certification renewal is
  with whoever holds `CERTIFY`.
- **The operator loses the terminal mid-transfer.** Re-run the same mode. A
  completed move is detected from balances and recorded rather than
  re-transferred. If a same-nonce transaction is still pending, preserve its
  evidence and reconcile it before changing signing modes.
- **The canary redemption or mint fails on Turnkey.** Roll back and resume
  Fireblocks **on the same database**. Never restore the pre-window backup once
  a Turnkey-backed write exists.
- **The rollback itself is refused** (Turnkey policy gap that the preflight
  somehow missed). Custody is safe at Turnkey and the Turnkey service is
  operational — keep running on Turnkey while the policy is fixed; there is no
  emergency.
- **A service starts against the wrong wallet mid-window.** Every balance reader
  — the startup backfiller included — applies readings through the aggregate's
  custody guard, which refuses wrong-wallet readings per vault at ERROR without
  writing anything; stop that service. No inventory is lost.
- **Anything else that does not reconcile.** Stop, keep both services down, and
  compare the step-4 export against `balanceOfBatch` at both wallets. The
  receipts cannot leave those two addresses without a signed transfer, so that
  fully determines the state.

## Open questions, ordered by what they block

1. Is each vault's certification valid, and when does it expire? A read-only
   `isCertificationExpired()` check answers it before the window.
2. Is an owner freeze set on any vault, and do the always-allowed lists cover
   the pair? Also read-only.
3. Fireblocks approval quorum mechanics for the whitelisting and TAP rule —
   tracked in RAI-1546/RAI-1544, answered by the first batch clearing the
   process.
