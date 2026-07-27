# Plan: migrating deposit receipts to the Turnkey wallet

Status: the migration CLI executes the live move end to end. The narrow
Fireblocks client was restored (submit `CONTRACT_CALL`, poll to terminal,
whitelist resolution, vault-address lookup), so `issuer migrate-receipts`
submits the forward transfer through the Fireblocks API itself and signs the
rollback with Turnkey. Every wallet address is derived — from the Fireblocks
API, the Turnkey configuration, or on-chain state — never typed. What remains
before the window is the Fireblocks-side authorization (whitelisting + TAP rule,
RAI-1546/RAI-1544) and the preflights below.

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

## Custodian authorisation: the Fireblocks policy changes

The full Fireblocks integration was deleted with the Turnkey switch; the narrow
client restored for this migration (`src/fireblocks/`) submits `CONTRACT_CALL`
operations to an `ExternalWallet` destination resolved from the whitelisted
contract wallet list, and `resolve_contract_wallet` rejects any address not
already whitelisted. The vault was the only call target historically; the
Receipt contract was never one. That is the gap the authorization work closes.

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
  "dst": { "ids": [["*", "UNMANAGED", "CONTRACT"]] },
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
- `dst` should be narrowed from the wildcard above to the specific whitelisted
  Receipt contract for a tightly scoped rule. The wildcard form is what the
  Fireblocks documentation shows; scoping it to the individual contract is the
  safer configuration and is worth doing given this is a one-shot migration.
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

### The shortcut worth asking about first

If the workspace policy still permits RAW operations for that vault account, the
transfer can be signed without whitelisting the Receipt contract or adding any
rule. The workspace was originally configured for RAW signing. Establishing
whether that permission survives is a single question to the workspace
administrator and, if the answer is yes, removes both steps above from the
critical path.

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
holder move its own balance, and the holder is the Fireblocks wallet. The narrow
Fireblocks client restored for this purpose submits the batch as a
`CONTRACT_CALL` through the whitelisted Receipt contract (so TAP policy
applies), with a deterministic `externalTxId` so a crashed or retried run
resumes the original transaction instead of double-submitting, and polls it to a
terminal status — waiting through any console approvals. The rollback is signed
by Turnkey directly.

`issuer migrate-receipts <UNDERLYING> --network base --chain-id 8453` requires
both custodians' configurations and decides direction from the inventory's
recorded custody:

- **Custody at the Fireblocks wallet** → forward: every gate runs in-binary
  (quiescence, exact inventory/chain agreement, certification and owner-freeze
  re-read, per-identifier post-condition deltas measured as the recipient's
  gain), and the transfer is submitted via Fireblocks. The engine's holder is
  the wallet the Fireblocks API says this configuration controls — a wrong
  workspace or vault account holds none of the tracked receipts and the
  exact-balance check refuses before anything is submitted. Ownership
  verification is the check; no addresses are compared, and none are typed.
- **Custody at the Turnkey wallet** → rollback: Turnkey signs the same batch
  back to the Fireblocks wallet, fetched from the Fireblocks API. Same gates.
- A move already completed (e.g. executed manually in the Fireblocks console, or
  a re-run after a lost terminal) is detected — source empty, recipient holding
  every tracked balance — and recorded instead of transferred again.

Configuration comes entirely from the service's own environment: `RPC_URL`,
`DATABASE_URL`, the `TURNKEY_*` group, and the `FIREBLOCKS_*` group the old
service already uses. The production secret file carries **both** custodians'
variable sets; each binary reads only its own, so which binary runs is the only
signer selector and no env edit happens mid-window.

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

## Preflights — run before the window opens

1. **`issuer confirm-custody <UNDERLYING> --network base --chain-id 8453`**,
   once per vault. Fetches the Fireblocks wallet from the Fireblocks API,
   verifies on-chain that it holds exactly every tracked balance, and records it
   as the inventory's custody holder. This arms the displacement guard for every
   vault — production history predates custody tracking, and an unarmed guard
   treats a zero balance as "spent". Run it for **all** vaults before any
   service starts against a rotated wallet, not just the rehearsal asset.
2. **`issuer verify-custodians <UNDERLYING> --network base --chain-id 8453`**.
   Proves both custodian connections before the forward move can become a
   one-way door: authenticates against Fireblocks and resolves the whitelisted
   Receipt contract (the authorization work, proven present), and signs the
   exact rollback-shaped transaction with Turnkey **without broadcasting it**
   (credentials, organization, address, and signing policy, proven against the
   real transaction shape). Also reports both wallets' gas and refuses if the
   Turnkey wallet holds none.
3. **`--smoke`** on `verify-custodians` additionally submits a zero-amount
   transfer of one receipt id through the full Fireblocks path — whitelisting,
   TAP rule, signing, the vault's authorization gates — while moving nothing. A
   zero-amount transfer cannot create the inventory divergence a real dust
   transfer would (the migration refuses whenever tracked and on-chain balances
   disagree, so **never** smoke-test with a non-zero amount). This is the
   strongest Fireblocks-side proof available before the real batch.

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

**Pass 2 — remaining authorization.** Fireblocks authorization for every other
receipt token (RAI-1544), one authorization type at a time across all tokens if
that is how it goes fastest.

**Pass 3 — the real cutover.** After **regular market close** — not after
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
3. **Drain and stop the issuer service.** `/admin/stuck` clear, every pending
   transaction terminal, then stop the service completely. The migration
   independently refuses while any mint or redemption is non-terminal, but the
   drain is what makes that gate pass.
4. **Record pre-cutover state.** Application revision, block height, wallet
   nonces, checkpoints, a consistent database backup, and a receipt/share
   inventory export for reconciliation.
5. **Move custody.**
   `issuer migrate-receipts <UNDERLYING> --network base
   --chain-id 8453` with
   the service environment loaded. The transfer submits through Fireblocks and
   may wait on console approvals; the command polls it to completion and
   verifies per-identifier post-conditions (the recipient's **gain**, not its
   absolute balance) before reporting success.
6. **Verify custody against the export.** The Fireblocks wallet holds none of
   the migrated receipts; the Turnkey wallet's gain matches the export exactly.
7. **Start the replacement service** — the pinned Turnkey + multichain artifact,
   Base-only configuration, on the same database. Confirm startup resolves the
   Turnkey address and chain 8453.
8. **Canary redemption, then canary mint.** Redemption first: it burns a
   just-migrated receipt, proving custody and inventory reconciliation. The
   redemption is driven manually from st0x.liquidity while rebalancing stays
   paused.
9. **Rehearsal: roll back now** — stop the service, run the same
   `migrate-receipts` (custody is at Turnkey, so it rolls back to the Fireblocks
   wallet fetched from the Fireblocks API), verify custody returned, restart the
   old Fireblocks service on the same database, and verify the canary receipt is
   redeemable there. **Real pass: continue** — production verification, then
   re-enable rebalancing.

## Rolling back

Rollback is the same command: with custody recorded at the Turnkey wallet, the
CLI signs the batch with Turnkey back to the Fireblocks wallet fetched from the
Fireblocks API. No address is typed; ownership verification before (Turnkey
holds exactly every tracked balance) and after (the Fireblocks wallet's gain
matches) is the check. The rollback moves **everything tracked**, including
receipts the Turnkey service minted during its window.

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

- **Fireblocks rejects the transfer (policy, whitelisting, approvals).** Nothing
  moved. Fix with Alastair and retry; the Fireblocks service can restart at any
  time in the meantime.
- **The transfer reverts on-chain** (certification expired, owner freeze on the
  vault). Nothing moved; the command names the gate. Certification renewal is
  with whoever holds `CERTIFY`.
- **The operator loses the terminal mid-transfer.** Re-run the same command: the
  deterministic `externalTxId` makes Fireblocks return the original transaction
  instead of accepting a second one, and a completed move is detected and
  recorded rather than re-transferred.
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
