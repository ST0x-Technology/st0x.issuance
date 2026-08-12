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
  orchestrator stack merges to main: cut the `st0x-issuance-client` /
  `st0x-issuance-dto` tag `0.3.0` from main, swap the liquidity repo's
  `Cargo.toml` git-branch pin back to that tag (the pin carries a swap-back
  comment marking the spot), and deploy the liquidity bot from the swapped pin.
  Step 7's cutover pre-check ("pin is on the release tag") verifies this
  happened.

All `issuer` subcommands below run on the issuer host (over SSH) with the
service's own environment; they refuse a local-key signer because every fact
they verify or establish is keyed to the Turnkey bot wallet. The orchestrator
address is never typed — it comes from the TOML config file — the bot wallet
from `TURNKEY_ADDRESS`, and vault/receipt addresses from the listing view and
on-chain resolution. The one address argument in this document,
`move-receipts --to`, exists only for the wallet-rotation path, refuses the
configured orchestrator address, and is guarded by the kind-aware corroboration
witness (see SPEC "Receipt custody").

## 1. Ship the orchestrator address in the config (stays dark)

Add to `config.prod.toml`:

```toml
[orchestrator]
address = "0x…"   # from st0x.deploy
```

Do **not** add any `[assets.<SYM>]` section — with no `vault_mode` overrides
every asset stays vault-direct, so this deploys dark. Parsing is strict (unknown
keys and a malformed or zero address are startup errors, even while dark).
Verify locally with `cargo run --bin validate-config`, then deploy. The config
file is baked into the systemd unit (`CONFIG=<nix store path>`, see
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
`[orchestrator].address` is refused rather than granted an unlimited allowance.
Idempotent: a re-run reports "already unlimited" and sends nothing, so batching
every asset's approval early is safe — approvals are inert until the asset's
`vault_mode` flips. Success is re-verified by an on-chain allowance read. When
this step actually submits, the transaction is also live proof that the policy's
`approve` allowance works; the idempotent no-op path proves nothing new — step
4's signing proof covers `approve` in that case.

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

## 7. Cutover pre-checks (gate on exit codes, not eyeballs)

All of these must hold for the asset being cut over, immediately before its
window:

- Step 6's preflight exits zero for this asset:
  `issuer orchestrator-preflight --asset <SYM> …`.
- Step 4's signing proof is clean for this asset — it covers the ERC-1155
  `safeBatchTransferFrom` shape the receipt move (step 10) submits.
- No stuck mints or redemptions for this asset in `GET /admin/stuck`.
- The liquidity bot is deployed with MintAuthV1 delivery live (RAI-1243): it
  reads `vault_mode` from the status endpoint, its `[orchestrator]` config
  section is deployed, its issuance client/dto pin is on the release tag, and
  its Turnkey policy allows `ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2` (raw-payload
  signing is a separate policy surface from transaction signing — proven by that
  repo's ignored `turnkey_digest_signing_integration` test run against prod
  Turnkey).

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

The destination is read from `[orchestrator].address` — never typed — and
corroborated as an ERC-1155-receiving contract before anything is signed. The
command prompts with the asset, vault, holder, destination and its corroborated
kind, and the tracked receipt count. A vault tracking more than 14 receipts
moves in multiple bounded transactions, each verified before the next. A re-run
after any interruption is safe: an interrupted move resumes with only the
remaining receipts, and a completed move reports "already migrated" and submits
nothing.

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
`Minted`/`Burned` events, Alpaca journals/callbacks, `/admin` health. Repeat
over a soak window (≥1 week with the asset left in orchestrator mode): every
attempt completes, no unexplained `/admin/stuck` entries, and any transient
failure exercises a recovery path observably. Record the go/no-go that gates the
full rollout (RAI-1246).

## 14. Rollback (per asset; rehearsed on Anvil)

Touches only this asset:

1. Freeze the asset and let in-flight work drain (step 8's procedure).
2. Flip its `vault_mode` back to `"vault_direct"` in the TOML config.
3. Page the `EMERGENCY_ROLE` holder (recorded below):
   `withdrawReceipt(token, id, amount, bot_wallet)` for every migrated receipt
   returns them on-chain. Verify bot-wallet `balanceOf` matches the step-9
   snapshot.
4. Arm the deployment hold, then re-record custody:
   `issuer confirm-custody <SYM> --network base --chain-id 8453
   --rpc-url "$RPC_URL"`
   — recorded custody still names the orchestrator after the withdrawal, and
   reconciliation stays skipped until this verifies the bot wallet holds every
   tracked balance and records it as holder.
5. Release the hold, deploy, verify startup reconciliation reads the vault
   normally again, unfreeze.

Redemptions already burned through the orchestrator keep their persisted
`burn_mode`; their recovery and verification follow the persisted mode, so they
stay recoverable after the flip back.

## Record of prod facts

Fill in as the steps execute; this table is the standing record the acceptance
criteria ask for.

| Fact                                                                | Value | Date | Verified by                   |
| ------------------------------------------------------------------- | ----- | ---- | ----------------------------- |
| Orchestrator address                                                |       |      | config.prod.toml + deploy     |
| `MINT_ROLE` grant tx / holder check                                 |       |      | `orchestrator-preflight`      |
| `BURN_ROLE` grant tx / holder check                                 |       |      | `orchestrator-preflight`      |
| `DEPOSIT`/`WITHDRAW` grants (orchestrator on each vault authorizer) |       |      | `orchestrator-preflight`      |
| `EMERGENCY_ROLE` holder (for escalation)                            |       |      | st0x.deploy                   |
| Turnkey policy name(s)                                              |       |      | `verify-orchestrator-signing` |

Per-asset approvals:

| Asset | Approval tx hash | Date | Operator |
| ----- | ---------------- | ---- | -------- |
| RKLB  |                  |      |          |

Per-asset cutovers (steps 7–13; RAI-1222 acceptance record):

| Asset | Final receipt-move tx | Snapshot ref | Flip deploy | Validation mints/redemptions | Soak end | Go/no-go |
| ----- | --------------------- | ------------ | ----------- | ---------------------------- | -------- | -------- |
| RKLB  |                       |              |             |                              |          |          |

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
