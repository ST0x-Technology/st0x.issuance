# Orchestrator onboarding: Turnkey policy, role grants, approvals (RAI-1221)

The ops procedure that must be complete for an asset **before** that asset's
`vault_mode` flips to `"orchestrator"` (the per-asset cutover itself is
RAI-1222's runbook; this one establishes its preconditions). Roles and the
Turnkey policy are orchestrator-wide — done once, before the pilot. Approvals
are per asset — RKLB's before the pilot cutover, each remaining asset's before
its own.

There is no testnet or staging chain for this: every step below runs against
prod (Base mainnet) and is verified by on-chain reads. The first live end-to-end
mint/burn through Turnkey is the RKLB pilot's manual exercise, which everything
here must fully precede.

## Prerequisites

- Turnkey is the live signer (RAI-1123 done, Fireblocks retired): the service
  runs with the `TURNKEY_*` env group, and the bot wallet is `TURNKEY_ADDRESS`.
- The orchestrator is deployed by st0x.deploy (PR #222/#223) and its address is
  known; the permissions scripts have run.
- The liquidity-bot counterpart (RAI-1243) is tracked separately — it gates the
  RAI-1222 cutover, not this procedure.

All `issuer` subcommands below run on the issuer host (over SSH) with the
service's own environment; they refuse a local-key signer because every fact
they verify or establish is keyed to the Turnkey bot wallet. None of them takes
an address argument — the orchestrator address comes from the TOML config file,
the bot wallet from `TURNKEY_ADDRESS`, and vault/receipt addresses from the
listing view and on-chain resolution.

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
issuance bot needs — it never calls the emergency/admin functions.
`EMERGENCY_ROLE` / `DEFAULT_ADMIN_ROLE` holders are st0x.deploy's governance
call; record who holds `EMERGENCY_ROLE` in the table below, because the
shortfall escalation (last section) pages them.

Verification is step 6's preflight (`hasRole` reads) — no trust in the
deploy-side report is required.

## 3. Turnkey signing policy

Create (or extend) the Turnkey policy for the issuer wallet to allow:

| Allowance                          | Target contract               | Why                                         |
| ---------------------------------- | ----------------------------- | ------------------------------------------- |
| `mint(...)`                        | orchestrator                  | orchestrator-mode mints                     |
| `burn(...)`                        | orchestrator                  | orchestrator-mode burns                     |
| `approve(...)`                     | each vault share token        | the one-time approval (step 5)              |
| `safeBatchTransferFrom(...)` (ERC-1155) | each vault's receipt contract | receipt migration during cutover (RAI-1222) — the BATCH selector: every Turnkey-signed receipt move submits it, and a policy grants contract + selector |

This replaces the per-vault Fireblocks whitelist/TAP entries with one policy
against the orchestrator. Keep the vault-direct transaction path working under
Turnkey (Fireblocks stays retired) — during the per-asset rollout BOTH mint/burn
paths run in production simultaneously, until RAI-1223 retires vault-direct
mode.

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
sending, the command verifies the configured address answers as an
orchestrator (interface reads plus a healthy `vaultLogicIsExpected()`), so a
typo'd or stale `[orchestrator].address` is refused rather than granted an
unlimited allowance. Idempotent:
a re-run reports "already unlimited" and sends nothing, so batching every
asset's approval early is safe — approvals are inert until the asset's
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
projection catch-up before the asset lookup). Checks
`hasRole(MINT_ROLE, bot)`, `hasRole(BURN_ROLE, bot)`,
`vaultLogicIsExpected()`, and per asset `allowance(bot, orchestrator)` plus
the orchestrator's `DEPOSIT`/`WITHDRAW` grants on the vault's authorizer
(omitting `--asset` checks the assets whose configured `vault_mode` resolves
to orchestrator — the assets actually cutting over). The gate is the **exit
status**: zero only when every check passes, so the RAI-1222 pre-checks gate on
the exit code for the asset being cut over; `Overall: READY` is the
human-readable rendering of the same verdict.

## Record of prod facts

Fill in as the steps execute; this table is the standing record the acceptance
criteria ask for.

| Fact                                     | Value | Date | Verified by                   |
| ---------------------------------------- | ----- | ---- | ----------------------------- |
| Orchestrator address                     |       |      | config.prod.toml + deploy     |
| `MINT_ROLE` grant tx / holder check      |       |      | `orchestrator-preflight`      |
| `BURN_ROLE` grant tx / holder check      |       |      | `orchestrator-preflight`      |
| `EMERGENCY_ROLE` holder (for escalation) |       |      | st0x.deploy                   |
| Turnkey policy name(s)                   |       |      | `verify-orchestrator-signing` |

Per-asset approvals:

| Asset | Approval tx hash | Date | Operator |
| ----- | ---------------- | ---- | -------- |
| RKLB  |                  |      |          |

## Shortfall escalation (InsufficientReceipts)

An orchestrator burn that reverts `InsufficientReceipts(token, shortfall)` puts
the redemption in a failed, manual-recovery-only state — the bot never
auto-retries through it and never mints to cover a shortfall (see SPEC "Failure
States"). Escalation:

1. Page the `EMERGENCY_ROLE` holder (recorded above) — recovery is an on-chain
   emergency action: transfer the missing receipts into the orchestrator (or
   `setBurnIndex`), owned by st0x.deploy governance.
2. Once receipts are in place, re-drive the redemption via the existing admin
   recovery surface; the failure classification blocks only _automatic_ retries,
   not the manual re-drive.

A pre-submit `VaultLogicMismatch` halt is different — the orchestrator is
version-locked against upgraded vault beacons, no redemption has failed, and the
bot defers until `vaultLogicIsExpected()` reads true again (visible in
`GET /admin/orchestrator-health` and the preflight). When the mismatch instead
races a transaction that was already submitted, the redemption records a
classified `BurningFailed` (`VaultLogicMismatch`/`ReceiptLogicMismatch`) that is
never auto-retried; once the health check reads true again, re-drive it via the
manual `ResumeBurn` admin recovery — same as the shortfall's step 2.
