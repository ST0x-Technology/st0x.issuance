# Ethereum multichain validation runbook (RAI-1210 / RAI-1508)

Validate the deployed issuer against Alpaca and Ethereum without enabling
multichain trading or rebalancing. CI Anvil tests prove routing logic; this
runbook proves the configured artifact, issuer wallet, Alpaca request, and
on-chain result together.

Use this runbook for:

- Alpaca sandbox validation on staging ([RAI-1210](https://linear.app/makeitrain/issue/RAI-1210));
- the bounded production Ethereum activation after the Turnkey Base soak
  ([RAI-1508](https://linear.app/makeitrain/issue/RAI-1508)).

The production activation must use the same pinned issuer artifact that passed
the Base cutover. Do not use this runbook to change trading, hedging, wrapping,
or Raindex orders.

## Entry gates

Do not configure or register the Ethereum asset until every applicable gate is
green.

| Gate | Required evidence |
| --- | --- |
| [RAI-1095](https://linear.app/makeitrain/issue/RAI-1095) / [RAI-1096](https://linear.app/makeitrain/issue/RAI-1096) | Ethereum vault deployed; issuer roles verified |
| [RAI-1100](https://linear.app/makeitrain/issue/RAI-1100) | Ethereum RPC endpoint ready |
| [RAI-1102](https://linear.app/makeitrain/issue/RAI-1102) | Turnkey policy and Ethereum DEPOSIT / WITHDRAW / CERTIFY grants verified for the active signer |
| [RAI-1103](https://linear.app/makeitrain/issue/RAI-1103) | Active signer funded with Ethereum gas |
| [RAI-1104](https://linear.app/makeitrain/issue/RAI-1104) | Ethereum receipt subgraph/indexer ready |
| [RAI-1099](https://linear.app/makeitrain/issue/RAI-1099) | Alpaca environment accepts `network=ethereum` |
| [PR #284](https://app.graphite.com/github/pr/ST0x-Technology/st0x.issuance/284) | Issuer parses complete `CHAIN_ETHEREUM_*` groups |
| [PR #1110](https://app.graphite.com/github/pr/ST0x-Technology/st0x.liquidity/1110) | Operator CLI sends Ethereum mint/redeem requests and observes the Ethereum wallet |

For production, the Base-only Turnkey validation and soak must also be complete
before RAI-1508 starts. Stop if the pinned revisions, active signer address,
Ethereum vault/token addresses, canary symbol, or bounded quantity are not
recorded and independently checked.

### Privileged operator handoff

The remaining work is operational, not a request to repair repository docs or
implement another mint path. The operator executing this runbook needs access
to:

- the Ethereum Safe/Turnkey policy and role-grant evidence in RAI-1102;
- the deployment environment and service lifecycle;
- a consistent issuer database backup/restore mechanism;
- the internal issuer API from an allowlisted host;
- the Alpaca-authorized token-list endpoint and the liquidity operator CLI;
- Base and Ethereum chain explorers/RPC evidence.

That operator owns the ordered execution below and attaches only sanitized
identifiers and evidence to Linear. Never paste API keys, signing material, or
secret file contents into Linear, logs, or the handoff.

The old RAI-1212 freeze-guard gate does not apply. The detail endpoint requires
`?network=`, but `GET /tokenized-assets/{underlying}/status` remains
underlying-scoped and has no network query parameter.

## 1. Configure and validate the Ethereum runtime

A second runtime is supported by the merged environment parser. Configure all
four fields as one complete group:

- `CHAIN_ETHEREUM_RPC_URL`
- `CHAIN_ETHEREUM_CHAIN_ID=1`
- `CHAIN_ETHEREUM_SUBGRAPH_URL`
- `CHAIN_ETHEREUM_BACKFILL_START_BLOCK`

Supplying only part of the group fails startup. The grouped Base form
(`CHAIN_BASE_*`) overrides the legacy flat Base variables when both are present;
never mix individual values from the two forms. The deployment unit runs the
shipped `validate-config` binary against the exact decrypted environment before
it allows a restart; preserve that gate rather than validating a reconstructed
shell environment.

Before restart, record:

- pinned issuer revision and deployment identifier;
- active Turnkey address;
- Base and Ethereum chain IDs;
- Ethereum vault and token addresses;
- Ethereum backfill start block, set close enough to activation to avoid an
  unintended historical scan while still covering registration/canary blocks;
- pre-activation Base balances, checkpoints, `/admin/stuck`, and wallet nonce;
- a consistent database backup identifier.

Restart exactly one issuer instance. Runtime construction queries each RPC and
rejects a chain ID that differs from the configured Base `8453` or Ethereum `1`;
a failed start is a hard stop. Independently record the configured and
RPC-reported IDs because the startup `Chain runtime configured` INFO line
currently identifies Base only. A signer, role, RPC, or subgraph mismatch is
also a hard stop.

## 2. Issuance HTTP preflight

Run from the deployment host or a bastion included in `INTERNAL_IP_RANGES`.
Internal endpoints require both `X-API-KEY` and an allowed client IP.

```bash
export ISSUER_BASE_URL=https://staging-issuance.example   # deployment URL
export ISSUER_API_KEY=...                                 # internal key
export STAGING_UNDERLYING=RKLB                            # agreed canary

./scripts/multichain-staging-smoke.nu preflight
# If the checkout has no execute bit:
# nu ./scripts/multichain-staging-smoke.nu preflight
```

The preflight verifies the current contracts:

- Base detail with `?network=base` returns 200 or 404;
- detail without `?network=` returns 422;
- Ethereum detail with `?network=ethereum` returns 200 or 404;
- underlying freeze status without `?network=` returns 200 or 404.

A 403 means the key or source IP is not authorized. Any other status is a hard
stop.

## 3. Base parity canary

Before registering Ethereum, run the already-approved bounded Base canary on
the pinned artifact. Confirm:

1. exactly one Base mint transaction and callback;
2. expected Base balance delta;
3. no Ethereum transaction or balance change; the configured Ethereum poller
   remains healthy but has no Ethereum asset to act on yet;
4. no unexplained `/admin/stuck` entry.

If Base regresses, stop. Do not register the Ethereum asset.

## 4. Register the Ethereum asset

Registration is the first persistent step. `POST /tokenized-assets` rejects an
unconfigured network with 422 before writing an event, but a successful
registration persists immediately and makes the Ethereum runtime mandatory on
subsequent starts.

With the service idle and the database backup recorded:

```bash
export STAGING_UNDERLYING=RKLB
export STAGING_TOKEN=tRKLB
export STAGING_ETHEREUM_VAULT=0x...   # verified Ethereum vault

./scripts/multichain-staging-smoke.nu register-ethereum-asset
./scripts/multichain-staging-smoke.nu verify-ethereum-asset
```

From an Alpaca-authorized source IP, also run:

```bash
./scripts/multichain-staging-smoke.nu verify-token-list
```

The token-list row must include `ethereum` in `networks[]`.

Restart exactly one issuer instance after registration. The transfer poller and
periodic receipt backfill already re-read runtime-added assets, but this explicit
restart forces the new vault through startup receipt backfill, reconciliation,
and live-network validation before a canary can write to it. Confirm startup
logs contain `Spawning dynamic transfer poller for network` with
`network=ethereum`, and confirm the Ethereum vault's receipt-backfill checkpoint
has initialized or advanced before any redemption test.

## 5. Bounded Ethereum mint canary

Use the merged liquidity operator CLI. Ethereum requires the explicit token
address because the liquidity config holds Base token addresses.

```bash
nix run .#st0x-cli -- \
  --config <config-path> \
  --secrets <secrets-path> \
  alpaca-tokenize \
  --symbol RKLB \
  --quantity <approved-bounded-quantity> \
  --network ethereum \
  --token <ethereum-token-address>
```

The CLI generates one issuer request ID and preserves it across bounded Alpaca
backpressure retries. Do not start a second command while the first request is
pending.

Record and verify all of the following:

1. CLI output reports `Network: Ethereum`, the expected token, receiving wallet,
   request ID, and final Alpaca status.
2. Issuer logs show the same request progressing through mint intent, submit,
   confirmation, and callback exactly once.
3. Ethereum shows exactly one expected vault transaction and the expected share
   balance increase at the receiving wallet.
4. Base shows no corresponding transaction or balance change.
5. `/admin/stuck` has no new unexplained entry.

The Ethereum transaction and Base non-event are the authoritative routing proof.
A callback alone is insufficient.

## 6. Bounded Ethereum redemption canary

Only continue after the mint canary is reconciled and the Ethereum poller is
confirmed running.

```bash
nix run .#st0x-cli -- \
  --config <config-path> \
  --secrets <secrets-path> \
  alpaca-redeem \
  --symbol RKLB \
  --quantity <approved-bounded-quantity> \
  --network ethereum \
  --token <ethereum-token-address>
```

Verify:

1. the CLI sends the token from the Ethereum wallet and records the transfer
   hash;
2. issuer logs detect that transfer and report the Alpaca redeem call with
   `network=ethereum`;
3. exactly one Ethereum burn completes and the expected balance decreases;
4. Base remains unchanged;
5. Alpaca reaches the completed state and `/admin/stuck` remains explained.

Do not retry the raw token transfer automatically. It has no issuer-side
idempotency key; resolve an ambiguous submission from chain evidence first.

## Failure and back-out

- **Before Ethereum asset registration:** remove the incomplete Ethereum group,
  fix the gate, and restart Base-only.
- **After registration but before any Ethereum write:** stop the service,
  restore the pre-registration database backup, then remove the Ethereum group.
- **After any Ethereum mint/redeem write:** do not restore the old database or
  remove the Ethereum group. Stop new traffic, preserve request/transaction
  evidence, and roll forward. Restoring pre-write state can duplicate financial
  side effects.
- **Stuck aggregate:** use `/admin/stuck` and
  [`ops-recovery-guide.md`](../ops-recovery-guide.md). Recovery routes by the
  aggregate's persisted network; never substitute Base.
- **Registration rerun:** the helper skips an exact existing token/vault/network
  match and aborts on mismatch. Investigate mismatches; do not overwrite them.

## Close-out evidence

Attach the following to the issue that was actually executed:

- deployment environment, date, pinned revisions, and canary symbol/quantity;
- active signer, Ethereum vault/token, and verified role-grant references;
- pre/post Base and Ethereum balances, checkpoints, and `/admin/stuck` output;
- mint and redemption request IDs, transaction hashes, and callback outcomes;
- confirmation that each side effect occurred exactly once and only on
  Ethereum;
- operator sign-off.

Close staging [RAI-1210](https://linear.app/makeitrain/issue/RAI-1210) only
after the sandbox mint and redemption were actually executed. Close production
[RAI-1508](https://linear.app/makeitrain/issue/RAI-1508) only after activation,
Base-isolation checks, receipt backfill, mint, and redemption are complete.
Do not mark either issue done merely because this runbook or a supporting PR
merged.

## References

- [RAI-1098](https://linear.app/makeitrain/issue/RAI-1098) — issuance
  multichain umbrella
- [RAI-1213](https://linear.app/makeitrain/issue/RAI-1213) — broader CLI
  inventory tooling; wrapping remains separate from these mint/redeem canaries
- [`docs/ops-recovery-guide.md`](../ops-recovery-guide.md) — stuck transaction
  recovery
- [`tokenized-asset-aggregate-rekey.md`](tokenized-asset-aggregate-rekey.md) —
  database cutover/rollback rules
