# Onboard a tokenized asset on a configured chain

How a newly deployed token becomes mintable and redeemable by the bot.

**There is no code or configuration change.** A listing is runtime state in the
`TokenizedAsset` aggregate (SPEC "TokenizedAsset Aggregate"), written by one
`POST /tokenized-assets` call against the running service. No symbol, vault, or
wrapper address is compiled in or baked into `config.prod.toml`. Ship nothing;
call the endpoint.

This runbook covers a network that is **already configured** — the ordered
procedure for standing up a *new* network is
`docs/runbooks/multichain-staging-validation.md`, and the orchestrator cutover
for an already-listed asset is `docs/runbooks/orchestrator-onboarding.md`.

## Prerequisites

1. **The token is deployed and the addresses are final.** They come from the
   sft-ops CD run that broadcast the deploy: its `cd/ledger-<run_id>` branch
   updates `ops/launches.json` with the entry's `sft` (the
   `OffchainAssetReceiptVault`) and `wrapper` (the ERC-4626 wrapper). The `sft`
   address is the `vault` this endpoint wants — never the `wrapper`.
2. **The bot wallet can move the vault.** The vault's authorizer must grant the
   bot wallet `DEPOSIT` and `WITHDRAW` (a deploy-side grant).
3. **The Turnkey policy lists the vault.** Mint is
   `multicall([deposit, transfer])` and redemption is `multicall([redeem × N])`
   (`src/vault/service.rs`), and the policies that match that outer selector
   carry an explicit per-chain receipt-vault list. A vault missing from that
   list is refused at signing with 403 on every mint and burn — see
   ST0x-Technology/turnkey-policy-spec#32. Get the policy change applied and
   verified **before** the first mint.
4. **The network is configured on this deployment.** Registration is rejected
   with 422 before any event is written when the network has no
   `CHAIN_<NETWORK>_*` group, because an asset on an unconfigured network aborts
   the next boot in `validate_configured_asset_networks`
   (`src/tokenized_asset/api.rs`).

## Register the asset

`POST /tokenized-assets` takes `InternalAuth` (`src/auth/mod.rs`): the
`X-API-KEY` header **and** a client IP inside `INTERNAL_IP_RANGES`. Run it on
the issuer host against the service's own listener.

```bash
# On the issuer host.
export ISSUER_BASE_URL=http://localhost:8000
export ISSUER_API_KEY=…            # the service's own key; never echo it

export UNDERLYING=BIRD             # equity symbol
export TOKEN=tBIRD                 # launches.json `symbol`
export NETWORK=base
export VAULT=0x…                   # launches.json `sft` for this chain
```

**Pre-check first.** Re-adding an existing asset with a *different* vault is not
an error — it emits `VaultAddressUpdated` and silently repoints the listing. A
`404` here is the proof that this is a new listing:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  -H "X-API-KEY: $ISSUER_API_KEY" \
  "$ISSUER_BASE_URL/tokenized-assets/$UNDERLYING?network=$NETWORK"
# expect 404 (200 means it is already listed — stop and compare the vault)
```

Then register:

```bash
curl -sS -X POST \
  -H "X-API-KEY: $ISSUER_API_KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"underlying\":\"$UNDERLYING\",\"token\":\"$TOKEN\",\"network\":\"$NETWORK\",\"vault\":\"$VAULT\"}" \
  "$ISSUER_BASE_URL/tokenized-assets"
# expect 201 and {"underlying":"BIRD"}
```

`422` means one of: an unconfigured network, an empty or invalid symbol, or that
vault address already serving another underlying on this network.

## Verify

```bash
curl -sS -H "X-API-KEY: $ISSUER_API_KEY" \
  "$ISSUER_BASE_URL/tokenized-assets/$UNDERLYING?network=$NETWORK"
# 200, and token/network/vault match exactly what was posted
```

From an Alpaca-whitelisted source IP, `GET /tokenized-assets` must now carry a
row for `(underlying, token)` with the network in `networks[]`.

**No restart is required on an already-running chain.** The transfer poller
re-reads the enabled-asset set every pass
(`list_enabled_assets`, `src/redemption/poller.rs`) and the periodic receipt
backfill re-reads it every tick (`src/lib.rs`), so the new vault is picked up
within one poll interval. A restart is only needed when the listing is the first
one on a newly configured network, which is the multichain-validation runbook's
job.

## Optional: watch the wrapper

The inbound wrapped-token backstop is configured separately, per deployment, by
the `[wrapped_tokens.<network>]` tables in the TOML config
(`config.example.toml`); it takes the `wrapper` address, not the `sft`. It is
off for any chain with no table (startup WARN). If this deployment runs the
watcher, add the new token's `wrapper` to that chain's table in the same change
that ships the listing — an unwatched wrapper means a wrapped-token transfer to
the issuer wallet is lost silently (SPEC "Per network monitoring").
