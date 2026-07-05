# Multichain staging validation runbook (RAI-1210)

Manual Alpaca **sandbox** end-to-end on a second ITN `network` wire (default:
`ethereum`). CI Anvil tests cover routing logic; this runbook proves the
deployed stack against real Alpaca + chain B infrastructure.

## External gates (must be green before starting)

| Issue                                                                                                               | What                                                              |
| ------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| [RAI-1099](https://linear.app/makeitrain/issue/RAI-1099)                                                            | Alpaca sandbox issuer enabled on target `network`                 |
| [RAI-1095](https://linear.app/makeitrain/issue/RAI-1095) / [RAI-1096](https://linear.app/makeitrain/issue/RAI-1096) | Chain B vault deployed and permissioned                           |
| [RAI-1100](https://linear.app/makeitrain/issue/RAI-1100)-[RAI-1104](https://linear.app/makeitrain/issue/RAI-1104)   | RPC, Fireblocks whitelist, gas for chain B                        |
| [RAI-1212](https://linear.app/makeitrain/issue/RAI-1212)                                                            | Liquidity freeze guard sends `?network=` (lockstep with RAI-1205) |

Issuance multichain code (RAI-1204-1208) must be on the staging host. If
upgrading an existing single-chain DB, run
[`tokenized-asset-aggregate-rekey.md`](tokenized-asset-aggregate-rekey.md)
first.

**Additionally:** production/staging parsing of multichain chain config
(`CHAIN_ETHEREUM_*` / `[[chains]]` -> a second `ChainRegistry` entry) is **not
yet implemented** -- `Config::chains` only ever holds the single Base entry
mapped from the legacy env vars; extra entries are appended by the test harness
alone. This runbook is not executable until that config path lands; treat it as
a blocking gate alongside the table above.

## 1. Staging config checklist

On the staging host, confirm:

- **Base (legacy block):** `RPC_URL`, `CHAIN_ID=8453`, `BACKFILL_START_BLOCK` --
  unchanged from pre-multichain.
- **Ethereum registry entry:** second chain wired (via deploy secrets / planned
  `CHAIN_ETHEREUM_*` env -- see `.env.example`). Startup must not log
  `NetworkNotConfigured` for any live asset.
- **Fireblocks:** `FIREBLOCKS_CHAIN_ASSET_IDS` includes both chains, e.g.
  `8453:BASECHAIN_ETH,1:ETH`.
- **Bot wallet** funded with native gas on **both** chains.

Record the chain B vault address from
[RAI-1095](https://linear.app/makeitrain/issue/RAI-1095) deploy output -- needed
for asset registration below.

## 2. Preflight (issuance HTTP)

From a host that can reach staging with the internal API key:

```bash
export ISSUER_BASE_URL=https://staging-issuance.example   # adjust
export ISSUER_API_KEY=...                                   # internal key
./scripts/multichain-staging-smoke.nu preflight
```

Expect:

- `GET .../status?network=base` -> **200** for a known Base asset (**404** if
  staging has no Base asset registered -- the script accepts either).
- `GET .../status` without `?network=` -> **422**.
- `GET .../status?network=ethereum` -> **200** or **404** (404 before
  registration).

## 3. Base parity smoke

Before exercising chain B, confirm Base-only behaviour is unchanged:

1. Alpaca sandbox **mint on `base`** for an existing asset (e.g. smoke symbol
   used in staging today).
2. Confirm journal -> on-chain mint -> callback completes (existing monitoring).
3. Optional: sandbox **redeem on `base`** and verify burn on Base.

If Base regresses, stop -- do not proceed to chain B.

## 4. Register chain B asset

Register the RAI-1095 vault on `ethereum` (replace symbols / vault):

```bash
export STAGING_UNDERLYING=TSLA
export STAGING_TOKEN=tTSLA
export STAGING_ETHEREUM_VAULT=0x...   # chain B vault from RAI-1095

./scripts/multichain-staging-smoke.nu register-ethereum-asset
./scripts/multichain-staging-smoke.nu verify-ethereum-asset
```

Optionally, from an Alpaca-whitelisted IP, run
`./scripts/multichain-staging-smoke.nu verify-token-list` to confirm ITN list
merge (`networks[]` includes `ethereum` for the test row).

## 5. Alpaca sandbox mint on `ethereum`

This step is **Alpaca-driven** -- issuance receives ITN callbacks; you do not
POST mint initiate yourself.

1. In Alpaca sandbox, start a tokenization **mint** with `"network": "ethereum"`
   for the registered `(underlying, token)`.
2. Watch issuance logs for the mint progressing through submit, confirm, and
   callback (entries carry the `issuer_request_id`). The mint path does not log
   a `network` field, so the routing proof is the on-chain check below.
3. Verify on-chain: share balance increased on the **Ethereum** vault
   (Etherscan), not Base. This is the authoritative wrong-chain check.
4. Confirm Alpaca received a successful mint callback.

Failure modes:

| Symptom                       | Likely cause                                             |
| ----------------------------- | -------------------------------------------------------- |
| Mint stuck in `MintingFailed` | Wrong-chain RPC, Fireblocks asset id, or gas on Ethereum |
| Callback never sent           | Alpaca API error -- check issuance `alpaca` logs         |
| Shares on wrong chain         | Registry misconfiguration -- see RAI-1206                |

Use `GET /admin/stuck` (see [`ops-recovery-guide.md`](../ops-recovery-guide.md))
if a mint does not reach a terminal state.

## 6. Alpaca sandbox redeem on `ethereum`

1. Ensure the test wallet holds chain B shares from step 5.
2. In Alpaca sandbox, initiate **redeem** with `"network": "ethereum"`.
3. User sends shares to the bot wallet on the **Ethereum** vault (Alpaca flow).
4. Watch issuance logs:
   - Startup logged `Spawning transfer poller for network` with
     `network=ethereum` (per-detection entries carry `tx_hash`, not `network`).
   - `Alpaca redeem API call succeeded` carries `network=ethereum`.
   - Burn submits on Ethereum Fireblocks/RPC, not Base -- confirmed by the
     on-chain balance check below.
5. Confirm Alpaca redeem completes and on-chain balance decreased on Ethereum.

## 7. Close-out checklist (RAI-1210 done)

- [ ] Preflight script green on staging.
- [ ] Base parity mint (and optionally redeem) unchanged.
- [ ] Chain B asset registered; ITN list shows expected `networks[]`.
- [ ] Alpaca sandbox mint on `ethereum` completed end-to-end.
- [ ] Alpaca sandbox redeem on `ethereum` completed end-to-end.
- [ ] No stuck mints/redemptions (`/admin/stuck` empty or explained).
- [ ] Linear [RAI-1210](https://linear.app/makeitrain/issue/RAI-1210) updated
      with date, staging host, and test asset symbols.
- [ ] Umbrella [RAI-1098](https://linear.app/makeitrain/issue/RAI-1098) and gate
      [RAI-1200](https://linear.app/makeitrain/issue/RAI-1200) closed with PR
      links once all MVP success criteria in the implementation plan are met.

## References

- [`docs/multichain-implementation-plan.md`](../multichain-implementation-plan.md)
  -- MVP success criteria
- [`docs/ops-recovery-guide.md`](../ops-recovery-guide.md) -- stuck tx recovery
- [`tokenized-asset-aggregate-rekey.md`](tokenized-asset-aggregate-rekey.md) --
  DB cutover before multichain deploy
