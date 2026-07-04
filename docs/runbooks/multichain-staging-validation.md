# Multichain staging validation runbook (RAI-1210)

Manual Alpaca **sandbox** end-to-end on a second ITN `network` wire (default:
`ethereum`). CI Anvil tests cover routing logic; this runbook proves the
deployed stack against real Alpaca + chain B infrastructure.

## External gates (must be green before starting)

| Issue                                                                                                               | What                                                              |
| ------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| [RAI-1099](https://linear.app/makeitrain/issue/RAI-1099)                                                            | Alpaca sandbox issuer enabled on target `network`                 |
| [RAI-1095](https://linear.app/makeitrain/issue/RAI-1095) / [RAI-1096](https://linear.app/makeitrain/issue/RAI-1096) | Chain B vault deployed and permissioned                           |
| [RAI-1100](https://linear.app/makeitrain/issue/RAI-1100)-[RAI-1104](https://linear.app/makeitrain/issue/RAI-1104)   | RPC, Turnkey/local signing, gas, subgraph URL for chain B         |
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

- **Base (legacy block):** `RPC_URL`, `CHAIN_ID=8453`, `SUBGRAPH_URL`,
  `BACKFILL_START_BLOCK` -- unchanged from pre-multichain.
- **Ethereum registry entry:** second chain wired (via deploy secrets / planned
  `CHAIN_ETHEREUM_*` env -- the shape is documented in `.env.secrets.example`
  and the SPEC.md Multi-chain section). A live asset whose network has no
  registry entry **aborts startup** (the process exits with a
  `network ethereum is not configured` error in the failure chain) -- there is
  no running-but-degraded state to grep for; if the service is up, this check
  passed.
- **Signing backend:** exactly one of Turnkey (`TURNKEY_ORG_ID`,
  `TURNKEY_API_PRIVATE_KEY`, `TURNKEY_ADDRESS`) or local dev (`EVM_PRIVATE_KEY`)
  — see `src/wallet/mod.rs` and `.env.secrets.example`. The bot address must be
  permissioned to sign burns on **both** chains.
- **Bot wallet** funded with native gas on **both** chains.

Record the chain B vault address from
[RAI-1095](https://linear.app/makeitrain/issue/RAI-1095) deploy output -- needed
for asset registration below.

## 2. Preflight (issuance HTTP)

From a host that can reach staging with the internal API key. The preflight,
register, and verify commands all hit `InternalAuth` endpoints, which require
**both** a valid `X-API-KEY` **and** a client IP inside `INTERNAL_IP_RANGES`
(default: localhost + Docker ranges only) -- a whitelisted key from a
non-whitelisted host gets **403**, which the script reports as an unexpected
status. Run from the staging host itself or a bastion whose IP is in
`INTERNAL_IP_RANGES`:

```bash
export ISSUER_BASE_URL=https://staging-issuance.example   # adjust
export ISSUER_API_KEY=...                                   # internal key
./scripts/multichain-staging-smoke.nu preflight
# If the execute bit is missing on the checkout, invoke through nu instead:
# nu ./scripts/multichain-staging-smoke.nu preflight
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

**Do not register before the Ethereum chain config is live** (section 1
checklist complete). `POST /tokenized-assets` rejects an unconfigured `network`
with **422** before any event is written (`ConfiguredNetworks` guard in
`src/tokenized_asset/api.rs`), so a failed registration attempt leaves nothing
to remediate. Once config is present and registration succeeds, the
`TokenizedAsset` is persisted immediately; on the next process start, startup
validation requires every live asset's network to have a registry entry.

**Back up the staging database first.** A successful registration is the first
irreversible step of this runbook (see "If validation fails" below): copy the
SQLite file (e.g. `cp issuance.db issuance.db.pre-chain-b`) while the service is
stopped or idle, so a full back-out stays possible.

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

**Then restart the issuance service before proceeding.** Transfer pollers are
spawned once at startup, per network that has at least one **live** asset on
that network at boot (`list_enabled_assets`: `Enabled` or `Frozen` — same set
startup validation walks). The ethereum asset did not exist when the service
came up, so no ethereum poller is running and the redeem in section 6 would
never be detected. After the restart, confirm the startup log shows
`Spawning transfer poller for network` with `network=ethereum`. Note the
checkpoint behaviour: a first-ever ethereum poll has no `transfer_poll:ethereum`
checkpoint and starts scanning from that chain's `backfill_start_block`, so set
it near the current head block to avoid a long historic scan.

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

| Symptom                       | Likely cause                                                 |
| ----------------------------- | ------------------------------------------------------------ |
| Mint stuck in `MintingFailed` | Wrong-chain RPC, signer misconfiguration, or gas on Ethereum |
| Callback never sent           | Alpaca API error -- check issuance `alpaca` logs             |
| Shares on wrong chain         | Registry misconfiguration -- see RAI-1206                    |

Use `GET /admin/stuck` (see [`ops-recovery-guide.md`](../ops-recovery-guide.md))
if a mint does not reach a terminal state.

## 6. Alpaca sandbox redeem on `ethereum`

1. Ensure the test wallet holds chain B shares from step 5.
2. In Alpaca sandbox, initiate **redeem** with `"network": "ethereum"`.
3. User sends shares to the bot wallet on the **Ethereum** vault (Alpaca flow).
4. Watch issuance logs:
   - The post-registration restart (section 4) logged
     `Spawning transfer poller for network` with `network=ethereum` at startup.
   - Successful detection logs `Redemption transfer detected` with
     `issuer_request_id` (the value is the tx hash; the field name is not
     `tx_hash`) and `from`.
   - `Alpaca redeem API call succeeded` carries `network=ethereum`.
   - Burn submits on Ethereum RPC via the Turnkey/local signer, not Base —
     confirmed by the on-chain balance check below.
5. Confirm Alpaca redeem completes and on-chain balance decreased on Ethereum.

## If validation fails

- **Stuck mint or redemption:** use `GET /admin/stuck` and the recovery
  endpoints per [`ops-recovery-guide.md`](../ops-recovery-guide.md). Recovery
  routes by the aggregate's persisted `network`, so recovering a chain B
  aggregate acts on chain B — it cannot touch Base state.
- **Retrying:** no de-registration is needed. `register-ethereum-asset` GETs the
  asset first and skips POST when token/vault/network already match; it aborts
  on mismatch (a different vault emits `VaultAddressUpdated` on re-POST, and
  token typos are not compared server-side). A failed mint/redeem can be
  re-driven after recovery without touching the asset.
- **Do not remove the `CHAIN_ETHEREUM_*` config while the ethereum asset
  exists.** There is no de-listing command, and freezing does not help: startup
  validation checks every live asset — frozen included — against the registry,
  so pulling the chain config after registration leaves the host unable to boot
  (the premature-registration warning in section 4 applies in reverse).
- **Full back-out:** stop the service, restore the database from the
  pre-registration backup taken in section 4, and only then remove the chain B
  config. This is the only supported way to unwind the registration itself.

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
      links once all MVP success criteria on RAI-1098 are met.

## References

- [`docs/ops-recovery-guide.md`](../ops-recovery-guide.md) -- stuck tx recovery
- [`tokenized-asset-aggregate-rekey.md`](tokenized-asset-aggregate-rekey.md) --
  DB cutover before multichain deploy
