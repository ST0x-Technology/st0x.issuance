# Multichain MVP -- implementation plan

**MVP scope:** Upgrade `st0x.issuance` for **full multichain ITN operation** on
every chain Alpaca enables -- not mint-only routing. Issuance code covers:

| Capability                                            | Issuance work (Linear) | Prerequisite (external)                            |
| ----------------------------------------------------- | ---------------------- | -------------------------------------------------- |
| Per-chain RPC, signer, registry                       | RAI-1204               | RAI-1100, RAI-1102, RAI-1103                       |
| Token listing + asset registration                    | RAI-1205               | RAI-1095 vault deployed on second chain (RAI-1094) |
| Mint ITN (initiate -> confirm -> callback)            | RAI-1206               | RAI-1099, RAI-1094                                 |
| Redemption + burn (detect -> Alpaca -> on-chain burn) | RAI-1207               | RAI-1095, RAI-1096                                 |
| Receipt inventory backfill/reconcile                  | RAI-1208               | RAI-1104                                           |
| Config templates + staging E2E                        | RAI-1209, RAI-1210     | RAI-1099                                           |

**Contract deployment**
([RAI-1095](https://linear.app/makeitrain/issue/RAI-1095),
[RAI-1096](https://linear.app/makeitrain/issue/RAI-1096),
[RAI-1211](https://linear.app/makeitrain/issue/RAI-1211)) is **not issuance
code** -- `st0x.deploy` lands vaults on the target chain. Issuance consumes
vault addresses when registering assets via add-asset; burns and mints route to
those vaults through `ChainRegistry`. Factory availability gates chain choice
via [RAI-1094](https://linear.app/makeitrain/issue/RAI-1094).

Base-only config stays identical until each multichain PR merges.

**Design gate:** [RAI-1200](https://linear.app/makeitrain/issue/RAI-1200) --
sign-off before implementation PRs merge. Decisions live in this plan and the
SPEC multichain section.

**Implementation umbrella:**
[RAI-1098](https://linear.app/makeitrain/issue/RAI-1098)\
**Status:** In progress\
**Stack base:** `main`

Do not start implementation PRs until
[RAI-1200](https://linear.app/makeitrain/issue/RAI-1200) gate items are
satisfied.

---

## Linear hierarchy

| Issue                                                                                                             | Role                                  | Parent   |
| ----------------------------------------------------------------------------------------------------------------- | ------------------------------------- | -------- |
| [RAI-1200](https://linear.app/makeitrain/issue/RAI-1200)                                                          | Design sign-off / gate                | --       |
| [RAI-1203](https://linear.app/makeitrain/issue/RAI-1203)                                                          | Gate doc + SPEC (Graphite #209)       | RAI-1200 |
| [RAI-1098](https://linear.app/makeitrain/issue/RAI-1098)                                                          | Implementation umbrella               | --       |
| [RAI-1204](https://linear.app/makeitrain/issue/RAI-1204)-[RAI-1210](https://linear.app/makeitrain/issue/RAI-1210) | Implementation work (see stack table) | RAI-1098 |

---

## External gates (not issuance PRs)

| Issue                                                    | Blocks                                                                                                                                                                                                                                                                                       |
| -------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [RAI-1099](https://linear.app/makeitrain/issue/RAI-1099) | Alpaca enables issuer on target `network` for staging/prod (after enum covers chosen chain)                                                                                                                                                                                                  |
| [RAI-1094](https://linear.app/makeitrain/issue/RAI-1094) | Pick first non-Base chain (**HyperEVM** or **Ethereum**), add `Network` variant, addresses. Alpaca ITN `network` enum today: `solana`, `base`, `arbitrum`, `ethereum`, `binance`, `ton`, `tron`, `mantle` (+ `cronos` in issuer guide) -- **no HyperEVM or Tempo** until Alpaca adds a value |
| [RAI-1095](https://linear.app/makeitrain/issue/RAI-1095) | Vault addresses on new chain                                                                                                                                                                                                                                                                 |
| [RAI-1096](https://linear.app/makeitrain/issue/RAI-1096) | On-chain permissions                                                                                                                                                                                                                                                                         |
| [RAI-1100](https://linear.app/makeitrain/issue/RAI-1100) | Per-chain RPC creds                                                                                                                                                                                                                                                                          |
| [RAI-1102](https://linear.app/makeitrain/issue/RAI-1102) | Fireblocks whitelist                                                                                                                                                                                                                                                                         |
| [RAI-1103](https://linear.app/makeitrain/issue/RAI-1103) | Gas funding                                                                                                                                                                                                                                                                                  |
| [RAI-1104](https://linear.app/makeitrain/issue/RAI-1104) | Subgraph indexer deployment (ops supplies URL for chain config)                                                                                                                                                                                                                              |
| [RAI-1211](https://linear.app/makeitrain/issue/RAI-1211) | Zoltu factory on candidate chain (feeds RAI-1094 chain choice)                                                                                                                                                                                                                               |

Deploy, contract rollout (`st0x.deploy`), Fireblocks, gas, and subgraph indexer
work are **external**. Issuance reads vault addresses at asset-registration time
and `subgraph_url` from per-chain config -- same fields as legacy single-chain
startup.

**Cross-repo ([RAI-1205](https://linear.app/makeitrain/issue/RAI-1205) +
[RAI-1212](https://linear.app/makeitrain/issue/RAI-1212)):** when internal
`GET /tokenized-assets/{underlying}` gains required `?network=`,
`st0x-issuance-client` and liquidity freeze guard must ship in the same deploy
window (Nix `outputHashes` in `st0x.liquidity`). **RAI-1212 blocks the RAI-1205
deploy** -- liquidity-side PR, not multichain liquidity scope.

---

## Graphite stack

| # | Branch                               | Linear   | Delivers                                                          |
| - | ------------------------------------ | -------- | ----------------------------------------------------------------- |
| 1 | `docs/multichain-spec-and-plan`      | RAI-1203 | This plan + SPEC multichain section                               |
| 2 | `feat/multichain-chain-registry`     | RAI-1204 | `ChainRegistry`, Base parity, startup validation                  |
| 3 | `feat/multichain-asset-key`          | RAI-1205 | AssetKey, aggregate-store rekey, token listing, `?network=` break |
| 4 | `feat/multichain-mint-chain-b`       | RAI-1206 | Mint job routing, second chain in CI                              |
| 5 | `feat/multichain-redemption-chain-b` | RAI-1207 | Redemption detect/burn, poller, BurnManager registry routing      |
| 6 | `feat/multichain-receipt-backfill`   | RAI-1208 | Per-chain receipt backfill/reconcile                              |
| 7 | `chore/multichain-config-templates`  | RAI-1209 | `.env.example`, clippy                                            |
| 8 | `ops/multichain-staging-validation`  | RAI-1210 | Alpaca sandbox E2E, deploy close-out                              |

Implementation PRs (RAI-1204 onward) are gated on
[RAI-1200](https://linear.app/makeitrain/issue/RAI-1200) design sign-off and
merge in stack order.

**Order:** RAI-1203 -> RAI-1204 -> RAI-1205 -> RAI-1206 -> RAI-1207 -> RAI-1208
-> RAI-1210. RAI-1209 (templates/clippy) can land in parallel with
RAI-1205-1208.

---

## Acceptance criteria

### Gate -- RAI-1200 / RAI-1203

- SPEC multichain section matches this plan (AssetKey, API break, deploy
  constraints).
- Alpaca `network` enum covers the chosen chain
  ([RAI-1094](https://linear.app/makeitrain/issue/RAI-1094)); request Alpaca add
  the value if missing (e.g. HyperEVM). ITN wire mechanics (mint `network`,
  redemption callback, token list shape) match what we already serve -- no comms
  gate for design sign-off
  ([RAI-1099](https://linear.app/makeitrain/issue/RAI-1099) tracks go-live
  enablement).
- [RAI-1205](https://linear.app/makeitrain/issue/RAI-1205) **aggregate rekey
  runbook** is a deliverable of the RAI-1205 PR (not a post-merge artifact).
  Must cover: backup/restore, dry run against a copy of the prod DB, and
  idempotency (safe to re-run if migration dies mid-flight). Reviewed before
  merge.

### RAI-1204 -- Chain registry

- `ChainRegistry` at startup; legacy env -> single `base` entry.
- All existing paths use `registry.base()` -- behaviour identical to today.
- Startup fails if a live asset's `network` has no chain entry.
- Unit tests: duplicate network, `get` miss, Fireblocks asset-id guard.
  Duplicate chain_id test lands with the RAI-1094 `Network` variant (HyperEVM or
  Ethereum).

### RAI-1205 -- AssetKey + token listing

Not SQL-only: includes in-place **aggregate-store rekey**
(`events.aggregate_id`, `snapshots`) and all live `store.send` call sites -- not
just a SQL migration/view rebuild.

- `TokenizedAsset` aggregate id `{underlying}:{network}`; SQL migration,
  aggregate-store rekey/backfill (`events.aggregate_id`, `snapshots`), and view
  rebuild.
- **Rekey runbook (gate):**
  [`docs/runbooks/tokenized-asset-aggregate-rekey.md`](runbooks/tokenized-asset-aggregate-rekey.md)
  (backup/restore, dry run on a prod DB copy, idempotent migration steps) --
  shipped in the RAI-1205 PR (#214).
- Rekey live write path: `store.send(&asset_key, ...)` in
  `tokenized_asset/api.rs` (add-asset) and all other `store.send` call sites.
  `AddTokenizedAssetResponse` wire shape is unchanged (`underlying` remains the
  bare ticker).
- **Token listing:** `GET /tokenized-assets` (Alpaca ITN) merges rows when the
  same `(underlying, token)` exists on multiple chains -- union of `networks[]`.
  JSON shape `{ tokens, networks[] }` is unchanged, but **row count drops**
  relative to today (one row per chain). Sort `tokens` by underlying ascending;
  sort each row's `networks[]` by network wire string. Add-asset registers a
  vault on the target `network` (requires vault address from
  [RAI-1095](https://linear.app/makeitrain/issue/RAI-1095)).
- Mint initiate, confirm, and recovery: network-aware asset/vault lookup.
- Admin/CLI freeze, unfreeze, status: network-aware asset key (not
  underlying-only).
- Required `?network=` on internal detail/status; dto + client break.
- Liquidity PR coordinated in same deploy window
  ([RAI-1212](https://linear.app/makeitrain/issue/RAI-1212)).

### RAI-1206 -- Mint chain B

- Mint jobs resolve `VaultService` from `registry.get(mint.network)`.
- E2e mint on second Anvil chain in CI; Base unchanged.
- **Deploy:** enables Chain B mint ITN (requires external gates for prod).

### RAI-1207 -- Redemption + burn

- `network` persisted on redemption metadata/events (`Detect`, `Reprocessed`,
  `BurnResumed`, admin replay).
- Per-chain transfer poller: each chain scans only its vault set on the correct
  RPC.
- `Redemption::Services = ChainRegistry`; `BurnTokens` / `ConfirmBurn` resolve
  `registry.get(metadata.network)`.
- **BurnManager:** live `handle_burning_started`, recovery, and direct
  `check_fireblocks_tx` / `verify_burn_tx` paths use registry routing (not the
  global Base `VaultService`).
- Alpaca redeem notify/callback carries the aggregate's `network`.
- `#[serde(default)]` on historical redemption snapshots where needed.
- E2e: transfer to bot wallet on chain B vault -> Alpaca redeem -> burn on chain
  B.
- **Deploy:** enables Chain B redemption (requires
  [RAI-1095](https://linear.app/makeitrain/issue/RAI-1095) +
  [RAI-1096](https://linear.app/makeitrain/issue/RAI-1096)).

### RAI-1208 -- Receipt backfill

- Startup + periodic backfill partitioned by asset `network`.
- Per-chain provider from `ChainRegistry` (not a single global provider).
- **Deploy:** enables Chain B receipt inventory (requires
  [RAI-1104](https://linear.app/makeitrain/issue/RAI-1104) for prod URL).

### RAI-1209 -- Config templates

- `.env.example` documents per-chain registry env shape.
- `cargo clippy` / `cargo fmt` clean on touched files.

### RAI-1210 -- Staging close-out

- Alpaca sandbox E2E mint + redeem on second `network` wire.
- Deploy runbook executed;
  [RAI-1098](https://linear.app/makeitrain/issue/RAI-1098) and
  [RAI-1200](https://linear.app/makeitrain/issue/RAI-1200) closed with PR links.
- **Deploy:** Alpaca prod/staging new wire (requires
  [RAI-1099](https://linear.app/makeitrain/issue/RAI-1099) + RAI-1204-1208 +
  RAI-1209).

---

## Deploy constraints (partial deploys)

| Chain B capability              | Requires merged | Also requires (external)  |
| ------------------------------- | --------------- | ------------------------- |
| Register asset + ITN token list | RAI-1205        | RAI-1095 vault on chain B |
| Mint ITN                        | RAI-1206        | RAI-1099, RAI-1094        |
| Redemption + on-chain burn      | RAI-1207        | RAI-1095, RAI-1096        |
| Receipt inventory               | RAI-1208        | RAI-1104                  |
| Alpaca prod/staging new wire    | RAI-1210        | RAI-1099 + ops gates      |

Base-only config must stay behaviour-identical after each merge. Do not expose
Chain B vaults to user redemption traffic before
[RAI-1207](https://linear.app/makeitrain/issue/RAI-1207).

---

## MVP success criteria (closes RAI-1098)

- [ ] Target chain vaults deployed and permissioned (RAI-1095, RAI-1096).
- [ ] Alpaca `network` enum covers chosen chain; issuer enabled on target
      `network` for staging/prod (RAI-1094, RAI-1099).
- [ ] Staging/prod: Base + target chain configured in registry.
- [ ] Token listing: chain B asset registered; `GET /tokenized-assets` shows
      merged `networks[]`.
- [ ] Mint ITN: initiate -> confirm -> on-chain -> callback on second chain.
- [ ] Redemption: detect -> Alpaca (correct `network`) -> burn on second chain.
- [ ] Receipt inventory backfill + reconcile correct for vaults on both chains.
- [ ] Base-only config: zero regression.
- [ ] SPEC.md documents multichain config, AssetKey, token listing, burn
      routing, and HTTP break.

---

## Post-cutover cleanup: sunset the legacy Base-only config path

The legacy flat env vars (`RPC_URL`, `CHAIN_ID`, `SUBGRAPH_URL`,
`BACKFILL_START_BLOCK` -> one `base` registry entry) and the legacy checkpoint
fallback (`transfer_poll` -> `transfer_poll:base`) exist only so current
Base-only production upgrades in place while the external gates clear. They are
transitional compatibility, not a supported long-term configuration -- keeping
two config paths invites silent drift between them.

One deploy cycle after staging and production have moved to the multichain
config format:

- [ ] Migrate staging and prod `.env` to the `[[chains]]` / `CHAIN_*` format.
- [ ] Delete `legacy_base_chain_config` and the flat-var -> registry mapping.
- [ ] Delete the legacy `transfer_poll` checkpoint fallback read.
- [ ] Drop the "Base (legacy block)" section from `.env.example` and the staging
      runbook.

---

## References

- [RAI-1200](https://linear.app/makeitrain/issue/RAI-1200) -- design gate
- [RAI-1098](https://linear.app/makeitrain/issue/RAI-1098) -- implementation
  umbrella
- `SPEC.md` -- Multi-chain section (Graphite #209)
- `docs/workflow.md` -- failing test before fix
