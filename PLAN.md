# Issuance Bot Implementation Plan

**Total: ~TBD issues, all assigned to @0xgleb**

**Approach: Feature-by-feature development** - Each phase implements a complete
vertical slice (HTTP endpoints → commands/events → aggregates → views → wiring →
tests) to avoid dead code and enable incremental delivery.

---

## Phase 0: Project Bootstrap (3 issues, `enhancement`)

1. Setup Nix flake with Rust toolchain + Cargo project (starting with a single
   crate)
2. Setup Rocket.rs web framework skeleton + SQLite with sqlx
3. Setup CI (GitHub Actions - linting, tests)

---

## Phase 1: ES/CQRS Foundation (2 issues, `enhancement`)

Foundation needed before any features can be implemented.

1. **Implement SqliteEventRepository in crates/sqlite-es** - Custom event store
   implementation for SQLite following postgres-es pattern, includes events and
   snapshots tables
2. **Implement SqliteViewRepository in crates/sqlite-es** - Repository trait
   implementation for persisting views to SQLite

---

## Phase 2: Domain Model (4 issues, `enhancement`)

1. Define newtypes & value objects (TokenizationRequestId, Quantity, etc.)
2. Implement Mint aggregate (commands, events, handle/apply methods)
3. Implement Redemption aggregate
4. Implement AccountLink & TokenizedAsset aggregates

---

## Phase 3: Views & Projections (6 issues, `enhancement`)

1. Implement MintView
2. Implement RedemptionView
3. Implement AccountLinkView
4. Implement TokenizedAssetView
5. Implement ReceiptInventoryView
6. Implement InventorySnapshotView

---

## Phase 4: External Services (3 issues, `enhancement`)

1. Implement AlpacaService (use `apca` crate, API key+secret auth)
2. Implement BlockchainService (alloy for RPC, tx signing, gas estimation)
3. Implement MonitorService (WebSocket subscription for redemption wallet)

---

## Phase 5: Issuer HTTP Endpoints (4 issues, `enhancement`)

1. POST /accounts/connect (account linking)
2. GET /tokenized-assets (list supported assets)
3. POST /inkind/issuance (mint request handler)
4. POST /inkind/issuance/confirm (journal confirmation handler)

---

## Phase 6: Alpaca Client Integration (3 issues, `enhancement`)

1. POST mint callback endpoint client
2. POST redeem endpoint client
3. GET requests polling client

---

## Phase 7: Blockchain Integration (4 issues, `enhancement`)

1. Generate OffchainAssetReceiptVault contract bindings (alloy)
2. Implement deposit (mint) with receipt metadata
3. Implement withdraw (burn) with receipt tracking
4. Implement gas management & receipt inventory

---

## Phase 8: Business Flows (3 issues, `enhancement`)

1. Complete mint flow orchestration (request � journal � mint � callback)
2. Complete redemption flow (detect � alpaca � poll � burn)
3. Error handling & recovery strategies

---

## Phase 9: Testing (4 issues, `enhancement`)

1. Aggregate unit tests (Given-When-Then pattern)
2. Service integration tests (mock Alpaca/blockchain)
3. End-to-end flow tests
4. Test framework & mock services

---

## Phase 10: Operations & Deployment (6 issues, `enhancement`)

1. Private key management strategy (encrypted file/HSM/KMS)
2. API authentication for our endpoints (API key or mTLS)
3. Configuration & secrets management
4. Integrate HyperDX for observability
5. Integrate Grafana for metrics
6. Setup CD (GitHub Actions - deployment)

---

## Phase 11: Documentation (3 issues, `documentation`)

1. API documentation (OpenAPI spec)
2. Deployment & operations guide
3. Recovery procedures runbook

---

## Notes

- Phase 0 approved 
- Phase 1 revised - removed migrations (part of sqlx setup) and snapshot
  implementation (built-in feature)
- Removed OAuth 2.0 requirement (Alpaca uses simple API key+secret via `apca`
  crate)
