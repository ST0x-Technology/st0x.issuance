# Issuance Bot Implementation Plan

**Total: 31 issues, all assigned to @0xgleb**

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

## Phase 2: Account Linking Feature (2 issues, `enhancement`)

Simple feature to start with - links AP accounts to our system.

**Endpoints:**

- POST /accounts/connect

**Flow:** AP provides email + alpaca_account → we validate → return client_id

**Issues:**

1. **Add POST /accounts/connect endpoint stub**
   - Returns 200 with dummy client_id
   - Define request/response types (Email, AlpacaAccountNumber, ClientId
     newtypes)
2. **Implement LinkAccount feature and connect endpoint**
   - LinkAccount command, AccountLinked event, DomainEvent trait
   - AccountLink aggregate (handle LinkAccount → produce AccountLinked)
   - AccountLinkView (update on AccountLinked)
   - Wire up CqrsFramework
   - Connect endpoint to execute LinkAccount command
   - Add validation (404 if email not found, 409 if already linked)
   - Add tests

---

## Phase 3: Asset Management Feature (2 issues, `enhancement`)

Manage which tokenized assets we support.

**Endpoints:**

- GET /tokenized-assets (read-only, lists supported assets)

**Issues:**

1. **Add GET /tokenized-assets endpoint stub**
   - Returns hardcoded list of assets
   - Define response types (UnderlyingSymbol, TokenSymbol, Network newtypes)
2. **Implement AddAsset feature and connect endpoint**
   - AddAsset command, AssetAdded event, DomainEvent trait
   - TokenizedAsset aggregate (handle AddAsset → produce AssetAdded)
   - TokenizedAssetView (update on AssetAdded)
   - Wire up CqrsFramework
   - Seed assets on server startup (execute AddAsset commands for initial
     assets)
   - Connect GET /tokenized-assets endpoint to query TokenizedAssetView
   - Add tests

---

## Phase 4: Mint Request Feature (2 issues, `enhancement`)

First part of mint flow - receive and validate mint requests from Alpaca.

**Endpoints:**

- POST /inkind/issuance (receive mint request)

**Flow:** Alpaca sends mint request → we validate → store with pending_journal
status

**Issues:**

1. **Add POST /inkind/issuance endpoint stub**
   - Returns 200 with dummy issuer_request_id
   - Define request/response types (TokenizationRequestId, IssuerRequestId,
     Quantity newtypes)
2. **Implement InitiateMint feature and connect endpoint**
   - InitiateMint command, MintInitiated event, DomainEvent trait
   - Mint aggregate (handle InitiateMint → produce MintInitiated)
   - MintView (update on MintInitiated)
   - Wire up CqrsFramework
   - Connect endpoint to execute InitiateMint command
   - Add validation (check client_id exists, asset is supported, wallet valid,
     qty reasonable)
   - Add tests

---

## Phase 5: Mint Execution Feature (5 issues, `enhancement`)

Complete the mint flow - journal confirmation through callback.

**Endpoints:**

- POST /inkind/issuance/confirm (journal confirmation from Alpaca)

**External services:**

- BlockchainService (mint tokens on-chain)
- AlpacaService (send callback)

**Flow:** Journal confirmed → mint on-chain → callback to Alpaca

**Issues:**

1. **Add POST /inkind/issuance/confirm endpoint stub**
   - Returns 200 OK
   - Parses journal confirmation status (completed/rejected)
2. **Implement ConfirmJournal and RejectJournal features**
   - Add commands: ConfirmJournal, RejectJournal
   - Add events: JournalConfirmed, JournalRejected, MintingStarted, MintFailed
   - Extend Mint aggregate to handle these commands
   - Connect endpoint to execute appropriate command based on status
   - Add tests
3. **Implement BlockchainService and on-chain minting**
   - Define BlockchainService trait with mint_tokens() method
   - Create mock implementation for testing
   - Add RecordMintSuccess and RecordMintFailure commands
   - Add TokensMinted and MintingFailed events
   - Extend Mint aggregate to handle these commands
   - Wire orchestrator: listen to MintingStarted → call mint_tokens() → execute
     RecordMintSuccess/Failure
   - Add contract bindings generation (alloy) as subtask
   - Add tests
4. **Implement AlpacaService and mint callback**
   - Define AlpacaService trait with send_mint_callback() method
   - Create mock implementation for testing
   - Use apca crate for real implementation
   - Add RecordCallback command
   - Add CallbackSent and MintCompleted events
   - Extend Mint aggregate to handle RecordCallback command
   - Wire orchestrator: listen to TokensMinted → call send_mint_callback() →
     execute RecordCallback
   - Add tests
5. **End-to-end mint flow tests**
   - Test complete flow from POST /inkind/issuance through callback
   - Test error scenarios (journal rejected, mint failed, callback failed)

---

## Phase 6: Redemption Feature (5 issues, `enhancement`)

Complete redemption flow - detect transfer through burn.

**Endpoints:**

- None (initiated by on-chain transfer)

**External services:**

- MonitorService (watch redemption wallet)
- AlpacaService (call redeem endpoint, poll status)
- BlockchainService (burn tokens)

**Flow:** Detect transfer → call Alpaca redeem → poll for completion → burn
on-chain

**Issues:**

1. **Implement DetectRedemption feature and MonitorService**
   - DetectRedemption command, RedemptionDetected event, DomainEvent trait
   - Redemption aggregate (handle DetectRedemption → produce RedemptionDetected)
   - RedemptionView (update on RedemptionDetected)
   - Wire up CqrsFramework
   - Define MonitorService trait with watch_transfers() method
   - Create mock implementation
   - Wire MonitorService to detect transfers → execute DetectRedemption command
   - Real implementation uses WebSocket to watch redemption wallet
   - Add tests
2. **Implement Alpaca redeem call feature**
   - Add RecordAlpacaCall and RecordAlpacaFailure commands
   - Add AlpacaCalled, AlpacaCallFailed, RedemptionFailed events
   - Extend Redemption aggregate to handle these commands
   - Extend AlpacaService with call_redeem_endpoint() method
   - Wire orchestrator: listen to RedemptionDetected → call
     call_redeem_endpoint() → execute RecordAlpacaCall/Failure
   - Add tests
3. **Implement Alpaca journal polling feature**
   - Add ConfirmAlpacaComplete command
   - Add AlpacaJournalCompleted and BurningStarted events
   - Extend Redemption aggregate to handle ConfirmAlpacaComplete
   - Extend AlpacaService with poll_request_status() method
   - Wire orchestrator: listen to AlpacaCalled → poll until complete → execute
     ConfirmAlpacaComplete
   - Add tests
4. **Implement token burning feature**
   - Add RecordBurnSuccess and RecordBurnFailure commands
   - Add TokensBurned, BurningFailed, RedemptionCompleted events
   - Extend Redemption aggregate to handle these commands
   - Extend BlockchainService with burn_tokens() method
   - Extend ReceiptInventoryView to handle TokensBurned
   - Wire orchestrator: listen to BurningStarted → call burn_tokens() → execute
     RecordBurnSuccess/Failure
   - Add tests
5. **End-to-end redemption flow tests**
   - Test complete flow from detection through burn
   - Test error scenarios (Alpaca call failed, burn failed)

---

## Phase 7: Operations & Deployment (6 issues, `enhancement`)

Operational concerns for production deployment.

**Issues:**

1. **Design and implement private key management**
   - Evaluate options: encrypted file, HSM, or KMS
   - Implement chosen solution with secure storage
   - Add key rotation procedures
2. **Implement API authentication for our endpoints**
   - Choose between API key or mTLS
   - Add authentication middleware to Rocket.rs
   - Add tests
3. **Setup configuration and secrets management**
   - Environment-based configuration
   - Secure secrets handling (not in git)
4. **Integrate HyperDX for observability**
   - Add HyperDX SDK
   - Instrument key flows (mint, redemption)
   - Add distributed tracing
5. **Integrate Grafana for metrics**
   - Export Prometheus metrics
   - Create Grafana dashboards
   - Add alerting rules
6. **Setup CD (GitHub Actions)**
   - Deployment pipeline
   - Environment promotion (staging → prod)
   - Rollback procedures

---

## Phase 8: Documentation (3 issues, `documentation`)

1. **Write API documentation**
   - OpenAPI/Swagger spec for all endpoints
   - Request/response examples
   - Error codes and meanings
2. **Write deployment and operations guide**
   - Setup instructions
   - Configuration reference
   - Monitoring and maintenance
3. **Write recovery procedures runbook**
   - Common failure scenarios
   - Step-by-step recovery procedures
   - Emergency contacts and escalation

---

## Notes

- **Feature-by-feature approach** to avoid dead code and enable incremental
  delivery
- Each feature phase is a complete vertical slice: endpoints → commands/events →
  aggregates → views → external services → wiring → tests
- Issue titles focus on the main deliverable (usually endpoint or major
  component), with implementation details in nested bullets
- Phase 0-1 are foundational infrastructure needed before any features
- Phases 2-6 build features incrementally: account linking → asset management →
  mint request → mint execution → redemption
- Phase 7-8 focus on production readiness and documentation
