# Issuance Bot Roadmap

**Milestone:**
[Issuance Bot v1.0](https://github.com/ST0x-Technology/st0x.issuance/milestone/1)

## Overview

This roadmap outlines the implementation plan for the issuance bot, which acts
as the **Issuer** in Alpaca's Instant Tokenization Network (ITN). The bot
implements the Issuer-side endpoints that Alpaca calls during mint/redeem
operations, and coordinates with the Rain `OffchainAssetReceiptVault` contracts
to execute the actual on-chain minting and burning of tokenized shares.

**Development Approach:** Feature-by-feature development - each phase implements
a complete vertical slice (HTTP endpoints → commands/events → aggregates → views
→ wiring → tests) to avoid dead code and enable incremental delivery.

---

## Phase 0: Project Bootstrap

Foundation for the project - specification, planning, tooling, framework, and
CI/CD setup.

### Issues

- [x] [#1](https://github.com/ST0x-Technology/st0x.issuance/issues/1) - Update
      SPEC.md to use ES/CQRS
  - **PR:** [#2](https://github.com/ST0x-Technology/st0x.issuance/pull/2)
- [x] [#3](https://github.com/ST0x-Technology/st0x.issuance/issues/3) - Create
      ROADMAP.md for bot implementation
  - **PR:** [#4](https://github.com/ST0x-Technology/st0x.issuance/pull/4)
- [x] [#5](https://github.com/ST0x-Technology/st0x.issuance/issues/5) - Setup
      Nix flake with Rust toolchain + Cargo project
  - **PR:** [#37](https://github.com/ST0x-Technology/st0x.issuance/pull/37)
- [x] [#6](https://github.com/ST0x-Technology/st0x.issuance/issues/6) - Setup
      Rocket.rs web framework skeleton + SQLite with sqlx
  - **PR:** [#38](https://github.com/ST0x-Technology/st0x.issuance/pull/38)
- [x] [#7](https://github.com/ST0x-Technology/st0x.issuance/issues/7) - Setup CI
      (GitHub Actions - linting, tests)
  - **PR:** [#39](https://github.com/ST0x-Technology/st0x.issuance/pull/39)

---

## Phase 1: ES/CQRS Foundation

**Dependencies:** Phase 0 must be complete

Core event sourcing infrastructure needed before any features can be
implemented.

### Issues

- [x] [#8](https://github.com/ST0x-Technology/st0x.issuance/issues/8) -
      Implement SqliteEventRepository in crates/sqlite-es
  - **PR:** [#40](https://github.com/ST0x-Technology/st0x.issuance/pull/40)
- [x] [#9](https://github.com/ST0x-Technology/st0x.issuance/issues/9) -
      Implement SqliteViewRepository in crates/sqlite-es
  - **PR:** [#40](https://github.com/ST0x-Technology/st0x.issuance/pull/40)

---

## Phase 2: Account Linking Feature

**Dependencies:** Phase 1 must be complete

**Endpoint:** `POST /accounts/connect`

Simple feature to start with - links AP accounts to our system.

### Issues

- [x] [#10](https://github.com/ST0x-Technology/st0x.issuance/issues/10) - Add
      POST /accounts/connect endpoint stub
  - **PR:** [#42](https://github.com/ST0x-Technology/st0x.issuance/pull/42)
- [x] [#11](https://github.com/ST0x-Technology/st0x.issuance/issues/11) -
      Implement LinkAccount feature and connect endpoint
  - **PR:** [#43](https://github.com/ST0x-Technology/st0x.issuance/pull/43)

---

## Phase 3: Asset Management Feature

**Dependencies:** Phase 1 must be complete

**Endpoint:** `GET /tokenized-assets`

Manage which tokenized assets we support.

### Issues

- [x] [#12](https://github.com/ST0x-Technology/st0x.issuance/issues/12) - Add
      GET /tokenized-assets endpoint stub
  - **PR:** [#44](https://github.com/ST0x-Technology/st0x.issuance/pull/44)
- [x] [#13](https://github.com/ST0x-Technology/st0x.issuance/issues/13) -
      Implement AddAsset feature and connect endpoint
  - **PR:** [#45](https://github.com/ST0x-Technology/st0x.issuance/pull/45)

---

## Phase 4: Mint Request Feature

**Dependencies:** Phases 2 and 3 must be complete

**Endpoint:** `POST /inkind/issuance`

First part of mint flow - receive and validate mint requests from Alpaca.

### Issues

- [x] [#14](https://github.com/ST0x-Technology/st0x.issuance/issues/14) - Add
      POST /inkind/issuance endpoint stub
  - **PR:** [#46](https://github.com/ST0x-Technology/st0x.issuance/pull/46)
- [x] [#15](https://github.com/ST0x-Technology/st0x.issuance/issues/15) -
      Implement InitiateMint feature and connect endpoint
  - **PR:** [#47](https://github.com/ST0x-Technology/st0x.issuance/pull/47)

---

## Phase 5: Mint Execution Feature

**Dependencies:** Phase 4 must be complete

**Endpoint:** `POST /inkind/issuance/confirm`

Complete the mint flow - journal confirmation through on-chain minting to
callback.

### Issues

- [x] [#16](https://github.com/ST0x-Technology/st0x.issuance/issues/16) - Add
      POST /inkind/issuance/confirm endpoint stub
  - **PR:** [#48](https://github.com/ST0x-Technology/st0x.issuance/pull/48)
- [x] [#17](https://github.com/ST0x-Technology/st0x.issuance/issues/17) -
      Implement ConfirmJournal and RejectJournal features
  - **PR:** [#49](https://github.com/ST0x-Technology/st0x.issuance/pull/49)
- [x] [#18](https://github.com/ST0x-Technology/st0x.issuance/issues/18) -
      Implement BlockchainService and on-chain minting
  - **PR:** [#50](https://github.com/ST0x-Technology/st0x.issuance/pull/50)
- [x] [#19](https://github.com/ST0x-Technology/st0x.issuance/issues/19) -
      Implement AlpacaService and mint callback
  - **PR:** [#51](https://github.com/ST0x-Technology/st0x.issuance/pull/51)
- [x] [#20](https://github.com/ST0x-Technology/st0x.issuance/issues/20) -
      End-to-end mint flow tests
  - **PR:** [#53](https://github.com/ST0x-Technology/st0x.issuance/pull/53)

---

## Phase 6: Redemption Feature

**Dependencies:** Phase 5 must be complete

**Endpoints:** None (initiated by on-chain transfer)

Complete redemption flow - detect transfer through burn.

### Issues

- [x] [#21](https://github.com/ST0x-Technology/st0x.issuance/issues/21) -
      Implement DetectRedemption feature and MonitorService
  - **PR:** [#54](https://github.com/ST0x-Technology/st0x.issuance/pull/54)
- [x] [#22](https://github.com/ST0x-Technology/st0x.issuance/issues/22) -
      Implement Alpaca redeem call feature
  - **PR:** [#55](https://github.com/ST0x-Technology/st0x.issuance/pull/55)
- [x] [#23](https://github.com/ST0x-Technology/st0x.issuance/issues/23) -
      Implement Alpaca journal polling feature
  - **PR:** [#56](https://github.com/ST0x-Technology/st0x.issuance/pull/56)
- [x] [#24](https://github.com/ST0x-Technology/st0x.issuance/issues/24) -
      Implement ReceiptInventoryView
  - **PR:** [#59](https://github.com/ST0x-Technology/st0x.issuance/pull/59)
- [x] [#25](https://github.com/ST0x-Technology/st0x.issuance/issues/25) -
      Implement token burning feature
  - **PR:** [#61](https://github.com/ST0x-Technology/st0x.issuance/pull/61)
- [x] [#26](https://github.com/ST0x-Technology/st0x.issuance/issues/26) -
      End-to-end redemption flow tests
  - **PR:** [#58](https://github.com/ST0x-Technology/st0x.issuance/pull/58)

---

## Phase 7: Operations & Deployment

**Dependencies:** Phase 6 must be complete

Operational concerns for production deployment.

### Issues

- [ ] [#27](https://github.com/ST0x-Technology/st0x.issuance/issues/27) - Design
      and implement private key management
- [x] [#28](https://github.com/ST0x-Technology/st0x.issuance/issues/28) -
      Implement API authentication for our endpoints
  - **PR:** [#63](https://github.com/ST0x-Technology/st0x.issuance/pull/63)
- [ ] [#29](https://github.com/ST0x-Technology/st0x.issuance/issues/29) - Setup
      configuration and secrets management
- [x] [#30](https://github.com/ST0x-Technology/st0x.issuance/issues/30) -
      Integrate OpenTelemetry tracing with HyperDX
  - **PR:** [#60](https://github.com/ST0x-Technology/st0x.issuance/pull/60)
- [ ] [#31](https://github.com/ST0x-Technology/st0x.issuance/issues/31) -
      Integrate Grafana for metrics
- [x] [#32](https://github.com/ST0x-Technology/st0x.issuance/issues/32) - Setup
      CD (GitHub Actions)
  - **PR:** [#66](https://github.com/ST0x-Technology/st0x.issuance/pull/66)
- [ ] [#33](https://github.com/ST0x-Technology/st0x.issuance/issues/33) - Add
      staging environment (nice-to-have)
- [ ] [#62](https://github.com/ST0x-Technology/st0x.issuance/issues/62) -
      Abstract database view queries behind traits

---

## Phase 8: Documentation

**Dependencies:** Phase 7 must be complete

Comprehensive documentation for APIs, operations, and incident response.

### Issues

- [ ] [#34](https://github.com/ST0x-Technology/st0x.issuance/issues/34) - Write
      API documentation
- [ ] [#35](https://github.com/ST0x-Technology/st0x.issuance/issues/35) - Write
      deployment and operations guide
- [ ] [#36](https://github.com/ST0x-Technology/st0x.issuance/issues/36) - Write
      recovery procedures runbook

---

## Fixes & Improvements

Bug fixes and improvements that don't fit into the original phases.

### Backlog

#### Features

- [x] [#115](https://github.com/ST0x-Technology/st0x.issuance/issues/115) -
      Un-whitelist wallet addresses
  - **PR:** [#116](https://github.com/ST0x-Technology/st0x.issuance/pull/116)
- [ ] [#65](https://github.com/ST0x-Technology/st0x.issuance/issues/65) - Allow
      linking more than one wallet to an account
- [ ] [#96](https://github.com/ST0x-Technology/st0x.issuance/issues/96) - Create
      a sandbox environment for Alpaca to test integrations against

#### Reliability

- [ ] [#88](https://github.com/ST0x-Technology/st0x.issuance/issues/88) - Add
      automatic recovery for burns detected as already completed on-chain
- [ ] [#110](https://github.com/ST0x-Technology/st0x.issuance/issues/110) -
      IssuerRedemptionRequestId has 4-byte ID space with collision risk at scale
- [x] [#111](https://github.com/ST0x-Technology/st0x.issuance/issues/111) -
      Resume failed mints when receipt is discovered by monitor
  - **PR:** [#112](https://github.com/ST0x-Technology/st0x.issuance/pull/112)

### Completed

#### Recovery & Resilience

- [x] [#107](https://github.com/ST0x-Technology/st0x.issuance/issues/107) - Mint
      auto recovery fails due to used fireblocks external tx id
  - **PR:** [#102](https://github.com/ST0x-Technology/st0x.issuance/pull/102)
- [x] [#85](https://github.com/ST0x-Technology/st0x.issuance/issues/85) -
      Implement redemption recovery system
  - **PR:** [#104](https://github.com/ST0x-Technology/st0x.issuance/pull/104)
- [x] [#84](https://github.com/ST0x-Technology/st0x.issuance/issues/84) -
      Implement mint recovery system
  - **PR:** [#104](https://github.com/ST0x-Technology/st0x.issuance/pull/104)
- [x] [#83](https://github.com/ST0x-Technology/st0x.issuance/issues/83) - Add
      retry policy for Alpaca API calls
  - **PR:** [#98](https://github.com/ST0x-Technology/st0x.issuance/pull/98)

#### Bug Fixes

- [x] [#108](https://github.com/ST0x-Technology/st0x.issuance/issues/108) -
      IssuerRequestId conflates mint and redemption ID formats
- [x] [#87](https://github.com/ST0x-Technology/st0x.issuance/issues/87) - Fix
      Alpaca authentication and improve logging
  - **PR:** [#106](https://github.com/ST0x-Technology/st0x.issuance/pull/106)
- [x] [#86](https://github.com/ST0x-Technology/st0x.issuance/issues/86) - Fix
      mints being detected as redemptions
  - **PR:** [#79](https://github.com/ST0x-Technology/st0x.issuance/pull/79)
- [x] [#82](https://github.com/ST0x-Technology/st0x.issuance/issues/82) - Fix
      nonce conflicts in concurrent operations
  - **PR:** [#97](https://github.com/ST0x-Technology/st0x.issuance/pull/97)
- [x] [#81](https://github.com/ST0x-Technology/st0x.issuance/issues/81) - Fix
      Alpaca account ID handling
  - **PR:** [#79](https://github.com/ST0x-Technology/st0x.issuance/pull/79)
- [x] [#78](https://github.com/ST0x-Technology/st0x.issuance/issues/78) - Fix
      internal IP whitelisting
  - **PR:** [#77](https://github.com/ST0x-Technology/st0x.issuance/pull/77)
- [x] [#74](https://github.com/ST0x-Technology/st0x.issuance/issues/74) - Fix IP
      whitelisting and tokenized assets response format issues
  - **PR:** [#73](https://github.com/ST0x-Technology/st0x.issuance/pull/73)
- [x] [#70](https://github.com/ST0x-Technology/st0x.issuance/issues/70) - Fix
      /tokenized-assets response format
  - **PR:** [#69](https://github.com/ST0x-Technology/st0x.issuance/pull/69)

#### Configuration

- [x] [#93](https://github.com/ST0x-Technology/st0x.issuance/issues/93) -
      ALPACA_IP_RANGES not passed through during deployment
  - **PR:** [#94](https://github.com/ST0x-Technology/st0x.issuance/pull/94)
- [x] [#91](https://github.com/ST0x-Technology/st0x.issuance/issues/91) -
      Hardcoded tokenized asset seeding instead of admin endpoint
  - **PR:** [#92](https://github.com/ST0x-Technology/st0x.issuance/pull/92)
- [x] [#90](https://github.com/ST0x-Technology/st0x.issuance/issues/90) -
      BOT_WALLET should be derived from PRIVATE_KEY instead of separate config
  - **PR:** [#95](https://github.com/ST0x-Technology/st0x.issuance/pull/95)
- [x] [#89](https://github.com/ST0x-Technology/st0x.issuance/issues/89) -
      VaultService uses global config vault address instead of per-asset vault
  - **PR:** [#95](https://github.com/ST0x-Technology/st0x.issuance/pull/95)

#### API & Integration

- [x] [#76](https://github.com/ST0x-Technology/st0x.issuance/issues/76) - Split
      account creation into Registration and Alpaca Linking
  - **PR:** [#75](https://github.com/ST0x-Technology/st0x.issuance/pull/75)
- [x] [#72](https://github.com/ST0x-Technology/st0x.issuance/issues/72) - Switch
      auth header to X-API-KEY
  - **PR:** [#71](https://github.com/ST0x-Technology/st0x.issuance/pull/71)
- [x] [#67](https://github.com/ST0x-Technology/st0x.issuance/issues/67) - Create
      an AP wallet whitelisting endpoint
  - **PR:** [#68](https://github.com/ST0x-Technology/st0x.issuance/pull/68)
- [x] [#52](https://github.com/ST0x-Technology/st0x.issuance/issues/52) - Create
      Rust client library for issuance bot
