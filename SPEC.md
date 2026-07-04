# Issuance Bot Specification

## Overview

The issuance bot acts as the **Issuer** in Alpaca's Instant Tokenization Network
(ITN). It implements the Issuer-side endpoints that Alpaca calls during
mint/redeem operations, and coordinates with the Rain
`OffchainAssetReceiptVault` contracts to execute the actual on-chain minting and
burning of tokenized shares.

**This is general infrastructure** - any Authorized Participant (AP) can use it
to mint and redeem tokenized equities. The issuance bot serves as the bridge
between traditional equity holdings (at Alpaca) and on-chain (semi-fungible)
tokenized representations (Rain SFT contracts).

## Background & Context

**Our Role:** We are the **Issuer** of tokenized equities. Alpaca acts as the
settlement layer between Authorized Participants (APs) and us.

**Flow Summary:**

- **Minting:** AP requests mint -> Alpaca calls our endpoint -> We validate ->
  Alpaca journals shares from AP to our custodian account -> Alpaca confirms
  journal -> We mint tokens on-chain -> We call Alpaca's callback
- **Redeeming:** AP sends tokens to our redemption wallet -> We detect
  redemption -> We call Alpaca's redeem endpoint -> Alpaca journals shares from
  our account to AP -> We burn tokens on-chain

**Use Cases:**

- **Market Makers & Arbitrageurs:** Can mint/burn to rebalance inventory and
  maintain price parity across venues
- **Institutions:** Can convert equity holdings to tokenized form for on-chain
  settlement, DeFi integration, or cross-border transfer
- **Retail Platforms:** Can facilitate tokenized equity access for their users
- **Our Arbitrage Bot:** Can use this infrastructure to complete the arbitrage
  cycle by rebalancing on/off-chain holdings. See
  [st0x.liquidity](https://github.com/ST0x-Technology/st0x.liquidity) for more
  details on the bot.

## Architecture

### Off-Chain Infrastructure

**Our HTTP Server:**

- Implements Alpaca ITN Issuer endpoints
- Handles account linking, mint requests, and journal confirmations
- Built with Rust (Rocket.rs web framework)
- SQLite database for tracking operations
- Async runtime for coordination

**Alpaca ITN:**

- Alpaca's settlement layer
- Handles journal transfers between accounts automatically
- Provides endpoints for callbacks and status queries

### On-Chain Infrastructure

**Rain OffchainAssetReceiptVault Contract:**

- ERC-1155 receipts tracking individual deposit IDs
- ERC-20 shares representing vault ownership
- `deposit()` function for minting
- `withdraw()` function for burning

**Redemption Wallet:**

- On-chain address where APs send tokens to redeem
- We monitor this address for incoming transfers

### ES/CQRS Architecture

The issuance bot uses **Event Sourcing (ES)** and **Command Query Responsibility
Segregation (CQRS)** patterns to maintain a complete audit trail, enable
time-travel debugging, and provide a single source of truth for all operations.

**Core Concepts:**

- **Aggregates**: Business entities that encapsulate state and business logic
  (e.g., `Mint`, `Redemption`, `Account`, `TokenizedAsset`)
- **Commands**: Requests to perform actions, representing user or system intent
  (e.g., `Initiate`, `ConfirmJournal`, `Deposit`)
- **Events**: Immutable facts about what happened, always in past tense (e.g.,
  `Initiated`, `JournalConfirmed`, `TokensMinted`)
- **Event Store**: Single source of truth - an append-only log of all domain
  events stored in SQLite
- **Views**: Read-optimized projections built from events for efficient querying
- **Services**: External dependencies that aggregates use (Alpaca API client,
  blockchain client, monitoring service)

**Key Flow:**

```mermaid
graph LR
    A[Command] --> B[Aggregate.handle]
    B --> C[Validate & Produce Events]
    C --> D[Persist Events]
    D --> E[Apply to Aggregate]
    E --> F[Update Views]
```

**Critical Methods:**

- `handle(command) -> Result<Vec<Event>, Error>`: Business logic lives here.
  Validates the command against current aggregate state and returns a list of
  events (can be 0+ events). Most commands produce a single event, but some
  commands may produce multiple events when one action has several state
  consequences.
- `apply(event)`: Deterministically updates aggregate state from events. This
  method is pure and should never fail - events are historical facts that have
  already occurred.

**Benefits:**

- **Complete Audit Trail**: Every state change is captured as an immutable event
- **Time Travel Debugging**: Replay events to reconstruct system state at any
  point in history
- **Testability**: Business logic tested via Given-When-Then pattern (given
  events, when command, then expect events)
- **Rebuild Views**: If a view becomes corrupted or a new projection is needed,
  simply replay all events
- **Multiple Projections**: Same events can feed different views (operational
  dashboard, analytics, Grafana metrics)
- **Single Source of Truth**: Event store is authoritative; all other data is
  derived

## Data Types

Throughout this specification, we use newtypes to provide type safety and
prevent mixing up different kinds of identifiers and values:

```rust
use rust_decimal::Decimal;
use chrono::{DateTime, Utc};

struct TokenizationRequestId(String);

/// Mint operations use a UUID-based issuer request ID.
/// Serializes as a UUID string, e.g. "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
struct IssuerMintRequestId(Uuid);

/// Redemption operations derive their issuer request ID from the full on-chain
/// tx_hash that triggered the redemption.
/// Serializes as a 32-byte hash, e.g. "0x1234...abcd".
/// Legacy "red-{first4bytes}" IDs remain readable for historical events.
struct IssuerRedemptionRequestId(TxHash);

struct ClientId(String);
struct AlpacaAccountNumber(String);
struct UnderlyingSymbol(String);
struct TokenSymbol(String);
struct Network(String);
struct Quantity(Decimal);
struct Email(String);
```

## Aggregates

This section defines the domain aggregates, their commands, and the events they
produce. Each aggregate represents a business concept with its own lifecycle and
invariants.

### Mint Aggregate

The `Mint` aggregate manages the complete lifecycle of a mint operation, from
initial request through journal confirmation to on-chain minting and callback.

**Aggregate State:**

- `issuer_request_id: IssuerMintRequestId`: Our unique identifier for this mint
- `tokenization_request_id`: Alpaca's identifier
- `quantity`, `underlying`, `token`, `network`, `client_id`, `wallet`: Request
  details
- `status`: Current state in the mint lifecycle
- `tx_hash`, `receipt_id`, `shares_minted`: On-chain transaction details
- Timestamps for each lifecycle stage

**Commands:**

- `Initiate { tokenization_request_id, quantity, underlying, token, network, client_id, wallet }` -
  Create a new mint request from Alpaca
- `ConfirmJournal { issuer_request_id }` - Alpaca confirmed shares journal
  transfer
- `RejectJournal { issuer_request_id, reason }` - Alpaca rejected shares journal
  transfer
- `Deposit { issuer_request_id }` - Record intent to mint. Pure state transition
  from `JournalConfirmed` to `Minting` — no network call. Produces
  `MintingStarted`. Intent is persisted before the network call so that a crash
  during submission leaves the aggregate in a recoverable `Minting` state rather
  than `JournalConfirmed` (which would lose track of the submission)
- `PrepareMint { issuer_request_id }` - Build and sign the exact on-chain
  deposit transaction. Requires `Minting` state. Produces `MintTxIntended`,
  which persists the raw transaction, hash, nonce, signing time, and stable
  external transaction ID before any broadcast
- `SubmitMint { issuer_request_id }` - Broadcast the exact transaction stored by
  `MintTxIntended`. Requires `MintIntended` state. Produces `MintTxSubmitted` on
  success. An uncertain broadcast failure leaves the aggregate in
  `MintIntended`, so recovery rebroadcasts the same bytes
- `ConfirmMint { issuer_request_id, tx_id }` - Confirm a previously submitted
  mint transaction. Re-fetches the on-chain receipt for the stored `tx_id` and
  produces `TokensMinted` or `MintingFailed`
- `SendCallback { issuer_request_id }` - Send the callback to Alpaca confirming
  mint completion
- `Recover { issuer_request_id, mode }` - Recover a mint stuck in an incomplete
  state. Startup recovery drives any mint in `JournalConfirmed`, `Minting`,
  `MintIntended`, `TxSubmitted`, `MintingFailed`, or `CallbackPending` state; at
  runtime, live retry scheduling is triggered specifically when a mint lands in
  `MintingFailed` during the journal-confirmation flow. Both paths hand a mint
  that is waiting on a retry window to a background scheduled-recovery task, so
  retries fire on schedule without waiting for a restart. Queries the receipt
  inventory for a receipt matching the `issuer_request_id`. If a matching
  receipt is found, the mint already succeeded on-chain, so recovery records the
  existing mint (`ExistingMintRecovered`) and proceeds to callback. If no
  receipt is found and the previous transaction is terminally failed, automatic
  recovery submits up to four retry transactions after 1m, 10m, 30m, and 1h
  delays. Manual admin reprocess uses the same recovery path but bypasses the
  automatic retry cap so an operator can retry after fixing the underlying
  cause. This prevents double-minting after crashes while ensuring terminal
  failures can be retried with new `externalTxId`s
- `RecoverWalletStep { issuer_request_id, mode }` - Internal recovery variant
  used only while the wallet lock is held. It performs the same recoverable
  on-chain steps as `Recover`, but becomes a no-op if a concurrent transition
  already reached `CallbackPending`; the next recovery iteration then sends the
  callback without the wallet lock
- `RecoverFromReceipt { issuer_request_id, tx_hash }` - Recover a mint that
  failed during the minting step, or whose broadcast outcome was not persisted,
  when an ITN receipt is discovered on-chain. Triggered by the receipt monitor
  when it finds a Deposit event with a matching `issuer_request_id`. Accepts
  `MintIntended`, because the persisted transaction may have been mined before
  submission recording, and `MintingFailed` when the non-failed predecessor was
  `Minting`. Rejects `JournalConfirmed` and `Minting` because neither state
  proves a transaction was signed or submitted. Emits `ExistingMintRecovered`
  and proceeds to callback

**Events:**

- `Initiated` - Mint request created (carries all request details)
- `JournalConfirmed` - Alpaca journal transfer confirmed
- `JournalRejected` - Alpaca journal transfer rejected (terminal)
- `MintingStarted` - Mint intent recorded (aggregate moves to `Minting`)
- `MintTxIntended` - Exact signed mint transaction persisted before broadcast
  (carries raw bytes, hash, nonce, signing time, and external transaction ID)
- `MintTxSubmitted` - Persisted signed mint transaction broadcast (carries
  `external_tx_id` and `tx_id` — the on-chain tx hash — for crash recovery)
- `TokensMinted` - On-chain mint succeeded (carries tx details)
- `MintingFailed` - On-chain mint failed
- `MintCompleted` - Alpaca callback sent, mint fully completed (terminal)
- `ExistingMintRecovered` - Existing on-chain mint discovered during recovery
  (carries tx details)
- `MintRetryStarted` - Mint retry started during recovery

Newly persisted transaction IDs use an explicitly tagged `hash` or `legacy`
representation so replay preserves the original `TxId` variant, including a
legacy ID whose text is valid 32-byte hex. Replay also accepts the historical
flat-string representation; because those rows have no variant discriminator,
32-byte hex flat strings retain the historical hash inference. Legacy
`FireblocksSubmitted` events and their `fireblocks_tx_id` field replay as
`MintTxSubmitted`.

The tagged writer is a one-way persistence cutover. Deployment must stop event
writers, back up the database, replace every service instance with the
dual-format reader, and only then resume traffic. After the first tagged
transaction ID is persisted, a binary that only understands flat strings must
not be restored against that database; recovery is to roll forward with the
dual-format reader or restore the pre-cutover backup.

**Command -> Event Mappings:**

| Command              | Events                  | Notes                                          |
| -------------------- | ----------------------- | ---------------------------------------------- |
| `Initiate`           | `Initiated`             | Mint request created                           |
| `ConfirmJournal`     | `JournalConfirmed`      | Journal confirmed                              |
| `RejectJournal`      | `JournalRejected`       | Terminal failure                               |
| `Deposit`            | `MintingStarted`        | Records intent (no network call)               |
| `PrepareMint`        | `MintTxIntended`        | Persists exact signed tx before broadcast      |
| `SubmitMint`         | `MintTxSubmitted`       | Broadcasts the persisted transaction           |
| `ConfirmMint`        | See below               | Confirms submitted tx                          |
| `SendCallback`       | `MintCompleted`         | Calls Alpaca callback                          |
| `Recover`            | See below               | Checks receipt inventory                       |
| `RecoverWalletStep`  | See below               | Never sends callbacks while wallet-locked      |
| `RecoverFromReceipt` | `ExistingMintRecovered` | Receipt recovery from intended or failed state |

`Deposit` emits only `MintingStarted` (business intent). `PrepareMint` builds
and signs the transaction, then persists the exact bytes and hash in
`MintTxIntended`. Only `SubmitMint` may broadcast those persisted bytes. A crash
before `MintTxIntended` cannot have broadcast anything; a crash after it causes
recovery to rebroadcast or poll that same transaction, never prepare a second
one. A crash after broadcast but before `MintTxSubmitted` therefore remains safe
because rebroadcasting identical signed bytes is idempotent. Preparing,
persisting, and initially broadcasting a mint transaction share one wallet
critical section. Startup mint recovery processes its persisted intents in nonce
order before mint states that may prepare a new transaction. Mint and redemption
recovery run concurrently so persisted transactions from either domain can fill
lower nonce gaps while higher transactions await confirmation. Live mint and
burn preparation query the authoritative event log and are blocked while any
other wallet intent remains unresolved; this safety check does not depend on a
fallible read-model projection. Together these rules prevent two aggregate
commands from signing the same wallet nonce without relying on in-memory nonce
state that would be lost on restart. Each live burn attempt waits at most 30
seconds behind an earlier unresolved wallet intent. On timeout it prepares and
broadcasts nothing, leaves the redemption recoverable, and defers the burn to
recovery rather than occupying the live flow indefinitely.

The issuer is a single-writer service: exactly one process may own a given
SQLite event store and signing wallet at a time. Horizontal replicas sharing a
wallet are unsupported because the wallet critical section is process-local.
Deployments must terminate the old process before the replacement begins serving
or recovering work.

`ConfirmMint` re-fetches the on-chain receipt for the submitted `tx_id` and
emits either `TokensMinted` (success) or `MintingFailed` (failure).

`Recover` checks the receipt inventory for a receipt matching the
`issuer_request_id`. If found, emits `ExistingMintRecovered`. If not found and
in `Minting` state, prepares and persists `MintTxIntended`. If in
`MintIntended`, rebroadcasts the persisted raw transaction. If in `TxSubmitted`
(or `MintingFailed` with a known prior transaction), calls `ConfirmMint` with
the stored `tx_id` to re-fetch the on-chain receipt. `TxSubmitted` means the
persisted transaction was broadcast; it does not mean the transaction succeeded
on-chain. `ConfirmMint` waits for the receipt and emits `TokensMinted` for a
successful transaction or `MintingFailed` for a reverted or otherwise failed
transaction. Retry transactions use `mint-{issuer_request_id}-retry-{n}` where
automatic retries use n = 1..4 and the delay schedule is 1m, 10m, 30m, then 1h.

The retry-delay/exhaustion schedule is driven by a `MintingFailed` attempt
counter. A transaction-preparation failure records `MintingFailed`; an uncertain
broadcast failure instead preserves `MintIntended` and recovery rebroadcasts the
exact same signed bytes without advancing the attempt. A running service keeps
driving deferred retries via a background scheduled-recovery task (also spawned
at startup and after a manual reprocess), rather than waiting for the next
restart.

`RecoverFromReceipt` is triggered when the receipt monitor discovers an on-chain
receipt for a mint in `MintIntended`, or in `MintingFailed` where the
predecessor was `Minting`. It emits `ExistingMintRecovered`, transitions to
`CallbackPending`, then continues through the existing `SendCallback` ->
`MintCompleted` flow without rebroadcasting. Automated recovery persists the
`CallbackPending` boundary before delivering the callback, so receipt polling
and Alpaca requests do not hold the wallet transaction lock.

### Redemption Aggregate

The `Redemption` aggregate manages the redemption lifecycle, from detecting an
on-chain transfer through calling Alpaca to burning tokens.

**Aggregate State:**

- `issuer_request_id: IssuerRedemptionRequestId`: Our unique identifier for this
  redemption
- `tokenization_request_id`: Alpaca's identifier (received after calling their
  API)
- `underlying`, `token`, `wallet`, `quantity`: Redemption details
- `detected_tx_hash`: On-chain transfer that triggered redemption
- `status`: Current state in the redemption lifecycle
- `burn_tx_hash`, `receipt_id`, `shares_burned`: Burn transaction details
- Timestamps for each lifecycle stage

**Commands:**

- `DetectRedemption` - Transfer to redemption wallet detected
- `RecordAlpacaCall` - Alpaca redeem API called successfully
- `RecordAlpacaFailure` - Alpaca redeem API call failed
- `ConfirmAlpacaComplete` - Alpaca journal transfer completed
- `IntendBurn` - Prepare and sign the exact burn transaction, then persist its
  raw bytes, hash, nonce, and receipt plan in `BurnIntended` before any
  broadcast. Only valid from `Burning`.
- `BurnTokens` - Broadcast the exact transaction persisted by `IntendBurn`.
  Produces `BurnTxSubmitted` on success; it never signs a replacement.
- `ConfirmBurn { tx_id, dust_shares }` - Confirm a previously submitted burn
  transaction. Produces `TokensBurned` on success
- `RecordBurnRecoveryAttempt` - Persist one automatic recovery action before its
  external side effect
- `RecordBurnPreparationRecoveryAttempt` - Persist one automatic retry before
  resuming a failed redemption that has no signed burn transaction
- `ReplaceDeadBurn` - Re-check that the persisted transaction is provably dead,
  then sign and persist a replacement at a fresh nonce
- `RecordBurnRecoveryExhausted` - Persist that the redemption-wide automatic
  recovery budget is spent
- `RecordBurnPreparationRecoveryExhausted` - Persist exhaustion when repeated
  burn preparation failures never produced a transaction identity
- `RecordBurnFailure` - Record on-chain burn failure (from `Burning` or
  `BurnSubmitted` state). Carries optional `tx_id` and `planned_burns` for
  recovery
- `RecordExistingBurn` - Record an existing on-chain burn discovered during
  recovery via on-chain transaction lookup
- `MarkFailed` - Mark redemption as failed
- `Reprocess { issuer_request_id, metadata }` - Reset a failed redemption back
  to `Detected` state for reprocessing. Only valid from `Failed` state when no
  `AlpacaCalled` event exists in the history — post-Alpaca failures use
  `ResumeBurn` instead. The `metadata` field carries the original
  `RedemptionMetadata` (extracted by the API layer from the event store's first
  `Detected` event), since the `Failed` aggregate state does not preserve
  metadata. Emits `Reprocessed` event with the metadata, previous state name,
  and timestamp for audit trail. The existing recovery logic then picks it up
  naturally from `Detected` state.
- `ResumeBurn { issuer_request_id, metadata, tokenization_request_id,
  alpaca_quantity, dust_quantity, called_at, alpaca_journal_completed_at,
  external_tx_id }` -
  Resume a failed redemption directly to `Burning` state, bypassing the Alpaca
  call step. Only valid from `Failed` state. Used for post-Alpaca failures where
  Alpaca has already completed the journal. The API layer polls Alpaca to verify
  journal completion before issuing this command — refuses if Pending or
  Rejected. If the failed redemption already has a terminal failed burn,
  `external_tx_id` is set to the next deterministic retry id:
  `burn-{detected_tx_hash}-retry-{n}`. If a retry submission fails before the
  transaction lands, recovery reuses the same retry id. Emits `BurnResumed`
  event. The admin recovery path immediately invokes burn recovery in-process so
  the on-chain burn does not wait for a service restart.
- `CloseRedemption { issuer_request_id, reason,
  acknowledged_unresolved_burn_tx_hash }` -
  Admin-close a redemption that cannot be automatically recovered, recording an
  operator `reason`. Valid from `Failed`, `Burning`, `BurnIntended`, or
  `BurnSubmitted`. A redemption with a persisted signed transaction is rejected
  by default because that transaction may still land. Closing it requires
  `acknowledged_unresolved_burn_tx_hash` to equal the persisted hash exactly;
  the signed identity is retained across a transition to `Failed`, and the
  acknowledgement is recorded in the terminal event. An acknowledgement is
  rejected when no persisted signed burn exists. This is the honest terminal
  path for a redemption whose burn cannot or should not be re-submitted and is
  **not** verifiable on-chain (e.g. a `Failed -> Burning` recovery regression
  where no burn ever landed, or an ambiguous case pending off-chain
  reconciliation). A held receipt reservation is **left in place** (the
  conservative policy for `Closed`), since an ambiguous burn may still have
  landed. Emits `RedemptionClosed`.
- `ForceCompleteBurn { issuer_request_id, burn_tx_hash, block_number, reason,
  acknowledged_unresolved_burn_tx_hash }` -
  Admin-terminalize a redemption stuck in `BurnIntended`/`BurnSubmitted` whose
  persisted exact burn transaction **already landed on-chain** but was never
  recorded (e.g. the bot crashed between the burn and `TokensBurned`). The admin
  layer verifies the operator-supplied `burn_tx_hash` on-chain first — the
  receipt must have succeeded and contain a real burn
  (`Transfer(bot_wallet -> 0x0)`) of the vault's shares — then records the
  proving tx hash and block number. The receipt reservation is settled (mirror
  reduced) just like a normal burn completion. Emits `BurnForceCompleted`,
  transitioning to `Completed`. The persisted bytes must decode with matching
  hash and nonce and recover the configured bot wallet as signer; the supplied
  hash must then equal that exact transaction hash unless the operator
  explicitly acknowledges the persisted hash with
  `acknowledged_unresolved_burn_tx_hash`. A different proving hash is rejected
  by default while the persisted transaction may still land. The acknowledgement
  must equal the persisted hash exactly and is recorded in the terminal event.
  The alternate transaction's per-receipt withdrawals, recipient wallet, and
  dust share transfer must also match the persisted burn semantics exactly,
  including the aggregate burned-share total. Its signer nonce must equal the
  persisted transaction's nonce, proving it is a mined replacement rather than
  an unrelated burn and ensuring the acknowledged transaction can no longer
  land. This prevents another redemption's same-vault burn from being used as
  proof. Legacy or pre-intent states without a trustworthy persisted transaction
  are **not** force-completed; ops use `CloseRedemption` after off-chain
  reconciliation instead.

**Events:**

- `RedemptionDetected` - Transfer to redemption wallet detected
- `AlpacaCalled` - Alpaca redeem endpoint called
- `AlpacaCallFailed` - Alpaca API call failed (terminal)
- `AlpacaJournalCompleted` - Alpaca confirmed journal transfer
- `BurnTxSubmitted` - Persisted signed burn transaction broadcast (carries
  `external_tx_id`, `tx_id` — the pending tx identifier — and `planned_burns`).
  The burn manager reserves the `planned_burns` in the receipt inventory
  **before** this broadcast, so a concurrent redemption that already committed
  the same receipt balance fails to reserve and never submits an unbacked burn.
  On confirmation the reservation is settled (mirror balance reduced); on a
  definitive terminal/reverted failure it is released.
- `BurnIntended` - Persists the exact signed raw transaction and its hash before
  any broadcast attempt. A prepared transaction is immutable: live execution and
  recovery may only broadcast those persisted bytes while they can still land.
  Recovery classifies the hash before acting. A fresh transaction may be signed
  only when the old transaction is provably dead under the replacement predicate
  below. If the RPC result is uncertain, the redemption stays unresolved with
  its receipt reservation held.
- `BurnRecoveryAttempted` - Records one accepted automatic recovery action for
  the persisted transaction, including its hash, nonce, action, and timestamp.
  These events form the durable redemption-wide recovery budget across process
  restarts.
- `BurnPreparationRecoveryAttempted` - Records an automatic retry before a
  failed redemption without a signed burn transaction resumes preparation. These
  attempts share the same redemption-wide budget.
- `BurnRecoveryExhausted` - Records that the automatic recovery budget is spent,
  including the latest hash, nonce, attempt count, and timestamp. It leaves the
  aggregate unresolved and the receipt reservation held for operator recovery.
  Its first persistence emits the single actionable operator error; later
  periodic passes observe the marker and perform no RPC or signing side effects.
- `BurnPreparationRecoveryExhausted` - Records the same durable stop when
  repeated preparation failures never produced a burn hash and nonce.
- `TokensBurned` - On-chain burn succeeded, redemption complete (terminal
  success). Payload contains `burns: Vec<BurnRecord>` where each `BurnRecord`
  has `receipt_id` and `shares_burned`, supporting multi-receipt burns when a
  single redemption spans multiple ERC-1155 receipts
- `BurningFailed` - On-chain burn failed. Carries optional `tx_id` and
  `planned_burns` for recovery of previously submitted transactions
- `ExistingBurnRecovered` - Existing on-chain burn discovered during recovery
- `RedemptionClosed` - Admin-closed redemption (terminal). Carries the operator
  `reason`, `closed_at`, and optional `acknowledged_unresolved_burn_tx_hash`.
  Closed redemptions do not appear in stuck queries. Receipt reservations remain
  held, including after an acknowledged close, because the unresolved
  transaction may still land.
- `BurnForceCompleted` - Admin-recorded terminal success for a stuck
  `BurnIntended`/`BurnSubmitted` redemption whose persisted exact burn was
  verified on-chain. Carries the proving `burn_tx_hash`, `block_number`,
  operator `reason`, optional `acknowledged_unresolved_burn_tx_hash`, and
  `completed_at`. Transitions to `Completed` (terminal success). Its receipt
  reservation is settled after the terminal event, including when the operator
  acknowledged the persisted transaction superseded by the proving same-nonce
  replacement.
- `Reprocessed` - Redemption reset to `Detected` state for reprocessing. Carries
  the original `RedemptionMetadata`, the previous state name, and a timestamp.
  Used for audit trail — shows when and from what state a manual reprocess was
  triggered.
- `BurnResumed` - Redemption resumed directly to `Burning` state from `Failed`.
  Carries the original `RedemptionMetadata`, `tokenization_request_id`,
  `alpaca_quantity`, `dust_quantity`, `called_at` (from the original
  `AlpacaCalled` event), `alpaca_journal_completed_at`, optional retry
  `external_tx_id`, and `resumed_at` timestamp. Used for post-Alpaca recovery
  where the journal already completed.

**Command -> Event Mappings:**

| Command                                  | Events                             | Notes                                             |
| ---------------------------------------- | ---------------------------------- | ------------------------------------------------- |
| `DetectRedemption`                       | `RedemptionDetected`               | Transfer detected                                 |
| `RecordAlpacaCall`                       | `AlpacaCalled`                     | Alpaca API called                                 |
| `RecordAlpacaFailure`                    | `AlpacaCallFailed`                 | Terminal failure                                  |
| `ConfirmAlpacaComplete`                  | `AlpacaJournalCompleted`           | Journal complete                                  |
| `IntendBurn`                             | `BurnIntended`                     | Persist exact signed tx before broadcasting       |
| `BurnTokens`                             | `BurnTxSubmitted`                  | Broadcasts persisted signed transaction           |
| `ConfirmBurn`                            | `TokensBurned`                     | Confirms burn, terminal success                   |
| `RecordBurnRecoveryAttempt`              | `BurnRecoveryAttempted`            | Reserve one durable automatic recovery action     |
| `RecordBurnPreparationRecoveryAttempt`   | `BurnPreparationRecoveryAttempted` | Reserve a retry before burn preparation           |
| `ReplaceDeadBurn`                        | `BurnIntended`                     | Re-check dead predicate, then persist replacement |
| `RecordBurnRecoveryExhausted`            | `BurnRecoveryExhausted`            | Stop automatic recovery durably                   |
| `RecordBurnPreparationRecoveryExhausted` | `BurnPreparationRecoveryExhausted` | Stop preparation retries durably                  |
| `RecordBurnFailure`                      | `BurningFailed`                    | Records failure with optional tx metadata         |
| `RecordExistingBurn`                     | `ExistingBurnRecovered`            | Recovery from Failed with known tx                |
| `Reprocess`                              | `Reprocessed`                      | Reset to Detected for reprocessing                |
| `ResumeBurn`                             | `BurnResumed`                      | Resume to Burning for post-Alpaca recovery        |
| `CloseRedemption`                        | `RedemptionClosed`                 | Admin close an unresolved redemption              |
| `ForceCompleteBurn`                      | `BurnForceCompleted`               | Admin terminalize a verified-on-chain burn        |

Burn transaction recovery runs once during startup and every five minutes while
the service is running. Before any recovery side effect, the issuer classifies
the latest persisted signed transaction `(H, N)` for wallet `W` in this order:

1. A receipt for `H` with a block number and successful status is **mined** and
   is confirmed and recorded; no transaction is signed or re-broadcast.
2. A receipt for `H` with a block number and failed status is **reverted**. The
   nonce is consumed and the failed attempt is recorded before any later retry.
3. With no receipt, if the finalized account nonce for `W` is at most `N`, the
   transaction is **still mineable**. Recovery may only re-broadcast the exact
   persisted bytes, producing the same hash `H`.
4. With no receipt, if the finalized account nonce for `W` is greater than `N`,
   the transaction is **provably dead** and can never land. Only this case
   permits signing a replacement at a fresh nonce. The replacement preserves the
   persisted transaction's destination, value, and calldata exactly; only its
   nonce, gas limit, and fee fields are regenerated. Its fresh nonce is the
   authoritative pending account nonce so it cannot collide with another live
   transaction from the signing wallet.

Equivalently, the exact replacement predicate is
`receipt(H) = None AND finalized_nonce(W) > N`. Receipt lookup is evaluated
before the nonce comparison. A latest-but-unfinalized nonce advance is not proof
of death because a reorganization can remove it. A missing block number,
mismatched receipt hash, provider error, timeout, signer that differs from `W`,
or any other identity/RPC uncertainty is unclassified and fails closed: the old
transaction remains live, no replacement is signed, and its reservation remains
held. Same-nonce fee replacement is not supported.

Automatic recovery is capped at five accepted recovery actions across the
redemption's complete event history, including preparation retries,
re-broadcasts, and fresh-nonce replacements. The initial live submission is not
a recovery action. Once the budget is exhausted, `BurnRecoveryExhausted` or
`BurnPreparationRecoveryExhausted` is persisted once and automatic recovery
stops. An accepted action is resumed after restart without consuming another
budget slot until its side effect is complete: an accepted re-broadcast
resubmits the same bytes, and a persisted replacement intent is submitted as the
continuation of the action that created it only while it remains mineable. If
its nonce became finalized while the service was down, signing a further
replacement requires another budgeted action. Classification and confirmation of
a transaction produced by the fifth action remain allowed because they do not
create another side effect. If that transaction is still non-terminal or
provably dead, the history check persists exhaustion before accepting any
further action. A failed fifth replacement preparation retains the preceding
exact transaction identity long enough to persist exhaustion safely. An
exhausted persisted intent cannot be re-armed through the admin recover
endpoint: the operator must force-complete a verified landed burn or close only
after off-chain reconciliation. At every point there is at most one transaction
hash that can still land for a redemption.

### Account Aggregate

The `Account` aggregate manages the relationship between AP accounts and our
system. The account lifecycle follows these steps:

1. **Registration**: We manually create an account for the AP with their email,
   generating a `client_id`.
2. **Alpaca Linking**: When Alpaca calls `/accounts/connect` with email +
   account number, we look up the account by email and link it to the Alpaca
   account.
3. **Wallet Whitelisting**: APs whitelist wallet addresses they'll use for
   minting and redemption.

**Aggregate State:**

- `client_id`: Our identifier for the account
- `email`: AP's email address (set at registration)
- `alpaca_account`: Alpaca account number (set when linked)
- `whitelisted_wallets`: List of on-chain wallet addresses authorized for
  minting and redemptions
- Timestamps

**Commands:**

- `Register { email }` - Create a new AP account (before Alpaca linking)
- `LinkToAlpaca { alpaca_account }` - Link existing account to Alpaca account
  number
- `WhitelistWallet { wallet }` - Authorize a wallet address for minting and
  redemptions
- `UnwhitelistWallet { wallet }` - Remove a wallet address from the whitelist

**Events:**

- `Registered { client_id, email, registered_at }` - New AP account created
- `LinkedToAlpaca { alpaca_account, linked_at }` - Account linked to Alpaca
  account number
- `WalletWhitelisted { wallet, whitelisted_at }` - Wallet address authorized for
  minting and redemptions
- `WalletUnwhitelisted { wallet, unwhitelisted_at }` - Wallet address removed
  from whitelist

**Command -> Event Mappings:**

| Command             | Events Produced       | Notes                                 |
| ------------------- | --------------------- | ------------------------------------- |
| `Register`          | `Registered`          | New AP account created with email     |
| `LinkToAlpaca`      | `LinkedToAlpaca`      | Existing account linked to Alpaca     |
| `WhitelistWallet`   | `WalletWhitelisted`   | Wallet authorized (multiple allowed)  |
| `UnwhitelistWallet` | `WalletUnwhitelisted` | Wallet removed (idempotent if absent) |

**Account State Machine:**

```mermaid
stateDiagram-v2
    [*] --> Registered: Register
    Registered --> LinkedToAlpaca: LinkToAlpaca
    LinkedToAlpaca --> LinkedToAlpaca: WhitelistWallet
    LinkedToAlpaca --> LinkedToAlpaca: UnwhitelistWallet
    Note right of Registered: Has email, client_id
    Note right of LinkedToAlpaca: Has alpaca_account, can whitelist/unwhitelist wallets
```

### TokenizedAsset Aggregate

The `TokenizedAsset` aggregate manages which assets are supported for
tokenization. The aggregate id is the `AssetKey` — `{underlying}:{network}`
(e.g. `AAPL:base`) — so the same underlying can be listed per network. See the
[Multi-chain](#multi-chain) section for the identity model, and
`docs/runbooks/tokenized-asset-aggregate-rekey.md` for migrating a
pre-multichain store keyed by bare `UnderlyingSymbol`.

**Aggregate State:**

- `underlying`, `token`: Symbol identifiers
- `network`: Blockchain network
- `vault`: On-chain vault contract address
- `status`: `AssetStatus` — `Enabled` (mints accepted) or `Frozen` (mints
  rejected, but the asset stays supported and in-flight redemptions still
  complete)
- `added_at`: Timestamp

**Commands:**

- `Add { underlying, token, network, vault }` - Add a new supported asset.
  Re-adding with a different vault updates the vault address; re-adding with the
  same vault is a no-op.
- `Freeze` - Stop accepting new mints for this asset (idempotent — freezing a
  frozen asset is a no-op).
- `Unfreeze` - Resume accepting mints (idempotent).

**Events:**

- `Added { underlying, token, network, vault, added_at }` - New asset added
- `VaultAddressUpdated { vault, previous_vault, updated_at }` - Vault address
  changed
- `Frozen { frozen_at }` - Asset frozen (new mints rejected)
- `Unfrozen { unfrozen_at }` - Asset unfrozen (mints resume)

**Command -> Event Mappings:**

| Command    | Events Produced                 | Notes                                                         |
| ---------- | ------------------------------- | ------------------------------------------------------------- |
| `Add`      | `Added` / `VaultAddressUpdated` | New asset, or vault update if re-added with a different vault |
| `Freeze`   | `Frozen`                        | No event if already frozen (idempotent)                       |
| `Unfreeze` | `Unfrozen`                      | No event if already enabled (idempotent)                      |

**Freeze State Machine:**

```
Enabled ⇄ Frozen
   Freeze:   Enabled -> Frozen
   Unfreeze: Frozen  -> Enabled
```

**Freeze invariant — frozen is not de-listed.** Freezing only gates _new_ mints:
`POST /inkind/issuance` rejects a frozen asset with a distinct `AssetFrozen`
error (separate from `AssetNotAvailable`), so the rejection is observable and
not conflated with de-listing. A frozen asset stays in `list_enabled_assets()`,
so in-flight redemption detection (`src/redemption/`) keeps working — issuance
reacts to on-chain transfers and has no "reject redemption" point. Preventing
_new_ redemptions of a frozen asset is the liquidity rebalance guard's job,
which reads the per-asset status endpoint (see "Tokenized Assets Data
Endpoint"). This issuance-side freeze plus the liquidity guard form the single
dividend freeze/unfreeze mechanism; no on-chain wrapper-contract freeze is
involved here (that is separate, heavier supply-control work and out of scope).

The `Freeze` / `Unfreeze` commands are emitted manually via the issuer-host CLI
in M1 and automatically by the dividend scheduler in M3 — the same command path
either way.

**Issuer CLI.** A dedicated `issuer` binary (separate from the HTTP server
binary) is the M1 manual freeze trigger. It runs on the issuer host (over SSH)
against the local SQLite store, and is where future issuer actions (e.g. `mint`,
`donate`) will live as additional subcommands:

- `issuer freeze <UNDERLYING>` — dispatch the `Freeze` command.
- `issuer unfreeze <UNDERLYING>` — dispatch the `Unfreeze` command.
- `issuer status <UNDERLYING>` — print the asset's current freeze status.

Each subcommand opens the same event store, prints the resolved asset and its
current status, requires confirmation before a mutating action, and dispatches
the CQRS command through the `Store` (never writing the `events` table
directly); freeze/unfreeze are idempotent. The trigger is deliberately a local
action on the issuer host, not a remotely pushable endpoint.

From the multichain cutover (see the [Multi-chain](#multi-chain) section) every
subcommand is network-aware: it takes a required `--network <NETWORK>` flag
(wire value) and resolves the asset by `{underlying}:{network}` —
underlying-only lookups are no longer possible once the aggregate is rekeyed,
and there is deliberately no default network so an operator can never freeze the
wrong chain's listing by omission.

## Services

Aggregates use services to interact with external systems while keeping business
logic testable and isolated. Services are injected into aggregate command
handlers, making aggregates testable with mock services.

### AlpacaService

- HTTP client for Alpaca API
- Methods: `call_redeem_endpoint()`, `send_mint_callback()`,
  `poll_request_status()`
- Handles authentication, retries, and error mapping

### VaultService

- RPC client for on-chain vault interaction
- Methods: `deposit()`, `withdraw()`
- Two implementations: local key signing — `EVM_PRIVATE_KEY` or Turnkey —
  `TURNKEY_ORG_ID` + `TURNKEY_API_PRIVATE_KEY` + `TURNKEY_ADDRESS` (prod)
- Turnkey transaction signing uses `ACTIVITY_TYPE_SIGN_TRANSACTION_V2` with the
  exact unsigned EIP-2718 transaction bytes. The returned signed envelope is
  decoded locally and its signature must recover `TURNKEY_ADDRESS` over those
  exact bytes before the transaction is accepted for broadcast. Decode,
  recovery, content, or signer mismatches fail closed, and signed transaction
  response bodies are never logged or embedded in decode errors.

### ReceiptService

Tracks on-chain ERC-1155 receipts across all vaults for burn planning and mint
recovery. Methods:

- `register_minted_receipt()` - Registers a receipt immediately after mint
- `for_burn(vault, shares_to_burn, dust) -> BurnPlan` - Plans a multi-receipt
  burn, selecting receipts in descending balance order
- `find_by_issuer_request_id(vault, id) -> Option<RecoveredReceipt>` - Looks up
  a receipt by ITN issuer_request_id for mint recovery

Receipts enter the inventory through backfill (scanning historic Deposit and
ERC-1155 transfer events at startup from the block after the per-vault
`receipt_backfill` checkpoint stored in the `poll_checkpoints` table), live
monitoring (WebSocket subscription to Deposit and ERC-1155 transfer events at
runtime), or direct registration after a mint. After startup backfill succeeds,
periodic receipt backfill safely scans the small runtime range from the durable
checkpoint to the current block and advances the checkpoint row only after
ordered range processing succeeds. Live monitoring processes observed logs
opportunistically but does not advance the durable checkpoint, because WebSocket
logs can arrive out of order within or across blocks. This prevents long-running
services from restarting with a stale receipt checkpoint that forces a large
historical scan before Rocket can serve requests without allowing one live log
to checkpoint past another unprocessed log in the same block.

The Receipt contract is an ERC-1155 token that emits `TransferSingle` and
`TransferBatch` events on all token movements. The receipt monitor and
backfiller track these events to discover inbound receipt transfers (to ==
bot_wallet) and reconcile outbound transfers (from == bot_wallet). Mint/burn
transfers (from/to == address(0)) are filtered out since those are already
covered by the vault's Deposit/Withdraw events.

Each receipt tracks two quantities: a `balance` that mirrors the on-chain
ERC-1155 balance the bot believes it holds, and a `reserved` map of shares
committed to submitted-but-unconfirmed burns, keyed by the redemption that
submitted them. **Available inventory for burn planning is
`balance - sum(reserved)`**. Keeping the on-chain mirror and reservations
separate is what lets reconciliation compare against the true on-chain balance
without ever clobbering a pending reservation.

Receipts leave the inventory (or have their balance corrected) through
reconciliation — querying `balanceOf(bot_wallet, receipt_id)` on-chain and
emitting `BalanceReconciled` when the on-chain balance falls outside the
`[available, balance]` band. While a submitted burn is pending, on-chain
legitimately sits anywhere in that band (it has not landed yet at `balance`;
once it lands it drops toward `available`), so reconciliation treats those
values as "no external change" and leaves the reservation intact. Settlement,
not reconciliation, consumes a reservation once its burn confirms.
Reconciliation is triggered by:

- **Startup**: After receipt backfill, reconciles all receipts with positive
  aggregate balance before redemption recovery and live monitoring begin
- **Post-burn**: After every burn attempt (successful or failed), reconciles the
  affected vault immediately
- **Live monitoring**: WebSocket subscription to Withdraw events and ERC-1155
  TransferSingle/TransferBatch events. When a Withdraw event fires with
  `owner == bot_wallet`, or an outbound transfer from bot_wallet is detected,
  reconciles the specific receipt. This detects external burns (manual burns by
  stakeholders) and direct token transfers in real time, mirroring the Deposit
  event subscription for receipt discovery

#### ReceiptInventory Aggregate

Commands:

- `DiscoverReceipt` - Register a newly discovered receipt
- `ReconcileBalance { receipt_id, on_chain_balance }` - Correct the mirror
  balance to match on-chain state. No-op if the on-chain balance is within the
  `[available, balance]` band (no external change while a reservation is
  pending) or the receipt is unknown
- `ReserveBurn { redemption_issuer_request_id, burns }` - **Atomic
  clear-and-reserve** issued **before** submitting a burn to the signing
  backend. It drops any prior reservation the redemption held (e.g. from a
  retried attempt that planned different receipts) and installs the new one in a
  single serialized transaction, validated against availability excluding the
  redemption's own prior reservation. Because clear and reserve are one step,
  the prior reservation is never returned to global availability (no concurrent
  redemption can grab it) and no stale reservation can survive to be
  over-consumed at settle. The vault inventory is a single serialized aggregate,
  so a concurrent _different_ redemption that already committed the same balance
  fails to reserve and never submits an unbacked burn. Burn planning
  (`for_burn`) likewise excludes the planning redemption's own reservation, so a
  retry re-plans its full burn
- `ReleaseBurn { redemption_issuer_request_id }` - Clear the redemption's
  reservation (wherever held), restoring availability without changing the
  mirror balance (the burn consumed nothing on-chain). No-op when the redemption
  holds no reservation. Issued **only** on a definitive terminal/reverted
  failure; pending or ambiguous statuses are not released because the burn may
  still land
- `SettleBurn { redemption_issuer_request_id }` - Consume the redemption's
  reservation after its burn confirmed on-chain: clear the reservation and
  reduce the mirror balance by the reserved amount. Idempotent; emits `Depleted`
  for any receipt the settlement empties

Release and settle are keyed only by redemption; `apply` uses the stored
`reserved` amounts, so neither carries a `burns` payload.

Events:

- `Discovered` - Receipt discovered with initial balance
- `BalanceReconciled { receipt_id, previous_balance, on_chain_balance }` -
  Mirror balance corrected to on-chain state (external change outside the
  reservation band). A receipt drained to zero on-chain while it still holds a
  reservation is preserved (mirror set to zero) rather than removed, so the
  reservation can still resolve
- `Depleted` - Receipt fully consumed (mirror balance reached zero) with no
  outstanding reservation
- `BurnReserved { redemption_issuer_request_id, burns }` - Submitted burn
  allocation removed from available inventory (recorded in `reserved`, leaving
  the mirror balance unchanged) so concurrent redemptions cannot reuse it
- `BurnReleased { redemption_issuer_request_id }` - Reservation cleared after a
  definitive terminal/reverted failure, restoring availability
- `BurnSettled { redemption_issuer_request_id }` - Reservation consumed after
  on-chain confirmation; mirror balance reduced by the reserved amount

**Startup reservation recovery** (`recover_stuck_reservations`) runs after
redemption recovery. For each vault it enumerates the redemptions holding a
reservation and resolves each against the redemption's terminal state: a
`Completed` redemption is **settled** (the burn confirmed but settlement was
missed, e.g. a crash in the confirm→settle window). All other states are left
untouched — a definitive failure already released its reservation in the
live/recovery paths, so a reservation surviving on a `Failed`/`Closed`
redemption is from an _ambiguous_ failure whose burn may still have landed;
releasing it would over-credit inventory and risk a duplicate burn, so it is
left for on-chain settlement or manual intervention.

The per-vault receipt backfill cursor (previously `BackfillCheckpoint` events on
this aggregate) is not domain state — it is a single mutable counter with no
audit value, so it is persisted in the `poll_checkpoints` SQL table under the
key `receipt_backfill:<vault_address_lowercase>`. See the polling checkpoints
persistence section below.

## Core Functionality

### 1. Account Lifecycle

The account lifecycle has three phases:

1. **Registration** - We manually create an account for the AP (before Alpaca
   involvement)
2. **Alpaca Linking** - Alpaca calls `/accounts/connect` to link their account
3. **Wallet Whitelisting** - We whitelist wallet addresses for the AP

#### Phase 1: Account Registration (Internal)

Before an AP can be linked to Alpaca, we must first create an account for them.
This is a manual internal process where we create an account with the AP's email
and generate a `client_id`.

**Endpoint:** `POST /accounts` (internal, not exposed to Alpaca)

**Request Body:**

```json
{
  "email": "customer@firm.com"
}
```

**Our Response:**

```json
{
  "client_id": "5505-1234-ABC-4G45"
}
```

**Status Codes:**

- `201`: Account created
- `409`: Email already registered

**Data Structure:**

```rust
struct RegisterAccountRequest {
    email: Email,
}

struct RegisterAccountResponse {
    client_id: ClientId,
}
```

#### Phase 2: Alpaca Linking

When an AP tells Alpaca they want to use our tokenization services, Alpaca calls
this endpoint to link their Alpaca account with the AP's existing account on our
platform. Using the email, we look up the account and return the `client_id`
that Alpaca will use for subsequent mint/redeem requests.

**Endpoint:** `POST /accounts/connect`

**Request Body:**

```json
{
  "email": "customer@firm.com",
  "account": "alpaca_account_number"
}
```

**Our Response:**

```json
{
  "client_id": "5505-1234-ABC-4G45"
}
```

**Status Codes:**

- `200`: Successful link
- `404`: Email not found on our platform (AP must register with us first)
- `409`: Account already linked to Alpaca

**Data Structure:**

```rust
struct AccountLinkRequest {
    email: Email,
    account: AlpacaAccountNumber,
}

struct AccountLinkResponse {
    client_id: ClientId,
}
```

#### Phase 3: Wallet Whitelisting

After account linking, APs must whitelist their wallet addresses before they can
mint or redeem tokens. This is an internal endpoint (not exposed to Alpaca).

**Endpoint:** `POST /accounts/{client_id}/wallets` (internal, not exposed to
Alpaca)

**Request Body:**

```json
{
  "wallet": "0x1234567890abcdef1234567890abcdef12345678"
}
```

**Our Response:**

```json
{
  "success": true
}
```

**Status Codes:**

- `200`: Wallet successfully whitelisted (or already whitelisted - idempotent)
- `404`: Client ID not found

**Data Structure:**

```rust
struct WhitelistWalletRequest {
    wallet: Address,
}

struct WhitelistWalletResponse {
    success: bool,
}
```

**Notes:**

- An account can have multiple whitelisted wallets
- Wallet addresses must be whitelisted before minting or redeeming
- During minting, we validate the provided `wallet_address` is whitelisted for
  the `client_id`
- During redemption, we look up which `client_id` owns the wallet that sent the
  tokens

#### Un-whitelist Wallet

Removes a wallet from the account's whitelist. After removal, the wallet can no
longer be used for minting or redemption.

**Endpoint:** `DELETE /accounts/{client_id}/wallets/{wallet}` (internal, not
exposed to Alpaca)

**Our Response:**

```json
{
  "success": true
}
```

**Status Codes:**

- `200`: Wallet successfully removed (or already absent - idempotent)
- `404`: Client ID not found

**In-flight Operations:**

Unwhitelisting takes effect immediately in the account view. The impact on
operations already in progress depends on the operation type:

- **Mints**: In-flight mints complete normally. The wallet is only validated at
  initiation (`POST /inkind/issuance`). Once initiated, all subsequent stages
  (journal confirmation, on-chain deposit, callback) use the wallet address
  stored in the mint's events — they do not re-check the account view. This
  means a mint that was authorized before the unwhitelist will still deliver
  shares to the wallet.
- **Redemptions**: In-flight redemptions become stuck. Every stage of the
  redemption flow (detection, Alpaca redeem call, journal polling) looks up the
  wallet via `find_by_wallet` on the account view. After unwhitelist, this
  lookup returns nothing, causing the stage to fail with a logged warning. On
  each service restart, recovery retries stuck redemptions — so re-whitelisting
  the wallet would unblock them automatically.

**Data Structure:**

No request body (wallet is in the URL path). The response reuses the same
`WhitelistWalletResponse` schema as the whitelist endpoint.

### 2. Tokenized Assets Data Endpoint

Alpaca needs to query which assets we support:

**Endpoint:** `GET /tokenized-assets`

**Our Response:**

```json
{
  "tokens": [
    {
      "underlying": "AAPL",
      "token": "tAAPL",
      "networks": ["base"]
    },
    {
      "underlying": "TSLA",
      "token": "tTSLA",
      "networks": ["base"]
    }
  ]
}
```

**Data Structure:**

```rust
struct TokenizedAssetsListResponse {
    tokens: Vec<TokenizedAssetResponse>,
}

struct TokenizedAssetResponse {
    underlying: UnderlyingSymbol,
    token: TokenSymbol,
    networks: Vec<Network>,
}
```

With multichain registration (see the [Multi-chain](#multi-chain) section)
responses **merge rows** when the same `(underlying, token)` is registered on
multiple chains (union of `networks[]`) -- a breaking semantic change for
clients that relied on one row per network -- and `tokens` are sorted by
`(underlying, token)`, each `networks[]` by `Network` wire value for
deterministic ordering.

#### Adding Tokenized Assets

**Endpoint:** `POST /tokenized-assets`

**Request:**

```json
{
  "underlying": "AAPL",
  "token": "tAAPL",
  "network": "base",
  "vault": "0x..."
}
```

**Response:** `201 Created` for new assets, `200 OK` if asset already exists
(idempotent).

```json
{
  "underlying": "AAPL"
}
```

#### Per-Asset Freeze Status

**Endpoint:** `GET /tokenized-assets/<underlying>/status`

Internal service-to-service endpoint (internal auth) consumed by the liquidity
rebalance guard to skip frozen assets before starting a rebalancing flow.
Returns the asset's `status` (`enabled` or `frozen`), or `404` if the asset is
unknown.

From the multichain cutover (see the [Multi-chain](#multi-chain) section) this
endpoint and its sibling detail lookup `GET /tokenized-assets/{underlying}`
(same internal auth, returning the full asset record instead of just the status)
require a `?network=` query parameter and return `422` when it is missing; the
liquidity freeze guard fail-closes on 422.

**Response:**

```json
{
  "underlying": "SGOV",
  "status": "frozen"
}
```

- `status` — `"enabled"` when the asset accepts new mints, or `"frozen"` when
  new mints are gated (the rebalance guard skips frozen assets). A frozen asset
  stays supported/listed (see the freeze invariant under "TokenizedAsset
  Aggregate") — freezing gates only new minting, it never de-lists.

**Status Codes:**

- `200`: asset found — returns its `status` (`"enabled"` or `"frozen"`)
- `401`: missing or invalid internal API key
- `404`: asset unknown
- `422`: missing `?network=` (from the multichain cutover -- see the Multi-chain
  section). Consumers must treat this as fail-closed, never as `"enabled"`
- `500`: database or view-deserialization failure — the status is
  **indeterminate**. A consumer must NOT treat any non-`404` failure as
  `"enabled"`; treat `500` as "unknown, retry" rather than proceeding.

`status: "enabled"` reflects the **projected** view state and is only as fresh
as the projection. The view is updated asynchronously after a
`Freeze`/`Unfreeze` event commits, so there is a brief window in which a
just-committed freeze still reports `status: "enabled"`. A consumer gating an
irreversible action on this signal (e.g. the rebalance guard) should confirm
propagation — poll until `status: "frozen"`, or apply a safety delay after
issuing a freeze — rather than trusting a single read.

### 3. Token Minting (Alpaca ITN Flow)

#### Receipts and Backing

ERC-1155 receipts are the on-chain proof that tokenized shares are backed by
real underlying shares held in a traditional brokerage account. Each receipt
tracks a specific deposit (mint) — how many shares were deposited, when, and by
whom.

- **Receipts are created during mints** — when underlying shares are deposited
  into the vault, the contract mints both ERC-20 shares (fungible, transferable
  to the user) and ERC-1155 receipts (non-fungible proof of the deposit).
- **Receipts are burned during redemptions** — when shares are redeemed, the
  vault burns the receipt alongside the shares, removing the proof of backing
  because the underlying shares are being returned to the brokerage account.
- **`ReceiptInformation`** is metadata about the receipt (the deposit that
  created it). It is serialized to JSON bytes and passed to the vault's
  `deposit()` call, which emits it as an on-chain event. This metadata links the
  on-chain receipt to the off-chain Alpaca tokenization request.

The vault's `withdraw()` also accepts a `receiptInformation` bytes parameter
(emitted as an event). When burning, we pass the original mint's
`ReceiptInformation` — the metadata that was recorded when the receipt was
created — because it identifies the receipt being burned.

#### Receipt Custody Model

**IMPORTANT:** The bot's wallet retains custody of all ERC-1155 receipts while
users hold ERC-20 shares. This design:

- Allows the bot to manage burns while only receiving a share transfer (it holds
  both shares and receipts during redemption)
- Maintains a clear audit trail (all receipts remain with the issuer)

**Mint Flow:** Bot receives shares + receipts -> Bot transfers shares to user ->
Bot keeps receipts

**Redemption Flow:** User sends shares to bot -> Bot has both shares + receipts
-> Bot burns

#### Complete Mint Flow

```mermaid
sequenceDiagram
    participant AP as Authorized Participant
    participant Alpaca as Alpaca ITN
    participant Us as Issuance Bot
    participant Blockchain as Blockchain

    AP->>Alpaca: Mint request (10 AAPL)
    Alpaca->>Alpaca: Validate AP account & authorization
    Alpaca->>Us: POST /inkind/issuance
    Note right of Us: Initiate command<br/>Event: Initiated<br/>Status: pending_journal
    Us->>Alpaca: {issuer_request_id, status: "created"}

    Alpaca->>Alpaca: Journal 10 AAPL shares<br/>From: AP -> To: Issuer account
    Alpaca->>Us: POST /inkind/issuance/confirm<br/>{status: "completed"}
    Note right of Us: ConfirmJournal command<br/>Event: JournalConfirmed
    Note right of Us: Deposit command<br/>Event: MintingStarted
    Note right of Us: PrepareMint command<br/>Event: MintTxIntended<br/>(signed transaction persisted)

    rect rgb(200, 220, 250)
        Note over Us,Blockchain: Single Atomic Transaction (multicall)
        Us->>Blockchain: SubmitMint: broadcast persisted transaction
        Note right of Us: Event: MintTxSubmitted
        Note right of Blockchain: 1. deposit(10 AAPL, bot_wallet)
        Note right of Blockchain: Bot receives shares + receipts
        Note right of Blockchain: 2. transfer(ap_wallet, 10 AAPL)
        Note right of Blockchain: Bot transfers shares to AP<br/>(keeps receipts)
    end
    Blockchain->>Us: Transaction confirmed (both steps succeeded)
    Note right of Us: ConfirmMint command<br/>Event: TokensMinted

    Us->>Alpaca: POST /tokenization/callback/mint<br/>{tx_hash, wallet_address}
    Note right of Us: SendCallback command<br/>Event: MintCompleted<br/>Status: completed

    Alpaca->>AP: Mint completed ✓
    Note left of AP: AP now has 10 AAPL0x<br/>share tokens in their wallet<br/>(Bot holds receipts)
```

#### Step 1: Receive Mint Request from Alpaca

**Endpoint:** `POST /inkind/issuance`

**Request Body:**

```json
{
  "tokenization_request_id": "12345-678-90AB",
  "qty": "1.23",
  "underlying_symbol": "TSLA",
  "token_symbol": "TSLAx",
  "network": "solana",
  "client_id": "98765432",
  "wallet_address": "<AP's wallet address to deposit the tokenized asset>"
}
```

**Note:** The JSON uses `qty` but our internal code uses `quantity` with
`#[serde(rename = "qty")]` to maintain API compatibility.

**Our Validation:**

1. Verify `underlying_symbol` is supported
2. Verify `token_symbol` matches our convention
3. Verify `network` is supported (our EVM chain)
4. Verify `client_id` is a valid/linked AP
5. Verify `wallet_address` is a valid EVM address whitelisted for the AP
6. Verify `qty` is reasonable (positive, not exceeding limits)

**Note:** We do NOT check if we have sufficient off-chain shares at this stage.
The AP is supposed to have sent shares to Alpaca, and Alpaca will journal them
to us. We simply validate the request format and respond. If the journal fails
in Step 2, we'll find out in Step 3.

**Our Response:**

```json
{
  "issuer_request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "created"
}
```

**Status Codes:**

- `200`: Request validated and created
- `400`: Invalid request with specific error:
  - "Invalid Wallet: Wallet does not belong to client"
  - "Invalid Token: Token not available on the network"
  - "Insufficient Eligibility: Client not eligible"
  - "Failed Validation: Invalid data payload"

**Data Storage:** Store in database with status `pending_journal`

**Data Structures:**

```rust
struct AlpacaMintRequest {
    tokenization_request_id: TokenizationRequestId,
    #[serde(rename = "qty")]
    quantity: Quantity,
    #[serde(rename = "underlying_symbol")]
    underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    token: TokenSymbol,
    network: Network,
    client_id: ClientId,
    #[serde(rename = "wallet_address")]
    wallet: Address,
}

struct MintRequestResponse {
    issuer_request_id: IssuerMintRequestId,
    status: String,  // "created"
}
```

#### Step 2: Alpaca Journals Shares

**Alpaca's Action:** Automatically journals the underlying shares from the AP's
account into our designated tokenization account at Alpaca.

**Our Action:** None - we wait for confirmation in Step 3

#### Step 3: Receive Journal Confirmation from Alpaca

**Endpoint:** `POST /inkind/issuance/confirm`

**Request Body:**

```json
{
  "tokenization_request_id": "12345-678-90AB",
  "issuer_request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "completed"
}
```

**Status Values:**

- `completed`: Journal succeeded, proceed to mint tokens on-chain
- `rejected`: Journal failed, mark request as failed and do NOT mint

**Our Response:** `200 OK` (acknowledge receipt)

**Our Actions:**

- If `completed`: Update database status to `journal_completed` and proceed to
  Step 4
- If `rejected`: Update database status to `failed` with reason
  "journal_rejected"

**Data Structure:**

```rust
enum AlpacaConfirmationStatus {
    Completed,
    Rejected,
}

struct AlpacaJournalConfirmation {
    tokenization_request_id: TokenizationRequestId,
    issuer_request_id: IssuerMintRequestId,
    status: AlpacaConfirmationStatus,
}
```

#### Step 4: Mint Tokens On-Chain

Once journal is confirmed, we mint tokens using the Rain vault.

**On-Chain Call:** `OffchainAssetReceiptVault.multicall()`

To ensure atomicity, we use the vault's `multicall()` function to execute both
deposit and transfer in a single transaction:

**Parameters:**

- `data`: Array of two encoded calls:
  1. `deposit(assets, bot_wallet, minShareRatio, receiptInformation)`
  2. `transfer(user_wallet, assets)`

**Key Design Points:**

- **Atomicity:** Both operations succeed or both fail - no intermediate state
- **1:1 Share Ratio:** We always use `minShareRatio = 1e18`, giving 1 share per
  asset. This allows us to know the transfer amount (`assets`) when encoding the
  multicall.
- **Result:** Bot's wallet receives ERC1155 receipts, user's wallet receives
  ERC20 shares

**Multicall Execution:**

1. `deposit(assets, bot_wallet, ...)` - Bot receives both shares and receipts
2. `transfer(user_wallet, assets)` - Bot transfers shares (keeping receipts)
3. Both succeed in same transaction, or entire transaction reverts

**Result:** AP receives shares, bot retains receipts. This separation enables
the redemption flow where the bot can atomically burn (it will have both shares
and receipts once the AP sends shares back).

**Receipt Information Structure:**

```rust
struct ReceiptInformation {
    tokenization_request_id: TokenizationRequestId,
    issuer_request_id: IssuerMintRequestId,
    underlying: UnderlyingSymbol,
    quantity: Quantity,
    timestamp: DateTime<Utc>,
    notes: Option<String>,
}
```

**Metadata for this mint (stored on-chain with the receipt):**

- Alpaca `tokenization_request_id`
- Our `issuer_request_id` (typed as `IssuerMintRequestId` since receipts are
  only created during mints)
- Symbol and quantity
- Timestamp

**Authorization Check:** Before attempting to mint, verify that our operator
address is authorized for the `DEPOSIT` permission on the vault. The
`OffchainAssetReceiptVault` uses an authorizer contract to control permissions.
If not authorized, the transaction will revert.

**Gas Management:**

- Estimate gas before submitting transaction
- Use reasonable gas price (e.g., median + 10% from recent blocks)
- Set appropriate gas limit with buffer (e.g., estimated * 1.2)
- Monitor for stuck transactions and implement escalation if needed
- Track gas costs per operation for operational metrics

**On Success:**

- Parse transaction receipt to extract:
  - Receipt ID created (from deposit event)
  - Shares minted (from deposit event)
  - Gas used
  - Block number
- Update database status to `callback_pending`
- Store transaction details (tx hash, receipt ID, shares, gas used, block
  number)
- Proceed to Step 5

**Data Structure:**

```rust
struct MintResult {
    tx_hash: B256,
    receipt_id: U256,
    shares_minted: U256,
    gas_used: u64,
    block_number: u64,
}
```

#### Step 5: Callback to Alpaca

After successful on-chain minting, we call Alpaca's callback endpoint to confirm
completion.

**Endpoint:** `POST /v1/accounts/{account_id}/tokenization/callback/mint`

Where `{account_id}` is our designated tokenization account ID at Alpaca.

**Request Body:**

```json
{
  "tokenization_request_id": "12345-678-90AB",
  "client_id": "5505-1234-ABC-4G45",
  "wallet_address": "<AP's wallet address where tokens were deposited>",
  "tx_hash": "0x12345678",
  "network": "base"
}
```

**On Success:**

- Update database status to `completed`
- Record completion timestamp

**On Failure:**

- Retry with exponential backoff
- If persistent failure, alert operators (mint succeeded on-chain but Alpaca not
  notified)
- Keep status as `callback_pending` until successful

#### Mint Request State Machine

```mermaid
stateDiagram-v2
    [*] --> PendingJournal: Initiate
    PendingJournal --> JournalConfirmed: ConfirmJournal
    PendingJournal --> JournalRejected: RejectJournal
    JournalConfirmed --> Minting: Deposit (MintingStarted)
    Minting --> MintIntended: PrepareMint (MintTxIntended)
    Minting --> MintingFailed: PrepareMint (MintingFailed)
    MintIntended --> TxSubmitted: SubmitMint (MintTxSubmitted)
    MintIntended --> MintIntended: SubmitMint uncertain failure (no event)
    MintIntended --> CallbackPending: Recover or RecoverFromReceipt (ExistingMintRecovered)
    TxSubmitted --> CallbackPending: ConfirmMint (TokensMinted)
    TxSubmitted --> MintingFailed: ConfirmMint (MintingFailed)
    MintingFailed --> MintIntended: Recover after automatic retry delay, or manual reprocess (MintRetryStarted + MintTxIntended)
    MintingFailed --> CallbackPending: Recover (ExistingMintRecovered)
    MintingFailed --> CallbackPending: RecoverFromReceipt (ExistingMintRecovered)
    CallbackPending --> Completed: SendCallback
    JournalRejected --> [*]
    Completed --> [*]
```

**Data Structures:**

```rust
struct StoredMintRequest {
    id: i64,
    tokenization_request_id: TokenizationRequestId,
    issuer_request_id: IssuerMintRequestId,
    quantity: Quantity,
    underlying: UnderlyingSymbol,
    token: TokenSymbol,
    network: Network,
    client_id: ClientId,
    wallet: Address,
    status: MintStatus,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

enum MintStatus {
    PendingJournal,
    JournalCompleted,
    Minting,
    CallbackPending,
    Completed,
    Failed(String),
}
```

### 4. Token Redemption (Alpaca ITN Flow)

#### Complete Redemption Flow

```mermaid
sequenceDiagram
    participant AP as Authorized Participant
    participant Blockchain as Blockchain
    participant Us as Issuance Bot
    participant Alpaca as Alpaca ITN

    AP->>Blockchain: Transfer 10 AAPL0x shares to bot wallet
    Note right of Us: Bot now has BOTH<br/>shares + receipts
    Blockchain->>Us: Transfer event detected
    Note right of Us: DetectRedemption command<br/>Event: RedemptionDetected<br/>Status: detected

    Us->>Alpaca: POST /tokenization/callback/redeem<br/>{issuer_request_id, qty, tx_hash}
    Alpaca->>Us: {tokenization_request_id, status: "pending"}
    Note right of Us: RecordAlpacaCall command<br/>Event: AlpacaCalled<br/>Status: alpaca_called

    Alpaca->>Alpaca: Journal 10 AAPL shares<br/>From: Issuer account -> To: AP

    loop Poll for completion
        Us->>Alpaca: GET /v1/accounts/{account_id}/tokenization/requests/{tokenization_request_id}
        Alpaca->>Us: {status: "pending" | "completed"}
    end

    Note right of Us: ConfirmAlpacaComplete command<br/>AlpacaJournalCompleted<br/>Status: burning

    Note right of Us: IntendBurn command<br/>Event: BurnIntended<br/>Exact signed burn multicall persisted
    Note right of Us: BurnTokens command
    Us->>Blockchain: Broadcast persisted burn multicall
    Note right of Us: Event: BurnTxSubmitted
    Blockchain->>Us: Transaction confirmed
    Note right of Us: ConfirmBurn command<br/>Event: TokensBurned (final success state)

    Us->>AP: Redemption completed ✓
    Note left of AP: AP now has 10 AAPL shares<br/>in their Alpaca account
```

#### Step 1: Monitor Bot Wallet for Redemptions

We continuously monitor the bot's wallet for incoming share transfers (which
signal redemption requests) on **every vault** associated with an enabled
tokenized asset.

**Monitoring Approach:**

- For each enabled tokenized asset, subscribe to `Transfer` events on that
  asset's vault contract
- Filter for transfers where `to` address is the bot's wallet
- Use WebSocket subscription for live monitoring
- At startup, backfill historic Transfer events to detect transfers that
  occurred while the service was down (mirrors the receipt backfilling pattern)

**Transfer Backfilling:**

At startup, the service starts historic Transfer backfill in the background so
HTTP serving and health checks are not blocked by avoidable chain catch-up work.
The backfiller scans Transfer events on **each vault independently** to detect
any redemption transfers that occurred while the service was down. This ensures
no redemptions are silently missed due to downtime.

The configured `backfill_start_block` is only the first-run seed; after a
successful range, the service persists a single `transfer_poll` row in the
`poll_checkpoints` SQL table and the next startup resumes at
`last_processed_block + 1`. The checkpoint advances only after the requested
range succeeds, and writes are monotonic so a shorter later range cannot move
progress backward. Idempotency is still guaranteed by the
`IssuerRedemptionRequestId` derived from each transaction hash — the Redemption
aggregate rejects duplicate detections.

This mirrors the receipt backfill pattern, where per-vault checkpoints are
tracked under the keys `receipt_backfill:<vault_lowercase>` in the same
`poll_checkpoints` table. Both checkpoints are intentionally not event-sourced:
they are single mutable values whose history has no audit worth keeping, and
modeling them as aggregates was the root cause of the 2026-05-19 OOM (RAI-617).

**Note:** The bot's wallet serves as the redemption destination. When users send
shares to this wallet, they're initiating a redemption. Since the bot already
holds the corresponding ERC1155 receipts (from the original mint), it can
atomically burn both shares and receipts once Alpaca confirms the journal.

**On Detection:**

- Parse transfer details (from address, amount, tx hash, block number)
- Determine symbol from vault/token context
- Convert amount from U256 to decimal quantity string
- Generate our internal `issuer_request_id`
- Store in database with status `detected`
- Proceed to Step 2

**Data Structure:**

```rust
struct TransferEvent {
    from: Address,      // AP's wallet that sent the tokens
    to: Address,        // Our redemption wallet
    amount: U256,       // Token amount transferred
    tx_hash: B256,
    block_number: u64,
    block_timestamp: u64,
}
```

#### Step 2: Call Alpaca's Redeem Endpoint

When we detect a redemption, we notify Alpaca.

**Endpoint:** `POST /v1/accounts/{account_id}/tokenization/callback/redeem`

Where `{account_id}` is our designated tokenization account ID at Alpaca.

**Request Body:**

```json
{
  "issuer_request_id": "0x574378e0d4f3a8b9c2e1f0a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5",
  "underlying_symbol": "AAPL",
  "token_symbol": "AAPL0x",
  "client_id": "5505-1234-ABC-4G45",
  "qty": "1.23",
  "network": "base",
  "wallet_address": "<the originating wallet address for the redeemed tokens>",
  "tx_hash": "0x12345678"
}
```

The `issuer_request_id` is the full redemption tx hash
(`IssuerRedemptionRequestId::Full`). Redemptions recorded before this format
still render legacy `red-{first4bytes}` IDs for their historical events.

**Alpaca's Response:**

```json
{
  "tokenization_request_id": "12345-678-90AB",
  "issuer_request_id": "0x574378e0d4f3a8b9c2e1f0a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5",
  "created_at": "2025-09-12T17:28:48.642437-04:00",
  "type": "redeem",
  "status": "pending",
  "underlying_symbol": "TSLA",
  "token_symbol": "TSLAx",
  "qty": "123.45",
  "issuer": "xstocks",
  "network": "base",
  "wallet_address": "0x1234567A",
  "tx_hash": "0x1234567A",
  "fees": "0.567"
}
```

**Status Values:**

- `pending`: Redemption request received, journal in progress
- `completed`: Journal completed successfully
- `rejected`: Redemption rejected

**Our Actions:**

- Store `tokenization_request_id` from response
- Update database status to `alpaca_called`
- Proceed to Step 3 (polling for completion)

**Client ID Lookup:** We need to look up the AP's `client_id` based on their
wallet address. This requires maintaining a mapping between wallet addresses and
client IDs from the account linking process.

**Data Structures:**

```rust
struct AlpacaRedeemRequest {
    issuer_request_id: IssuerRedemptionRequestId,
    #[serde(rename = "underlying_symbol")]
    underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    token: TokenSymbol,
    client_id: ClientId,
    #[serde(rename = "qty")]
    quantity: Quantity,
    network: Network,
    #[serde(rename = "wallet_address")]
    wallet: Address,
    tx_hash: B256,
}

enum TokenizationRequestType {
    Mint,
    Redeem,
}

enum RedeemRequestStatus {
    Pending,
    Completed,
    Rejected,
}

struct Fees(Decimal);

struct AlpacaRedeemResponse {
    tokenization_request_id: TokenizationRequestId,
    issuer_request_id: IssuerRedemptionRequestId,
    created_at: DateTime<Utc>,
    #[serde(rename = "type")]
    r#type: TokenizationRequestType,
    status: RedeemRequestStatus,
    #[serde(rename = "underlying_symbol")]
    underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    token: TokenSymbol,
    #[serde(rename = "qty")]
    quantity: Quantity,
    issuer: String,
    network: Network,
    #[serde(rename = "wallet_address")]
    wallet: Address,
    tx_hash: B256,
    fees: Fees,
}
```

#### Step 3: Poll for Journal Completion

**Alpaca's Action:** Automatically journals the underlying shares from our
tokenization account to the AP's account.

**Our Action:** Poll Alpaca's per-request endpoint to check status.

**Endpoint:**
`GET /v1/accounts/{account_id}/tokenization/requests/{tokenization_request_id}`
— returns a single request object. A real HTTP `404` maps to `RequestNotFound`
(the journal loop keeps polling it until timeout rather than aborting, since a
freshly-created request may briefly be invisible — empirical/precautionary
assumption, not a documented Alpaca guarantee).

**Polling Strategy:**

- Start with 250ms intervals
- Exponential backoff up to 30-second max
- Timeout after 1 hour
- Execute `ConfirmAlpacaComplete` command when status is "completed"
- Handle "rejected" status by marking redemption as failed

**On Completion:** A polling manager listens for `AlpacaCalled` events, polls
until the status is "completed", then executes the `ConfirmAlpacaComplete`
command. This produces the `AlpacaJournalCompleted` event and transitions the
aggregate to `Burning` state. A burn manager then orchestrates the on-chain
token burning.

#### Step 4: Burn Tokens On-Chain

Once Alpaca confirms the journal is completed, we burn the tokens on-chain.

**On-Chain Call:** `OffchainAssetReceiptVault.withdraw()`

**Parameters:**

- `assets`: Quantity to burn (convert from string to U256)
- `receiver`: Can be zero address (tokens going off-chain)
- `owner`: Bot's wallet (owns both the shares AND receipts - received during
  mint, shares returned during redemption)
- `id`: Receipt ID to burn from (need to track which receipt to use)
- `receiptInformation`: The original mint's `ReceiptInformation` for the receipt
  being burned

**Key Design Point:** The burn succeeds because the bot's wallet holds both:

1. **ERC20 shares** - Received from the AP during the transfer that initiated
   redemption
2. **ERC1155 receipts** - Retained from the original mint operation

The `OffchainAssetReceiptVault.withdraw()` function requires the `owner` to hold
both shares and receipts. Our receipt custody model ensures this invariant is
satisfied.

**Receipt Tracking:** We need to determine which receipt ID has sufficient
balance to burn from. This requires:

- Maintaining an inventory of active receipt IDs
- Querying on-chain balances for the bot's wallet
- Selecting an appropriate receipt with sufficient balance

**Authorization Check:** Verify our operator address is authorized for the
`WITHDRAW` permission on the vault.

**Gas Management:** Same strategy as minting:

- Estimate gas before submitting
- Use reasonable gas price with buffer
- Monitor and escalate if stuck
- Track costs

**On Success:**

- Parse transaction receipt to extract shares burned and gas used
- Update database status to `completed`
- Record completion timestamp

**Data Structure:**

```rust
struct BurnResult {
    tx_hash: B256,
    receipt_id: U256,
    shares_burned: U256,
    gas_used: u64,
    block_number: u64,
}
```

#### Redemption Request State Machine

```mermaid
stateDiagram-v2
    [*] --> Detected: DetectRedemption
    Detected --> AlpacaCalled: RecordAlpacaCall
    Detected --> Failed: MarkFailed
    AlpacaCalled --> Burning: ConfirmAlpacaComplete
    AlpacaCalled --> Failed: RecordAlpacaFailure / MarkFailed
    Burning --> BurnIntended: IntendBurn
    Burning --> Failed: RecordBurnFailure / MarkFailed
    Burning --> Closed: CloseRedemption (admin)
    BurnIntended --> BurnSubmitted: BurnTokens (BurnTxSubmitted)
    BurnIntended --> Completed: ConfirmBurn (TokensBurned, crash recovery)
    BurnIntended --> Failed: RecordBurnFailure
    BurnIntended --> BurnIntended: ReplaceDeadBurn / recovery annotations
    BurnIntended --> Completed: ForceCompleteBurn (admin, verified on-chain)
    BurnIntended --> Closed: CloseRedemption (admin)
    BurnSubmitted --> BurnIntended: ReplaceDeadBurn
    BurnSubmitted --> BurnSubmitted: recovery annotations
    BurnSubmitted --> Completed: ConfirmBurn (TokensBurned)
    BurnSubmitted --> Failed: RecordBurnFailure / MarkFailed
    BurnSubmitted --> Completed: ForceCompleteBurn (admin, verified on-chain)
    BurnSubmitted --> Closed: CloseRedemption (admin)
    Failed --> Failed: MarkFailed (re-classify failure)
    Failed --> Detected: Reprocess (pre-Alpaca)
    Failed --> Burning: ResumeBurn (post-Alpaca)
    Failed --> Closed: CloseRedemption (admin)
    Failed --> [*]
    Completed --> [*]
    Closed --> [*]
```

**Data Structures:**

```rust
struct StoredRedemption {
    id: i64,
    issuer_request_id: IssuerRedemptionRequestId,
    tokenization_request_id: Option<TokenizationRequestId>,
    underlying: UnderlyingSymbol,
    token: TokenSymbol,
    wallet: Address,
    tx_hash: B256,
    quantity: Quantity,
    status: RedemptionStatus,
    detected_at: DateTime<Utc>,
    alpaca_called_at: Option<DateTime<Utc>>,
    alpaca_completed_at: Option<DateTime<Utc>>,
    burned_at: Option<DateTime<Utc>>,
}

enum RedemptionStatus {
    Detected,
    AlpacaCalled,
    Burning,
    Completed,
    Failed(String),
}
```

## Alpaca ITN Integration Details

### Endpoints We Implement

We run an HTTP server that implements these endpoints.

**Endpoints Alpaca calls:**

1. **`POST /accounts/connect`** - Link existing account to Alpaca (looks up by
   email)
2. **`GET /tokenized-assets`** - List supported assets
3. **`POST /inkind/issuance`** - Mint request from Alpaca
4. **`POST /inkind/issuance/confirm`** - Journal confirmation from Alpaca

**Internal endpoints (not exposed to Alpaca):**

1. **`POST /accounts`** - Register new AP account with email
2. **`POST /accounts/{client_id}/wallets`** - Whitelist wallet address for AP
3. **`DELETE /accounts/{client_id}/wallets/{wallet}`** - Un-whitelist wallet
   address for AP
4. **`POST /tokenized-assets`** - Add a new tokenized asset
5. **`GET /tokenized-assets/<underlying>/status`** - Per-asset listing + freeze
   status, consumed by the liquidity rebalance guard

### Endpoints We Call

We call these Alpaca endpoints:

1. **`POST /v1/accounts/{account_id}/tokenization/callback/mint`** - Confirm
   mint completed
2. **`POST /v1/accounts/{account_id}/tokenization/callback/redeem`** - Initiate
   redemption
3. **`GET /v1/accounts/{account_id}/tokenization/requests/{tokenization_request_id}`**
   - Poll a single request's status

### Authentication

**For calling Alpaca endpoints:**

- **OAuth 2.0** with API key and secret
- Store credentials securely
- Handle token refresh before expiration

**For Alpaca calling our endpoints:**

- **API Key** authentication via `X-API-KEY: <key>` header
- **IP Whitelisting** to restrict requests to Alpaca's known IP ranges
- Rate limiting on failed authentication attempts
- Comprehensive audit logging of all authentication attempts

### Error Handling

**Mint Request Errors (400 responses):**

- "Invalid Wallet: Wallet does not belong to client"
- "Invalid Token: Token not available on the network"
- "Insufficient Eligibility: Client not eligible"
- "Failed Validation: Invalid data payload"

**Redemption Errors:**

- Journal failed/rejected
- Insufficient balance in tokenization account
- Unknown client_id
- Invalid transaction hash

**Recovery Strategies:**

1. **Journal Failed**: Mark mint as failed, do not mint tokens
2. **Callback Failed**: Retry callback with exponential backoff, alert if
   persistent
3. **Burn Failed (insufficient balance)**: If the bot's on-chain share balance
   is insufficient for a `Burning` redemption, the burn likely already succeeded
   on-chain but wasn't recorded. Auto-fail via `MarkFailed` to prevent infinite
   retries.
4. **Alpaca Redeem Failed (AccountNotFound)**: Redemptions stuck in `Detected`
   or `AlpacaCalled` state where the wallet has no linked account (e.g., old
   wallet redemptions) are auto-failed via `MarkFailed`. The `Failed` state is
   terminal — these operations are never retried.
5. **Failed Mint with On-Chain Receipt**: When a mint is marked as
   `MintingFailed` but the on-chain transaction actually succeeded (e.g., the
   transaction was submitted but the service failed before confirming it), the
   receipt monitor detects the Deposit event, discovers the receipt with a
   matching `issuer_request_id`, and triggers mint recovery. Recovery finds the
   existing receipt, transitions through `ExistingMintRecovered` ->
   `CallbackPending` -> `MintCompleted`, completing the flow without waiting for
   a service restart.
6. **Receipt balance reconciliation**: At startup (after receipt backfill),
   after every burn (both successful and failed), on live Withdraw events, and
   on inbound/outbound ERC-1155 transfers, the service reconciles each receipt's
   aggregate balance against its on-chain `balanceOf`. Emits `BalanceReconciled`
   to correct the inventory in either direction (increases from inbound
   transfers, decreases from burns or outbound transfers). This single mechanism
   handles all balance changes — burns by this service, manual burns by
   stakeholders, direct token transfers, or any other on-chain activity.
7. **BurnFailed auto-fail**: Redemptions stuck in `BurnFailed` state (after a
   `BurningFailed` event) are auto-failed via `MarkFailed` when recovery
   determines they cannot be retried — either because the on-chain share balance
   is insufficient (burn likely already succeeded) or because the receipt
   inventory has insufficient balance. This prevents infinite retry loops where
   the same unrecoverable redemption is attempted every startup.
8. **Post-burn reconciliation**: After every burn attempt (both successful and
   failed), receipt reconciliation is triggered immediately for the affected
   vault. This is the primary mechanism for updating inventory after burns
   performed by this service.
9. **Live transfer monitoring**: The receipt monitor subscribes to Withdraw
   events on each vault contract and ERC-1155 TransferSingle/TransferBatch
   events on the Receipt contract via WebSocket. When a Withdraw event fires
   with `owner == bot_wallet`, or an outbound/inbound transfer involving the bot
   wallet is detected, reconciliation is triggered for that receipt. Inbound
   transfers also attempt discovery (for new receipt IDs). This detects external
   burns, direct token transfers, and inbound receipts in real time.

**Startup view rebuild:** All view tables are cleared before replay on every
startup, ensuring views are rebuilt cleanly from events and eliminating stale or
corrupt view state.

## Database Schema

The database uses **SQLite** with an event sourcing architecture. The event
store is the single source of truth, and all other tables are read-optimized
views derived from events.

### Event Store Tables

These tables store the immutable event log that serves as the authoritative
source of truth.

```sql
-- Events table: stores all domain events
CREATE TABLE events (
    aggregate_type TEXT NOT NULL,      -- 'Mint', 'Redemption', 'Account', 'TokenizedAsset'
    aggregate_id TEXT NOT NULL,        -- Unique identifier for the aggregate instance
    sequence BIGINT NOT NULL,          -- Sequence number for this aggregate (starts at 1)
    event_type TEXT NOT NULL,          -- Event name (e.g., 'MintInitiated', 'TokensMinted')
    event_version TEXT NOT NULL,       -- Event schema version (e.g., '1.0')
    payload JSON NOT NULL,             -- Event data as JSON
    metadata JSON NOT NULL,            -- Correlation IDs, timestamps, user context, etc.
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);

CREATE INDEX idx_events_type ON events(aggregate_type);
CREATE INDEX idx_events_aggregate ON events(aggregate_id);

-- Snapshots table: aggregate state cache for performance
CREATE TABLE snapshots (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    last_sequence BIGINT NOT NULL,    -- Last event sequence included in this snapshot
    payload JSON NOT NULL,             -- Serialized aggregate state
    timestamp TEXT NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
);
```

**Note on Snapshots**: The snapshots table is a performance optimization that
caches aggregate state at specific sequence numbers. When loading an aggregate,
the framework loads the latest snapshot (if any) and replays only events since
that snapshot, rather than replaying all events from the beginning. Snapshots
can be deleted at any time - aggregates can always be rebuilt from the event
store alone.

**Snapshot schema versioning**: event-sorcery wraps each aggregate in
`Lifecycle<Entity>` (`Uninitialized` / `Live` / `Failed`). Snapshot payloads
therefore serialize as `{"Live": {<entity>}}`, not as the bare entity enum (e.g.
`{"Completed": {...}}`). A snapshot written before a wire-format change is
incompatible and bricks startup on deserialize.

Each `EventSourced` aggregate declares `SCHEMA_VERSION`. On every
`StoreBuilder::build`, the schema reconciler compares the stored version
(registered in the `SchemaRegistry` event stream) against the code version. When
they differ, all snapshots for that aggregate type are deleted before any load
or projection catch-up runs. **Any change to aggregate snapshot serialization —
including wrapping in `Lifecycle` — MUST bump `SCHEMA_VERSION`.** Startup also
purges any snapshot rows whose payload is not `Lifecycle`-shaped (covers the
case where the registry already records the new version but stale rows remain).
Canonical `Table` projections for the same aggregate are cleared when schema
reconciliation detects a version change, before `StoreBuilder::build` projection
catch-up.

### View Tables

All view tables follow the same pattern: `view_id` (primary key), `version`
(last event sequence applied), and `payload` (JSON containing the view state).
Views implement the `View` trait and are automatically updated by `GenericQuery`
processors when events are committed. If a view becomes corrupted or a new
projection is needed, simply drop the table and replay all events to rebuild it.

See `migrations/` for exact table definitions and indexes.

## Views and Queries

Views are read models that listen to events and maintain queryable state. Each
view implements the `View` trait with an `update()` method that processes
events.

**How Views Work:**

1. When events are committed to the event store, the `CqrsFramework` dispatches
   them to all registered queries
2. Each `GenericQuery` loads the current view state, applies the new events via
   the `update()` method, and persists the updated view
3. Views track the last event sequence they've processed to ensure exactly-once
   processing
4. If a view is missing or outdated, it can be rebuilt by replaying all events
   for that aggregate type

**Example View Implementations:**

**MintView** - Maintains current state of mint operations:

- Listens to: `Initiated`, `JournalConfirmed`, `MintingStarted`, `TokensMinted`,
  `MintingFailed`, `MintCompleted`, `JournalRejected`, `ExistingMintRecovered`,
  `MintRetryStarted`
- Updates: Status, timestamps, transaction details
- Used for: Querying current mint status, operational dashboards, API responses

**RedemptionView** - Maintains current state of redemptions:

- Listens to: `RedemptionDetected`, `AlpacaCalled`, `AlpacaJournalCompleted`,
  `TokensBurned`, `AlpacaCallFailed`, `BurningFailed`
- Updates: Status, timestamps, transaction details
- Used for: Tracking redemption progress, status queries

**ReceiptInventoryView** - Tracks receipt balances through state transitions:

- Listens to: `MintEvent::Initiated` (captures underlying/token),
  `MintEvent::TokensMinted` (creates active receipt),
  `RedemptionEvent::TokensBurned` (decreases balance, transitions to Depleted)
- State transitions: Unavailable -> Pending -> Active -> Depleted
- Updates: Accumulates data across event sequence to track each receipt's
  lifecycle from creation through complete depletion
- Used for: Selecting which receipt to burn from during redemptions, inventory
  management

**InventorySnapshotView** - Periodic inventory metrics:

- Listens to: `TokensMinted`, `TokensBurned`
- Updates: Calculates periodic snapshots of on-chain vs off-chain inventory
- Used for: Grafana dashboards, monitoring, alerting

**AccountView** - Current accounts:

- Listens to: `Registered`, `LinkedToAlpaca`, `WalletWhitelisted`,
  `WalletUnwhitelisted`
- Updates: Account status, relationship data, whitelisted wallets
- Used for: Validating client IDs, looking up accounts by email or Alpaca
  account number, checking wallet whitelisting

**TokenizedAssetView** - Supported assets:

- Listens to: `Added`, `VaultAddressUpdated`, `Frozen`, `Unfrozen`
- Updates: Asset configuration and freeze `status` (`Enabled` / `Frozen`)
- Used for: Validating mint/redemption requests, listing available assets
  (including frozen ones), and serving the per-asset freeze status endpoint

## Framework Wiring

The CQRS framework ties together the event store, aggregates, and views into a
cohesive system.

**Implementation Note:** While `cqrs-es` doesn't officially support SQLite, the
`mysql-es` crate uses `sqlx` which supports SQLite as a backend. We'll implement
our own `SqliteEventRepository` and `SqliteViewRepository` following the pattern
from `mysql-es` since it uses sqlx (which has SQLite support).

**Setup Steps:**

1. **Configure Event Repository:**
   - Create SQLite connection pool using `sqlx`
   - Implement `SqliteEventRepository` following the `mysql-es` pattern
   - Wrap in `PersistedEventStore`

2. **Create View Repositories:**
   - For each view, implement a `SqliteViewRepository` following the `mysql-es`
     pattern
   - Each repository handles loading, updating, and persisting view state

3. **Wrap Views in GenericQuery:**
   - Create view instance implementing `View` trait
   - Wrap in `GenericQuery` with corresponding view repository
   - `GenericQuery` handles the mechanics of loading, updating, and saving views

4. **Create CQRS Framework:**
   - Instantiate `CqrsFramework` with event store and vector of queries
   - Separate frameworks for each aggregate type (Mint, Redemption, Account,
     TokenizedAsset)
   - Or single framework if using the same event store for all aggregates

5. **Execute Commands:**
   - `cqrs.execute(&aggregate_id, command)` - Execute without metadata
   - `cqrs.execute_with_metadata(&aggregate_id, command, metadata)` - Execute
     with correlation IDs, etc.
   - Framework loads aggregate (from snapshot + events), calls `handle()`,
     persists events, applies to aggregate, updates views

**Example Wiring:**

```mermaid
graph TB
    ES[Event Store - SQLite] --> CQRS[CqrsFramework]
    CQRS --> MA[Mint Aggregate]
    CQRS --> MV[MintView]
    CQRS --> RIV[ReceiptInventoryView]
    CQRS --> ISV[InventorySnapshotView]

    subgraph "Command Execution Flow"
        CMD[Initiate] --> FW[Framework]
        FW --> LOAD[Load Aggregate]
        LOAD --> HANDLE[handle]
        HANDLE --> EVENTS[Initiated]
        EVENTS --> PERSIST[Persist Events]
        PERSIST --> APPLY[Apply to Aggregate]
        APPLY --> UPDATE[Update Views]
    end
```

## Testing Domain Logic

ES/CQRS enables highly testable business logic through the Given-When-Then
pattern.

**Testing Approach:**

- **Given**: Set up initial aggregate state by providing previous events
- **When**: Execute a command
- **Then**: Assert expected events are produced (or expected error)

**Example Tests:**

```rust
// Happy path: mint initiated successfully
#[test]
fn test_initiate_mint() {
    MintTestFramework::with(mock_services)
        .given_no_previous_events()
        .when(Initiate {
            tokenization_request_id: "alp-123",
            quantity: Decimal::from(100),
            // ...
        })
        .then_expect_events(vec![
            Initiated { /* ... */ }
        ]);
}

// Journal confirmed
#[test]
fn test_journal_confirmed() {
    MintTestFramework::with(mock_services)
        .given(vec![
            Initiated { issuer_request_id: "iss-456", /* ... */ }
        ])
        .when(ConfirmJournal { issuer_request_id: "iss-456" })
        .then_expect_events(vec![
            JournalConfirmed { /* ... */ }
        ]);
}

// Journal rejected (terminal failure)
#[test]
fn test_journal_rejected() {
    MintTestFramework::with(mock_services)
        .given(vec![
            Initiated { issuer_request_id: "iss-789", /* ... */ }
        ])
        .when(RejectJournal {
            issuer_request_id: "iss-789",
            reason: "insufficient funds"
        })
        .then_expect_events(vec![
            JournalRejected { reason: "insufficient funds" }
        ]);
}

// Error case: can't confirm journal for non-existent mint
#[test]
fn test_journal_confirmed_for_missing_mint() {
    MintTestFramework::with(mock_services)
        .given_no_previous_events()
        .when(ConfirmJournal { issuer_request_id: "unknown" })
        .then_expect_error("Mint not found or already completed");
}
```

## Admin API

Internal endpoints for operational management, protected by `InternalAuth`
(X-API-KEY header + internal IP whitelist).

### Recover Stuck Aggregates

Recovers a stuck or failed aggregate so existing recovery logic picks it up.

**Redemption:** `POST /admin/recover/redemption/<issuer_request_id>`

Auto-detects the right recovery path from the event history:

- **Pre-Alpaca failures** (no `AlpacaCalled` event): Dispatches `Reprocess` to
  reset to `Detected`. `RedeemCallManager` re-calls Alpaca on next restart.
- **Post-Alpaca failures** (`AlpacaCalled` event exists): Polls Alpaca to verify
  journal completion, then dispatches `ResumeBurn` to transition to `Burning`.
  Refuses if Alpaca journal is `Pending` or `Rejected` (to avoid burning without
  backing). `BurnManager` executes the burn immediately in the same recovery
  request.

**Mint:** `POST /admin/reprocess/mint/<aggregate_id>`

Dispatches the manual `Recover` command which handles `JournalConfirmed`,
`Minting`, `MintingFailed`, and `CallbackPending` states. Manual reprocess can
submit the next deterministic retry even after automatic retries have exhausted.

**Post-Alpaca with existing on-chain burn:** If a `BurningFailed` event carries
a tx ID, the endpoint scans on-chain for the transaction. If the tx completed
on-chain, dispatches `RecordExistingBurn` → `ExistingBurnRecovered` event →
`Completed` state. This handles the scenario where burns landed on-chain but
weren't recorded (e.g. a crash between submit and confirmation). The completed
transaction receipt must include its block number; recovery fails without
emitting a permanent event when that proof is incomplete. A successfully mined
receipt without a block number returns `500`. A replacement burn is authorized
only after the prior transaction is confirmed reverted. Pending transactions,
RPC failures, unknown outcomes, and legacy transaction IDs that cannot be
verified on-chain return `422`, preserve the failed state and receipt
reservation, and require manual intervention instead of risking a second burn.

**Examples:**

- `POST /admin/recover/redemption/0x61e089c6a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8`
  (legacy `red-61e089c6` IDs are also accepted for historical redemptions)
- `POST /admin/reprocess/mint/358508d1-54eb-4e3a-b1c5-c08fb0424f82`

**Status Codes:**

- `200`: Recovery initiated
- `404`: Aggregate not found
- `409`: Aggregate already completed or closed (cannot recover)
- `422`: Aggregate in a state that cannot be recovered, Alpaca journal not
  completed, or prior burn not confirmed reverted (for post-Alpaca redemption
  recovery)
- `500`: A successfully mined burn receipt is missing its block number
- `502`: Failed to poll Alpaca for journal status, or failed to execute the burn
  recovery (post-Alpaca recovery only)

### Close Redemption

Closes a failed redemption that cannot be automatically recovered (e.g., Alpaca
permanently rejected the journal, or tokens were consumed by other operations).

**Endpoint:** `POST /admin/close/redemption/<issuer_request_id>`

**Request body:**
`{ "reason": "string", "acknowledged_unresolved_burn_tx_hash": "0x..." }`. The
acknowledgement is optional unless the redemption has a persisted signed burn
transaction. In that case omission or a hash mismatch returns `422`; the
operator must echo the exact persisted hash shown by `/admin/stuck` after
off-chain reconciliation. Supplying an acknowledgement when no persisted signed
burn exists also returns `422`.

**Commands/Events:**

- `CloseRedemption` → `RedemptionClosed` event → `Closed` state (terminal)

**Status Codes:**

- `200`: Redemption closed
- `409`: Already completed or closed
- `422`: Invalid state, or a persisted signed burn was not acknowledged exactly

Close responses and structured logs include the acknowledged persisted hash when
an override is used. Closing never releases a held receipt reservation.

### Force-complete Redemption Burn

**Endpoint:** `POST /admin/force-complete/redemption/<issuer_request_id>`

**Request body:**
`{ "burn_tx_hash": "0x...", "reason": "string",
"acknowledged_unresolved_burn_tx_hash": "0x..." }`.

The proving `burn_tx_hash` must normally equal the redemption's persisted signed
transaction hash. If a different transaction proves the burn, the request is
rejected unless `acknowledged_unresolved_burn_tx_hash` exactly echoes the
persisted hash being superseded. The terminal event, response, and structured
logs record both identities, and the alternate proof's per-receipt withdrawals,
receiver, dust transfer, and signer nonce must equal the persisted burn
semantics, so the mined proof is a replacement and the persisted transaction can
no longer land. The held receipt reservation is settled after the terminal
event; operators must reconcile the acknowledged transaction before using this
override.

### List Stuck Aggregates

Lists all non-completed aggregates that may need manual intervention.

**Endpoint:** `GET /admin/stuck`

Returns all redemptions in `Failed` or `BurnFailed` state (excluding `Closed`),
and all mints in recoverable states (`JournalConfirmed`, `Minting`,
`MintingFailed`, `CallbackPending`).

## Configuration

### Environment Variables

```bash
# HTTP Server Configuration
SERVER_HOST=0.0.0.0
SERVER_PORT=8080

# Authentication (for Alpaca calling our endpoints)
ISSUER_API_KEY=<api_key_that_alpaca_uses_to_authenticate>
ALPACA_IP_RANGES=<comma_separated_cidr_ranges>  # e.g., "1.2.3.0/24,5.6.7.8/32"

# Alpaca Configuration
ALPACA_API_KEY=<api_key>
ALPACA_API_SECRET=<api_secret>
ALPACA_BASE_URL=https://broker-api.alpaca.markets
ALPACA_TOKENIZATION_ACCOUNT_ID=<our_designated_tokenization_account_at_alpaca>

# Blockchain Configuration
RPC_WS_URL=<ethereum_websocket_url>
CHAIN_ID=8453  # Base
CHAIN_NAME=base
REDEMPTION_WALLET_ADDRESS=<address_where_aps_send_tokens_to_redeem>

# Database
DATABASE_URL=sqlite:issuance.db

# Encryption
ENCRYPTION_KEY=<32_byte_hex_key>

# Operational Parameters
MAX_GAS_PRICE_GWEI=100
REDEMPTION_POLL_INTERVAL=30  # seconds between checking for redemptions
ALPACA_STATUS_POLL_INTERVAL=5  # seconds between status checks
ALPACA_STATUS_POLL_TIMEOUT=3600  # max seconds to wait for redemption completion

# Monitoring
LOG_LEVEL=info
METRICS_PORT=9090
```

### Private Key Management

**TBD** - Private key management strategy needs to be worked out in greater
detail including:

- Storage approach (encrypted file, HSM, KMS)
- Access controls
- Rotation procedures
- Backup and recovery
- Separation between minting and burning keys if needed

This is a critical security consideration that requires careful planning.

### Multi-chain

**MVP scope:** Full multichain operation of the issuance service -- mints,
redemptions, burns, token listing, receipt inventory, and transfer polling
routed by aggregate `network`. Contract deployment on new chains is prerequisite
work delivered via `st0x.deploy`; issuance registers deployed vaults and routes
all side effects through `ChainRegistry`. Base-only config stays identical until
each multichain PR merges.

**Token listing:** Alpaca ITN `GET /tokenized-assets` keeps its JSON shape — a
`tokens` array whose rows each carry a per-token `networks` array — but **row
cardinality changes**: when the same `(underlying, token)` is registered on
multiple chains, responses merge into one row whose `networks` is the union
(single-chain deployments emit one row per registered network). `tokens` are
sorted by `(underlying, token)` ascending; `networks[]` within each row are
sorted by network wire string. Add-asset registers vaults per `network`.

**Redemption + burn:** Detect, Alpaca orchestration, aggregate
`BurnTokens`/`ConfirmBurn`, and BurnManager recovery all sign on the aggregate's
`network` runtime -- not Base by default.

**Architecture:** One issuance process. `ChainRegistry` maps each `Network` to a
`ChainRuntime` — the per-network bundle of everything needed for on-chain side
effects on that chain:

- HTTP JSON-RPC provider (Alloy)
- `VaultService` (Turnkey or local signer, bound to that chain's `chain_id`)
- `backfill_start_block` for receipt backfill
- Subgraph URL for receipt indexing

Constructed once at startup from config; immutable for the process lifetime.
Alpaca calls a single issuer URL; payload `network` selects the runtime.

**ChainRegistry:** Legacy env vars (`RPC_URL`, `CHAIN_ID`, `SUBGRAPH_URL`,
`BACKFILL_START_BLOCK`) map to one `base` registry entry. Behaviour identical to
single-chain production. The flat vars are transitional compatibility, not a
supported long-term configuration: the target shape is one
`CHAIN_<NETWORK>_RPC_URL` / `CHAIN_<NETWORK>_CHAIN_ID` /
`CHAIN_<NETWORK>_SUBGRAPH_URL` / `CHAIN_<NETWORK>_BACKFILL_START_BLOCK` block
per configured network, with `.env.example` as the authoritative record of that
shape. Parsing those variables into `Config::chains` is its own change; until it
lands, the flat legacy vars remain the only live config path. Checkpoints are
keyed per network: transfer polling under `transfer_poll:{network}` and receipt
backfill under `receipt_backfill:<network>:<vault_address_lowercase>`, with the
pre-multichain rows (`transfer_poll`, `receipt_backfill:<vault_lowercase>`)
readable as Base-only fallbacks. Once staging and production migrate, the
flat-var mapping and those legacy checkpoint fallbacks are deleted.

**Asset identity (breaking):** `TokenizedAsset` aggregate id becomes the
`AssetKey` — `{underlying}:{network}` (e.g. `AAPL:base`). The internal asset
endpoints (the `InternalAuth`-guarded `GET /tokenized-assets/{underlying}`
detail lookup and its `GET /tokenized-assets/{underlying}/status` freeze-status
companion, consumed by `st0x-issuance-client`) require `?network=` (422 if
missing). This is a **lockstep break** with `st0x-issuance-client` and the
liquidity freeze guard -- no dual-read or optional-default transition. Alpaca
ITN list (`GET /tokenized-assets`) keeps `{ tokens, networks[] }`; see token
listing above for merge semantics.

**Cutover:** Lockstep deploy -- issuance, `st0x-issuance-client`, and the
liquidity freeze guard must ship in the same deploy window. No dual-read or
versioned transition; callers without `?network=` get **422** immediately after
cutover. Liquidity freeze guard **fail-closes** on 422 (rebalancing stops) if it
calls issuance without `?network=` after cutover.

**Rollback:** Roll back all three deployables together. If issuance rolls back
alone (with the pre-deployment database restore applied) while liquidity still
sends `?network=`, freeze/status calls succeed -- the old server ignores the
unknown query parameter. Without the restore, reverted code looks assets up by
the old `{underlying}` keys, every lookup against the rekeyed store returns
**404**, and consumers read 404 as "asset unknown" rather than a fail-closed
error -- a code-only rollback silently un-gates frozen assets. If liquidity
rolls back alone while issuance requires `?network=`, freeze guard gets 422 and
rebalancing **fail-closes** until liquidity is restored or issuance is rolled
back. Do not leave a mixed-version window in production. The same cutover
applies the aggregate-store rekey: a code rollback after the rekey has run must
be accompanied by a database restore from the pre-deployment backup. The
backup/restore procedure is the
`docs/runbooks/tokenized-asset-aggregate-rekey.md` runbook, which ships with the
rekey change itself.

**Invariants:**

1. Every on-chain side effect uses the aggregate's persisted `network` runtime.
2. `registry.get(network)` miss -> typed failure; never fall back to Base.
3. Startup fails if a live asset's `network` has no chain config entry.

**Alternatives considered:**

| Alternative                                               | Rejected because                                                                                        |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| One process per chain                                     | Duplicate SQLite/event store; Alpaca expects one issuer URL                                             |
| Lazy provider connect                                     | Violates fail-fast; hung chain could block unrelated HTTP                                               |
| Shared `VaultService` with runtime chain_id switch        | Signing backends bind `chain_id` at construction; a runtime switch is error-prone                       |
| Optional `?network=` defaulting to `base` for one release | Would decouple the three deployables but hides misconfiguration; lockstep cutover preferred for clarity |
