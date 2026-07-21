# 02. Consume corporate-action mutations durably

- Status: Proposed
- Date: 2026-07-21
- Issue:
  [RAI-1043](https://linear.app/makeitrain/issue/RAI-1043/source-issuances-freeze-schedule-from-alpaca-corporate-actions-rai)

## Context

Issuance owns the dividend freeze schedule and must keep it aligned with
Alpaca's current corporate-action data. A schedule entry is not immutable:
Alpaca can insert, update, or delete a corporate action. An update can move an
ex-date after jobs have already been armed, and a deletion can cancel the
requirement entirely. Retaining the old jobs in either case can freeze an
underlying on the wrong date or leave it frozen without a valid corporate
action.

The implementation initially polled Alpaca's Broker API announcements endpoint
by ex-date and derived idempotency solely from the symbol and window boundaries.
That endpoint is deprecated. Alpaca's replacement
[Market Data GET endpoint](https://docs.alpaca.markets/us/reference/corporateactions-1)
is paginated and filtered by `process_date`, not `ex_date`; polling a moving
date range therefore does not establish that an absent action was deleted, nor
does it guarantee rediscovery of an old announcement whose future ex-date
changes. The replacement
[corporate-actions event stream](https://docs.alpaca.markets/us/reference/subscribetocorporateactionseventssse)
explicitly carries `insert`, `update`, and `delete` mutations and supports
replay from an event ID.

The system must preserve these invariants across retries and restarts:

- each upstream action has at most one current source-owned freeze window;
- applying the same upstream mutation more than once is a no-op;
- an update replaces the old window rather than adding another hold;
- a deletion cancels pending transitions and releases any active hold owned by
  that action;
- a cursor never advances past a mutation that has not been durably reconciled.

### Trust boundaries and abuse cases

The authenticated SSE response crosses from Alpaca into issuance's typed domain,
and the persisted replay cursor crosses from SQLite back into the external
reconnect request. The protected asset is the underlying supply gate: tampered
identity, dates, mutation kinds, or cursor order can enable minting during a
real freeze window or deny issuance indefinitely.

- Spoofing and tampering: authenticate only to Alpaca's documented stream,
  accept only the documented dividend event discriminators and mutation kinds,
  validate action IDs, symbols, dates, and event IDs at deserialization, and
  reject a cursor regression.
- Repudiation: persist every accepted event ID with the resulting schedule
  revision so the upstream mutation and local gate state remain attributable.
- Information disclosure: never log authentication headers or response objects;
  telemetry names only the safe event ID, action ID, mutation kind, and bounded
  processing outcome.
- Denial of service: bound an SSE frame and action identifier before allocation,
  reconnect with backoff, and stop explicitly on a poison event rather than
  advancing the cursor or retrying it in a tight loop.
- Elevation of privilege: an event can reconcile only the hold derived from its
  own validated Alpaca action ID; it cannot acquire or release the operator hold
  or another action's hold.

The abuse-case tests must exercise the real SSE decoder and reconciliation path:
duplicate replay, cursor regression, malformed or oversized frames, unknown
discriminators, an update that moves an armed window, and a delete racing an
active window.

### Operational questions and signals

- **Is the source connected?** One structured connection-state event on each
  state transition, with a bounded state field.
- **Is reconciliation advancing?** One structured event after a cursor commits,
  carrying the safe event ID and bounded mutation outcome.
- **What is blocking progress?** One structured error event for the poison
  event, carrying its safe event ID and typed failure kind; reconnect noise
  stays at debug level.

## Decision

Consume Alpaca's corporate-actions SSE feed as a durable mutation stream,
filtered to the dividend event types relevant to the freeze policy. Persist the
last applied Alpaca event ID locally and reconnect with replay enabled. Treat
the replay as at-least-once delivery and deduplicate by event ID.

Project the stream into a local corporate-action schedule keyed by Alpaca's
stable corporate-action ID. That ID, rather than ex-date-derived window
boundaries, owns the scheduled jobs and `FreezeHoldId`. Applying an insert or
update reconciles the source-owned window to the latest symbol and ex-date;
applying a delete removes its pending transitions and releases its active hold.
Persist the mutation result and replay cursor in one SQLite transaction so a
crash can only replay work, never skip it.

Bootstrap through the stream's historical replay rather than treating a GET
snapshot as authoritative for deletions. A bounded GET request may be used for
diagnostics or explicit repair, but it is not the schedule's source of truth.
Malformed or unsupported event payloads do not advance the cursor and surface a
terminal operational failure for that event instead of silently discarding it.

## Alternatives Considered

### Poll the replacement GET endpoint and diff snapshots

- Pros: Preserves the existing periodic control flow and avoids a long-lived
  connection.
- Cons: The endpoint filters and orders by `process_date`, pages results, and
  does not provide a deletion mutation. Absence from one bounded response does
  not prove cancellation, while retaining every previously seen action leaves
  stale windows after deletions.
- Rejected because: No polling window can simultaneously prove deletion and
  guarantee rediscovery of every revised future ex-date without an unbounded
  rescan.

### Continue using the Broker API announcements endpoint

- Pros: It directly supports an ex-date query and matches the existing wire
  model.
- Cons: Alpaca has deprecated the endpoint, and multiple announcements for one
  corporate action require lifecycle reconciliation that boundary-based
  idempotency cannot express.
- Rejected because: A deprecated contract is not a durable foundation for an
  automated supply gate, and it still does not make cancellation handling
  explicit.

### Keep every observed window as a conservative freeze

- Pros: A missed cancellation cannot cause issuance to mint during the old
  window.
- Cons: Revisions accumulate obsolete holds, and canceled actions can halt
  issuance with no upstream requirement and no automatic recovery.
- Rejected because: Fail-closed behavior must be recoverable and attributable;
  indefinitely preserving superseded requirements turns stale data into an
  unbounded availability failure.

## Consequences

Corporate-action identity becomes a domain input to scheduling and hold
ownership. The scheduler needs reconciliation operations that can replace and
cancel source-owned windows, including safely releasing a hold when a delete or
update arrives during an active window. The local projection and cursor require
schema migration, replay tests, and crash-boundary tests.

The service gains a long-lived Alpaca Market Data connection and must expose
whether that stream is connected, how far its cursor has advanced, and whether
an event is blocking progress. Replayed and duplicated events become routine
rather than exceptional. A poison event deliberately stops forward progress
until repaired, because skipping it could leave the supply gate inconsistent
with its source.

The periodic stateless sync and its deprecated Broker API dependency are
removed. Manual operator holds remain independent, and reconciling an Alpaca
action can affect only the hold owned by that action.
