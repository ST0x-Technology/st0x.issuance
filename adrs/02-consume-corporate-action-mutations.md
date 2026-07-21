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

- Spoofing and tampering: authenticate only to Alpaca's documented stream and
  require `region = "us"`. The complete accepted event-type allowlist is
  `cash_dividend_corporateaction_event` and
  `stock_dividend_corporateaction_event`; the complete mutation allowlist is
  `insert`, `update`, and `delete`. A new discriminator, another region, or an
  unsupported mutation is a typed poison event, never an ignored frame. Action
  IDs, symbols, ex-dates, and uppercase ULID event IDs are parsed before use. A
  valid accepted event for an unlisted underlying is the one non-poison
  boundary: it advances the cursor as an explicit no-op without creating a
  schedule, transition, or hold, but retains the canonical mutation payload for
  listing-time recovery. After that underlying is listed, a service-owned
  listing reactor promotes the latest retained mutation for each action into a
  pending schedule revision without moving the stream cursor. A retained delete
  remains an attributable no-op because no source-owned hold was ever acquired.
  Unlisted state is partitioned by `(region, underlying, action_id)` and upserts
  one latest canonical mutation per action; it never appends another payload
  version. A separate audit table retains only `event_id`, `action_id`, mutation
  kind, typed outcome, payload fingerprint, and acceptance time. Production
  startup requires positive audit-TTL and global unlisted-action-capacity
  settings. Cleanup removes audit rows only after the TTL and removes canonical
  rows only after listing promotion has reconciled, or after a delete/elapsed
  window has no schedule or hold. Reaching the capacity before rows are eligible
  for cleanup blocks the feed instead of evicting state and continuing. This is
  the cleanup invariant: no row needed to promote, align, release, or attribute
  a mutation may be removed.
- Repudiation: persist each accepted event ID within that bounded audit window
  with either `schedule_revision` and the resulting revision or
  `no_op_unlisted_underlying`. The current cursor is retained independently of
  audit compaction. The outcome and cursor advance share the projection
  transaction, so even an event that intentionally creates no local gate state
  remains attributable for the configured retention period.
- Information disclosure: never log authentication headers or response objects;
  telemetry names only the safe event ID, action ID, mutation kind, and bounded
  processing outcome.
- Denial of service: bound one SSE frame to 64 KiB before JSON decoding and an
  action identifier to 1 through 128 bytes before domain allocation, reconnect
  with backoff, and stop explicitly on a poison event rather than advancing the
  cursor or retrying it in a tight loop.
- Elevation of privilege: an event can reconcile only the hold derived from its
  own validated Alpaca action ID; it cannot acquire or release the operator hold
  or another action's hold.

The abuse-case tests must exercise the real SSE decoder and reconciliation path:
duplicate replay, cursor regression, malformed or oversized frames, unknown
discriminators, an update that moves an armed window, and a delete racing an
active window. Hold-ownership tests install an operator hold and two Alpaca
actions on the same underlying, then update and delete each action through real
SSE frames. They prove the mutation changes only the hold derived from its own
action ID while the operator and sibling-action holds remain active.

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
requesting only the two US dividend event types enumerated above. Persist the
last applied Alpaca event ID locally and reconnect with replay enabled. Treat
the replay as at-least-once delivery and deduplicate by event ID.

Event IDs are canonical uppercase ULIDs and their encoded lexicographic order is
the cursor order. Repeating the current cursor is an idempotent duplicate; a
previously unseen lower ID is a poison regression, not a reason to move the
cursor backward. Before terminating, the service durably records the typed
blocked reason and the event ID when the rejected frame exposes one. A poison
frame without a valid identity still records an identity-free blocked boundary.
Startup refuses to reconnect while a blocked boundary exists, so a restart
cannot skip the rejected frame and continue from a later cursor.

Inclusive replay checks the accepted-mutation row keyed by `event_id` before
performing any projection write. A duplicate whose validated canonical payload
matches the stored payload is a no-op: it does not insert another mutation,
change a schedule or revision, or advance the cursor. Reuse of an `event_id`
with different canonical content persists a typed poison boundary and stops the
feed. The duplicate lookup, payload comparison, and any poison-boundary write
share the same SQLite transaction as the ordinary projection path.

Project the stream into a local corporate-action schedule keyed by Alpaca's
stable corporate-action ID. The source-owned `FreezeHoldId` is derived from that
action ID alone — not from ex-date-derived window boundaries — so every revision
of one action addresses the same hold. Window boundaries still key the scheduled
transition jobs' idempotency (two jobs per window), and the operator's manually
scheduled windows remain a separate hold family keyed by their window; neither
can collide with an action-owned hold.

Reconciliation has two durable phases. For a new listed event, the first SQLite
transaction inserts the accepted mutation with a `schedule_revision` outcome,
replaces the action's schedule row with its latest event ID, revision,
underlying, ex-date, deletion state, and a null reconciled marker, then advances
the replay cursor. For a new unlisted event, that transaction instead inserts
the accepted mutation with a `no_op_unlisted_underlying` outcome, retains its
canonical payload, and advances the cursor without creating a schedule row. When
an asset listing commits, a service-owned reactor selects the latest retained
mutation per action for that underlying. It atomically creates a pending
revision from each latest non-delete mutation and records a listing-recovery
link to the source event; it neither rewrites the historical no-op outcome nor
changes the stream cursor. The ordinary second phase then aligns each promoted
revision. Those accepted-event, projection, outcome, and cursor writes either
all commit or all roll back. Exact duplicate events follow the unchanged-state
path above. Apalis transition rows, aggregate hold events, and the final
reconciled marker are deliberately not in that transaction: they are recoverable
second-phase effects. The second phase enqueues idempotent alignment jobs for
the pending revision and marks it reconciled only after enqueueing succeeds.
Startup reconciles every pending revision to completion — successful enqueue and
reconciled marker for each — before connecting to the stream. A reconciliation
failure keeps the feed disconnected and preserves the pending revision; the feed
never opens over an unreconciled revision.

Crash-boundary tests cover rollback before the projection transaction commits,
recovery after that commit but before enqueue, and idempotent recovery after
enqueue but before the reconciled marker. The cursor therefore never skips the
durable schedule intent, while aggregate hold changes remain recoverable job
effects rather than being misrepresented as part of the projection transaction.

Every revision enqueues an immediate alignment plus the transitions needed for
its latest ex-date. Projection commits and the alignment job's expected-event
check plus action-owned hold effects share one process-wide revision guard. The
issuer is a single-writer service, so this serialization prevents a newer
revision from committing between a job's check and its effects; jobs that read a
superseded event ID remain no-ops. For an active-to-active update, alignment
acquires the action-owned hold on the latest underlying before releasing that
same action's hold from any prior underlying. When the revision keeps the same
underlying, the release step explicitly skips the underlying just held —
acquire-then-release of the one action-owned hold must not unfreeze an
underlying whose window is still active, and a test proves the underlying
remains frozen across a same-underlying revision. For an update to a future or
elapsed window, or for a deletion, immediate alignment releases the old active
hold; a future transition reacquires it only when the replacement window begins.
Distinct corporate actions retain distinct hold IDs, so one action's update or
deletion cannot release another overlapping window or an operator hold.
Alignment retries are bounded. The durable alignment state is the combination of
the revision's reconciled marker and its keyed apalis row: a null marker with a
pending or running row is `pending`, a null marker with a killed or
retry-exhausted row is `terminal_failed`, and a marker equal to the current
event ID is `reconciled`. A terminal failure never advances the reconciled
marker. Before opening the stream, startup finds every `terminal_failed`
revision, enqueues one deduplicated sync-failure lifecycle notification for its
alignment key, and resets that same job to pending with a fresh attempt budget.
Restart tests persist both killed and retry-exhausted rows, recreate the
service, and prove that each revision is re-armed while its reconciled marker
remains null until alignment succeeds.

On first install, the cursor is absent and `since=1970-01-01T00:00:00Z` replay
is not trusted as complete: Alpaca documents no retention floor, so the first
retained frame proves nothing about earlier events, and absence of a committed
cursor means the retention-gap check below cannot fire. The documented GET
endpoint also exposes no snapshot watermark or consistency token relating its
pages to an SSE event ID. Production therefore has no automatic first-install
baseline path under the current provider contract: startup keeps minting gated
until an operator restores an issuance database backup containing the action
projection and a committed cursor that Alpaca still accepts for inclusive
replay. Development may establish a fixture cursor from its controlled mock
stream, but that path is unavailable in production. On every subsequent
connection, the request uses `since_id=<committed-event-id>` rather than
`Last-Event-Id`, relying on Alpaca's documented inclusive `since_id` replay. The
first decoded data frame must therefore repeat the committed cursor. A server
rejection of the replay anchor refuses the connection and accepts no traffic. A
successful response whose first data frame has another ID proves a retention
gap: persist a blocked replay-gap boundary and accept no later frame or live
traffic.

A retention gap likewise has no automatic snapshot repair under the documented
provider contract. A paginated GET response cannot prove which SSE mutations it
contains, and `since_id` replay defines no relationship between an SSE ID and a
REST page set. Issuance therefore never resets the cursor, never treats snapshot
absence as deletion, and never waits for a first live event to invent a cutover.
It remains gated at the durable replay-gap boundary until an operator restores a
full database backup whose cursor can still be echo-verified by inclusive
replay, or a future Alpaca contract supplies a documented atomic snapshot/replay
boundary. If neither is available, recovery is intentionally unavailable rather
than potentially omitting a supply-gate mutation.

The GET endpoint is diagnostic only. Every diagnostic request exhausts
pagination with `region=us`, `data_quality=all`, and exactly
`types=cash_dividend,stock_dividend`; the longer `*_corporateaction_event`
values are SSE discriminators only. Each decoded row must also declare the US
region, and a missing or non-US region rejects the diagnostic result. No GET row
or absence mutates the projection, releases a hold, or advances the cursor.

Malformed or unsupported event payloads do not advance the cursor and terminate
the service fail-closed instead of being accepted or silently discarded. Before
stopping, issuance persists a blocked boundary containing the last accepted
cursor, the provider event ID when one can be parsed canonically, the typed
rejection reason, and a fixed 32-byte SHA-256 fingerprint of the rejected frame.
The decoder computes the fingerprint incrementally, so an oversized frame need
not be retained beyond the 64 KiB decoding limit. The boundary therefore remains
durable even when a frame has no valid identity or its ULID sorts below the
committed cursor. The operator diagnoses the typed failure, repairs the
source-contract support or upstream event, and explicitly clears the boundary
only after replay or repair reconciles it. There is no operator skip path that
can advance past an unreconciled supply-gate mutation. Restart tests cover
malformed and oversized frames with no parseable event ID.

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
