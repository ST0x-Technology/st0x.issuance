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
  IDs, symbols, ex-dates, and uppercase ULID event IDs are parsed before use.
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

Project the stream into a local corporate-action schedule keyed by Alpaca's
stable corporate-action ID. The source-owned `FreezeHoldId` is derived from that
action ID alone — not from ex-date-derived window boundaries — so every revision
of one action addresses the same hold. Window boundaries still key the scheduled
transition jobs' idempotency (two jobs per window), and the operator's manually
scheduled windows remain a separate hold family keyed by their window; neither
can collide with an action-owned hold.

Reconciliation has two durable phases. The first SQLite transaction inserts the
accepted mutation, replaces the action's schedule row with its latest event ID,
revision, underlying, ex-date, deletion state, and a null reconciled marker,
then advances the replay cursor. Those projection and cursor writes either all
commit or all roll back. Apalis transition rows, aggregate hold events, and the
final reconciled marker are deliberately not in that transaction: they are
recoverable second-phase effects. The second phase enqueues idempotent alignment
jobs for the pending revision and marks it reconciled only after enqueueing
succeeds. Startup reconciles every pending revision to completion — successful
enqueue and reconciled marker for each — before connecting to the stream. A
reconciliation failure keeps the feed disconnected and preserves the pending
revision; the feed never opens over an unreconciled revision.

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
Alignment retries are bounded; startup re-arms a terminal alignment because its
projection remains pending and enqueues one deduplicated sync-failure lifecycle
notification for that alignment key.

On first install, the cursor is absent and `since=1970-01-01T00:00:00Z` replay
is not trusted as complete: Alpaca documents no retention floor, so the first
retained frame proves nothing about earlier events, and absence of a committed
cursor means the retention-gap check below cannot fire. First install therefore
establishes its baseline through the same snapshot-plus-buffer procedure as
retention repair — minting stays gated until the rebuilt projection and its
cutover cursor commit. On every subsequent connection, the request uses
`since_id=<committed-event-id>` rather than `Last-Event-Id`, relying on Alpaca's
documented inclusive `since_id` replay. The first decoded data frame must
therefore repeat the committed cursor. A server rejection of the replay anchor
refuses the connection and accepts no traffic. A successful response whose first
data frame has another ID proves a retention gap: persist a blocked replay-gap
boundary and accept no later frame or live traffic.

A retention gap cannot be repaired by silently choosing a newer cursor. The
explicit repair procedure keeps minting gated, opens the live stream into a
bounded durable buffer, then fetches and exhausts the paginated GET snapshot
with an explicit scope: `types` set to exactly the two accepted dividend types,
and `start`/`end` — which the endpoint defaults to the current day and filters
by `process_date` — widened to cover every process date whose window could still
gate issuance. The endpoint has no region parameter; the US boundary comes from
the US endpoint itself plus the accepted event types. The snapshot is
authoritative for deletions only within that declared complete scope: an action
absent from a complete scoped snapshot is deleted, while anything outside the
scope keeps the buffered SSE mutations as the sole authority for revisions and
deletions. The repair rebuilds the current action projection and source-owned
holds from the snapshot, then applies every buffered mutation in order. The
snapshot itself carries no SSE cursor, so the cutover cursor is the last
buffered mutation's event ID; the rebuilt projection and that cursor become
authoritative in one SQLite transaction. If the buffer is empty when the
snapshot is exhausted, the transaction commits the projection without a cursor
and the still-open stream continues: the first accepted live frame commits as
the new cursor, and a disconnect before that frame restarts the repair rather
than reconnecting over an undefined anchor. Only after pending alignment
completes may normal live consumption begin. Buffer overflow, disconnect,
incomplete pagination, or any unvalidated row aborts the repair. The GET
endpoint is otherwise diagnostic and is never the ordinary source of truth for
deletions.

Malformed or unsupported event payloads do not advance the cursor and terminate
the service fail-closed instead of being accepted or silently discarded. The
blocked boundary makes the poison event durable even when its ULID sorts below
the committed cursor; a frame whose identity itself is malformed records the
same boundary without an event ID. The operator diagnoses the typed failure,
repairs the source-contract support or upstream event, and explicitly clears the
boundary only after replay or repair reconciles it. There is no operator skip
path that can advance past an unreconciled supply-gate mutation.

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
