# 01. Model freezes as owned holds

- Status: Proposed
- Date: 2026-07-15
- Issue:
  [RAI-1045](https://linear.app/makeitrain/issue/RAI-1045/schedule-freezing-with-apalis)

## Context

`Underlying`, keyed by `UnderlyingSymbol`, currently represents supply gating as
a single `Enabled` or `Frozen` status. Both the issuer CLI and each scheduled
corporate-action window dispatch the same idempotent `Freeze` and `Unfreeze`
commands.

That model cannot preserve the business invariant when freeze requirements
overlap. Given windows `[A, C)` and `[B, D)`, the first window's `Unfreeze` at
`C` enables the asset even though the second window still requires it to remain
frozen until `D`. Adjacent windows have the same problem because their jobs can
run in either order at the shared timestamp. Retrying the commands safely does
not solve the ownership problem: an idempotent toggle records the target state,
not which requirement still owns that state.

The asset must remain frozen while any independent freeze requirement is active.
Releasing one requirement must never release another, regardless of job delivery
order, retries, restarts, or whether the requirement came from an operator or
corporate-action automation.

## Decision

Represent freezing in the `Underlying` domain as a set of owned holds. Each hold
has a typed, stable `FreezeHoldId` and a typed source identifying the operator
action or corporate-action window that created it. Acquiring an already-active
hold and releasing an absent hold are idempotent operations.

The underlying is supply-enabled exactly when its active-hold set is empty.
Issuance gates new mints on that derived capability; liquidity separately uses
the published status to hold pre-send rebalance redemptions. Issuance does not
introduce a general redemption hold gate. A scheduled freeze job acquires its
window's hold and the paired unfreeze job releases that same hold, so delivery
order between different windows cannot release the wrong requirement. Manual CLI
operations acquire and release an explicit operator hold rather than bypassing
hold ownership.

The event model will record hold acquisition and release. Existing freeze events
remain replayable and are interpreted as the legacy operator hold so historical
aggregates retain their current status during migration.

## Alternatives Considered

### Reject or merge overlapping windows in the scheduler

- Pros: Keeps the aggregate and its existing commands unchanged.
- Cons: Requires a second durable interval registry, atomic coordination between
  that registry and the apalis queue, and special handling for operator freezes.
- Rejected because: The scheduler would become the authority for a domain
  invariant while other freeze sources could still violate it, and a crash
  between registry and queue writes could strand inconsistent ownership.

### Inspect queued jobs before executing an unfreeze

- Pros: Can be implemented without changing historical domain events.
- Cons: Couples domain correctness to apalis table layout, serialized job
  payloads, cleanup policy, and queue status transitions; manual freezes remain
  indistinguishable from scheduled freezes.
- Rejected because: Queue storage is an execution detail and cannot be the
  source of truth for whether the asset is allowed to mint or resume held
  redemptions.

### Rely on timestamp ordering or queue priority

- Pros: Small change for adjacent windows.
- Cons: Does not handle genuinely overlapping intervals, retries, delayed jobs,
  or a manual freeze active at the same time.
- Rejected because: Correctness would depend on incidental delivery order rather
  than an explicit domain invariant.

## Consequences

Overlapping, adjacent, retried, and out-of-order scheduled windows become safe
by construction. Freeze ownership is auditable in the event stream, and new
freeze sources can compose without learning about scheduler internals.

The `Underlying` state, commands, events, projection, CLI, scheduler jobs, and
lifecycle notifications must carry hold identity. Backward-compatible event
replay and projection migration need explicit regression coverage. Operator
unfreeze semantics become precise: it releases the operator hold and cannot
override an active corporate-action hold.
