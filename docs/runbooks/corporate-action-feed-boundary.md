# Corporate-action feed blocked boundary

Use this runbook when issuance stops with a persisted corporate-action
`cursor_regression`, `poison`, or `replay_gap` boundary. Keep mint initiation
gated and keep the issuance service stopped throughout the investigation.

## Inspect and preserve the evidence

On the issuer host, use the SQLite file named by the service's `DATABASE_URL`.
The repository default is `data.db`; production deployments may use a different
path. Inspect the boundary and committed cursor without modifying either:

```bash
sqlite3 -readonly data.db "SELECT singleton, event_id, reason, blocked_at FROM corporate_action_blocked_event;"
sqlite3 -readonly data.db "SELECT singleton, event_id FROM corporate_action_cursor;"
```

Record both outputs in the incident, together with the issuer build revision,
the UTC failure time, the matching structured error, and Alpaca's confirmation
of the event identity or retention window. For `poison`, retain the rejected
frame supplied by Alpaca in the restricted incident evidence store; do not paste
credentials, request headers, or raw frame data into logs or chat.

## Recovery contract

There is deliberately no SQL override and no cursor-reset subcommand in this
release. Deleting `corporate_action_blocked_event`, updating
`corporate_action_cursor`, or deleting a mutation is unsafe: inclusive replay
will either hit the same poison again or an advanced cursor will silently omit
unknown mutations and can leave source-owned freezes and schedules incorrect.

The feed may resume only through the snapshot repair operation specified under
[Corporate-actions sourcing](../../SPEC.md#corporate-actions-sourcing). That
operation must:

- durably buffer a newly connected live stream;
- exhaust Alpaca's paginated current-action snapshot for the accepted US
  dividend types;
- rebuild every source-owned schedule and hold;
- apply the buffered mutations in order; and
- atomically replace the cursor and clear the blocked boundary only after all
  pending alignment work succeeds.

No mutation is classified as “skipped.” The current snapshot reconstructs the
effect of mutations no longer retained by the stream, while buffered mutations
preserve changes that arrive during reconstruction. Any failed prerequisite
leaves the original boundary and cursor intact and the service stopped.

Until a release containing that repair operation is deployed and validated, the
production corporate-actions feed must remain disabled after a blocked boundary.
Escalate with the recorded evidence rather than editing the database.
