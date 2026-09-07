# Corporate-action feed boundary and first-install bootstrap

Use this runbook either to authorize one bounded-history first install or when
issuance stops with a persisted corporate-action `cursor_regression`, `poison`,
or `replay_gap` boundary. A first-install bootstrap never repairs a blocked
boundary.

## Authorize a first-install boundary

An authenticated deployment with no committed cursor remains disabled unless an
operator explicitly configures `ALPACA_CORPORATE_ACTIONS_BOOTSTRAP_SINCE` as a
non-future RFC3339 instant. Choose the instant from independently verified
operational history. This accepts that Alpaca does not certify complete
retention before or after that instant; it does not create a complete snapshot.

Before setting it, inspect the database read-only and confirm that both the
cursor and blocked-boundary queries below return no row. A blocked boundary must
follow the recovery procedure instead. Add the timestamp to the managed
encrypted service environment, run the normal configuration validation, and
deploy.

Before the HTTP service accepts traffic, the issuer captures a UTC cutoff and
requests `since=<timestamp>&until=<cutoff>`. Alpaca's
[pinned OpenAPI contract](https://github.com/alpacahq/alpaca-java/blob/22438ede043590ad464df7abe375e599c8cd0adc/specs/data/openapi.yaml#L77-L132)
documents `until` as an inclusive boundary that closes the stream after the
final bounded event. Startup waits for that closure and for every replayed
current hold to align. EOF with a partial SSE frame is a failed replay, not a
completed window. Any replay, projection, scheduling, or alignment failure
aborts startup. The live connection then uses inclusive `since_id=<event-id>`
when replay established a cursor. If the bounded interval was empty, it
continues from `since=<cutoff>` until the first validated mutation commits a
cursor.

Remove `ALPACA_CORPORATE_ACTIONS_BOOTSTRAP_SINCE` from the managed environment
once `corporate_action_cursor` contains a verified event ID. If the bounded
interval was empty, keep the setting only until the first live event establishes
that cursor. The issuer cannot distinguish an intentional first install from a
lost, empty, or unmounted database while a bootstrap value remains configured.
Leaving it set could silently reuse the old lower bound after storage loss.
Treat a missing cursor on an established deployment as a storage incident, not
as permission to bootstrap again.

## Inspect and preserve blocked-boundary evidence

On the issuer host, read the deployed service's exact `DATABASE_URL`; do not
assume the repository default `data.db`. Resolve its SQLite path and strip any
connection query before inspecting the boundary and committed cursor:

```bash
case "$DATABASE_URL" in
  sqlite:*) ;;
  *) echo "DATABASE_URL is not a SQLite URL" >&2; exit 1 ;;
esac
DB_PATH="${DATABASE_URL#sqlite://}"
DB_PATH="${DB_PATH#sqlite:}"
DB_PATH="${DB_PATH%%\?*}"
test -n "$DB_PATH"

sqlite3 -readonly "$DB_PATH" "SELECT singleton, event_id, reason, blocked_at FROM corporate_action_blocked_event;"
sqlite3 -readonly "$DB_PATH" "SELECT singleton, event_id FROM corporate_action_cursor;"
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

The feed may resume only after one of these supported recovery paths succeeds:

- restore a full database backup whose corporate-action cursor can be
  echo-verified by inclusive replay; or
- deploy and validate a recovery implementation against an Alpaca-documented
  atomic snapshot/replay boundary.

Do not combine Alpaca's paginated current-action snapshot with a buffered live
stream. Alpaca documents neither a snapshot watermark nor a consistency
relationship between GET pagination and an SSE event ID, so that procedure
cannot prove that no mutation was lost during cutover.

Until one supported recovery path is available and validated, the production
corporate-actions feed must remain stopped after a blocked boundary. Escalate
with the recorded evidence rather than editing the database.
