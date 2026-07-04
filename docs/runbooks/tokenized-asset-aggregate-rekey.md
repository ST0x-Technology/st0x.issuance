# TokenizedAsset aggregate rekey runbook (RAI-1205)

Rekeys live `TokenizedAsset` aggregate ids from `{underlying}` to
`{underlying}:{network}` (e.g. `AAPL` -> `AAPL:base`).

## Preconditions

- Issuance binary includes migration
  `20260703235904_rekey_tokenized_asset_aggregate_id`.
- Coordinate lockstep deploy with `st0x-issuance-client` and liquidity
  (RAI-1212): internal detail/status callers must send `?network=`.
- Maintenance window long enough for backup + single deploy + smoke check.

## 1. Backup

```bash
cp /path/to/issuance.db /path/to/issuance.db.pre-asset-key-$(date -u +%Y%m%dT%H%M%SZ)
```

Verify the backup opens and row counts match production:

```bash
sqlite3 /path/to/issuance.db.pre-asset-key-<timestamp> \
  "SELECT COUNT(*) FROM events WHERE aggregate_type = 'TokenizedAsset';"
```

## 2. Dry run on a copy

```bash
cp /path/to/issuance.db /tmp/issuance-dry-run.db
DATABASE_URL=sqlite:/tmp/issuance-dry-run.db sqlx migrate run
```

Validate rekey idempotency and view rebuild:

```sql
-- No underlying-only ids remain
SELECT aggregate_id FROM events
WHERE aggregate_type = 'TokenizedAsset' AND aggregate_id NOT LIKE '%:%';

-- Expected shape
SELECT aggregate_id FROM events
WHERE aggregate_type = 'TokenizedAsset'
LIMIT 5;
```

Start issuance against the copy
(`DATABASE_URL=sqlite:/tmp/issuance-dry-run.db`). Confirm:

- `GET /tokenized-assets` lists seeded assets.
- `GET /tokenized-assets/AAPL/status?network=base` returns 200 (not 422).
- `GET /tokenized-assets/AAPL/status` without `?network=` returns 422.

Re-run `sqlx migrate run` on the same copy -- row counts and aggregate ids must
be unchanged (migration is idempotent).

## 3. Production cutover

1. Stop issuance.
2. Take a fresh backup (step 1).
3. Deploy the RAI-1205 binary; startup runs migrations automatically.
4. Deploy matching `st0x-issuance-client` + liquidity freeze guard (RAI-1212).
5. Smoke: token list, `?network=base` status for a known asset, one mint on
   Base.

## 4. Rollback

Stop issuance. Restore the pre-cutover DB backup. Redeploy the previous
issuance, client, and liquidity builds together. Do not leave a mixed-version
window (see SPEC multichain rollback section).

## 5. Idempotency notes

- SQL migration only appends `:base` when `aggregate_id` has no `:` suffix.
- `TokenizedAsset` schema version bump clears stale projections; catch-up
  rebuilds `tokenized_asset_view` from rekeyed events.
- Safe to re-run migration on a partially migrated copy during dry-run
  validation.
