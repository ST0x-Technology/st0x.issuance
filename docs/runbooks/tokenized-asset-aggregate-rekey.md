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
```

Before migrating, check for a partially migrated store. If a bare id and its
rekeyed form both exist (e.g. `AAPL` **and** `AAPL:base`), appending `:base` to
the bare rows would collide with the existing rows on the events primary key
`(aggregate_type, aggregate_id, sequence)` and abort the migration
mid-transaction. This query must return no rows; if it returns any, resolve the
duplicate aggregate manually before proceeding:

```sql
SELECT bare.aggregate_id
FROM events AS bare
JOIN events AS rekeyed
  ON rekeyed.aggregate_type = 'TokenizedAsset'
 AND rekeyed.aggregate_id = bare.aggregate_id || ':base'
WHERE bare.aggregate_type = 'TokenizedAsset'
  AND bare.aggregate_id NOT GLOB '*:*'
GROUP BY bare.aggregate_id;
```

(The migration itself also aborts, before touching any row, if it finds an empty
`TokenizedAsset` aggregate id or a `:`-bearing id that is not a lowercase
rekeyed `{underlying}:base` key. The guard uses case-sensitive `GLOB`, not
`LIKE`, so mixed-case suffixes such as `AAPL:BASE` abort instead of being
silently skipped.)

Then apply the migration:

```bash
DATABASE_URL=sqlite:/tmp/issuance-dry-run.db sqlx migrate run
```

Validate rekey idempotency and view rebuild:

```sql
-- No underlying-only ids remain
SELECT aggregate_id FROM events
WHERE aggregate_type = 'TokenizedAsset' AND aggregate_id NOT GLOB '*:*';

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
3. Run the duplicate-aggregate check from step 2 against the stopped production
   database — it must return no rows before you deploy.
4. Deploy the RAI-1205 binary; startup runs migrations automatically.
5. Deploy matching `st0x-issuance-client` + liquidity freeze guard (RAI-1212).
6. Smoke: token list, `?network=base` status for a known asset, one mint on
   Base.

## 4. Rollback

1. Stop issuance.
2. Restore the pre-cutover backup over the live database:

   ```bash
   cp /path/to/issuance.db.pre-asset-key-<timestamp> /path/to/issuance.db
   ```

   The database restore is mandatory, not optional: reverted code looks assets
   up by the old `{underlying}` keys, so running it against a rekeyed store
   silently finds no assets at all.

3. Redeploy the previous issuance, `st0x-issuance-client`, and liquidity builds
   together.

Do not leave a mixed-version window: if liquidity still sends `?network=`
against rolled-back issuance, freeze/status calls succeed harmlessly, but if
liquidity is rolled back alone while issuance still requires `?network=`, the
freeze guard receives 422 and rebalancing fail-closes until the versions match
again.

## 5. Idempotency notes

- SQL migration only appends `:base` when `aggregate_id` has no `:` suffix.
- `TokenizedAsset` schema version bump clears stale projections; catch-up
  rebuilds `tokenized_asset_view` from rekeyed events.
- Safe to re-run migration on a partially migrated copy during dry-run
  validation.
