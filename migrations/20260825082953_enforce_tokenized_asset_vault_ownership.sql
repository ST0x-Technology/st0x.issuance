-- Authoritative one-underlying-per-(network, vault) guard for tokenized-asset
-- registration. The add-time and boot-time
-- validate_one_underlying_per_network_vault checks read the projection, which
-- lags the event store, so two concurrent POSTs for different underlyings
-- sharing one (network, vault) can both pass and both commit Added events to
-- different aggregate ids. Redemption transfer matching then cannot attribute a
-- share transfer to one asset, and the next boot guard aborts startup. Claiming
-- (network, vault) in the SAME transaction that appends the Added /
-- VaultAddressUpdated event makes the event append the arbitration point: the
-- loser's event insert is aborted before it can commit.
CREATE TABLE tokenized_asset_vault_owners (
    network TEXT NOT NULL,
    vault TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    PRIMARY KEY (network, vault),
    UNIQUE (aggregate_id)
);

-- Backfill the current owner of every (network, vault) from the event log so a
-- new registration against a vault an existing asset already uses is rejected.
-- The current vault of an asset is the vault on its latest VaultAddressUpdated
-- event, or its Added vault if it was never re-pointed. Added events without
-- network or vault metadata are ignored: they cannot name a listing to claim.
-- A live database may already hold two underlyings on one (network, vault): the
-- previous guard only rejected the same vault across different networks, so a
-- same network pair passed it. The backfill below would then hit the PRIMARY
-- KEY with a bare "UNIQUE constraint failed" naming nothing, taking the service
-- down at boot (the migration runs inside initialize_rocket). This temporary
-- trigger turns that into an actionable error. RAISE is trigger only and cannot
-- interpolate values, so find the offending listings with:
--   SELECT json_extract(payload, '$.Added.network') AS network,
--          json_extract(payload, '$.Added.vault') AS vault,
--          GROUP_CONCAT(aggregate_id)
--   FROM events
--   WHERE aggregate_type = 'TokenizedAsset'
--     AND event_type = 'TokenizedAssetEvent::Added'
--   GROUP BY network, vault HAVING COUNT(DISTINCT aggregate_id) > 1;
CREATE TEMP TRIGGER reject_ambiguous_vault_backfill
BEFORE INSERT ON tokenized_asset_vault_owners
WHEN EXISTS (
    SELECT 1
    FROM tokenized_asset_vault_owners
    WHERE network = NEW.network
      AND vault = NEW.vault
      AND aggregate_id != NEW.aggregate_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'tokenized asset vault ownership backfill: a (network, vault) serves two underlyings; resolve the duplicate listing before upgrading'
    );
END;

INSERT INTO tokenized_asset_vault_owners (network, vault, aggregate_id)
SELECT
    json_extract(added.payload, '$.Added.network'),
    COALESCE(
        (
            SELECT json_extract(upd.payload, '$.VaultAddressUpdated.vault')
            FROM events AS upd
            WHERE upd.aggregate_type = 'TokenizedAsset'
              AND upd.aggregate_id = added.aggregate_id
              AND upd.event_type = 'TokenizedAssetEvent::VaultAddressUpdated'
            ORDER BY upd.sequence DESC
            LIMIT 1
        ),
        json_extract(added.payload, '$.Added.vault')
    ),
    added.aggregate_id
FROM events AS added
WHERE added.aggregate_type = 'TokenizedAsset'
  AND added.event_type = 'TokenizedAssetEvent::Added'
  AND json_extract(added.payload, '$.Added.network') IS NOT NULL
  AND json_extract(added.payload, '$.Added.vault') IS NOT NULL;

DROP TRIGGER reject_ambiguous_vault_backfill;

-- Claim the (network, vault) when an asset is first added. The explicit RAISE is
-- load-bearing: the event store appends with INSERT OR IGNORE, and that IGNORE
-- propagates into this trigger, so a bare PRIMARY KEY violation would be
-- swallowed and the event silently backed out, misreported upstream as a
-- same-aggregate optimistic-lock conflict. RAISE(ABORT) overrides the outer
-- IGNORE and surfaces the real cause. The WHEN guard skips Added events without
-- network / vault metadata, which name no listing to claim.
CREATE TRIGGER claim_tokenized_asset_vault_on_added
AFTER INSERT ON events
WHEN NEW.aggregate_type = 'TokenizedAsset'
 AND NEW.event_type = 'TokenizedAssetEvent::Added'
 AND json_extract(NEW.payload, '$.Added.network') IS NOT NULL
 AND json_extract(NEW.payload, '$.Added.vault') IS NOT NULL
BEGIN
    -- This message must contain the Rust constant VAULT_CLAIM_CONFLICT_MESSAGE
    -- (src/tokenized_asset/mod.rs); the add handler matches it to return 422.
    SELECT RAISE(
        ABORT,
        'tokenized asset vault already serves another underlying on this network'
    )
    WHERE EXISTS (
        SELECT 1
        FROM tokenized_asset_vault_owners
        WHERE network = json_extract(NEW.payload, '$.Added.network')
          AND vault = json_extract(NEW.payload, '$.Added.vault')
          AND aggregate_id != NEW.aggregate_id
    );

    INSERT INTO tokenized_asset_vault_owners (network, vault, aggregate_id)
    VALUES (
        json_extract(NEW.payload, '$.Added.network'),
        json_extract(NEW.payload, '$.Added.vault'),
        NEW.aggregate_id
    );
END;

-- Move the claim when an asset is re-pointed at a new vault. Re-pointing onto a
-- vault another asset owns on the same network aborts the event append. The
-- network is fixed by the aggregate id, so it is read from this asset's own row.
-- The WHEN guard skips events without a new vault.
CREATE TRIGGER claim_tokenized_asset_vault_on_update
AFTER INSERT ON events
WHEN NEW.aggregate_type = 'TokenizedAsset'
 AND NEW.event_type = 'TokenizedAssetEvent::VaultAddressUpdated'
 AND json_extract(NEW.payload, '$.VaultAddressUpdated.vault') IS NOT NULL
BEGIN
    -- This message must contain the Rust constant VAULT_CLAIM_CONFLICT_MESSAGE
    -- (src/tokenized_asset/mod.rs); the add handler matches it to return 422.
    SELECT RAISE(
        ABORT,
        'tokenized asset vault already serves another underlying on this network'
    )
    WHERE EXISTS (
        SELECT 1
        FROM tokenized_asset_vault_owners AS other
        WHERE other.vault = json_extract(
                NEW.payload,
                '$.VaultAddressUpdated.vault'
            )
          AND other.aggregate_id != NEW.aggregate_id
          AND other.network = (
              SELECT owner.network
              FROM tokenized_asset_vault_owners AS owner
              WHERE owner.aggregate_id = NEW.aggregate_id
          )
    );

    UPDATE tokenized_asset_vault_owners
    SET vault = json_extract(NEW.payload, '$.VaultAddressUpdated.vault')
    WHERE aggregate_id = NEW.aggregate_id;
END;
