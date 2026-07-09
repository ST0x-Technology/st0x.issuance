-- Rekey ReceiptInventory aggregate ids from bare vault addresses to
-- `{chain_id}:{vault_lowercase}`. Pre-multichain deployments were Base-only,
-- so every legacy id belongs to chain 8453.
--
-- Legacy ids were written via `Address::to_string()` (EIP-55 mixed case) while
-- the multichain `ReceiptVaultKey` formats the vault with `{:#x}` (lowercase
-- hex), so `lower()` here both rekeys and normalizes the casing. Without this
-- migration a chain-qualified lookup finds no stream and pre-upgrade receipts
-- silently vanish from burn planning.
--
-- Guard the shape assumption instead of trusting it: any id that is neither a
-- bare `0x…` address (legacy) nor an already-rekeyed `{chain_id}:0x…` key
-- would be silently skipped by the update below and left permanently
-- unreadable. Inserting such an id into this CHECK-violating temp table
-- aborts the migration transaction before any row is touched.
CREATE TEMP TABLE receipt_rekey_precondition (
    unexpected_aggregate_id TEXT CHECK (unexpected_aggregate_id IS NULL)
);
INSERT INTO receipt_rekey_precondition (unexpected_aggregate_id)
SELECT aggregate_id
FROM events
WHERE aggregate_type = 'ReceiptInventory'
  AND NOT (
    (
      aggregate_id GLOB '0x*'
      AND aggregate_id NOT GLOB '*:*'
    )
    OR (
      instr(aggregate_id, ':') > 1
      AND substr(aggregate_id, 1, instr(aggregate_id, ':') - 1)
          NOT GLOB '*[^0-9]*'
      AND substr(aggregate_id, instr(aggregate_id, ':') + 1) GLOB '0x*'
    )
  );
DROP TABLE receipt_rekey_precondition;

UPDATE events
SET aggregate_id = '8453:' || lower(aggregate_id)
WHERE aggregate_type = 'ReceiptInventory'
  AND aggregate_id GLOB '0x*'
  AND aggregate_id NOT GLOB '*:*';

UPDATE snapshots
SET aggregate_id = '8453:' || lower(aggregate_id)
WHERE aggregate_type = 'ReceiptInventory'
  AND aggregate_id GLOB '0x*'
  AND aggregate_id NOT GLOB '*:*';
