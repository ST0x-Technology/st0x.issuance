-- Add wallet_indexed column to account_view table
ALTER TABLE account_view
ADD COLUMN wallet_indexed TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Account.wallet')) STORED;

-- Create index on wallet_indexed column
CREATE INDEX IF NOT EXISTS idx_account_view_wallet_indexed
    ON account_view(wallet_indexed);
