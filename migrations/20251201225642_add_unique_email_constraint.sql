-- Add unique constraint on email to prevent duplicate registrations
-- Email can be in $.Registered.email or $.LinkedToAlpaca.email

-- Drop old index that used outdated $.Account.email path
DROP INDEX IF EXISTS idx_account_view_email_indexed;

-- Create unique index on email extracted from either state
-- COALESCE returns the first non-null value
CREATE UNIQUE INDEX IF NOT EXISTS idx_account_view_email_unique
    ON account_view(
        COALESCE(
            json_extract(payload, '$.Registered.email'),
            json_extract(payload, '$.LinkedToAlpaca.email')
        )
    )
    WHERE json_extract(payload, '$.Registered.email') IS NOT NULL
       OR json_extract(payload, '$.LinkedToAlpaca.email') IS NOT NULL;
