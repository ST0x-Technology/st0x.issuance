-- Authoritative duplicate-email guard for account registration. The account_view
-- unique index only rejects a duplicate view ROW (the projection runs after the
-- event commits and swallows the violation), so it cannot stop two concurrent
-- registrations of the same email from both committing Registered events.
-- account_emails' PRIMARY KEY lets register_account atomically claim an email
-- BEFORE committing, so the loser of a concurrent race is rejected with a 409
-- instead of producing a committed-but-unprojectable phantom account.
CREATE TABLE account_emails (
    email TEXT PRIMARY KEY
);

-- Backfill from every account already in the event log so existing emails are
-- claimed before the guard takes effect.
INSERT OR IGNORE INTO account_emails (email)
SELECT json_extract(payload, '$.Registered.email')
FROM events
WHERE aggregate_type = 'Account'
  AND event_type = 'AccountEvent::Registered';
