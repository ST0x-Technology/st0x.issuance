# S01 Alpaca KMS JWT credentials

## Preconditions

The KMS flow uses the GCE metadata identity. Production currently runs on a
DigitalOcean droplet; its cutover is blocked until issuance has a production GCP
workload. Keep Basic there. Do not revoke production's key in this step.

The shared Alpaca retry fix must be released as v0.2.1 before the final release
dependency is pinned. S01 issuance consumes that fix for broker calls and
refresh backoff. The feed uses the shared stream transport and decoder; issuance
still owns cursors and freeze projection.

## Staging cutover

1. Create the staging signing key with the runtime service account's
   `roles/cloudkms.signerVerifier` binding scoped to that crypto key. Export the
   public PEM. Register an ES256 private-key JWT credential in sandbox
   BrokerDash with the issuance account's existing scopes. Label it
   `s01-issuance-staging`, record its client ID and expiry date.
2. Release the binary with KMS support while staging still uses Basic.
3. Rebase the compose change on current infra main. Pause issuance-main and
   staging-infra merges until cutover completes: image rolls also rewrite
   `images.env`. Create a new `s01-issuance-secrets-env` version omitting
   `ALPACA_API_KEY` and `ALPACA_API_SECRET`.
4. On the unmerged compose branch, select that new secret version and add
   `ALPACA_CLIENT_ID` plus `ALPACA_KMS_KEY_VERSION`. Apply locally with
   `-replace=module.bot_vm[0].google_compute_instance.this`. Expect several
   minutes of staging downtime. Merge the branch only after the new VM is
   healthy; merging first lets the old VM adopt an incompatible secret file.
5. Verify `validate-config`, a full sandbox mint callback, and redemption plus
   journal polling. No setting disables the feed: it stays disabled only while
   `corporate_action_cursor` is empty and
   `ALPACA_CORPORATE_ACTIONS_BOOTSTRAP_SINCE` is unset (see the
   [boundary runbook](corporate-action-feed-boundary.md) for the read-only
   cursor query). With a cursor, as on staging after its Basic runs, the feed
   connects at startup: confirm it connects with bearer auth. A sandbox bearer
   may not authorize the live stream host. A rejection stops the whole service:
   roll back as in [Rollback](#rollback) and hold the cutover until a separately
   reviewed stream-only live credential is wired. Never silently reuse Basic.
6. If no durable corporate-action cursor exists, follow the
   [bounded bootstrap procedure](corporate-action-feed-boundary.md), then remove
   the bootstrap setting once a cursor exists. The feed first connects here, so
   run the stream check and rollback rule from step 5 now.
7. Verify KMS `AsymmetricSign` audit entries identify the runtime service
   account. Temporarily append `st0x_alpaca::auth=info` to the workload's
   existing `RUST_LOG` directives; both console and HyperDX use this filter.
   Preserve the existing crate/domain directives. If `RUST_LOG` is set, also add
   `operational_alert=error`: the target is new in this release, an explicit
   `RUST_LOG` replaces [`default_log_filter`](../../src/config.rs), and without
   it the filter drops the alert. Keep that directive in the deployment's
   `RUST_LOG` after verification. If `RUST_LOG` is unset, use the configured
   log-level defaults from `default_log_filter` plus the new directive; setting
   only the dependency directive would suppress the bot's other logs. Observe an
   initial `Minted Alpaca access token` entry after a cold start before
   measuring refresh frequency. For 15-minute tokens, expect at most one mint
   per roughly 13 minutes per active client (mint callbacks, redemptions, and
   the feed have separate caches), rather than per request. Empty logs are not
   cache evidence. A deliberately invalid staging credential must produce
   `Alpaca credential rejected` on `operational_alert` and increment the central
   operational-alert metric. Then remove `st0x_alpaca::auth=info` again.
8. After those checks succeed, revoke the old sandbox key in BrokerDash. Destroy
   old Secret Manager versions containing it. Remove it from any retained
   staging agenix file, or retire that file.

Production repeats these steps only after its GCP migration, with an independent
live signing key and BrokerDash client. The issue remains open until production
is verified and its old key and secrets are removed.

## Rotation and expiry

Record the BrokerDash expiry alongside the client ID and schedule a reminder 30
days beforehand. Register a new private-key JWT credential against the same
public KMS key, replace the client ID, and replace the VM using the cutover
ordering above. Register a new public key before switching to a new KMS key
version. Never reuse signing keys between environments.

## Credential failures

Deterministic credential failures alert through `operational_alert`. The feed
stops the service. Mint callbacks retain bounded re-fetch retries and remain
`CallbackPending` for recovery once credentials are fixed. Journal polling
continues, but its deadline can still fail a redemption. A redeem token mint
that fails after the shared crate's retry budget records `RecordAlpacaFailure`
and terminally fails that redemption, including Retry-After values beyond the
call budget. Use the existing issuance recovery procedure after fixing the
credential; there is no periodic re-driver for `AlpacaCallClaimed`.

## Rollback

Before revocation, prepare an unmerged revert selecting the old secret version
and removing both KMS identifiers. Apply the VM replacement from that branch
first, check health, then merge. This avoids an old KMS compose adopting a Basic
secret file; mixed auth is refused. After revocation, Basic rollback requires a
new key from BrokerDash and a deployment.

Removing KMS IAM blocks new token mints, but cached bearers can remain usable
for about 15 minutes and an open stream can stay connected. Stop the workload
for an immediate stop.
