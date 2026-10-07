# Ops Recovery Guide: Stuck Transactions

This guide covers how to diagnose and recover stuck mints and redemptions in the
issuance bot.

## Prerequisites

All admin endpoints require the `X-API-KEY` header. On the droplet:

```bash
export ISSUER_API_KEY=$(grep ISSUER_API_KEY /mnt/volume_nyc3_02/.env | cut -d= -f2)
```

Every example below assumes this is set.

The app listens on port 8000 while it is its own public listener, and on
**8001**, bound to loopback, once `st0x.ingress.behindProxy` is on
(`nix/ingress.nix`). `/admin/*` is never proxied by nginx in either mode, so
these commands always run on the box; only the port moves. Confirm which one is
live and export it before you start; every example below uses it:

```bash
ss -tlnp | grep st0x-issuance
export ISSUER_API_PORT=8000   # 8001 once behindProxy is on
```

## Step 1: Check what's stuck

```bash
curl -s -H "X-API-KEY: $ISSUER_API_KEY" http://localhost:$ISSUER_API_PORT/admin/stuck | python3 -m json.tool
```

This returns all transactions in non-terminal, non-progressing states. Each
entry shows:

| Field                     | Meaning                                     |
| ------------------------- | ------------------------------------------- |
| `aggregate_type`          | `mint` or `redemption`                      |
| `aggregate_id`            | The ID to use in recovery endpoints         |
| `tokenization_request_id` | Alpaca's ID for cross-referencing           |
| `state`                   | Where it got stuck (see state tables below) |
| `detail`                  | Error message explaining why                |
| `tx_id`                   | Current stuck transaction ID, when recorded |
| `timestamp`               | When it entered this state                  |

`/admin/stuck` projects plain `Burning`, `BurnIntended`, and `BurnSubmitted`
aggregates as `state: "Burning"`. `detail: "Waiting for burn confirmation"` and
`tx_id` show that a transaction was recorded; "Waiting for burn submission"
means none was. A 32-byte `0x...` hash is force-complete eligible only when it
identifies the exact persisted signed intent. The endpoint verifies that
identity and rejects legacy or pre-intent transactions even if their burn was
reconciled separately.

## How automatic recovery works

On every startup, the bot automatically attempts to recover stuck transactions.
Mint recovery (`run_mint_recovery`) covers mints in `JournalConfirmed`,
`Minting`, `TxIntended`, `TxSubmitted`, `MintingFailed`, and `CallbackPending`.
Redemption recovery covers redemptions in `Detected`
(`recover_detected_redemptions`), `AlpacaCalled`
(`recover_alpaca_called_redemptions`), `Burning` (`recover_burning_redemptions`,
including aggregate `BurnIntended` and `BurnSubmitted` states), and `BurnFailed`
(`recover_burn_failed_redemptions`), plus stuck-reservation cleanup
(`recover_stuck_reservations`). This runs with a **30-second timeout** before
the HTTP server starts accepting requests.

Persisted burn transactions are also reconciled every five minutes. Recovery
confirms mined transactions, re-broadcasts the exact signed bytes while a
transaction can still land, and signs a fresh-nonce replacement only after the
old hash is provably dead. After five durable automatic actions across the
redemption's lifetime, it logs `Automatic burn recovery exhausted` once with the
request ID, transaction hash, nonce, and required operator action. Automatic
recovery does not sign again after exhaustion; the admin recovery endpoint can
authorize one replacement after independently proving the persisted hash dead.

- If recovery completes within 30 seconds, everything is handled automatically.
- If recovery times out (e.g., the RPC is slow or unavailable), the remaining
  stuck transactions are left for manual intervention via the admin endpoints
  below.
- If a burn recovery finds that the **on-chain balance is insufficient**, it
  skips the burn and logs `MANUAL INTERVENTION REQUIRED`. Verify the relevant
  transaction and receipt inventory before choosing an action. For a `Failed`
  redemption with a recorded transaction ID, use `/admin/recover/redemption`.
  Force-complete only a `BurnIntended` or `BurnSubmitted` redemption with a
  persisted signed transaction. Reconcile legacy or unverifiable burns off-chain
  before closing.

### Uncertain mint confirmation (do not force a second deposit)

Vault-direct mint confirm polls `eth_getTransactionReceipt` as `Option`. A null
receipt, timeout, transport error, or other uncertain observation **does not**
record `MintingFailed` and **must not** be "fixed" by submitting another
deposit. The aggregate stays in `TxSubmitted`; scheduled recovery re-polls
(~60s). After ~6h with no progress, automatic recovery abandons until process
restart or admin reprocess — restart is safe because the prepared hash is
rebroadcast / re-confirmed, not replaced.

**Reprocess / recover only rebroadcasts or reconfirms the exact signed hash**
already on the aggregate. A new deposit (replacement prepare) is authorized only
after the prior identity is terminal (`MinedReverted` or `ProvablyDead`), with a
wallet-guard TOCTOU recheck immediately before signing, and only when inventory
has **no** receipt for that `issuer_request_id`. Do not hand-craft a second
deposit while identity is still mineable, uncertain, or inventory already shows
a mint receipt.

If logs show `Duplicate Deposit/receipt for already-tracked issuer_request_id`,
a second on-chain deposit already landed for that mint. Do **not** mint again —
the excess shares must be burned with `burn-excess`. Both modes run through the
operator client (`st0x-issuance-client breakglass burn-excess ...`), with the
offline `issuer burn-excess` CLI as the fallback when the client cannot reach
the bot.

The observation is also recorded durably as a
`ReceiptInventoryEvent::ConflictingItnDepositObserved` event on the vault's
receipt-inventory stream, so the evidence survives the backfill checkpoint
advancing past that block. There is no admin health list of duplicates yet; read
the event (or the ERROR log fields) for both identities.

Collect these before running anything:

| Input              | Where it comes from                                                 |
| ------------------ | ------------------------------------------------------------------- |
| Issuer request id  | `issuer_request_id` on the event / log line                         |
| Tracked deposit    | `tracked` — the receipt and tx the mint is already accounted for    |
| Duplicate deposit  | `discovered_receipt_id` + `discovered_tx_hash` — the excess to burn |
| Excess shares      | The duplicate `Deposit` log's share amount, read from the explorer  |
| Network / chain id | The vault's listing (`--network`, cross-checked `--chain-id`)       |
| Vault              | The vault the duplicate deposit hit                                 |

Then pick the mode by where the excess shares sit **now** — burn-excess never
infers it:

- Shares still in the issuer wallet (the deposit's original recipient is the
  issuer):
  `st0x-issuance-client --env <env> breakglass burn-excess internal
  --issuer-request-id <id> --deposit-tx-hash <discovered_tx_hash> --receipt-id
  <discovered_receipt_id> --shares <decimal, e.g. 0.750> --network <net>
  --chain-id <id> --reason "<why>" --incident-id <id> --execute`
- Shares were minted to someone else and must be moved back first. **Before
  anything is transferred**, record the funding expectation with the same flags
  as above:
  `st0x-issuance-client --env <env> breakglass burn-excess
  expect-funding ... --execute`.
  Its `precondition` names the exact Transfer to send: the excess shares, from
  the deposit's original recipient to the issuer wallet. Only then have the
  shares sent back; the bot's redemption poller holds that Transfer instead of
  redeeming it. Then run `breakglass burn-excess external` with the same flags
  plus `--funding-tx-hash <that transfer's tx>`; the bot refuses (409) a stream
  with no expectation. If that 409 comes after the shares were already sent, do
  not record an expectation now: the poller may already have redeemed the
  transfer. Search the bot's logs for that transaction hash (a detected
  redemption is keyed by it). If none was detected, stop the issuer service and
  finish with the offline CLI below; if one was, burn-excess refuses that
  transfer (`FundingAlreadyRedeemedTx`) and the redemption it opened is the
  incident to handle, not something to retry through burn-excess. While the
  expectation is open, only the matching transfer is held; later redemptions on
  that vault are still processed, but the vault's poll checkpoint stays before
  the held transfer (the bot logs a
  `held at expected burn-excess
  funding transfers` WARN when a hold starts or
  changes and every five minutes while it lasts), so finish the run promptly. If
  a later redemption's shares reach the issuer wallet before `external` runs,
  `external` refuses until that redemption's burn lands: with
  `IssuerShareBalanceNotExact` before the burn is signed, then with
  `UnresolvedSignerIntent` (409) while it is in flight. Retry once it lands.
  `expect-funding` refuses (409) while another burn-excess recovery is
  unresolved; finish or close that one first. If the shares will not be sent,
  `expect-funding --close --execute` releases the hold; a transfer that did
  arrive is then redeemed as usual.

Run it without `--execute` first: the dry run proves the deposit and prints the
plan without signing or writing an exclusion. A 504 leaves an `--execute` run's
outcome unknown; re-running the same command reads the persisted burn and
resumes it.

The offline CLI takes the same flags
(`issuer burn-excess internal|external
...`) and needs SSH to the bot host.
Offline, nothing holds the funding transfer, so `external` there needs the
service stopped from before the funding transfer until the run finishes.

## Step 2: Diagnose the failure

### Common failure patterns

| Detail message contains                      | Cause                                | Action                                                                              |
| -------------------------------------------- | ------------------------------------ | ----------------------------------------------------------------------------------- |
| `error sending request for url`              | Transient network/API                | Reprocess/recover — will likely work on retry                                       |
| `Transaction reverted on-chain: 0x...`       | On-chain tx reverted                 | Check Basescan for the tx hash in `detail`, then reprocess/recover                  |
| `Event not found in transaction: 0x...`      | Tx succeeded but emitted no event    | Inspect it; recover `Failed`, or force-complete a persisted intended/submitted burn |
| `insufficient funds for gas * price + value` | Bot wallet out of gas                | Fund the bot wallet with native gas (ETH on Base), then reprocess/recover           |
| `Tokenization request not found`             | Request genuinely absent from Alpaca | 404 returned by per-request GET endpoint — see "Alpaca request not found" below     |
| `aggregate conflict` (409 response)          | Already recovered                    | No action needed — it already completed                                             |

When many transactions share `insufficient funds for gas * price + value` as the
cause, the bot wallet has run dry — fund it once, then recover them all.

### Checking on-chain (Basescan)

The bot wallet address is in the startup logs:

```bash
docker logs $(docker ps -q) 2>&1 | grep "Bot wallet address"
```

Look up the bot wallet on [Basescan](https://basescan.org) to see recent
transactions. This tells you whether a transaction actually made it on-chain and
whether it succeeded or reverted.

## Step 3: Recover

### Recovering mints

**Endpoint:** `POST /admin/reprocess/mint/<aggregate_id>`

```bash
curl -s -X POST -H "X-API-KEY: $ISSUER_API_KEY" \
  http://localhost:$ISSUER_API_PORT/admin/reprocess/mint/<aggregate_id> | python3 -m json.tool
```

This retries recovery inline (no restart needed). Reprocess re-drives the
**persisted** mint identity only: it rebroadcasts or reconfirms the exact signed
hash already on the aggregate. It does **not** create a second deposit for an
unresolved intent. A replacement prepare is signed only after classification
proves the prior hash is terminal (`MinedReverted` or `ProvablyDead`), with a
wallet-guard recheck and only when inventory has no receipt for that
`issuer_request_id` (same rules as Step 1 above). After a successful rebroadcast
or reconfirm path, a background task drives confirmation. A single reprocess is
usually enough; if recovery abandons again (e.g. still pending after the
budget), reprocess once more. A 409 Conflict response means it already completed
— no action needed.

### Recovering redemptions

**Endpoint:** `POST /admin/recover/redemption/<issuer_request_id>`

```bash
curl -s -X POST -H "X-API-KEY: $ISSUER_API_KEY" \
  http://localhost:$ISSUER_API_PORT/admin/recover/redemption/<issuer_request_id> | python3 -m json.tool
```

For failures before Alpaca was called, the endpoint returns the redemption to
`Detected` so `RedeemCallManager` can call Alpaca. It does not re-verify a
journal or submit a burn.

For post-Alpaca, pre-burn failures, this **executes the burn inline** — no
restart needed. The endpoint first re-verifies the journal status with Alpaca
(to avoid burning without backing), then resumes the redemption to `Burning` and
submits the burn, waiting for on-chain confirmation before responding.

For an exhausted `BurnIntended`, `BurnSubmitted`, or retained burn in `Failed`,
the endpoint reloads the aggregate while holding the network wallet lock and
verifies the exact persisted signed transaction. It signs a fresh-nonce
replacement only when that transaction is `ProvablyDead`, or when a transaction
retained in `Failed` has a failed receipt whose exact block is canonical at or
below the finalized head. A merely block-numbered, unfinalized revert never
authorizes another signature. A mined authorized replacement in `Failed` is
recorded as the existing completed burn only after confirmation succeeds; a
transient confirmation error is reported as deferred recovery. Pending, unknown,
invalid, and RPC-failure classifications fail closed without signing. When the
endpoint refuses a pending (`StillMineable`) transaction, it does not broadcast
it either: send the transaction in `old_tx_hash` out of band (see "Broadcasting
a retained burn out of band" below). For vault-direct burns, recovery first
tries to re-reserve the retained receipt plan. If those released receipts are no
longer available, it selects current inventory for the persisted Alpaca and dust
quantities and reserves that fresh plan before signing. It does not call Alpaca
again. Orchestrator burns keep exact-calldata replacement semantics. After the
reservation and signing checks pass, the authorization and replacement intent
commit atomically. Reservations are validated again before queue dispatch. A
successful JSON response includes `manual_replacement` with stable `code`,
`recovery_id`, old and new transaction hashes and nonces, and `queue_dispatch`.
A deferred dispatch is safe: the reconciler reconstructs the job from the
committed replacement intent after restart without signing another transaction.

Successful response messages:

| Response message                                                           | Meaning                                                                                                     |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `Recovered from Failed and executed burn immediately`                      | Success — burn submitted and confirmed on-chain.                                                            |
| `Recovered to Detected — RedeemCallManager will re-call Alpaca`            | Failed before Alpaca was called; it will re-call Alpaca automatically.                                      |
| `Recovered to Burning but burn skipped: on-chain balance insufficient ...` | The bot doesn't hold enough vault shares — manual intervention (see "Insufficient on-chain share balance"). |

Error and manual-replacement responses use the exact code and action below:

| Code                                               | Status | Meaning and action                                                                                                                                                                                                                                                               |
| -------------------------------------------------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `burn_replacement_queued`                          | 200    | The replacement is authorized and its submit job is queued.                                                                                                                                                                                                                      |
| `burn_replacement_reenqueued`                      | 200    | An authorized replacement was re-enqueued. Do not request another signature.                                                                                                                                                                                                     |
| `burn_replacement_confirmation_queued`             | 200    | The replacement landed and its confirmation job is queued.                                                                                                                                                                                                                       |
| `burn_replacement_existing_burn_recovered`         | 200    | The authorized replacement landed while the redemption was `Failed`; recovery recorded it as completed and no queue dispatch was required.                                                                                                                                       |
| `burn_replacement_existing_burn_recovery_deferred` | 200    | The authorized replacement is mined, but confirmation did not reach a terminal result. Retry recovery; completion was not reported or recorded.                                                                                                                                  |
| `burn_replacement_dispatch_deferred`               | 200    | The queue write failed after authorization. Leave the service running or restart it so the reconciler repairs dispatch.                                                                                                                                                          |
| `burn_replacement_committed_inspection_required`   | 200    | Inspect events using `recovery_id` and the old transaction identity, then repair dispatch without another signature.                                                                                                                                                             |
| `redemption_not_found`                             | 404    | No aggregate history exists. Verify the ID against `/admin/stuck`.                                                                                                                                                                                                               |
| `alpaca_request_not_found`                         | 404    | The request is absent from Alpaca. Follow “Alpaca request not found” below.                                                                                                                                                                                                      |
| `burn_replacement_already_queued`                  | 409    | An authorized replacement has an active submit job. Wait, then inspect `/admin/stuck`.                                                                                                                                                                                           |
| `redemption_terminal`                              | 409    | The redemption is complete or closed. Do not retry.                                                                                                                                                                                                                              |
| `recovery_refused`                                 | 422    | The Alpaca journal is pending or rejected, or the redemption network is not a published Alpaca `TokenizationNetwork`. Re-check the journal and network mapping.                                                                                                                  |
| `prior_burn_unverifiable`                          | 422    | Says broadcast again or not finalized: wait, then retry. Says rebroadcast: send the transaction in `old_tx_hash` out of band (see "Broadcasting a retained burn out of band"), then retry. Says ambiguous (also for a legacy ID that cannot be verified): reconcile it manually. |
| `redemption_command_rejected`                      | 422    | The aggregate rejected `ResumeBurn`. Inspect its event history and state before retrying.                                                                                                                                                                                        |
| `burn_recovery_not_exhausted`                      | 422    | Automatic attempts remain. Wait for the five-attempt budget to finish, then retry. No replacement was signed.                                                                                                                                                                    |
| `burn_not_provably_dead`                           | 422    | Neither safe replacement basis was established: the transaction is not provably dead and is not a finalized revert in `Failed`. No replacement was signed. A pending transaction stays pending: send it out of band (see "Broadcasting a retained burn out of band").            |
| `invalid_burn_identity`                            | 422    | The transaction is malformed, belongs to another wallet, or has a chain-ID mismatch. Check the network configuration before retrying. No replacement was signed.                                                                                                                 |
| `competing_signer_intent`                          | 422    | Another prepared transaction owns the wallet's next nonce. Let it submit or reconcile, then retry.                                                                                                                                                                               |
| `insufficient_receipt_inventory`                   | 422    | Current vault receipt inventory cannot cover the persisted Alpaca quantity after the retained plan became unavailable. Reconcile inventory or wait for capacity, then retry. No replacement was signed.                                                                          |
| `invalid_recovery_state`                           | 422    | The aggregate cannot enter manual replacement. Inspect its events before retrying.                                                                                                                                                                                               |
| `network_not_configured`                           | 422    | No vault service is active for the redemption's network. Restore its configuration before retrying.                                                                                                                                                                              |
| `burn_classification_unavailable`                  | 502    | Classification failed or returned an unusable receipt. No replacement was signed.                                                                                                                                                                                                |
| `burn_replacement_preparation_unavailable`         | 502    | Preparation failed before authorization. No replacement was signed.                                                                                                                                                                                                              |
| `upstream_unavailable`                             | 502    | Alpaca was unavailable or returned inconsistent data. Retry after it recovers.                                                                                                                                                                                                   |
| `internal_error`                                   | 500    | Recovery failed before a decision. Collect request logs and escalate.                                                                                                                                                                                                            |
| `burn_replacement_internal_error`                  | 500    | Replacement recovery failed before a decision. Collect request logs and investigate before retrying.                                                                                                                                                                             |

Never force-complete a burn that did not land.

Then check `/admin/stuck` again to verify the expected progress. Queued or
deferred replacements can remain until dispatch and confirmation complete, and
insufficient-balance recovery remains until funding or manual intervention.
Confirm the entry clears after its required follow-up completes.

### Broadcasting a retained burn out of band

Do this when a `422` tells you to send the transaction in `old_tx_hash`
yourself. Do it also for a dropped burn that stays `BurnSubmitted`: once its
automatic budget is exhausted, `/admin/recover` answers `burn_not_provably_dead`
with its `old_tx_hash`. The node does not hold the transaction, so you cannot
get it by its hash. Its signed bytes are only in the redemption's `BurnIntended`
event, stored as a JSON array of bytes. The same signed bytes cannot make a
second burn.

On the issuer host, as root:

```bash
nix shell nixpkgs#sqlite.bin nixpkgs#foundry   # run the lines below in it
set -a; . /run/agenix/st0x-issuance.env; set +a   # RPC URLs; never echo them
DATABASE_URL="$(systemctl show st0x-issuance -p Environment --value \
  | tr ' ' '\n' | sed -n 's/^DATABASE_URL=//p')"
DB_PATH="${DATABASE_URL#sqlite://}"
DB_PATH="${DB_PATH#sqlite:}"
DB_PATH="${DB_PATH%%\?*}"
test -f "$DB_PATH"
REDEMPTION=<issuer_request_id>
OLD_TX_HASH=<old_tx_hash from the 422>
# The redemption's network: CHAIN_<NETWORK>_RPC_URL. For Base, the service
# falls back to RPC_URL when CHAIN_BASE_RPC_URL is not set.
RPC="${CHAIN_BASE_RPC_URL:-$RPC_URL}"
test -n "$RPC"
RAW_TX=$(sqlite3 -readonly "$DB_PATH" "
  SELECT '0x' || group_concat(byte, '')
  FROM (
    SELECT printf('%02x', j.value) AS byte
    FROM events AS e,
         json_each(
           json_extract(e.payload, '\$.BurnIntended.sendable_tx.tx')
         ) AS j
    WHERE e.aggregate_type = 'Redemption'
      AND e.aggregate_id = '$REDEMPTION'
      AND e.event_type = 'RedemptionEvent::BurnIntended'
      AND lower(json_extract(e.payload, '\$.BurnIntended.sendable_tx.hash'))
          = lower('$OLD_TX_HASH')
    ORDER BY j.key
  );")
# The bytes must hash to old_tx_hash. If they do not, send nothing.
EXPECTED=$(printf '%s' "$OLD_TX_HASH" | tr 'A-F' 'a-f')
if [ "$(cast keccak "$RAW_TX")" = "$EXPECTED" ]; then
  cast publish --async --rpc-url "$RPC" "$RAW_TX"
else
  echo "FAIL: no BurnIntended bytes match old_tx_hash" >&2
fi
```

Read the node's answer:

- A transaction hash, or "already known": the node holds the burn. Wait for it
  to mine, then call `/admin/recover` again.
- "nonce too low": the nonce is already used, by this burn or by another
  transaction. Wait until that block is finalized, then call `/admin/recover`
  again.
- "underpriced", or the burn drops again: an unchanged broadcast cannot raise
  the fee. First check whether the burn was ever broadcast:

  ```bash
  sqlite3 -readonly "$DB_PATH" "
    SELECT COUNT(*)
    FROM events
    WHERE aggregate_type = 'Redemption'
      AND aggregate_id = '$REDEMPTION'
      AND event_type IN (
        'RedemptionEvent::BurnTxSubmitted',
        'RedemptionEvent::OrchestratorBurnSubmitted'
      )
      AND instr(lower(payload), lower('$OLD_TX_HASH')) > 0;"
  ```

  - More than 0: the submission event (`BurnTxSubmitted`, or
    `OrchestratorBurnSubmitted` in orchestrator mode) released the redemption's
    signer intent. Restart the service. The next signer then takes the dropped
    nonce, and the burn classifies as `ProvablyDead` once that nonce is
    finalized.
  - 0: the burn was never broadcast, and the redemption keeps its signer intent
    in the database. A restart does not free the nonce, because no other
    transaction signs at it. Escalate to engineering: a higher-fee transaction
    at that nonce from the bot wallet ends the stall. Call `/admin/recover`
    again after that transaction is finalized.

### Closing an unresolved redemption

Use close when a transaction cannot or should not be retried and no burn can be
verified on-chain. Closing is an acknowledgement of unresolved off-chain state,
not proof that a burn succeeded.

**Close a redemption:**

```bash
curl -s -X POST -H "X-API-KEY: $ISSUER_API_KEY" -H "Content-Type: application/json" \
  -d '{"reason": "Reconciled off-chain; do not retry this redemption"}' \
  http://localhost:$ISSUER_API_PORT/admin/close/redemption/<issuer_request_id> | python3 -m json.tool
```

**Close a mint:**

```bash
curl -s -X POST -H "X-API-KEY: $ISSUER_API_KEY" -H "Content-Type: application/json" \
  -d '{"reason": "Deposit succeeded on-chain but callback failed"}' \
  http://localhost:$ISSUER_API_PORT/admin/close/mint/<aggregate_id> | python3 -m json.tool
```

Closing just marks the transaction as done in our system. **It does not perform
or prove any on-chain action.** If the mint still holds a prepared deposit
identity (`MintTxIntended` / prepared bytes on `TxSubmitted`, including via
`MintingFailed`), the JSON body must also include
`"acknowledged_unresolved_mint_tx_hash": "0x..."` with that exact hash (422 on
miss/mismatch). If the redemption has a persisted signed burn, the JSON body
must also include `"acknowledged_unresolved_burn_tx_hash": "0x..."` with that
exact hash. The reservation remains held because the acknowledged transaction
may still land. The reason and acknowledgement are recorded in the event store
for audit.

### Force-completing a verified burn

Use force-complete when a `BurnIntended` or `BurnSubmitted` redemption's exact
persisted burn landed but the bot did not record completion:

```bash
curl -s -X POST -H "X-API-KEY: $ISSUER_API_KEY" -H "Content-Type: application/json" \
  -d '{"burn_tx_hash": "0x...", "reason": "Verified expected burn on-chain"}' \
  http://localhost:$ISSUER_API_PORT/admin/force-complete/redemption/<issuer_request_id> | python3 -m json.tool
```

The endpoint verifies a successful receipt and the expected vault burn before
recording `Completed`. The proving hash normally must equal the persisted burn
hash. A same-nonce replacement can be used only when the request also echoes the
persisted hash as `acknowledged_unresolved_burn_tx_hash` and the replacement
matches the persisted recipient, withdrawals, and dust transfer.

`POST /admin/recover/redemption` treats a previously recorded burn transaction
as follows:

| On-chain result                               | Recovery behavior                                                                            |
| --------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Completed                                     | Records the existing burn and completes the redemption.                                      |
| Finalized reverted                            | May prepare the next deterministic retry after the old transaction is irreversibly terminal. |
| Reverted but unfinalized                      | Returns `422`; no replacement is prepared because a reorganization can remove the receipt.   |
| Pending                                       | Returns `422` and leaves the state and reservation unchanged.                                |
| Unknown/RPC failure                           | Returns `422` and fails closed; no replacement is signed.                                    |
| Legacy transaction ID that cannot be verified | Returns `422`; reconcile manually, then close if no burn can be proven.                      |

## Before closing: verify on-chain state

**Closing is irreversible.** Before closing, always confirm the on-chain state
matches what you expect.

Check the bot wallet on [Basescan](https://basescan.org):

- Look at recent transactions from the bot wallet on the relevant vault contract
- `Transfer` events from bot to `0x0000...0000` are burns
- `Transfer` events from `0x0000...0000` to bot are mints (deposits)
- Compare the amounts and timestamps against the stuck transaction

**Only close if:**

- The situation has been reconciled outside the bot, and
- Any persisted signed burn hash has been explicitly acknowledged

If a `Failed` redemption has a recorded burn transaction, recover it so the
endpoint can inspect and record the receipt. If an intended/submitted persisted
burn succeeded, force-complete it instead of closing it. Legacy and pre-intent
states cannot be force-completed; close only after off-chain reconciliation.

Always include a descriptive reason when closing. Acknowledge the persisted
on-chain transaction hash only when one exists; never provide an unrelated or
fabricated hash.

## Insufficient on-chain share balance

`on-chain balance insufficient` means the bot wallet's ERC-20 vault share
balance is below the amount the redemption must burn. Refreshing the receipt
inventory cannot restore those shares. Check whether an earlier burn landed or
the shares moved elsewhere, then follow the state-specific recovery,
force-complete, or reconciliation guidance above. Escalate if the missing shares
cannot be accounted for.

## Insufficient receipt balance (ERC1155InsufficientBalance)

A revert containing `execution reverted: 0x03dee4c5` means the bot tried to
redeem more ERC-1155 receipt tokens than it holds for a specific receipt ID. The
receipt inventory therefore overstates that on-chain balance, for example
because it missed a prior burn or outbound receipt change, or did not settle a
reservation completely.

**Recovery:** Restart the container to refresh the receipt inventory (startup
reconciliation re-scans on-chain balances), then recover again. If it keeps
failing, the bot may genuinely not have enough receipts — escalate to
engineering.

**Alternative (less disruptive):** Wait until the next periodic
receipt-reconciliation pass refreshes balances, then recover again.

## Alpaca request rejected

If `/admin/recover/redemption` returns `422` with
`Cannot recover: Alpaca journal was rejected`, Alpaca refused to journal the
underlying shares, so the bot never received backing. Burning would destroy
on-chain tokens with no shares behind them, so the endpoint refuses.

This is a **business resolution**, not a retry: the AP's tokens are in the
redemption wallet but the redemption did not happen on Alpaca's side. Escalate
to coordinate returning the tokens to the AP or re-initiating the redemption.

## Alpaca request not found

Journal polling now uses the keyed per-request GET endpoint
(`/v1/accounts/{acct}/tokenization/requests/{id}`), which retrieves aged
requests that no longer appear in the list endpoint (verified 2026-06-12:
completed requests from June 11 were absent from the list at `?limit=500` but
returned 200 from the keyed endpoint). This means the journal loop and
`/admin/recover` can automatically handle requests that have aged out of the
list endpoint.

A genuine `Tokenization request not found` (404 from the keyed endpoint) means
the request truly does not exist in Alpaca — not merely that it is old.

If `/admin/recover/redemption` returns `404` and the logs show
`Tokenization request not found`, Alpaca has no record of this request at all.
Recovery re-verifies the journal before burning, so it cannot proceed.

These redemptions still owe a burn. They **cannot be cleared through the admin
endpoints** — escalate to engineering for a manual burn or a recovery path that
trusts the recorded `AlpacaJournalCompleted` event, gated on an on-chain balance
check.

## Quick reference

| Action              | Endpoint                                | Method           | Needs restart? |
| ------------------- | --------------------------------------- | ---------------- | -------------- |
| List stuck          | `/admin/stuck`                          | GET              | No             |
| Retry mint          | `/admin/reprocess/mint/<id>`            | POST             | No             |
| Recover redemption  | `/admin/recover/redemption/<id>`        | POST             | No             |
| Close redemption    | `/admin/close/redemption/<id>`          | POST (JSON body) | No             |
| Force-complete burn | `/admin/force-complete/redemption/<id>` | POST (JSON body) | No             |
| Close mint          | `/admin/close/mint/<id>`                | POST (JSON body) | No             |

## Checking container logs

```bash
# Recent logs for a specific aggregate
docker logs $(docker ps -q) 2>&1 | grep "<aggregate_id>" | tail -20

# All warnings and errors
docker logs $(docker ps -q) 2>&1 | grep -E "WARN|ERROR" | tail -50

# Full context around a failure
docker logs $(docker ps -q) 2>&1 | grep -B5 -A10 "<aggregate_id>" | tail -60
```
