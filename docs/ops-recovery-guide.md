# Ops Recovery Guide: Stuck Transactions

This guide covers how to diagnose and recover stuck mints and redemptions in the
issuance bot.

## Prerequisites

All admin endpoints require the `X-API-KEY` header. On the droplet:

```bash
export ISSUER_API_KEY=$(grep ISSUER_API_KEY /mnt/volume_nyc3_02/.env | cut -d= -f2)
```

Every example below assumes this is set.

## Step 1: Check what's stuck

```bash
curl -s -H "X-API-KEY: $ISSUER_API_KEY" http://localhost:8000/admin/stuck | python3 -m json.tool
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
request ID, transaction hash, nonce, and required operator action.

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
  http://localhost:8000/admin/reprocess/mint/<aggregate_id> | python3 -m json.tool
```

This retries recovery inline (no restart needed). If the previous on-chain mint
transaction failed, manual reprocess submits the next deterministic retry
transaction even after automatic retries are exhausted, and hands the mint to a
background task that drives that submitted transaction through confirmation. A
single reprocess is usually enough. The exception is a retry submitted past the
automatic cap (e.g. `retry-5`): if that transaction also fails, the background
task is exhausted and gives up, so you must reprocess again. A 409 Conflict
response means it already completed — no action needed.

### Recovering redemptions

**Endpoint:** `POST /admin/recover/redemption/<issuer_request_id>`

```bash
curl -s -X POST -H "X-API-KEY: $ISSUER_API_KEY" \
  http://localhost:8000/admin/recover/redemption/<issuer_request_id> | python3 -m json.tool
```

This **executes the burn inline** — no restart needed. The endpoint first
re-verifies the journal status with Alpaca (to avoid burning without backing),
then resumes the redemption to `Burning` and submits the burn, waiting for
on-chain confirmation before responding.

Possible responses:

| Response message                                                           | Meaning                                                                                                                      |
| -------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `Recovered from Failed and executed burn immediately`                      | Success — burn submitted and confirmed on-chain.                                                                             |
| `Recovered to Detected — RedeemCallManager will re-call Alpaca`            | Failed before Alpaca was called; it will re-call Alpaca automatically.                                                       |
| `Recovered to Burning but burn skipped: on-chain balance insufficient ...` | The bot doesn't hold enough vault shares — manual intervention (see "Insufficient on-chain share balance").                  |
| `409 Conflict`                                                             | Already recovered/completed — no action needed.                                                                              |
| `422 Unprocessable`                                                        | Recovery refused: Alpaca is pending/rejected, prior burn is ambiguous/legacy, or automatic recovery is exhausted. See below. |
| `404 Not Found`                                                            | The tokenization request is genuinely absent from Alpaca — see "Alpaca request not found" below.                             |
| `502 Bad Gateway`                                                          | The Alpaca journal re-verification call failed (e.g. rate-limited or network error). See below.                              |

Then check `/admin/stuck` again to confirm it cleared.

### Closing an unresolved redemption

Use close when a transaction cannot or should not be retried and no burn can be
verified on-chain. Closing is an acknowledgement of unresolved off-chain state,
not proof that a burn succeeded.

**Close a redemption:**

```bash
curl -s -X POST -H "X-API-KEY: $ISSUER_API_KEY" -H "Content-Type: application/json" \
  -d '{"reason": "Reconciled off-chain; do not retry this redemption"}' \
  http://localhost:8000/admin/close/redemption/<issuer_request_id> | python3 -m json.tool
```

**Close a mint:**

```bash
curl -s -X POST -H "X-API-KEY: $ISSUER_API_KEY" -H "Content-Type: application/json" \
  -d '{"reason": "Deposit succeeded on-chain but callback failed"}' \
  http://localhost:8000/admin/close/mint/<aggregate_id> | python3 -m json.tool
```

Closing just marks the transaction as done in our system. **It does not perform
or prove any on-chain action.** If the redemption has a persisted signed burn,
the JSON body must also include
`"acknowledged_unresolved_burn_tx_hash": "0x..."` with that exact hash. The
reservation remains held because the acknowledged transaction may still land.
The reason and acknowledgement are recorded in the event store for audit.

### Force-completing a verified burn

Use force-complete when a `BurnIntended` or `BurnSubmitted` redemption's exact
persisted burn landed but the bot did not record completion:

```bash
curl -s -X POST -H "X-API-KEY: $ISSUER_API_KEY" -H "Content-Type: application/json" \
  -d '{"burn_tx_hash": "0x...", "reason": "Verified expected burn on-chain"}' \
  http://localhost:8000/admin/force-complete/redemption/<issuer_request_id> | python3 -m json.tool
```

The endpoint verifies a successful receipt and the expected vault burn before
recording `Completed`. The proving hash normally must equal the persisted burn
hash. A same-nonce replacement can be used only when the request also echoes the
persisted hash as `acknowledged_unresolved_burn_tx_hash` and the replacement
matches the persisted recipient, withdrawals, and dust transfer.

`POST /admin/recover/redemption` treats a previously recorded burn transaction
as follows:

| On-chain result                               | Recovery behavior                                                               |
| --------------------------------------------- | ------------------------------------------------------------------------------- |
| Completed                                     | Records the existing burn and completes the redemption.                         |
| Reverted                                      | May prepare the next deterministic retry after the old transaction is terminal. |
| Pending                                       | Returns `422` and leaves the state and reservation unchanged.                   |
| Unknown/RPC failure                           | Returns `422` and fails closed; no replacement is signed.                       |
| Legacy transaction ID that cannot be verified | Returns `422`; reconcile manually, then close if no burn can be proven.         |

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
