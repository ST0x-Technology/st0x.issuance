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
| `timestamp`               | When it entered this state                  |

## How automatic recovery works

On every startup, the bot automatically attempts to recover stuck transactions
(mints in `MintingFailed` state, redemptions in `Burning` or `BurnFailed`
state). This runs with a **30-second timeout** before the HTTP server starts
accepting requests.

- If recovery completes within 30 seconds, everything is handled automatically.
- If recovery times out (e.g., Fireblocks is slow), the remaining stuck
  transactions are left for manual intervention via the admin endpoints below.
- If a burn recovery finds that the **on-chain balance is insufficient**, it
  skips the burn and logs `MANUAL INTERVENTION REQUIRED`. This usually means the
  burn already succeeded on-chain but wasn't recorded — verify on Basescan and
  use `/admin/close` if confirmed.

## Step 2: Diagnose the failure

### Common failure patterns

| Detail message contains                  | Cause                                | Action                                                                          |
| ---------------------------------------- | ------------------------------------ | ------------------------------------------------------------------------------- |
| `error sending request for url`          | Transient network/API                | Reprocess/recover — will likely work on retry                                   |
| `reached terminal status: Failed`        | Fireblocks tx failed                 | Check Fireblocks console (see below), then reprocess/recover                    |
| `sub_status: INSUFFICIENT_FUNDS_FOR_FEE` | Bot wallet out of gas                | Fund the bot wallet with native gas (ETH on Base), then reprocess/recover       |
| `is not whitelisted in Fireblocks`       | Missing contract                     | Whitelist the contract in Fireblocks, then reprocess                            |
| `Tokenization request not found`         | Request genuinely absent from Alpaca | 404 returned by per-request GET endpoint — see "Alpaca request not found" below |
| `aggregate conflict` (409 response)      | Already recovered                    | No action needed — it already completed                                         |

The `INSUFFICIENT_FUNDS_FOR_FEE` sub-status appears in the `fireblocks_status`
object of the `/admin/stuck` entry (the top-level `detail` just says
`reached terminal status: Failed`). When many transactions share this cause, the
bot wallet has run dry — fund it once, then recover them all.

### Checking Fireblocks

When the detail says a Fireblocks transaction failed, look up the Fireblocks TX
ID (the UUID in the `detail` field) in the Fireblocks console. The console shows
the transaction status, sub-status, and any error from the smart contract.

If there's an `Error from contract` starting with `execution reverted:`, you can
decode the error using Foundry:

```bash
# Get the error signature
cast 4byte <first-4-bytes-of-error>

# Example: 0x03dee4c5 = ERC1155InsufficientBalance
```

### Checking on-chain (Basescan)

The bot wallet address is in the startup logs:

```bash
docker logs $(docker ps -q) 2>&1 | grep "Bot wallet address"
```

Look up the bot wallet on [Basescan](https://basescan.org) to see recent
transactions. This tells you whether a transaction actually made it on-chain and
whether it succeeded or reverted — regardless of what Fireblocks reports.

## Step 3: Recover

### Recovering mints

**Endpoint:** `POST /admin/reprocess/mint/<aggregate_id>`

```bash
curl -s -X POST -H "X-API-KEY: $ISSUER_API_KEY" \
  http://localhost:8000/admin/reprocess/mint/<aggregate_id> | python3 -m json.tool
```

This retries recovery inline (no restart needed). If the previous Fireblocks
mint transaction terminally failed, manual reprocess submits the next
deterministic retry transaction even after automatic retries are exhausted, and
hands the mint to a background task that drives that submitted transaction
through confirmation. A single reprocess is usually enough. The exception is a
retry submitted past the automatic cap (e.g. `retry-5`): if that transaction
also fails, the background task is exhausted and gives up, so you must reprocess
again. A 409 Conflict response means it already completed — no action needed.

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

| Response message                                                           | Meaning                                                                                             |
| -------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `Recovered from Failed and executed burn immediately`                      | Success — burn submitted and confirmed on-chain.                                                    |
| `Recovered to Detected — RedeemCallManager will re-call Alpaca`            | Failed before Alpaca was called; it will re-call Alpaca automatically.                              |
| `Recovered to Burning but burn skipped: on-chain balance insufficient ...` | The bot doesn't hold the tokens to burn — manual intervention (see "Insufficient receipt balance"). |
| `409 Conflict`                                                             | Already recovered/completed — no action needed.                                                     |
| `422 Unprocessable`                                                        | Alpaca journal is still `Pending`, or was `Rejected` — cannot burn. See "Alpaca request rejected".  |
| `404 Not Found`                                                            | The tokenization request is genuinely absent from Alpaca — see "Alpaca request not found" below.    |
| `502 Bad Gateway`                                                          | The Alpaca journal re-verification call failed (e.g. rate-limited or network error). See below.     |

Then check `/admin/stuck` again to confirm it cleared.

> **Bulk recovery — pace your requests.** The recover endpoint makes a
> Fireblocks API call per request (status check on the prior burn tx). Firing
> many back-to-back trips Fireblocks' rate limit (`429 Too Many Requests`),
> which surfaces as `502 Bad Gateway` from this endpoint. When recovering a
> batch, space requests **~10s apart**. A `502` from rate-limiting is harmless —
> the redemption stays in its prior state, so just retry it (paced).

### Closing (manually marking as done)

Use close endpoints when a transaction cannot or should not be retried — for
example, when the on-chain action already succeeded but the bot didn't record
it.

**Close a redemption:**

```bash
curl -s -X POST -H "X-API-KEY: $ISSUER_API_KEY" -H "Content-Type: application/json" \
  -d '{"reason": "Burn succeeded on-chain (tx 0x...) but reported as failure"}' \
  http://localhost:8000/admin/close/redemption/<issuer_request_id> | python3 -m json.tool
```

**Close a mint:**

```bash
curl -s -X POST -H "X-API-KEY: $ISSUER_API_KEY" -H "Content-Type: application/json" \
  -d '{"reason": "Deposit succeeded on-chain but callback failed"}' \
  http://localhost:8000/admin/close/mint/<aggregate_id> | python3 -m json.tool
```

Closing just marks the transaction as done in our system. **It does not perform
any on-chain action.** The reason is recorded in the event store for audit.

## Before closing: verify on-chain state

**Closing is irreversible.** Before closing, always confirm the on-chain state
matches what you expect.

Check the bot wallet on [Basescan](https://basescan.org):

- Look at recent transactions from the bot wallet on the relevant vault contract
- `Transfer` events from bot to `0x0000...0000` are burns
- `Transfer` events from `0x0000...0000` to bot are mints (deposits)
- Compare the amounts and timestamps against the stuck transaction

**Only close if:**

- The on-chain action already succeeded (tokens minted/burned) but the bot
  didn't record it, OR
- The situation has been manually resolved outside the bot

Always include a descriptive reason with the on-chain tx hash when closing.

## Insufficient receipt balance (ERC1155InsufficientBalance)

A burn skipped with `on-chain balance insufficient` (or a Fireblocks error
containing `execution reverted: 0x03dee4c5`) means the bot tried to withdraw
more receipt tokens than it holds for a specific receipt ID. This happens when
deposit events were missed (e.g., a gap in the receipt-backfill poller), so the
bot doesn't know about receipts it actually owns.

**Recovery:** Restart the container to refresh the receipt inventory (startup
reconciliation re-scans on-chain balances), then recover again. If it keeps
failing, the bot may genuinely not have enough receipts — escalate to
engineering.

**Alternative (less disruptive):** Wait up to 60 seconds for the next periodic
receipt-backfill pass to discover the missing receipts, then recover again.

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

| Action              | Endpoint                         | Method           | Needs restart? |
| ------------------- | -------------------------------- | ---------------- | -------------- |
| List stuck          | `/admin/stuck`                   | GET              | No             |
| Retry mint          | `/admin/reprocess/mint/<id>`     | POST             | No             |
| Recover redemption  | `/admin/recover/redemption/<id>` | POST             | No             |
| Close redemption    | `/admin/close/redemption/<id>`   | POST (JSON body) | No             |
| Close mint          | `/admin/close/mint/<id>`         | POST (JSON body) | No             |
| Check Fireblocks tx | `/admin/fireblocks/tx/<id>`      | GET              | No             |

**Note:** The Fireblocks tx lookup endpoint (`/admin/fireblocks/tx/<id>`) may
not be deployed yet. Until it is, use the Fireblocks console directly.

## Checking container logs

```bash
# Recent logs for a specific aggregate
docker logs $(docker ps -q) 2>&1 | grep "<aggregate_id>" | tail -20

# All warnings and errors
docker logs $(docker ps -q) 2>&1 | grep -E "WARN|ERROR" | tail -50

# Full context around a failure
docker logs $(docker ps -q) 2>&1 | grep -B5 -A10 "<aggregate_id>" | tail -60
```
