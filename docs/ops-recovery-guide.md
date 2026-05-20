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

| Detail message contains             | Cause                 | Action                                                       |
| ----------------------------------- | --------------------- | ------------------------------------------------------------ |
| `error sending request for url`     | Transient network/API | Reprocess/recover — will likely work on retry                |
| `reached terminal status: Failed`   | Fireblocks tx failed  | Check Fireblocks console (see below), then reprocess/recover |
| `is not whitelisted in Fireblocks`  | Missing contract      | Whitelist the contract in Fireblocks, then reprocess         |
| `aggregate conflict` (409 response) | Already recovered     | No action needed — it already completed                      |

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

This transitions the redemption back to `Burning` state. **The actual burn only
executes on the next service restart** — it does not retry inline.

After recovering, restart the container:

```bash
docker restart $(docker ps -q)
```

Then wait ~30 seconds and check `/admin/stuck` again to see if it cleared.

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

If a burn's Fireblocks error contains `execution reverted: 0x03dee4c5`, it means
the bot tried to withdraw more receipt tokens than it holds for a specific
receipt ID. This can happen when deposit events were missed (e.g., due to
dropped WebSocket subscriptions), so the bot doesn't know about receipts it
actually owns.

**Recovery:** Usually a recover + restart resolves this, since the receipt
inventory gets refreshed on startup. If it keeps failing, the bot may genuinely
not have enough receipts — escalate to engineering.

## Quick reference

| Action              | Endpoint                         | Method           | Needs restart? |
| ------------------- | -------------------------------- | ---------------- | -------------- |
| List stuck          | `/admin/stuck`                   | GET              | No             |
| Retry mint          | `/admin/reprocess/mint/<id>`     | POST             | No             |
| Recover redemption  | `/admin/recover/redemption/<id>` | POST             | **Yes**        |
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
