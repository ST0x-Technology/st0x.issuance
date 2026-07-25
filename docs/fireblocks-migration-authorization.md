# Fireblocks authorization for the receipt custody migration

What the Fireblocks-held wallet must be authorized to do before the Turnkey
cutover can run, who has to grant it, and which approvals are involved.

Audience: whoever administers the Fireblocks workspace. Every claim below is
cited to Fireblocks documentation. Items we could not verify from the public
docs are called out as questions rather than guessed at, because getting them
wrong wastes an approval cycle.

## The one operation that needs authorizing

The Fireblocks wallet must send a single transaction per vault:

```
safeBatchTransferFrom(fireblocksWallet, turnkeyWallet, ids[], amounts[], "")
```

called on the vault's **Receipt** contract (an ERC-1155). It moves no native
value. It is not a transfer of a Fireblocks-held asset, so it does not fit the
ordinary "send funds to a whitelisted address" flow. In Fireblocks terms it is a
**contract call**.

Note the target: the **Receipt contract, not the vault contract**. They are
different addresses. The Receipt address is obtained per vault by calling the
vault's `receipt()` view function. The bot has only ever called the vault
contract, which is why the Receipt contract is not whitelisted today and why
this work is needed at all.

## Two routes, and why the choice is not free

### Route A: contract call to a whitelisted contract

This is what the issuer bot did for mints and burns before the Turnkey work: it
submitted `CONTRACT_CALL` operations whose destination was a whitelisted
contract, resolved by querying `GET /v1/contracts`. The relevant Fireblocks
concept is a **contract wallet** — documented as "a deposit address of an
on-chain smart contract", one of the three whitelist types alongside internal
and external wallets
([Whitelist addresses](https://developers.fireblocks.com/docs/whitelist-addresses)).

Requires: whitelisting work plus a policy rule, both detailed below.

### Route B: raw signing

Fireblocks `RAW` is "an off-chain message used to sign any message with your
private key"
([Configure Policies](https://developers.fireblocks.com/reference/configure-transaction-authorization-policy)).
The Fireblocks integration was originally built this way before being refactored
to contract calls. If the workspace policy still permits `RAW` for this vault
account, the transfer can be signed without whitelisting the Receipt contract
and without a new contract-call rule.

**This does not make the migration free.** Route B removes the Fireblocks
whitelisting and policy work; it does not remove the engineering work, because
the raw-signing code was deleted along with the rest of the Fireblocks
integration. Either route needs code re-added on our side. Route B is worth
asking about only because it removes an approval cycle from the critical path,
not because it removes effort.

Route B also leaves the entire transaction pipeline client-side: `RAW` only
returns a signature over a hash, so our code would have to build the exact
`safeBatchTransferFrom` transaction, have Fireblocks sign its hash, assemble the
signed transaction, and broadcast it through an RPC. The Receipt contract has no
meta-transaction path, so the transaction must genuinely originate from the
Fireblocks wallet — nothing about submission can be delegated.

Also relevant if a raw rule is being written: `RAW` rule types are an exception
that "should not include a destination limitation", and the rule's
`rawMessageSigning` object takes a mandatory `derivationPath` and an optional
`algorithm`.

## Route A, step by step

### Step 1: whitelist the Receipt contract as a contract wallet

| Operation                       | Method and path                               |
| ------------------------------- | --------------------------------------------- |
| Add a contract                  | `POST /v1/contracts`                          |
| Add an asset to that contract   | `POST /v1/contracts/{contractId}/{assetId}`   |
| Remove an asset from a contract | `DELETE /v1/contracts/{contractId}/{assetId}` |
| List whitelisted contracts      | `GET /v1/contracts`                           |
| Find one contract               | `GET /v1/contracts/{contractId}`              |
| Delete a contract               | `DELETE /v1/contracts/{contractId}`           |

The asset identifier is a bare path segment on the contract, per the API
reference. Prefer the Fireblocks SDK method over hand-rolling the request — the
SDK is versioned against the API. Cleanup mirrors setup: delete the asset entry
before deleting the contract container.

Endpoint permissions, per the Fireblocks API reference: adding a contract and
adding an asset to a contract are both available to **Admin, Non-Signing Admin,
Signer, Approver, or Editor**. Listing additionally allows Viewer. The same
objects are manageable from the Console; the API paths are given because they
are unambiguous and scriptable.

Two calls are needed, not one: the contract object is created first, then the
asset entry carrying the chain's asset identifier and the on-chain address is
attached to it. The bot's lookup matched on **both** the asset identifier and
the address, so both must be correct or the resolution fails and the migration
refuses to run rather than falling back to an unwhitelisted destination.

The asset identifier for Base is `BASECHAIN_ETH` — that was the default in the
deleted configuration's chain-to-asset mapping. Confirm it against the workspace
rather than trusting this document.

### Step 2: add a Transaction Authorization Policy rule

The Transaction Authorization Policy, referred to throughout Fireblocks as the
TAP, is the rule set that decides whether a submitted transaction is allowed. A
rule of this shape permits a given vault account to make contract calls to
whitelisted contract destinations:

```json
{
  "type": "TRANSFER",
  "action": "ALLOW",
  "transactionType": "CONTRACT_CALL",
  "dstAddressType": "WHITELISTED",
  "src": { "ids": [["<VAULT_ACCOUNT_ID>", "VAULT", "*"]] },
  "dst": { "ids": [["<CONTRACT_WALLET_ID>", "UNMANAGED", "CONTRACT"]] },
  "asset": "BASECHAIN_ETH",
  "amount": 0,
  "amountScope": "SINGLE_TX",
  "amountCurrency": "USD",
  "periodSec": 0,
  "operators": { "usersGroups": ["<GROUP_ID>"] }
}
```

Notes for whoever writes it:

- `src` is the bot's vault account. The deleted configuration defaulted to
  account `"0"`; confirm the account actually in use.
- `dst` should name the specific whitelisted Receipt contract. The Fireblocks
  documentation example uses a wildcard; scoping it to the one contract is safer
  and costs nothing here, since this is a one-shot migration.
- `amount` is 0 because the call moves no native value. **This does not mean the
  wallet needs no funds.** `amount` is the transferred value, not the fee: the
  transaction is still an on-chain contract call, and the network fee is paid
  from the source vault account's native balance. The Fireblocks vault account
  must hold enough ETH on Base to cover gas for one `safeBatchTransferFrom` per
  vault, or the transfer fails with insufficient funds. Confirm the balance
  before the window, and note Fireblocks' Gas Station can fund vault accounts
  automatically if it is configured.
- `operators.usersGroups` takes Fireblocks **user group IDs**, not user IDs: add
  the bot's API user to an authorized user group and put that group's ID in the
  rule. An automated run then initiates as that API user, authorized through its
  group membership.
- **Rule order matters.** Fireblocks evaluates policy rules in the order they
  are defined, so this rule must sit above any broader rule that would block it.

Policies can be edited in the Console Policy Editor or through the API, which
offers both direct publication and a draft workflow (get the active draft,
update it, publish it by identifier) for organizations that want a review step
before deployment
([Set Policies](https://developers.fireblocks.com/docs/set-transaction-authorization-policy)).

### What the rule does and does not restrict

The rule matches on transaction type and destination contract. There is **no
function-selector field** in this rule shape, so whitelisting the Receipt
contract permits the vault account to call _any_ method on it, including
`setApprovalForAll` — not just the batch transfer we need.

That is a wider grant than the operation requires. Mitigations, in order of
preference: scope `dst` to the single Receipt contract rather than a wildcard;
ask Fireblocks whether method-level restriction is available on this workspace
tier, since the public rule schema does not appear to express it; and remove
both the rule and the whitelisting once the migration completes, since the grant
has no ongoing purpose after cutover.

## Approvals: the part that decides whether this can happen without you

Both required changes fall inside domains that Fireblocks gates behind approval.
Per
[Define Approval Quorums](https://developers.fireblocks.com/docs/define-approval-quorums),
Admin Quorum approval covers, among others:

- **"Whitelisting addresses"** and other external destination addresses —
  presumed to cover Step 1, but whether **contract** wallet creation falls under
  it is exactly open question 1 below; treat Step 1's approval path as
  unconfirmed until that is answered.
- **"Changes to Policies"** — Step 2

The Admin Quorum "lists all users with Admin privileges (users assigned to
either an Owner, Admin, or Non-Signing Admin role)", and the threshold "defines
the number of Admins required to approve new workspace connections and changes".
Any Admin can deny a request outright before the threshold is met.

Separately, **approval groups** can replace the Admin Quorum for specific
domains, drawn from "a designated user group", explicitly to "segregate and
delegate responsibilities". External accounts and security/policy are named
domains.

### The question that has to be answered before anything is scheduled

Whether this can be completed without the workspace Owner personally approving
depends entirely on how this workspace is configured. The documentation states
that "you can remove owner approval from certain actions", while cautioning that
"key changes in your workspace will still require owner involvement" — and does
not enumerate which are which.

So, before scheduling the migration, confirm with the workspace administrator:

1. What is the current Admin Quorum threshold, and who is in it?
2. Are approval groups configured for external accounts (whitelisting) and for
   policy changes? If so, who is in them?
3. Given those answers, can both changes be approved without the Owner acting?
4. If the Owner must act, that is a hard dependency and the migration cannot be
   scheduled as an unattended operation.

Question 3 is the one that determines whether the scheduled migration window
holds.

## What the bot's API user needs

Nothing new is created for the bot itself. It authenticates as an existing API
user with its own API key and secret. What matters is that this user is named in
the `operators` of the new policy rule, and that its credentials are still
provisioned and valid — the integration was removed from the codebase, so the
credentials have not been exercised recently. Verify before the run, not during.

## Before the run

- Confirm the Receipt contract address per vault by calling `receipt()` on the
  vault, and confirm it against the whitelisted entry.
- Confirm the vault account identifier the bot signs from.
- Confirm the chain asset identifier for Base.
- Confirm the API credentials still authenticate.
- Confirm the receipt inventory has recorded holdings for every vault being
  migrated — the migration refuses to run against an empty inventory, so the
  backfill must have discovered the receipts before the window opens.
- Confirm the vault's certification is not expired and no owner freeze blocks
  the pair; both revert the transfer on-chain and neither is a Fireblocks
  concern — the migration engine re-checks both immediately before submission.

## After the run

Remove the policy rule and the whitelisted contract (asset entry first, then the
contract container) — but only after the migration is **verified successful**:
custody confirmed at the incoming wallet and the replacement service operating
on it, for **every** vault. A vault that refused to migrate (in-flight work,
balance mismatch) stays on Fireblocks until a later pass, and removing the rule
before that pass strands it. Once the last vault has migrated, the grant has no
remaining purpose, and leaving a rule that permits arbitrary method calls on the
Receipt contract is a standing risk with no benefit.

## Open questions for Fireblocks support

These could not be settled from the public documentation and are worth asking
directly, since each one can cost an approval cycle:

1. Does creating a **contract** wallet require Admin Quorum approval, or does
   the approval requirement documented for "whitelisting addresses" apply only
   to internal and external wallets?
2. Is method-level (function selector) restriction available for `CONTRACT_CALL`
   rules on this workspace tier?
3. Does the workspace currently permit `RAW` operations for the bot's vault
   account, and if so, under which rule?
