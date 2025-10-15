# Issuance Bot Specification

## Overview

The issuance bot acts as the **Issuer** in Alpaca's Instant Tokenization Network (ITN). It implements the Issuer-side endpoints that Alpaca calls during mint/redeem operations, and coordinates with the Rain `OffchainAssetReceiptVault` contracts to execute the actual on-chain minting and burning of tokenized shares.

**This is general infrastructure** - any Authorized Participant (AP) can use it to mint and redeem tokenized equities. The issuance bot serves as the bridge between traditional equity holdings (at Alpaca) and on-chain (semi-fungible) tokenized representations (Rain SFT contracts).

## Background & Context

**Our Role:** We are the **Issuer** of tokenized equities. Alpaca acts as the settlement layer between Authorized Participants (APs) and us.

**Flow Summary:**
- **Minting:** AP requests mint → Alpaca calls our endpoint → We validate → Alpaca journals shares from AP to our custodian account → Alpaca confirms journal → We mint tokens on-chain → We call Alpaca's callback
- **Redeeming:** AP sends tokens to our redemption wallet → We detect redemption → We call Alpaca's redeem endpoint → Alpaca journals shares from our account to AP → We burn tokens on-chain

**Use Cases:**
- **Market Makers & Arbitrageurs:** Can mint/burn to rebalance inventory and maintain price parity across venues
- **Institutions:** Can convert equity holdings to tokenized form for on-chain settlement, DeFi integration, or cross-border transfer
- **Retail Platforms:** Can facilitate tokenized equity access for their users
- **Our Arbitrage Bot:** Can use this infrastructure to complete the arbitrage cycle by rebalancing on/off-chain holdings. See [st0x.liquidity](https://github.com/ST0x-Technology/st0x.liquidity) for more details on the bot.

## Architecture

### Off-Chain Infrastructure

**Our HTTP Server:**
- Implements Alpaca ITN Issuer endpoints
- Handles account linking, mint requests, and journal confirmations
- Built with Rust (Axum/Actix web framework)
- SQLite database for tracking operations
- Async runtime for coordination

**Alpaca ITN:**
- Alpaca's settlement layer
- Handles journal transfers between accounts automatically
- Provides endpoints for callbacks and status queries

### On-Chain Infrastructure

**Rain OffchainAssetReceiptVault Contract:**
- ERC-1155 receipts tracking individual deposit IDs
- ERC-20 shares representing vault ownership
- `deposit()` function for minting
- `withdraw()` function for burning

**Redemption Wallet:**
- On-chain address where APs send tokens to redeem
- We monitor this address for incoming transfers

## Data Types

Throughout this specification, we use newtypes to provide type safety and prevent mixing up different kinds of identifiers and values:

```rust
use rust_decimal::Decimal;
use chrono::{DateTime, Utc};

struct TokenizationRequestId(String);
struct IssuerRequestId(String);
struct ClientId(String);
struct AlpacaAccountNumber(String);
struct UnderlyingSymbol(String);
struct TokenSymbol(String);
struct Network(String);
struct Quantity(Decimal);
struct Email(String);
```

## Core Functionality

### 1. Account Linking (Handshake Process)

Before an AP can mint or redeem tokens, Alpaca needs to link the AP's Alpaca account with their account on our platform.

**Endpoint:** `POST /accounts/connect`

**Request Body:**
```json
{
  "email": "customer@firm.com",
  "account": "alpaca_account_number"
}
```

**Our Response:**
```json
{
  "client_id": "5505-1234-ABC-4G45"
}
```

**Status Codes:**
- `200`: Successful link
- `404`: Email not found on our platform
- `409`: Account already linked

**Data Structure:**
```rust
struct AccountLinkRequest {
    email: Email,
    account: AlpacaAccountNumber,
}

struct AccountLinkResponse {
    client_id: ClientId,
}
```

### 2. Tokenized Assets Data Endpoint

Alpaca needs to query which assets we support:

**Endpoint:** `GET /tokenized-assets`

**Our Response:**
```json
[
  {
    "underlying_symbol": "AAPL",
    "token_symbol": "AAPL0x",
    "network": "base"
  },
  {
    "underlying_symbol": "TSLA",
    "token_symbol": "TSLA0x",
    "network": "base"
  }
]
```

**Data Structure:**
```rust
struct TokenizedAsset {
    #[serde(rename = "underlying_symbol")]
    underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    token: TokenSymbol,
    network: Network,
}
```

### 3. Token Minting (Alpaca ITN Flow)

#### Complete Mint Flow

```
[AP wants to mint 10 AAPL tokens]
         │
         ├─> AP calls Alpaca API with mint request
         │
         ├─> Alpaca validates AP account & authorization
         │
         ├─> Alpaca calls our POST /inkind/issuance
         │     {tokenization_request_id, qty: "10", underlying_symbol: "AAPL", ...}
         │
         ├─> We validate request & respond
         │     {issuer_request_id, status: "created"}
         │     DB Status: pending_journal
         │
         ├─> Alpaca journals 10 AAPL shares
         │     From: AP's Alpaca account
         │     To: Our tokenization account
         │
         ├─> Alpaca calls our POST /inkind/issuance/confirm
         │     {tokenization_request_id, issuer_request_id, status: "completed"}
         │     DB Status: journal_completed
         │
         ├─> We mint tokens on-chain
         │     vault.deposit(10 * 1e18, ap_wallet, receipt_info)
         │     DB Status: minting
         │
         ├─> On-chain transaction confirms
         │     Shares minted: 10 * 1e18
         │     Receipt ID: 123
         │     DB Status: callback_pending
         │
         ├─> We call Alpaca's callback endpoint
         │     POST /v1/accounts/{id}/tokenization/callback/mint
         │     {tokenization_request_id, tx_hash, wallet_address, ...}
         │     DB Status: completed
         │
         └─> AP now has 10 AAPL0x tokens in their wallet ✓
```

#### Step 1: Receive Mint Request from Alpaca

**Endpoint:** `POST /inkind/issuance`

**Request Body:**
```json
{
  "tokenization_request_id": "12345-678-90AB",
  "qty": "1.23",
  "underlying_symbol": "TSLA",
  "token_symbol": "TSLAx",
  "network": "solana",
  "client_id": "98765432",
  "wallet_address": "<AP's wallet address to deposit the tokenized asset>"
}
```

**Our Validation:**
1. Verify `underlying_symbol` is supported
2. Verify `token_symbol` matches our convention
3. Verify `network` is supported (our EVM chain)
4. Verify `client_id` is a valid/linked AP
5. Verify `wallet_address` is valid Ethereum address
6. Verify `qty` is reasonable (positive, not exceeding limits)

**Note:** We do NOT check if we have sufficient off-chain shares at this stage. The AP is supposed to have sent shares to Alpaca, and Alpaca will journal them to us. We simply validate the request format and respond. If the journal fails in Step 2, we'll find out in Step 3.

**Our Response:**
```json
{
  "issuer_request_id": "123-456-ABCD-7890",
  "status": "created"
}
```

**Status Codes:**
- `200`: Request validated and created
- `400`: Invalid request with specific error:
  - "Invalid Wallet: Wallet does not belong to client"
  - "Invalid Token: Token not available on the network"
  - "Insufficient Eligibility: Client not eligible"
  - "Failed Validation: Invalid data payload"

**Data Storage:** Store in database with status `pending_journal`

**Data Structures:**
```rust
struct AlpacaMintRequest {
    tokenization_request_id: TokenizationRequestId,
    qty: Quantity,
    #[serde(rename = "underlying_symbol")]
    underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    token: TokenSymbol,
    network: Network,
    client_id: ClientId,
    #[serde(rename = "wallet_address")]
    wallet: Address,
}

struct MintRequestResponse {
    issuer_request_id: IssuerRequestId,
    status: String,  // "created"
}
```

#### Step 2: Alpaca Journals Shares

**Alpaca's Action:** Automatically journals the underlying shares from the AP's account into our designated tokenization account at Alpaca.

**Our Action:** None - we wait for confirmation in Step 3

#### Step 3: Receive Journal Confirmation from Alpaca

**Endpoint:** `POST /inkind/issuance/confirm`

**Request Body:**
```json
{
  "tokenization_request_id": "12345-678-90AB",
  "issuer_request_id": "ABC-123-DEF-456",
  "status": "completed"
}
```

**Status Values:**
- `completed`: Journal succeeded, proceed to mint tokens on-chain
- `rejected`: Journal failed, mark request as failed and do NOT mint

**Our Response:** `200 OK` (acknowledge receipt)

**Our Actions:**
- If `completed`: Update database status to `journal_completed` and proceed to Step 4
- If `rejected`: Update database status to `failed` with reason "journal_rejected"

**Data Structure:**
```rust
enum AlpacaConfirmationStatus {
    Completed,
    Rejected,
}

struct AlpacaJournalConfirmation {
    tokenization_request_id: TokenizationRequestId,
    issuer_request_id: IssuerRequestId,
    status: AlpacaConfirmationStatus,
}
```

#### Step 4: Mint Tokens On-Chain

Once journal is confirmed, we mint tokens using the Rain vault.

**On-Chain Call:** `OffchainAssetReceiptVault.deposit()`

**Parameters:**
- `assets`: Quantity to mint (convert from string to U256, handling decimals)
- `receiver`: AP's wallet address from original request
- `receiptInformation`: Metadata bytes (see structure below)

**Receipt Information Structure:**
```rust
struct ReceiptInformation {
    alpaca_tokenization_request_id: TokenizationRequestId,
    issuer_request_id: IssuerRequestId,
    #[serde(rename = "underlying_symbol")]
    underlying: UnderlyingSymbol,
    quantity: Quantity,
    operation_type: OperationType,
    timestamp: chrono::DateTime<Utc>,
    notes: Option<String>,
}

enum OperationType {
    Mint,
    Redeem,
}
```

**Metadata for this mint:**
- Alpaca `tokenization_request_id`
- Our `issuer_request_id`
- Symbol and quantity
- Timestamp
- Operation type: "mint"

**Authorization Check:**
Before attempting to mint, verify that our operator address is authorized for the `DEPOSIT` permission on the vault. The `OffchainAssetReceiptVault` uses an authorizer contract to control permissions. If not authorized, the transaction will revert.

**Gas Management:**
- Estimate gas before submitting transaction
- Use reasonable gas price (e.g., median + 10% from recent blocks)
- Set appropriate gas limit with buffer (e.g., estimated * 1.2)
- Monitor for stuck transactions and implement escalation if needed
- Track gas costs per operation for operational metrics

**On Success:**
- Parse transaction receipt to extract:
  - Receipt ID created (from deposit event)
  - Shares minted (from deposit event)
  - Gas used
  - Block number
- Update database status to `callback_pending`
- Store transaction details (tx hash, receipt ID, shares, gas used, block number)
- Proceed to Step 5

**Data Structure:**
```rust
struct MintResult {
    tx_hash: B256,
    receipt_id: U256,
    shares_minted: U256,
    gas_used: u64,
    block_number: u64,
}
```

#### Step 5: Callback to Alpaca

After successful on-chain minting, we call Alpaca's callback endpoint to confirm completion.

**Endpoint:** `POST /v1/accounts/{account_id}/tokenization/callback/mint`

Where `{account_id}` is our designated tokenization account ID at Alpaca.

**Request Body:**
```json
{
  "tokenization_request_id": "12345-678-90AB",
  "client_id": "5505-1234-ABC-4G45",
  "wallet_address": "<AP's wallet address where tokens were deposited>",
  "tx_hash": "0x12345678",
  "network": "base"
}
```

**On Success:**
- Update database status to `completed`
- Record completion timestamp

**On Failure:**
- Retry with exponential backoff
- If persistent failure, alert operators (mint succeeded on-chain but Alpaca not notified)
- Keep status as `callback_pending` until successful

#### Mint Request State Machine

```
pending_journal ──> journal_completed ──> minting ──> callback_pending ──> completed
     │                    │                  │               │
     │                    │                  │               │
     └────────────────────┴──────────────────┴───────────────┴──> failed
```

**Data Structures:**

```rust
struct StoredMintRequest {
    id: i64,
    tokenization_request_id: TokenizationRequestId,
    issuer_request_id: IssuerRequestId,
    qty: Quantity,
    underlying: UnderlyingSymbol,
    token: TokenSymbol,
    network: Network,
    client_id: ClientId,
    wallet: Address,
    status: MintStatus,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

enum MintStatus {
    PendingJournal,
    JournalCompleted,
    Minting,
    CallbackPending,
    Completed,
    Failed(String),
}
```

### 4. Token Redemption (Alpaca ITN Flow)

#### Complete Redemption Flow

```
[AP wants to redeem 10 AAPL0x tokens]
         │
         ├─> AP sends 10 AAPL0x tokens to our redemption wallet
         │     On-chain Transfer event emitted
         │
         ├─> Our monitor detects the transfer
         │     Parse Transfer(from, to: redemption_wallet, amount: 10)
         │     DB Status: detected
         │
         ├─> We call Alpaca's redeem endpoint
         │     POST /v1/accounts/{id}/tokenization/redeem
         │     {issuer_request_id, underlying_symbol, qty: "10", tx_hash, ...}
         │     DB Status: alpaca_called
         │
         ├─> Alpaca responds with tokenization_request_id
         │     {tokenization_request_id, status: "pending", ...}
         │
         ├─> Alpaca journals 10 AAPL shares
         │     From: Our tokenization account
         │     To: AP's Alpaca account
         │
         ├─> We poll Alpaca's list endpoint
         │     GET /v1/accounts/{id}/tokenization/requests
         │     Wait for status: "completed"
         │     DB Status: alpaca_completed
         │
         ├─> We burn tokens on-chain
         │     vault.withdraw(10 * 1e18, address(0), redemption_wallet, receipt_id, ...)
         │     DB Status: burning
         │
         ├─> On-chain transaction confirms
         │     Shares burned: 10 * 1e18
         │     DB Status: completed
         │
         └─> AP now has 10 AAPL shares in their Alpaca account ✓
```

#### Step 1: Monitor Redemption Wallet

We continuously monitor our designated redemption wallet for incoming token transfers.

**Monitoring Approach:**
- Subscribe to `Transfer` events for the vault's ERC-20 shares
- Filter for transfers where `to` address is our redemption wallet
- Can use WebSocket subscription or polling depending on infrastructure

**On Detection:**
- Parse transfer details (from address, amount, tx hash, block number)
- Determine symbol from vault/token context
- Convert amount from U256 to decimal quantity string
- Generate our internal `issuer_request_id`
- Store in database with status `detected`
- Proceed to Step 2

**Data Structure:**
```rust
struct TransferEvent {
    from: Address,      // AP's wallet that sent the tokens
    to: Address,        // Our redemption wallet
    amount: U256,       // Token amount transferred
    tx_hash: B256,
    block_number: u64,
    block_timestamp: u64,
}
```

#### Step 2: Call Alpaca's Redeem Endpoint

When we detect a redemption, we notify Alpaca.

**Endpoint:** `POST /v1/accounts/{account_id}/tokenization/redeem`

Where `{account_id}` is our designated tokenization account ID at Alpaca.

**Request Body:**
```json
{
  "issuer_request_id": "ABC-123-DEF-456",
  "underlying_symbol": "AAPL",
  "token_symbol": "AAPL0x",
  "client_id": "5505-1234-ABC-4G45",
  "qty": "1.23",
  "network": "base",
  "wallet_address": "<the originating wallet address for the redeemed tokens>",
  "tx_hash": "0x12345678"
}
```

**Alpaca's Response:**
```json
{
  "tokenization_request_id": "12345-678-90AB",
  "issuer_request_id": "ABCDEF123",
  "created_at": "2025-09-12T17:28:48.642437-04:00",
  "type": "redeem",
  "status": "pending",
  "underlying_symbol": "TSLA",
  "token_symbol": "TSLAx",
  "qty": "123.45",
  "issuer": "xstocks",
  "network": "base",
  "wallet_address": "0x1234567A",
  "tx_hash": "0x1234567A",
  "fees": "0.567"
}
```

**Status Values:**
- `pending`: Redemption request received, journal in progress
- `completed`: Journal completed successfully
- `rejected`: Redemption rejected

**Our Actions:**
- Store `tokenization_request_id` from response
- Update database status to `alpaca_called`
- Proceed to Step 3 (polling for completion)

**Client ID Lookup:**
We need to look up the AP's `client_id` based on their wallet address. This requires maintaining a mapping between wallet addresses and client IDs from the account linking process.

**Data Structures:**
```rust
struct AlpacaRedeemRequest {
    issuer_request_id: IssuerRequestId,
    #[serde(rename = "underlying_symbol")]
    underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    token: TokenSymbol,
    client_id: ClientId,
    qty: Quantity,
    network: Network,
    #[serde(rename = "wallet_address")]
    wallet: Address,
    tx_hash: B256,
}

enum RedeemRequestType {
    Redeem,
}

enum RedeemRequestStatus {
    Pending,
    Completed,
    Rejected,
}

struct Fees(Decimal);

struct AlpacaRedeemResponse {
    tokenization_request_id: TokenizationRequestId,
    issuer_request_id: IssuerRequestId,
    created_at: DateTime<Utc>,
    request_type: RedeemRequestType,
    status: RedeemRequestStatus,
    #[serde(rename = "underlying_symbol")]
    underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    token: TokenSymbol,
    qty: Quantity,
    issuer: String,
    network: Network,
    #[serde(rename = "wallet_address")]
    wallet: Address,
    tx_hash: B256,
    fees: Fees,
}
```

#### Step 3: Poll for Journal Completion

**Alpaca's Action:** Automatically journals the underlying shares from our tokenization account to the AP's account.

**Our Action:** Poll Alpaca's list endpoint to check status.

**Endpoint:** `GET /v1/accounts/{account_id}/tokenization/requests`

**Polling Strategy:**
- Start with 5-second intervals
- Exponential backoff up to 30-second max
- Timeout after 1 hour
- Update database status to `alpaca_completed` when status is "completed"
- Handle "rejected" status by marking redemption as failed

#### Step 4: Burn Tokens On-Chain

Once Alpaca confirms the journal is completed, we burn the tokens on-chain.

**On-Chain Call:** `OffchainAssetReceiptVault.withdraw()`

**Parameters:**
- `assets`: Quantity to burn (convert from string to U256)
- `receiver`: Can be zero address (tokens going off-chain)
- `owner`: Our redemption wallet (owns the shares)
- `id`: Receipt ID to burn from (need to track which receipt to use)
- `receiptInformation`: Metadata bytes (similar structure to mint)

**Receipt Tracking:**
We need to determine which receipt ID has sufficient balance to burn from. This requires:
- Maintaining an inventory of active receipt IDs
- Querying on-chain balances for the redemption wallet
- Selecting an appropriate receipt with sufficient balance

**Authorization Check:**
Verify our operator address is authorized for the `WITHDRAW` permission on the vault.

**Gas Management:**
Same strategy as minting:
- Estimate gas before submitting
- Use reasonable gas price with buffer
- Monitor and escalate if stuck
- Track costs

**On Success:**
- Parse transaction receipt to extract shares burned and gas used
- Update database status to `completed`
- Record completion timestamp

**Data Structure:**
```rust
struct BurnResult {
    tx_hash: B256,
    receipt_id: U256,
    shares_burned: U256,
    gas_used: u64,
    block_number: u64,
}
```

#### Redemption Request State Machine

```
detected ──> alpaca_called ──> alpaca_completed ──> burning ──> completed
     │              │                  │               │
     │              │                  │               │
     └──────────────┴──────────────────┴───────────────┴──> failed
```

**Data Structures:**

```rust
struct StoredRedemption {
    id: i64,
    issuer_request_id: IssuerRequestId,
    tokenization_request_id: Option<TokenizationRequestId>,
    underlying: UnderlyingSymbol,
    token: TokenSymbol,
    wallet: Address,
    tx_hash: B256,
    qty: Quantity,
    status: RedemptionStatus,
    detected_at: DateTime<Utc>,
    alpaca_called_at: Option<DateTime<Utc>>,
    alpaca_completed_at: Option<DateTime<Utc>>,
    burned_at: Option<DateTime<Utc>>,
}

enum RedemptionStatus {
    Detected,
    AlpacaCalled,
    AlpacaCompleted,
    Burning,
    Completed,
    Failed(String),
}
```

## Alpaca ITN Integration Details

### Endpoints We Implement

We run an HTTP server that implements these endpoints for Alpaca to call:

1. **`POST /accounts/connect`** - Account linking
2. **`GET /tokenized-assets`** - List supported assets
3. **`POST /inkind/issuance`** - Mint request from Alpaca
4. **`POST /inkind/issuance/confirm`** - Journal confirmation from Alpaca

### Endpoints We Call

We call these Alpaca endpoints:

1. **`POST /v1/accounts/{account_id}/tokenization/callback/mint`** - Confirm mint completed
2. **`POST /v1/accounts/{account_id}/tokenization/redeem`** - Initiate redemption
3. **`GET /v1/accounts/{account_id}/tokenization/requests`** - List/poll requests

### Authentication

- **OAuth 2.0** for calling Alpaca endpoints
- **API Key** or **mTLS** for Alpaca calling our endpoints
- Store tokens securely with encryption
- Handle token refresh before expiration

### Error Handling

**Mint Request Errors (400 responses):**
- "Invalid Wallet: Wallet does not belong to client"
- "Invalid Token: Token not available on the network"
- "Insufficient Eligibility: Client not eligible"
- "Failed Validation: Invalid data payload"

**Redemption Errors:**
- Journal failed/rejected
- Insufficient balance in tokenization account
- Unknown client_id
- Invalid transaction hash

**Recovery Strategies:**
1. **Journal Failed**: Mark mint as failed, do not mint tokens
2. **Callback Failed**: Retry callback with exponential backoff, alert if persistent
3. **Burn Failed**: Tokens stuck in redemption wallet, manual intervention needed
4. **Alpaca Redeem Failed**: Tokens in redemption wallet but no journal, reconciliation required

## Database Schema

### Account Links Table

```sql
CREATE TABLE account_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL,
    alpaca_account TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE INDEX idx_account_links_email ON account_links(email);
CREATE INDEX idx_account_links_alpaca ON account_links(alpaca_account);
```

### Authorized Participants Table

```sql
CREATE TABLE authorized_participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL UNIQUE,
    name TEXT,
    status TEXT NOT NULL, -- 'active', 'suspended', 'inactive'
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX idx_aps_email ON authorized_participants(email);
CREATE INDEX idx_aps_status ON authorized_participants(status);
```

### Mint Requests Table

```sql
CREATE TABLE mint_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Alpaca identifiers
    tokenization_request_id TEXT NOT NULL UNIQUE,
    issuer_request_id TEXT NOT NULL UNIQUE,
    
    -- Request details
    qty TEXT NOT NULL,
    underlying_symbol TEXT NOT NULL,
    token_symbol TEXT NOT NULL,
    network TEXT NOT NULL,
    client_id TEXT NOT NULL,
    wallet_address TEXT NOT NULL,
    
    -- Status tracking
    status TEXT NOT NULL, -- 'pending_journal', 'journal_completed', 'minting', 'callback_pending', 'completed', 'failed'
    error_message TEXT,
    
    -- On-chain tracking
    tx_hash BLOB,
    receipt_id TEXT,  -- U256 as string
    shares_minted TEXT,  -- U256 as string
    block_number INTEGER,
    gas_used INTEGER,
    
    -- Timestamps
    created_at TEXT NOT NULL,
    journal_confirmed_at TEXT,
    minted_at TEXT,
    callback_sent_at TEXT,
    completed_at TEXT
);

CREATE INDEX idx_mint_tokenization_id ON mint_requests(tokenization_request_id);
CREATE INDEX idx_mint_issuer_id ON mint_requests(issuer_request_id);
CREATE INDEX idx_mint_status ON mint_requests(status);
CREATE INDEX idx_mint_symbol ON mint_requests(underlying_symbol);
CREATE INDEX idx_mint_client ON mint_requests(client_id);
```

### Redemption Requests Table

```sql
CREATE TABLE redemption_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Our identifier
    issuer_request_id TEXT NOT NULL UNIQUE,
    
    -- Alpaca identifier (received after calling redeem endpoint)
    tokenization_request_id TEXT UNIQUE,
    
    -- Request details
    underlying_symbol TEXT NOT NULL,
    token_symbol TEXT NOT NULL,
    wallet_address TEXT NOT NULL,
    qty TEXT NOT NULL,
    network TEXT NOT NULL,
    client_id TEXT,
    
    -- Detection
    detected_tx_hash TEXT NOT NULL,  -- On-chain transfer to redemption wallet
    detected_block_number INTEGER,
    detected_block_timestamp INTEGER,
    
    -- Alpaca tracking
    alpaca_fees TEXT,
    
    -- Status tracking
    status TEXT NOT NULL, -- 'detected', 'alpaca_called', 'alpaca_completed', 'burning', 'completed', 'failed'
    error_message TEXT,
    
    -- On-chain burn tracking
    burn_tx_hash BLOB,
    receipt_id TEXT,  -- Which receipt we burned from
    shares_burned TEXT,  -- U256 as string
    burn_block_number INTEGER,
    burn_gas_used INTEGER,
    
    -- Timestamps
    detected_at TEXT NOT NULL,
    alpaca_called_at TEXT,
    alpaca_completed_at TEXT,
    burned_at TEXT,
    completed_at TEXT
);

CREATE INDEX idx_redemption_issuer_id ON redemption_requests(issuer_request_id);
CREATE INDEX idx_redemption_tokenization_id ON redemption_requests(tokenization_request_id);
CREATE INDEX idx_redemption_status ON redemption_requests(status);
CREATE INDEX idx_redemption_symbol ON redemption_requests(underlying_symbol);
CREATE INDEX idx_redemption_tx ON redemption_requests(detected_tx_hash);
```

### Supported Symbols Table

```sql
CREATE TABLE supported_symbols (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    underlying_symbol TEXT NOT NULL UNIQUE,
    token_symbol TEXT NOT NULL UNIQUE,
    network TEXT NOT NULL,
    vault_address BLOB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TEXT NOT NULL
);

CREATE INDEX idx_symbols_underlying ON supported_symbols(underlying_symbol);
CREATE INDEX idx_symbols_enabled ON supported_symbols(enabled);
```

### Receipt Tracking Table

```sql
-- Track which receipts have been issued and their current balances
CREATE TABLE receipt_inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    receipt_id TEXT NOT NULL,  -- U256 as string
    vault_address BLOB NOT NULL,
    symbol TEXT NOT NULL,
    initial_amount TEXT NOT NULL,  -- U256 as string
    current_balance TEXT NOT NULL,  -- U256 as string, updated as burned
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    
    UNIQUE(receipt_id, vault_address)
);

CREATE INDEX idx_receipts_vault ON receipt_inventory(vault_address);
CREATE INDEX idx_receipts_symbol ON receipt_inventory(symbol);
CREATE INDEX idx_receipts_balance ON receipt_inventory(current_balance);
```

### Inventory Snapshots Table

```sql
CREATE TABLE inventory_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    onchain_balance REAL NOT NULL,
    offchain_balance REAL NOT NULL,
    total_balance REAL NOT NULL,
    onchain_ratio REAL NOT NULL,
    timestamp TEXT NOT NULL
);

CREATE INDEX idx_inventory_symbol ON inventory_snapshots(symbol);
CREATE INDEX idx_inventory_timestamp ON inventory_snapshots(timestamp);
```

### Reconciliation Table

```sql
CREATE TABLE reconciliation_issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_type TEXT NOT NULL, -- 'mint' or 'redemption'
    operation_id INTEGER NOT NULL, -- References mint_requests.id or redemption_requests.id
    issue_type TEXT NOT NULL, -- 'alpaca_failed_onchain_success', 'onchain_failed_alpaca_success', 'amount_mismatch'
    description TEXT NOT NULL,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TEXT,
    resolved_by TEXT,
    resolution_notes TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX idx_reconciliation_resolved ON reconciliation_issues(resolved);
CREATE INDEX idx_reconciliation_type ON reconciliation_issues(operation_type);
```

## Configuration

### Environment Variables

```bash
# HTTP Server Configuration
SERVER_HOST=0.0.0.0
SERVER_PORT=8080
SERVER_API_KEY=<our_api_key_for_alpaca_to_call_us>

# Alpaca Configuration
ALPACA_API_KEY=<api_key>
ALPACA_API_SECRET=<api_secret>
ALPACA_BASE_URL=https://broker-api.alpaca.markets
ALPACA_TOKENIZATION_ACCOUNT_ID=<our_designated_tokenization_account_at_alpaca>

# Blockchain Configuration
RPC_URL=<ethereum_rpc_url>
RPC_WS_URL=<ethereum_websocket_url>  # For monitoring redemption wallet
CHAIN_ID=8453  # Base
CHAIN_NAME=base
VAULT_ADDRESS=<offchain_asset_receipt_vault_address>
REDEMPTION_WALLET_ADDRESS=<address_where_aps_send_tokens_to_redeem>

# Database
DATABASE_URL=sqlite:issuance.db

# Encryption
ENCRYPTION_KEY=<32_byte_hex_key>

# Operational Parameters
MAX_GAS_PRICE_GWEI=100
REDEMPTION_POLL_INTERVAL=30  # seconds between checking for redemptions
ALPACA_STATUS_POLL_INTERVAL=5  # seconds between status checks
ALPACA_STATUS_POLL_TIMEOUT=3600  # max seconds to wait for redemption completion

# Monitoring
LOG_LEVEL=info
METRICS_PORT=9090
```

### Private Key Management

**TBD** - Private key management strategy needs to be worked out in greater detail including:
- Storage approach (encrypted file, HSM, KMS)
- Access controls
- Rotation procedures
- Backup and recovery
- Separation between minting and burning keys if needed

This is a critical security consideration that requires careful planning.
