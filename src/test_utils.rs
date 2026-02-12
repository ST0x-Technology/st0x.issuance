use alloy::hex;
use alloy::network::EthereumWallet;
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, B256, Bytes, U256, address, keccak256};
use alloy::providers::{PendingTransactionError, Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolValue;
use alloy::transports::{RpcError, TransportErrorKind};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use cqrs_es::persist::{GenericQuery, PersistedEventStore};
use rocket::routes;
use sqlite_es::{
    SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
};
use sqlx::sqlite::SqlitePoolOptions;
use std::sync::Arc;
use url::Url;

use crate::account::{Account, AccountView};
use crate::alpaca::mock::MockAlpacaService;
use crate::alpaca::service::AlpacaConfig;
use crate::auth::{FailedAuthRateLimiter, test_auth_config};
use crate::bindings::{
    CloneFactory, OffchainAssetReceiptVault,
    OffchainAssetReceiptVaultAuthorizerV1, Receipt,
};
use crate::config::{Config, LogLevel};
use crate::fireblocks::SignerConfig;
use crate::mint::{Mint, MintServices, MintView};
use crate::receipt_inventory::{CqrsReceiptService, ReceiptInventory};
use crate::tokenized_asset::{
    Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    TokenizedAssetView, UnderlyingSymbol,
};
use crate::vault::mock::MockVaultService;

/// Returns test Alpaca legacy auth credentials for mock Alpaca API requests.
///
/// Uses clearly fake test credentials: "test-key" / "test-secret"
/// Returns (basic_auth_header, api_key, api_secret) for legacy auth which
/// requires both Basic auth and APCA-API-KEY-ID/APCA-API-SECRET-KEY headers.
#[must_use]
pub fn test_alpaca_legacy_auth() -> (String, String, String) {
    let api_key = "test-key".to_string();
    let api_secret = "test-secret".to_string();
    let basic_auth =
        format!("Basic {}", BASE64.encode(format!("{api_key}:{api_secret}")));
    (basic_auth, api_key, api_secret)
}

/// Anvil local chain ID
pub const ANVIL_CHAIN_ID: u64 = 31337;

fn test_config() -> Result<Config, anyhow::Error> {
    Ok(Config {
        database_url: "sqlite::memory:".to_string(),
        database_max_connections: 5,
        rpc_url: Url::parse("wss://localhost:8545")?,
        chain_id: ANVIL_CHAIN_ID,
        signer: SignerConfig::Local(B256::ZERO),
        vault: address!("0x1111111111111111111111111111111111111111"),
        backfill_start_block: 0,
        auth: test_auth_config()?,
        log_level: LogLevel::Debug,
        hyperdx: None,
        alpaca: AlpacaConfig::test_default(),
    })
}

/// Sets up a test Rocket instance with in-memory database and mock services.
///
/// This function is NOT behind `#[cfg(test)]` because E2E tests in the `tests/` directory
/// need to call it. The mock services it constructs are also NOT behind `#[cfg(test)]` for
/// the same reason. However, all mock services are internal implementation details - E2E
/// tests should only interact with the returned Rocket instance through its public HTTP API.
///
/// # Errors
///
/// Returns an error if:
/// - Database creation fails
/// - Database migrations fail
/// - Asset seeding fails
/// - Rate limiter initialization fails
pub async fn setup_test_rocket() -> anyhow::Result<rocket::Rocket<rocket::Build>>
{
    // Create in-memory database
    let pool =
        SqlitePoolOptions::new().max_connections(5).connect(":memory:").await?;

    // Run migrations
    sqlx::migrate!("./migrations").run(&pool).await?;

    // Setup Account CQRS
    let account_view_repo =
        Arc::new(SqliteViewRepository::<AccountView, Account>::new(
            pool.clone(),
            "account_view".to_string(),
        ));

    let account_query = GenericQuery::new(account_view_repo);
    let account_cqrs =
        sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

    // Setup TokenizedAsset CQRS
    let tokenized_asset_view_repo = Arc::new(SqliteViewRepository::<
        TokenizedAssetView,
        TokenizedAsset,
    >::new(
        pool.clone(),
        "tokenized_asset_view".to_string(),
    ));

    let tokenized_asset_query = GenericQuery::new(tokenized_asset_view_repo);
    let tokenized_asset_cqrs =
        sqlite_cqrs(pool.clone(), vec![Box::new(tokenized_asset_query)], ());

    // Setup ReceiptInventory (needed by MintServices for receipt lookups and registration)
    let receipt_inventory_event_store = Arc::new(PersistedEventStore::<
        SqliteEventRepository,
        ReceiptInventory,
    >::new_event_store(
        SqliteEventRepository::new(pool.clone()),
    ));
    let receipt_inventory_cqrs =
        Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));

    // Create mock services for Mint aggregate
    let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let mint_services = MintServices {
        vault: Arc::new(MockVaultService::new_success()),
        alpaca: Arc::new(MockAlpacaService::new_success()),
        pool: pool.clone(),
        bot,
        receipts: Arc::new(CqrsReceiptService::new(
            receipt_inventory_event_store.clone(),
            receipt_inventory_cqrs,
        )),
    };

    // Setup Mint CQRS
    let mint_view_repo = Arc::new(SqliteViewRepository::<MintView, Mint>::new(
        pool.clone(),
        "mint_view".to_string(),
    ));

    let mint_query = GenericQuery::new(mint_view_repo);
    let mint_cqrs = Arc::new(sqlite_cqrs(
        pool.clone(),
        vec![Box::new(mint_query)],
        mint_services,
    ));

    let mint_event_repo = SqliteEventRepository::new(pool.clone());
    let mint_event_store = Arc::new(PersistedEventStore::<
        SqliteEventRepository,
        Mint,
    >::new_event_store(mint_event_repo));

    // Seed initial assets
    seed_test_assets(&tokenized_asset_cqrs).await?;

    let rate_limiter = FailedAuthRateLimiter::new()?;

    // Build rocket
    Ok(rocket::build()
        .manage(test_config()?)
        .manage(account_cqrs)
        .manage(tokenized_asset_cqrs)
        .manage(mint_cqrs)
        .manage(mint_event_store)
        .manage(rate_limiter)
        .manage(pool)
        .mount(
            "/",
            routes![
                crate::account::connect_account,
                crate::tokenized_asset::list_tokenized_assets,
                crate::mint::initiate_mint,
                crate::mint::confirm_journal
            ],
        ))
}

async fn seed_test_assets(
    cqrs: &SqliteCqrs<TokenizedAsset>,
) -> Result<(), anyhow::Error> {
    let assets = vec![
        (
            "AAPL",
            "tAAPL",
            "base",
            address!("0x1234567890abcdef1234567890abcdef12345678"),
        ),
        (
            "TSLA",
            "tTSLA",
            "base",
            address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
        ),
    ];

    for (underlying, token, network, vault) in assets {
        let command = TokenizedAssetCommand::Add {
            underlying: UnderlyingSymbol::new(underlying),
            token: TokenSymbol::new(token),
            network: Network::new(network),
            vault,
        };

        match cqrs.execute(underlying, command).await {
            Ok(()) | Err(cqrs_es::AggregateError::AggregateConflict) => {}
            Err(err) => {
                return Err(err.into());
            }
        }
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum LocalEvmError {
    #[error("Signer error: {0}")]
    Signer(#[from] alloy::signers::k256::ecdsa::Error),
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("Contract error: {0}")]
    Contract(#[from] alloy::contract::Error),
    #[error("Pending transaction error: {0}")]
    PendingTransaction(#[from] PendingTransactionError),
    #[error("Event not found in logs")]
    EventNotFound,
}

/// Local EVM instance for end-to-end testing with deployed contracts.
///
/// Spawns an Anvil instance and deploys the full contract suite:
/// - Receipt implementation
/// - CloneFactory
/// - OffchainAssetReceiptVault implementation
/// - OffchainAssetReceiptVaultAuthorizerV1 implementation
/// - Cloned vault instance
/// - Cloned authorizer instance
///
/// The vault is configured with the deployer address as the initial admin
/// and has the authorizer set.
pub struct LocalEvm {
    _anvil: AnvilInstance,
    pub vault_address: Address,
    pub authorizer_address: Address,
    pub wallet_address: Address,
    pub private_key: B256,
    pub endpoint: String,
}

impl LocalEvm {
    /// Creates a new LocalEvm instance with Anvil and deployed contracts.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Private key parsing fails
    /// - Provider connection fails
    /// - Any contract deployment step fails
    pub async fn new() -> Result<Self, LocalEvmError> {
        let anvil = Anvil::new().spawn();
        let endpoint = anvil.ws_endpoint();

        let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let signer = PrivateKeySigner::from_bytes(&private_key)?;
        let wallet_address = signer.address();
        let wallet = EthereumWallet::from(signer);

        let provider =
            ProviderBuilder::new().wallet(wallet).connect(&endpoint).await?;

        let (vault_address, authorizer_address) =
            Self::deploy_vault(&provider, wallet_address).await?;

        Ok(Self {
            _anvil: anvil,
            vault_address,
            authorizer_address,
            wallet_address,
            private_key,
            endpoint,
        })
    }

    async fn deploy_vault_implementation(
        provider: &impl Provider,
        receipt_address: Address,
        factory_address: Address,
    ) -> Result<Address, LocalEvmError> {
        let vault_config =
            OffchainAssetReceiptVault::ReceiptVaultConstructionConfigV2 {
                factory: factory_address,
                receiptImplementation: receipt_address,
            };

        let vault_impl_deploy =
            OffchainAssetReceiptVault::deploy(provider, vault_config).await?;

        Ok(*vault_impl_deploy.address())
    }

    async fn deploy_authorizer_implementation(
        provider: &impl Provider,
    ) -> Result<Address, LocalEvmError> {
        let authorizer_impl_deploy =
            OffchainAssetReceiptVaultAuthorizerV1::deploy(provider).await?;

        Ok(*authorizer_impl_deploy.address())
    }

    async fn clone_vault_instance(
        provider: &impl Provider,
        factory_address: Address,
        vault_impl_address: Address,
        initial_admin: Address,
    ) -> Result<Address, LocalEvmError> {
        let factory = CloneFactory::new(factory_address, provider);
        let vault_clone_data = (
            initial_admin,
            (Address::ZERO, "Test Vault".to_string(), "TEST".to_string()),
        )
            .abi_encode();

        let clone_receipt = factory
            .clone(vault_impl_address, vault_clone_data.into())
            .send()
            .await?
            .get_receipt()
            .await?;

        clone_receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                log.log_decode::<CloneFactory::NewClone>()
                    .ok()
                    .map(|decoded| decoded.data().clone)
            })
            .ok_or(LocalEvmError::EventNotFound)
    }

    async fn clone_authorizer_instance(
        provider: &impl Provider,
        factory_address: Address,
        authorizer_impl_address: Address,
        initial_admin: Address,
    ) -> Result<Address, LocalEvmError> {
        let factory = CloneFactory::new(factory_address, provider);
        let authorizer_clone_data = (initial_admin,).abi_encode();

        let authorizer_clone_receipt = factory
            .clone(authorizer_impl_address, authorizer_clone_data.into())
            .send()
            .await?
            .get_receipt()
            .await?;

        authorizer_clone_receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                log.log_decode::<CloneFactory::NewClone>()
                    .ok()
                    .map(|decoded| decoded.data().clone)
            })
            .ok_or(LocalEvmError::EventNotFound)
    }

    async fn set_vault_authorizer(
        provider: &impl Provider,
        vault_address: Address,
        authorizer_address: Address,
    ) -> Result<(), LocalEvmError> {
        let vault = OffchainAssetReceiptVault::new(vault_address, provider);
        vault
            .setAuthorizer(authorizer_address)
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(())
    }

    async fn grant_initial_deposit_role(
        provider: &impl Provider,
        authorizer_address: Address,
        initial_admin: Address,
    ) -> Result<(), LocalEvmError> {
        let authorizer = OffchainAssetReceiptVaultAuthorizerV1::new(
            authorizer_address,
            provider,
        );
        let deposit_role = keccak256("DEPOSIT");
        authorizer
            .grantRole(deposit_role, initial_admin)
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(())
    }

    async fn deploy_vault(
        provider: &impl Provider,
        initial_admin: Address,
    ) -> Result<(Address, Address), LocalEvmError> {
        let receipt_deploy = Receipt::deploy(provider).await?;
        let receipt_address = *receipt_deploy.address();

        let factory_deploy = CloneFactory::deploy(provider).await?;
        let factory_address = *factory_deploy.address();

        let vault_impl_address = Self::deploy_vault_implementation(
            provider,
            receipt_address,
            factory_address,
        )
        .await?;

        let authorizer_impl_address =
            Self::deploy_authorizer_implementation(provider).await?;

        let vault_address = Self::clone_vault_instance(
            provider,
            factory_address,
            vault_impl_address,
            initial_admin,
        )
        .await?;

        let authorizer_address = Self::clone_authorizer_instance(
            provider,
            factory_address,
            authorizer_impl_address,
            initial_admin,
        )
        .await?;

        Self::set_vault_authorizer(provider, vault_address, authorizer_address)
            .await?;

        Self::grant_initial_deposit_role(
            provider,
            authorizer_address,
            initial_admin,
        )
        .await?;

        Ok((vault_address, authorizer_address))
    }

    #[must_use]
    pub fn private_key_hex(&self) -> String {
        hex::encode_prefixed(self.private_key)
    }

    /// Grants the DEPOSIT role to an address via the authorizer contract.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Signer creation fails
    /// - Provider connection fails
    /// - Role granting transaction fails
    pub async fn grant_deposit_role(
        &self,
        to: Address,
    ) -> Result<(), LocalEvmError> {
        self.grant_role("DEPOSIT", to).await
    }

    /// Grants the WITHDRAW role to an address via the authorizer contract.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Signer creation fails
    /// - Provider connection fails
    /// - Role granting transaction fails
    pub async fn grant_withdraw_role(
        &self,
        to: Address,
    ) -> Result<(), LocalEvmError> {
        self.grant_role("WITHDRAW", to).await
    }

    /// Grants the CERTIFY role to an address via the authorizer contract.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Signer creation fails
    /// - Provider connection fails
    /// - Role granting transaction fails
    pub async fn grant_certify_role(
        &self,
        to: Address,
    ) -> Result<(), LocalEvmError> {
        self.grant_role("CERTIFY", to).await
    }

    /// Certifies the vault to enable deposits/withdrawals.
    ///
    /// # Errors
    ///
    /// Returns an error if signer creation, provider connection, or the
    /// certify transaction fails.
    pub async fn certify_vault(
        &self,
        until: U256,
    ) -> Result<(), LocalEvmError> {
        let signer = PrivateKeySigner::from_bytes(&self.private_key)?;
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&self.endpoint)
            .await?;

        let vault =
            OffchainAssetReceiptVault::new(self.vault_address, &provider);
        vault
            .certify(until, false, Bytes::new())
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(())
    }

    async fn grant_role(
        &self,
        role_name: &str,
        to: Address,
    ) -> Result<(), LocalEvmError> {
        let signer = PrivateKeySigner::from_bytes(&self.private_key)?;
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&self.endpoint)
            .await?;

        let authorizer = OffchainAssetReceiptVaultAuthorizerV1::new(
            self.authorizer_address,
            &provider,
        );
        let role = keccak256(role_name);
        authorizer.grantRole(role, to).send().await?.get_receipt().await?;

        Ok(())
    }

    /// Mints shares directly on-chain to a specified address.
    ///
    /// This bypasses the issuance API and mints directly via the vault contract.
    /// Useful for simulating historic mints that occurred before the service started.
    ///
    /// # Errors
    ///
    /// Returns an error if signer creation, provider connection, the deposit
    /// transaction fails, or the Deposit event is not found in the receipt.
    pub async fn mint_directly(
        &self,
        amount: U256,
        to: Address,
    ) -> Result<(U256, U256), LocalEvmError> {
        let (id, shares, _) =
            self.mint_directly_with_info(amount, to, Bytes::new()).await?;
        Ok((id, shares))
    }

    /// Mints shares directly on-chain with custom receipt information.
    ///
    /// Like `mint_directly`, but allows specifying the `receiptInformation` bytes
    /// that will be stored on-chain with the Deposit event.
    ///
    /// # Errors
    ///
    /// Returns an error if signer creation, provider connection, the deposit
    /// transaction fails, or the Deposit event is not found in the receipt.
    pub async fn mint_directly_with_info(
        &self,
        amount: U256,
        to: Address,
        receipt_information: Bytes,
    ) -> Result<(U256, U256, Bytes), LocalEvmError> {
        let signer = PrivateKeySigner::from_bytes(&self.private_key)?;
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&self.endpoint)
            .await?;

        let vault =
            OffchainAssetReceiptVault::new(self.vault_address, &provider);

        let share_ratio = U256::from(10).pow(U256::from(18));

        let receipt = vault
            .deposit(amount, to, share_ratio, receipt_information)
            .send()
            .await?
            .get_receipt()
            .await?;

        receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                log.log_decode::<OffchainAssetReceiptVault::Deposit>().ok().map(
                    |decoded| {
                        let event_data = decoded.data();
                        (
                            event_data.id,
                            event_data.shares,
                            event_data.receiptInformation.clone(),
                        )
                    },
                )
            })
            .ok_or(LocalEvmError::EventNotFound)
    }

    /// Deploys an additional vault instance on the same Anvil instance.
    ///
    /// This is useful for testing multi-vault scenarios where different
    /// tokenized assets use different vaults.
    ///
    /// Returns (vault_address, authorizer_address) for the new vault.
    ///
    /// # Errors
    ///
    /// Returns an error if contract deployment fails.
    pub async fn deploy_additional_vault(
        &self,
    ) -> Result<(Address, Address), LocalEvmError> {
        let signer = PrivateKeySigner::from_bytes(&self.private_key)?;
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&self.endpoint)
            .await?;

        Self::deploy_vault(&provider, self.wallet_address).await
    }

    /// Mints shares directly on a specific vault (not necessarily this LocalEvm's vault).
    ///
    /// This is useful for testing multi-vault scenarios.
    ///
    /// # Errors
    ///
    /// Returns an error if signer creation, provider connection, the deposit
    /// transaction fails, or the Deposit event is not found in the receipt.
    pub async fn mint_directly_on_vault(
        &self,
        vault_address: Address,
        amount: U256,
        to: Address,
    ) -> Result<(U256, U256), LocalEvmError> {
        let signer = PrivateKeySigner::from_bytes(&self.private_key)?;
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&self.endpoint)
            .await?;

        let vault = OffchainAssetReceiptVault::new(vault_address, &provider);
        let share_ratio = U256::from(10).pow(U256::from(18));

        let receipt = vault
            .deposit(amount, to, share_ratio, Bytes::new())
            .send()
            .await?
            .get_receipt()
            .await?;

        receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                log.log_decode::<OffchainAssetReceiptVault::Deposit>().ok().map(
                    |decoded| {
                        let event_data = decoded.data();
                        (event_data.id, event_data.shares)
                    },
                )
            })
            .ok_or(LocalEvmError::EventNotFound)
    }

    /// Grants a role on a specific authorizer (not necessarily this LocalEvm's authorizer).
    ///
    /// # Errors
    ///
    /// Returns an error if the role granting transaction fails.
    pub async fn grant_role_on_authorizer(
        &self,
        authorizer_address: Address,
        role_name: &str,
        to: Address,
    ) -> Result<(), LocalEvmError> {
        let signer = PrivateKeySigner::from_bytes(&self.private_key)?;
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&self.endpoint)
            .await?;

        let authorizer = OffchainAssetReceiptVaultAuthorizerV1::new(
            authorizer_address,
            &provider,
        );
        let role = keccak256(role_name);
        authorizer.grantRole(role, to).send().await?.get_receipt().await?;

        Ok(())
    }

    /// Certifies a specific vault (not necessarily this LocalEvm's vault).
    ///
    /// # Errors
    ///
    /// Returns an error if the certify transaction fails.
    pub async fn certify_specific_vault(
        &self,
        vault_address: Address,
        until: U256,
    ) -> Result<(), LocalEvmError> {
        let signer = PrivateKeySigner::from_bytes(&self.private_key)?;
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&self.endpoint)
            .await?;

        let vault = OffchainAssetReceiptVault::new(vault_address, &provider);
        vault
            .certify(until, false, Bytes::new())
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(())
    }
}

/// Checks whether any log line at the given level, scoped to the
/// caller's module, contains all snippets.
#[cfg(test)]
macro_rules! logs_contain_at {
    ($level:expr, $snippets:expr) => {
        $crate::test_utils::log_count_at!($level, $snippets) > 0
    };
}

/// Counts log lines at the given level, scoped to the caller's
/// module, that contain all snippets.
#[cfg(test)]
macro_rules! log_count_at {
    ($level:expr, $snippets:expr) => {{
        let logs = {
            let buf = tracing_test::internal::global_buf().lock().unwrap();
            String::from_utf8_lossy(&buf).into_owned()
        };
        let scope = module_path!();
        let scope = scope.strip_suffix("::tests").unwrap_or(scope);
        let level_str = match $level {
            tracing::Level::TRACE => "TRACE",
            tracing::Level::DEBUG => "DEBUG",
            tracing::Level::INFO => "INFO",
            tracing::Level::WARN => "WARN",
            tracing::Level::ERROR => "ERROR",
        };
        let snippets: &[&str] = $snippets;
        logs.lines()
            .filter(|line| {
                line.contains(scope)
                    && line.contains(level_str)
                    && snippets.iter().all(|snippet| line.contains(snippet))
            })
            .count()
    }};
}

#[cfg(test)]
pub(crate) use log_count_at;
#[cfg(test)]
pub(crate) use logs_contain_at;
