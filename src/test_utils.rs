use alloy::hex;
use alloy::network::EthereumWallet;
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, B256, address, keccak256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolValue;
use alloy::transports::RpcError;
use cqrs_es::persist::{GenericQuery, PersistedEventStore};
use rocket::routes;
use sqlite_es::{
    SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
};
use sqlx::sqlite::SqlitePoolOptions;
use std::sync::Arc;

use crate::account::{Account, AccountView};
use crate::alpaca::mock::MockAlpacaService;
use crate::bindings::{
    CloneFactory, OffchainAssetReceiptVault,
    OffchainAssetReceiptVaultAuthorizerV1, Receipt,
};
use crate::mint::mint_manager::MintManager;
use crate::mint::{CallbackManager, Mint, MintView};
use crate::tokenized_asset::{
    Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    TokenizedAssetView, UnderlyingSymbol,
};
use crate::vault::mock::MockVaultService;

/// Sets up a test Rocket instance with in-memory database and mock services.
///
/// This function is NOT behind `#[cfg(test)]` because E2E tests in the `tests/` directory
/// need to call it. The mock services it constructs are also NOT behind `#[cfg(test)]` for
/// the same reason. However, all mock services are internal implementation details - E2E
/// tests should only interact with the returned Rocket instance through its public HTTP API.
///
/// # Panics
/// Panics if database creation, migrations, or asset seeding fails.
pub async fn setup_test_rocket() -> rocket::Rocket<rocket::Build> {
    // Create in-memory database
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(":memory:")
        .await
        .expect("Failed to create in-memory database");

    // Run migrations
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

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

    // Setup Mint CQRS
    let mint_view_repo = Arc::new(SqliteViewRepository::<MintView, Mint>::new(
        pool.clone(),
        "mint_view".to_string(),
    ));

    let mint_query = GenericQuery::new(mint_view_repo);
    let mint_cqrs_raw =
        sqlite_cqrs(pool.clone(), vec![Box::new(mint_query)], ());
    let mint_cqrs = Arc::new(mint_cqrs_raw);

    let mint_event_repo = SqliteEventRepository::new(pool.clone());
    let mint_event_store = Arc::new(PersistedEventStore::<
        SqliteEventRepository,
        Mint,
    >::new_event_store(mint_event_repo));

    // Seed initial assets
    seed_test_assets(&tokenized_asset_cqrs).await;

    // Create managers with mock services
    let mint_manager = Arc::new(MintManager::new(
        Arc::new(MockVaultService::new_success()),
        mint_cqrs.clone(),
    ));

    let callback_manager = Arc::new(CallbackManager::new(
        Arc::new(MockAlpacaService::new_success()),
        mint_cqrs.clone(),
    ));

    // Build rocket
    rocket::build()
        .manage(account_cqrs)
        .manage(tokenized_asset_cqrs)
        .manage(mint_cqrs)
        .manage(mint_event_store)
        .manage(mint_manager)
        .manage(callback_manager)
        .manage(pool)
        .mount(
            "/",
            routes![
                crate::account::connect_account,
                crate::tokenized_asset::list_tokenized_assets,
                crate::mint::initiate_mint,
                crate::mint::confirm_journal
            ],
        )
}

async fn seed_test_assets(cqrs: &SqliteCqrs<TokenizedAsset>) {
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

    for (underlying, token, network, vault_address) in assets {
        let command = TokenizedAssetCommand::Add {
            underlying: UnderlyingSymbol::new(underlying),
            token: TokenSymbol::new(token),
            network: Network::new(network),
            vault_address,
        };

        cqrs.execute(underlying, command)
            .await
            .expect("Failed to seed test asset");
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LocalEvmError {
    #[error("Failed to parse private key: {0}")]
    InvalidPrivateKey(String),
    #[error("Failed to connect to Anvil")]
    ConnectionFailed(#[from] RpcError<alloy::transports::TransportErrorKind>),
    #[error("Contract deployment failed: {0}")]
    DeploymentFailed(String),
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
        let signer = PrivateKeySigner::from_bytes(&private_key)
            .map_err(|e| LocalEvmError::InvalidPrivateKey(e.to_string()))?;
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
            OffchainAssetReceiptVault::deploy(provider, vault_config)
                .await
                .map_err(|e| {
                    LocalEvmError::DeploymentFailed(format!(
                        "OffchainAssetReceiptVault implementation: {e}"
                    ))
                })?;

        Ok(*vault_impl_deploy.address())
    }

    async fn deploy_authorizer_implementation(
        provider: &impl Provider,
    ) -> Result<Address, LocalEvmError> {
        let authorizer_impl_deploy =
            OffchainAssetReceiptVaultAuthorizerV1::deploy(provider)
                .await
                .map_err(|e| {
                    LocalEvmError::DeploymentFailed(format!(
                        "OffchainAssetReceiptVaultAuthorizerV1 implementation: {e}"
                    ))
                })?;

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
            .await
            .map_err(|e| {
                LocalEvmError::DeploymentFailed(format!("Vault clone: {e}"))
            })?
            .get_receipt()
            .await
            .map_err(|e| {
                LocalEvmError::DeploymentFailed(format!(
                    "Vault clone receipt: {e}"
                ))
            })?;

        clone_receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                log.log_decode::<CloneFactory::NewClone>()
                    .ok()
                    .map(|decoded| decoded.data().clone)
            })
            .ok_or_else(|| {
                LocalEvmError::DeploymentFailed(
                    "Vault clone address not found in logs".to_string(),
                )
            })
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
            .await
            .map_err(|e| {
                LocalEvmError::DeploymentFailed(format!(
                    "Authorizer clone: {e}"
                ))
            })?
            .get_receipt()
            .await
            .map_err(|e| {
                LocalEvmError::DeploymentFailed(format!(
                    "Authorizer clone receipt: {e}"
                ))
            })?;

        authorizer_clone_receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                log.log_decode::<CloneFactory::NewClone>()
                    .ok()
                    .map(|decoded| decoded.data().clone)
            })
            .ok_or_else(|| {
                LocalEvmError::DeploymentFailed(
                    "Authorizer clone address not found in logs".to_string(),
                )
            })
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
            .await
            .map_err(|e| {
                LocalEvmError::DeploymentFailed(format!("Set authorizer: {e}"))
            })?
            .get_receipt()
            .await
            .map_err(|e| {
                LocalEvmError::DeploymentFailed(format!(
                    "Set authorizer receipt: {e}"
                ))
            })?;

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
            .await
            .map_err(|e| {
                LocalEvmError::DeploymentFailed(format!(
                    "Grant DEPOSIT role: {e}"
                ))
            })?
            .get_receipt()
            .await
            .map_err(|e| {
                LocalEvmError::DeploymentFailed(format!(
                    "Grant DEPOSIT role receipt: {e}"
                ))
            })?;

        Ok(())
    }

    /// Deploys the vault infrastructure including Receipt, CloneFactory, vault
    /// implementation, authorizer implementation, and cloned instances.
    ///
    /// # Errors
    ///
    /// Returns an error if any deployment or initialization step fails.
    async fn deploy_vault(
        provider: &impl Provider,
        initial_admin: Address,
    ) -> Result<(Address, Address), LocalEvmError> {
        let receipt_deploy = Receipt::deploy(provider).await.map_err(|e| {
            LocalEvmError::DeploymentFailed(format!("Receipt: {e}"))
        })?;
        let receipt_address = *receipt_deploy.address();

        let factory_deploy =
            CloneFactory::deploy(provider).await.map_err(|e| {
                LocalEvmError::DeploymentFailed(format!("CloneFactory: {e}"))
            })?;
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
        let signer = PrivateKeySigner::from_bytes(&self.private_key)
            .map_err(|e| LocalEvmError::InvalidPrivateKey(e.to_string()))?;
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&self.endpoint)
            .await?;

        let authorizer = OffchainAssetReceiptVaultAuthorizerV1::new(
            self.authorizer_address,
            &provider,
        );
        let deposit_role = keccak256("DEPOSIT");
        authorizer
            .grantRole(deposit_role, to)
            .send()
            .await
            .map_err(|e| {
                LocalEvmError::DeploymentFailed(format!(
                    "Grant DEPOSIT role: {e}"
                ))
            })?
            .get_receipt()
            .await
            .map_err(|e| {
                LocalEvmError::DeploymentFailed(format!(
                    "Grant DEPOSIT role receipt: {e}"
                ))
            })?;

        Ok(())
    }
}
