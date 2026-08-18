pub const ROLE_DEPOSIT: &str = "DEPOSIT";
pub const ROLE_WITHDRAW: &str = "WITHDRAW";
pub const ROLE_CERTIFY: &str = "CERTIFY";

use alloy::hex;
use alloy::network::EthereumWallet;
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{
    Address, B256, Bytes, U256, address, bytes, keccak256,
};
use alloy::providers::{PendingTransactionError, Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::SolCall;
use alloy::sol_types::SolValue;
use alloy::transports::{RpcError, TransportErrorKind};
use apalis_sqlite::SqlitePool as ApalisSqlitePool;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use event_sorcery::{Store, StoreBuilder};
use rocket::routes;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions,
};
use std::env::temp_dir;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use url::Url;
use uuid::Uuid;

use crate::account::Account;
use crate::alpaca::service::AlpacaConfig;
use crate::auth::{FailedAuthRateLimiter, test_auth_config};
use crate::bindings::{
    CloneFactory, OffchainAssetReceiptVault,
    OffchainAssetReceiptVaultAuthorizerV1, Receipt, ST0xOrchestrator,
};
use crate::config::{Config, Environment, LogLevel};
use crate::mint::Mint;
use crate::receipt_inventory::view::ReceiptInventoryViewReactor;
use crate::tokenized_asset::{
    AssetKey, Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    UnderlyingSymbol,
};
use crate::vault::mock::MockVaultService;
use crate::vault::{NetworkVaultServices, VaultService};
use crate::wallet::SignerConfig;

/// Builds Anvil with startup output enabled when stdout is piped by Alloy.
///
/// Foundry 1.6 suppresses its startup banner for non-TTY stdout in `auto`
/// color mode. Alloy 1.0.42 waits for that banner to discover the bound port,
/// so force non-colored output for every test instance. Nix wraps Anvil to
/// inject Git into `PATH`; on macOS, expanding a large `PATH` in that Bash
/// wrapper can exceed Alloy's startup timeout. The adjacent wrapped executable
/// is the real Anvil binary and does not need Git for these local test nodes.
pub(crate) fn test_anvil() -> Anvil {
    let builder = find_anvil().map_or_else(Anvil::new, |path| {
        let wrapped = path.with_file_name(".anvil-wrapped");
        if wrapped.is_file() { Anvil::at(wrapped) } else { Anvil::at(path) }
    });

    builder.args(["--color", "never"])
}

fn find_anvil() -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path)
            .map(|directory| directory.join("anvil"))
            .find(|candidate| candidate.is_file())
    })
}

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    UpgradeableBeacon,
    env!("ST0X_UPGRADEABLE_BEACON_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    BeaconProxy,
    env!("ST0X_BEACON_PROXY_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    StoxReceipt,
    env!("ST0X_STOX_RECEIPT_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    StoxReceiptVault,
    env!("ST0X_STOX_RECEIPT_VAULT_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    StoxOffchainAssetReceiptVaultBeaconSetDeployer,
    env!("ST0X_STOX_OARV_BEACON_SET_DEPLOYER_ABI")
);

/// The Zoltu deterministic CREATE2 factory (salt 0), as pinned by
/// rain-deploy's `LibRainDeploy.sol` (`ZOLTU_FACTORY`,
/// `ZOLTU_FACTORY_BYTECODE`). st0x.deploy deploys its production contracts
/// through this factory, so their addresses depend only on creation code —
/// etching the factory on Anvil and replaying the deploys reproduces the
/// exact production addresses baked into `ST0xOrchestrator`'s bytecode.
const ZOLTU_FACTORY: Address =
    address!("0x7A0D94F55792C434d74a40883C6ed8545E406D12");

const ZOLTU_FACTORY_BYTECODE: Bytes =
    bytes!("0x60003681823780368234f58015156014578182fd5b80825250506014600cf3");

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

/// Anvil local chain ID (Base test runtime).
pub const ANVIL_CHAIN_ID: u64 = 31337;

/// ReceiptInventory aggregate id for integration tests (`{chain_id}:{vault:#x}`).
#[must_use]
pub fn receipt_inventory_aggregate_id(chain_id: u64, vault: Address) -> String {
    format!("{chain_id}:{vault:#x}")
}

/// Chain ID for the Ethereum test runtime in multichain integration tests.
pub const ETHEREUM_TEST_CHAIN_ID: u64 = 1;

/// The default [`Config`] for tests.
///
/// The single definition every test builds on, so a new field is added in one
/// place and cannot acquire a different default per module. Override what a
/// test cares about with struct-update syntax:
///
/// ```ignore
/// let config = Config { vault_mode_config, ..test_config() };
/// ```
pub fn test_config() -> Config {
    Config {
        database_url: "sqlite::memory:".to_string(),
        database_max_connections: 5,
        rpc_url: Url::parse("wss://localhost:8545").expect("valid test URL"),
        chain_id: ANVIL_CHAIN_ID,
        signer: SignerConfig::Local(B256::ZERO),
        backfill_start_block: 0,
        receipt_poll_interval: crate::RECEIPT_POLL_INTERVAL,
        auth: test_auth_config().expect("valid test auth config"),
        behind_proxy: false,
        log_level: LogLevel::Debug,
        environment: Environment::Development,
        hyperdx: None,
        alpaca: AlpacaConfig::test_default(),
        subgraph_url: Url::parse("http://localhost:0/subgraph")
            .expect("valid test URL"),
        chains: Vec::new(),
        vault_mode_config: crate::config::VaultModeConfig::default(),
    }
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
    // Both sqlx major versions must address the same file: private in-memory
    // databases do not share the Jobs table used by the confirmation route.
    let database_path =
        temp_dir().join(format!("st0x-issuance-test-{}.db", Uuid::new_v4()));
    let database_url = format!("sqlite:{}", database_path.display());

    let options = SqliteConnectOptions::from_str(&database_url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    let apalis_options =
        apalis_sqlite::SqliteConnectOptions::from_str(&database_url)?
            .pragma("journal_mode", "WAL")
            .busy_timeout(Duration::from_secs(5));
    let apalis_pool = ApalisSqlitePool::connect_with(apalis_options).await?;

    // Setup Account store (event-sorcery)
    let (account_store, _account_projection) =
        StoreBuilder::<Account>::new(pool.clone()).build(()).await?;

    // Setup TokenizedAsset store (event-sorcery)
    let (tokenized_asset_store, _tokenized_asset_projection) =
        StoreBuilder::<TokenizedAsset>::new(pool.clone()).build(()).await?;

    // Setup Mint store (event-sorcery), mirroring the production wiring: the
    // reactor keeps receipt_inventory_view in sync with Mint events.
    let (mint_store, _mint_projection) =
        StoreBuilder::<Mint>::new(pool.clone())
            .with(Arc::new(ReceiptInventoryViewReactor::new(pool.clone())))
            .build(())
            .await?;

    // Seed initial assets
    seed_test_assets(&tokenized_asset_store).await?;

    let rate_limiter = FailedAuthRateLimiter::new()?;
    let vault: Arc<dyn VaultService> =
        Arc::new(MockVaultService::new_success());
    let vault_services = NetworkVaultServices::with_single_vault(
        Network::Base,
        ANVIL_CHAIN_ID,
        vault,
    );

    // Build rocket
    Ok(rocket::build()
        .manage(test_config())
        .manage(account_store)
        .manage(tokenized_asset_store)
        .manage(mint_store)
        .manage(rate_limiter)
        .manage(pool)
        .manage(apalis_pool)
        .manage(vault_services)
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
    store: &Store<TokenizedAsset>,
) -> Result<(), anyhow::Error> {
    let assets = vec![
        (
            "AAPL",
            "tAAPL",
            address!("0x1234567890abcdef1234567890abcdef12345678"),
        ),
        (
            "TSLA",
            "tTSLA",
            address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
        ),
    ];

    for (underlying, token, vault) in assets {
        let underlying = UnderlyingSymbol::new(underlying)?;
        let command = TokenizedAssetCommand::Add {
            underlying: underlying.clone(),
            token: TokenSymbol::new(token),
            network: Network::Base,
            vault,
        };

        let key = AssetKey::new(underlying.clone(), Network::Base);
        match store.send(&key, command).await {
            Ok(()) | Err(event_sorcery::AggregateError::AggregateConflict) => {}
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
    pub chain_id: u64,
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
        Self::with_chain_id(ANVIL_CHAIN_ID).await
    }

    async fn connect(&self) -> Result<impl Provider, LocalEvmError> {
        let signer = PrivateKeySigner::from_bytes(&self.private_key)?;
        let wallet = EthereumWallet::from(signer);

        Ok(ProviderBuilder::new()
            .wallet(wallet)
            .connect(&self.endpoint)
            .await?)
    }

    /// Creates a new [`LocalEvm`] on an Anvil instance with the given chain ID.
    ///
    /// # Errors
    ///
    /// Returns an error if Anvil startup, provider connection, or contract
    /// deployment fails.
    pub async fn with_chain_id(chain_id: u64) -> Result<Self, LocalEvmError> {
        let anvil = test_anvil().chain_id(chain_id).spawn();
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
            chain_id,
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
        self.grant_role(ROLE_DEPOSIT, to).await
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
        self.grant_role(ROLE_WITHDRAW, to).await
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
        self.grant_role(ROLE_CERTIFY, to).await
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
        let provider = self.connect().await?;

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
        let provider = self.connect().await?;

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
        let provider = self.connect().await?;

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
        let provider = self.connect().await?;

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
        let provider = self.connect().await?;

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

    /// Burns (withdraws) shares directly on-chain for a specific receipt.
    ///
    /// This bypasses the issuance API and calls `redeem()` directly via the
    /// vault contract. Useful for simulating external burns that occurred
    /// outside the service.
    ///
    /// # Errors
    ///
    /// Returns an error if signer creation, provider connection, or the redeem
    /// transaction fails.
    pub async fn withdraw_directly(
        &self,
        receipt_id: U256,
        shares: U256,
        receiver: Address,
    ) -> Result<(), LocalEvmError> {
        let provider = self.connect().await?;

        let vault =
            OffchainAssetReceiptVault::new(self.vault_address, &provider);

        vault
            .redeem(
                shares,
                receiver,
                self.wallet_address,
                receipt_id,
                Bytes::new(),
            )
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(())
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
        let provider = self.connect().await?;

        let authorizer = OffchainAssetReceiptVaultAuthorizerV1::new(
            authorizer_address,
            &provider,
        );
        let role = keccak256(role_name);
        authorizer.grantRole(role, to).send().await?.get_receipt().await?;

        Ok(())
    }

    /// Deploys `ST0xOrchestrator` behind a beacon proxy, wires it to the
    /// default vault, and grants `MINT_ROLE` + `BURN_ROLE` to the deployer
    /// wallet so tests can call `mint` / `burn` directly.
    ///
    /// Setup steps:
    /// 1. Reproduce the pinned production vault beacon set at its canonical
    ///    addresses so the orchestrator's vault-logic version lock passes
    ///    (see `deploy_pinned_vault_beacon_set`).
    /// 2. Deploy `ST0xOrchestrator` as the implementation contract. The
    ///    constructor calls `_disableInitializers()` on the impl's own storage
    ///    — this is the standard OZ upgradeable pattern and does NOT prevent
    ///    `initialize()` from succeeding on a proxy.
    /// 3. Deploy `UpgradeableBeacon(impl, wallet)` to hold the implementation.
    /// 4. Deploy `BeaconProxy(beacon, initialize_calldata)` — the constructor
    ///    delegatecalls `initialize(wallet)` via the beacon, setting up roles.
    /// 5. Grant `MINT_ROLE` + `BURN_ROLE` on the proxy to the deployer wallet.
    /// 6. Grant `DEPOSIT` + `WITHDRAW` on the vault's authorizer to the proxy.
    ///
    /// # Errors
    ///
    /// Returns an error if any deployment or role-granting transaction fails.
    pub async fn deploy_orchestrator(&self) -> Result<Address, LocalEvmError> {
        let provider = self.connect().await?;

        Self::deploy_pinned_vault_beacon_set(&provider).await?;

        // Deploy the implementation. Constructor calls _disableInitializers()
        // on the impl's own storage — that's intentional OZ pattern.
        let impl_instance = ST0xOrchestrator::deploy(&provider).await?;
        let impl_address = *impl_instance.address();

        // Deploy a beacon pointing to the implementation.
        let beacon = UpgradeableBeacon::deploy(
            &provider,
            impl_address,
            self.wallet_address,
        )
        .await?;
        let beacon_address = *beacon.address();

        // Encode initialize(owner) as BeaconProxy init calldata. The proxy
        // constructor delegatecalls this on the implementation via the beacon,
        // initialising the proxy's own storage (fresh — separate from impl).
        let init_data = Bytes::from(
            ST0xOrchestrator::initializeCall { owner: self.wallet_address }
                .abi_encode(),
        );

        let proxy =
            BeaconProxy::deploy(&provider, beacon_address, init_data).await?;
        let orchestrator_address = *proxy.address();

        // Grant MINT_ROLE + BURN_ROLE on the proxy to the deployer wallet.
        // This must happen before the grant_role_on_authorizer calls below:
        // `provider`'s recommended fillers cache nonces locally
        // (alloy-provider fillers/nonce.rs: `NonceFiller<M: NonceManager =
        // CachedNonceManager>`), while each helper builds a fresh provider —
        // interleaving their transactions leaves this provider's cached
        // nonce stale and its next send fails with "nonce too low".
        let orchestrator =
            ST0xOrchestrator::new(orchestrator_address, &provider);
        let mint_role = orchestrator.MINT_ROLE().call().await?;
        let burn_role = orchestrator.BURN_ROLE().call().await?;

        orchestrator
            .grantRole(mint_role, self.wallet_address)
            .send()
            .await?
            .get_receipt()
            .await?;

        orchestrator
            .grantRole(burn_role, self.wallet_address)
            .send()
            .await?
            .get_receipt()
            .await?;

        // Grant DEPOSIT + WITHDRAW on the vault's authorizer to the orchestrator,
        // using the same `provider` to avoid the nonce-caching hazard described above.
        let authorizer = OffchainAssetReceiptVaultAuthorizerV1::new(
            self.authorizer_address,
            &provider,
        );
        authorizer
            .grantRole(keccak256(ROLE_DEPOSIT), orchestrator_address)
            .send()
            .await?
            .get_receipt()
            .await?;

        authorizer
            .grantRole(keccak256(ROLE_WITHDRAW), orchestrator_address)
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(orchestrator_address)
    }

    // ST0xOrchestrator refuses to initialize (and mint/burn) unless the
    // production vault beacon set exists at the address baked into its
    // bytecode and its beacons point at the pinned implementations:
    // ST0xOrchestrator.initialize -> _checkVaultLogic (ST0xOrchestrator.sol)
    // reads IST0xVaultBeaconSet(LibProdDeployCurrent
    // .STOX_OFFCHAIN_ASSET_RECEIPT_VAULT_BEACON_SET_DEPLOYER) and compares
    // beacon implementations against LibProdDeployCurrent.STOX_RECEIPT_VAULT
    // and .STOX_RECEIPT. Those addresses are CREATE2 outputs of the Zoltu
    // factory (salt 0), so replaying the same deploys on Anvil reproduces
    // them exactly — this mirrors upstream's test setup
    // (st0x.deploy test/lib/LibTestDeploy.sol
    // `deployOffchainAssetReceiptVaultBeaconSet`). Order matters: the
    // beacon-set deployer's constructor points its beacons at the receipt
    // and vault implementations, which must already have code.
    async fn deploy_pinned_vault_beacon_set(
        provider: &impl Provider,
    ) -> Result<(), LocalEvmError> {
        provider
            .raw_request::<_, ()>(
                "anvil_setCode".into(),
                (ZOLTU_FACTORY, ZOLTU_FACTORY_BYTECODE),
            )
            .await?;

        for creation_code in [
            &StoxReceipt::BYTECODE,
            &StoxReceiptVault::BYTECODE,
            &StoxOffchainAssetReceiptVaultBeaconSetDeployer::BYTECODE,
        ] {
            let deploy_via_factory = TransactionRequest::default()
                .to(ZOLTU_FACTORY)
                .input(creation_code.clone().into());

            let receipt = provider
                .send_transaction(deploy_via_factory)
                .await?
                .get_receipt()
                .await?;

            if !receipt.status() {
                return Err(LocalEvmError::EventNotFound);
            }
        }

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
        let provider = self.connect().await?;

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

/// Maps `module_path!()` to the domain target used in `target:` on
/// tracing macros. Falls back to the module path itself for modules
/// that don't use custom targets.
#[cfg(test)]
pub(crate) fn domain_target_for_module(module: &str) -> &'static str {
    let module = module.strip_suffix("::tests").unwrap_or(module);

    if module.contains("::mint") {
        "mint"
    } else if module.contains("::redemption") {
        "redemption"
    } else if module.contains("::receipt_inventory") {
        "receipt"
    } else if module.contains("::account") {
        "account"
    } else if module.contains("::tokenized_asset")
        || module.contains("::underlying")
    {
        "asset"
    } else if module.contains("::alpaca") {
        "alpaca"
    } else if module.contains("::auth") {
        "auth"
    } else if module.contains("::wallet") {
        "wallet"
    } else if module.contains("::admin") {
        "admin"
    } else if module.contains("::vault") {
        "vault"
    } else {
        "startup"
    }
}

/// Checks whether any log line at the given level, scoped to the
/// caller's domain target, contains all snippets.
#[cfg(test)]
macro_rules! logs_contain_at {
    ($level:expr, $snippets:expr) => {
        $crate::test_utils::log_count_at!($level, $snippets) > 0
    };
}

/// Counts log lines at the given level, scoped to the caller's
/// domain target, that contain all snippets.
#[cfg(test)]
macro_rules! log_count_at {
    ($level:expr, $snippets:expr) => {{
        let logs = {
            let buf = tracing_test::internal::global_buf().lock().unwrap();
            String::from_utf8_lossy(&buf).into_owned()
        };
        let target =
            $crate::test_utils::domain_target_for_module(module_path!());
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
                line.contains(target)
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
