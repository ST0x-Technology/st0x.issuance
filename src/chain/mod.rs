//! Per-chain RPC, vault service, and backfill configuration.
//!
//! Wires a single Base runtime from legacy env vars at startup ([RAI-1204]).
//! Later multichain PRs route side effects through `ChainRegistry::get(network)`.

use alloy::providers::{Provider, ProviderBuilder};
use futures::stream::{self, StreamExt, TryStreamExt};
use itertools::Itertools;
use sqlx::{Pool, Sqlite};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

use crate::config::{ConfigError, wss_to_http};
use crate::fireblocks::{
    FireblocksVaultService, SignerConfig, resolve_local_signer,
};
use crate::tokenized_asset::Network;
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, list_enabled_assets,
};
use crate::vault::rain_meta::OaSchemaCache;
use crate::vault::{VaultService, service::RealBlockchainService};

/// Per-chain RPC, vault, and subgraph settings for [`ChainRegistry`].
#[derive(Clone)]
pub struct ChainConfig {
    pub network: Network,
    pub chain_id: u64,
    pub rpc_url: Url,
    pub subgraph_url: Url,
    pub backfill_start_block: u64,
}

pub(crate) struct ChainRuntime<P> {
    pub(crate) network: Network,
    pub(crate) chain_id: u64,
    pub(crate) vault_service: Arc<dyn VaultService>,
    pub(crate) http_provider: P,
    pub(crate) subgraph_url: Url,
    pub(crate) backfill_start_block: u64,
}

pub(crate) struct ChainRegistry<P> {
    runtimes: HashMap<Network, ChainRuntime<P>>,
}

pub(crate) async fn validate_configured_asset_networks<P: Sync>(
    pool: &Pool<Sqlite>,
    registry: &ChainRegistry<P>,
) -> Result<(), AssetNetworkValidationError> {
    list_enabled_assets(pool).await?.iter().try_for_each(|asset| {
        registry
            .get_required(&asset.network)
            .map(drop)
            .map_err(AssetNetworkValidationError::from)
    })
}

/// Startup validation error: every live asset's network must have a chain
/// config entry. Kept separate from [`ChainRegistryError`] so the internal
/// tokenized-asset view error stays `pub(crate)` instead of leaking through
/// the public `Config::parse` error chain.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AssetNetworkValidationError {
    #[error("failed to load tokenized assets for startup validation")]
    AssetView(#[from] TokenizedAssetViewError),
    #[error(transparent)]
    ChainRegistry(#[from] ChainRegistryError),
}

#[derive(Debug, thiserror::Error)]
pub enum ChainRegistryError {
    #[error("duplicate chain configuration for network {network}")]
    DuplicateNetwork { network: Network },
    #[error(
        "duplicate chain_id {chain_id}: configured for {first} and {second}"
    )]
    DuplicateChainId { chain_id: u64, first: Network, second: Network },
    #[error("network {network} is not configured")]
    NetworkNotConfigured { network: Network },
    #[error("Fireblocks chain asset id missing for chain_id {chain_id}")]
    MissingFireblocksChainAssetId { chain_id: u64 },
    #[error(transparent)]
    Config(Box<ConfigError>),
}

impl From<ConfigError> for ChainRegistryError {
    fn from(error: ConfigError) -> Self {
        Self::Config(Box::new(error))
    }
}

impl<P> ChainRegistry<P> {
    pub(crate) fn get(&self, network: &Network) -> Option<&ChainRuntime<P>> {
        self.runtimes.get(network)
    }

    pub(crate) fn get_required(
        &self,
        network: &Network,
    ) -> Result<&ChainRuntime<P>, ChainRegistryError> {
        self.get(network).ok_or(ChainRegistryError::NetworkNotConfigured {
            network: network.clone(),
        })
    }

    pub(crate) fn base(&self) -> Result<&ChainRuntime<P>, ChainRegistryError> {
        self.get_required(&Network::Base)
    }

    pub(crate) fn clone_vault_services(
        &self,
    ) -> HashMap<Network, Arc<dyn VaultService>> {
        self.runtimes
            .iter()
            .map(|(network, runtime)| {
                (network.clone(), runtime.vault_service.clone())
            })
            .collect()
    }

    pub(crate) fn runtimes(
        &self,
    ) -> impl Iterator<Item = (&Network, &ChainRuntime<P>)> {
        self.runtimes.iter()
    }
}

pub(crate) fn validate_chain_configs(
    configs: &[ChainConfig],
) -> Result<(), ChainRegistryError> {
    configs.iter().tuple_combinations().try_for_each(|(first, second)| {
        if first.network == second.network {
            return Err(ChainRegistryError::DuplicateNetwork {
                network: second.network.clone(),
            });
        }

        if first.chain_id == second.chain_id {
            return Err(ChainRegistryError::DuplicateChainId {
                chain_id: second.chain_id,
                first: first.network.clone(),
                second: second.network.clone(),
            });
        }

        Ok(())
    })
}

pub(crate) async fn build_chain_registry(
    configs: Vec<ChainConfig>,
    signer: &SignerConfig,
) -> Result<ChainRegistry<impl Provider + Clone + use<>>, ChainRegistryError> {
    validate_chain_configs(&configs)?;

    if let SignerConfig::Fireblocks(fireblocks) = signer {
        configs.iter().try_for_each(|config| {
            fireblocks.chain_asset_ids.get(config.chain_id).map(drop).ok_or(
                ChainRegistryError::MissingFireblocksChainAssetId {
                    chain_id: config.chain_id,
                },
            )
        })?;
    }

    let runtimes = stream::iter(configs)
        .then(|config| build_chain_runtime(config, signer))
        .map_ok(|runtime| (runtime.network.clone(), runtime))
        .try_collect()
        .await?;

    Ok(ChainRegistry { runtimes })
}

async fn build_chain_runtime(
    config: ChainConfig,
    signer: &SignerConfig,
) -> Result<ChainRuntime<impl Provider + Clone + use<>>, ChainRegistryError> {
    let ChainConfig {
        network,
        chain_id,
        rpc_url,
        subgraph_url,
        backfill_start_block,
    } = config;

    let http_url = wss_to_http(&rpc_url)?;
    let http_provider = ProviderBuilder::new().connect_http(http_url);

    let rpc_chain_id =
        http_provider.get_chain_id().await.map_err(ConfigError::Rpc)?;
    if rpc_chain_id != chain_id {
        return Err(ConfigError::ChainIdMismatch {
            configured: chain_id,
            from_rpc: rpc_chain_id,
        }
        .into());
    }

    let oa_schema_cache = Arc::new(
        OaSchemaCache::new(subgraph_url.clone())
            .map_err(ConfigError::Reqwest)?,
    );

    let vault_service: Arc<dyn VaultService> = match signer {
        SignerConfig::Local(key) => {
            let resolved = resolve_local_signer(key, chain_id)
                .map_err(|err| ConfigError::SignerResolve(Box::new(err)))?;

            let signing_provider = ProviderBuilder::new()
                .wallet(resolved.wallet)
                .connect_http(wss_to_http(&rpc_url)?);

            Arc::new(RealBlockchainService::new(
                signing_provider,
                oa_schema_cache,
            ))
        }

        SignerConfig::Fireblocks(env) => {
            let service = FireblocksVaultService::new(
                env,
                http_provider.clone(),
                chain_id,
                oa_schema_cache,
            )
            .map_err(|err| ConfigError::FireblocksVault(Box::new(err)))?;

            Arc::new(service)
        }
    };

    Ok(ChainRuntime {
        network,
        chain_id,
        vault_service,
        http_provider,
        subgraph_url,
        backfill_start_block,
    })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use event_sorcery::StoreBuilder;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;
    use crate::tokenized_asset::{
        AssetKey, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        UnderlyingSymbol,
    };

    fn base_config(chain_id: u64) -> ChainConfig {
        ChainConfig {
            network: Network::Base,
            chain_id,
            rpc_url: Url::parse("wss://localhost:8545").unwrap(),
            subgraph_url: Url::parse("http://localhost:0/subgraph").unwrap(),
            backfill_start_block: 1,
        }
    }

    #[test]
    fn validate_rejects_duplicate_chain_id_across_networks() {
        let configs = vec![
            base_config(8453),
            ChainConfig {
                network: Network::Ethereum,
                chain_id: 8453,
                rpc_url: Url::parse("wss://localhost:8546").unwrap(),
                subgraph_url: Url::parse("http://localhost:0/subgraph")
                    .unwrap(),
                backfill_start_block: 1,
            },
        ];

        assert!(matches!(
            validate_chain_configs(&configs),
            Err(ChainRegistryError::DuplicateChainId { .. })
        ));
    }

    #[test]
    fn validate_rejects_duplicate_network() {
        let configs = vec![base_config(8453), base_config(8453)];

        assert!(matches!(
            validate_chain_configs(&configs),
            Err(ChainRegistryError::DuplicateNetwork { .. })
        ));
    }

    #[test]
    fn get_required_errors_on_miss() {
        let registry: ChainRegistry<()> =
            ChainRegistry { runtimes: HashMap::new() };

        assert!(matches!(
            registry.get_required(&Network::Base),
            Err(ChainRegistryError::NetworkNotConfigured { .. })
        ));
    }

    #[tokio::test]
    async fn validate_configured_asset_networks_passes_with_no_assets() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let registry: ChainRegistry<()> =
            ChainRegistry { runtimes: HashMap::new() };

        validate_configured_asset_networks(&pool, &registry).await.unwrap();
    }

    #[tokio::test]
    async fn validate_configured_asset_networks_rejects_unconfigured_network() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let (store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .expect("Failed to build tokenized asset store");

        let underlying = UnderlyingSymbol::new("AAPL");
        let key = AssetKey::new(underlying.clone(), Network::Base);
        store
            .send(
                &key,
                TokenizedAssetCommand::Add {
                    underlying: underlying.clone(),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
                    vault: address!(
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                },
            )
            .await
            .expect("Failed to add asset");

        let registry: ChainRegistry<()> =
            ChainRegistry { runtimes: HashMap::new() };

        assert!(matches!(
            validate_configured_asset_networks(&pool, &registry).await,
            Err(AssetNetworkValidationError::ChainRegistry(
                ChainRegistryError::NetworkNotConfigured { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn build_rejects_missing_fireblocks_chain_asset_id() {
        use crate::fireblocks::{
            Environment, FireblocksConfig, parse_chain_asset_ids,
        };

        let fireblocks = FireblocksConfig {
            api_user_id: "test-user".to_string().into(),
            secret: vec![],
            vault_account_id: "0".to_string().into(),
            chain_asset_ids: parse_chain_asset_ids("1:ETH").unwrap(),
            environment: Environment::Sandbox,
        };

        let result = build_chain_registry(
            vec![base_config(8453)],
            &SignerConfig::Fireblocks(fireblocks),
        )
        .await;

        assert!(matches!(
            result,
            Err(ChainRegistryError::MissingFireblocksChainAssetId {
                chain_id: 8453
            })
        ));
    }
}
