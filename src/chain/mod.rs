//! Per-chain RPC, vault service, and backfill configuration.
//!
//! `ChainRegistry` maps each configured `Network` to the runtime bundle needed
//! for on-chain side effects, built once at startup — currently a single Base
//! entry wired from legacy env vars.

use alloy::providers::fillers::BlobGasFiller;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::transports::{RpcError, TransportErrorKind};
use futures::stream::{self, StreamExt, TryStreamExt};
use itertools::Itertools;
use sqlx::{Pool, Sqlite};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, info, warn};
use url::Url;

use crate::config::{InvalidRpcScheme, wss_to_http};
use crate::tokenized_asset::Network;
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, list_enabled_assets,
};
use crate::vault::rain_meta::OaSchemaCache;
use crate::vault::{VaultService, service::RealBlockchainService};
use crate::wallet::{
    SignerConfig, SignerResolveError, local::resolve_local_signer,
    turnkey::resolve_turnkey_signer,
};

const MAX_CHAIN_RUNTIME_BUILD_CONCURRENCY: usize = 4;

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

/// Networks that have a chain configuration, extracted from
/// [`ChainRegistry`] so HTTP handlers can reject asset registrations for
/// unconfigured networks without holding the provider-generic registry.
#[derive(Debug, Clone)]
pub(crate) struct ConfiguredNetworks(HashSet<Network>);

impl ConfiguredNetworks {
    pub(crate) fn contains(&self, network: Network) -> bool {
        self.0.contains(&network)
    }
}

impl FromIterator<Network> for ConfiguredNetworks {
    fn from_iter<I: IntoIterator<Item = Network>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

pub(crate) async fn validate_configured_asset_networks<P: Sync>(
    pool: &Pool<Sqlite>,
    registry: &ChainRegistry<P>,
) -> Result<(), AssetNetworkValidationError> {
    list_enabled_assets(pool).await?.iter().try_for_each(|asset| {
        registry.get_required(asset.network).map(drop).map_err(|error| {
            warn!(
                target: "startup",
                network = %asset.network,
                underlying = %asset.underlying,
                "Enabled asset network is not configured"
            );
            AssetNetworkValidationError::from(error)
        })
    })?;
    info!(
        target: "startup",
        "Validated configured networks for all enabled assets"
    );
    Ok(())
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
    #[error(
        "Chain ID mismatch: configured {configured}, RPC returned {from_rpc}"
    )]
    ChainIdMismatch { configured: u64, from_rpc: u64 },
    #[error("RPC error")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    InvalidRpcScheme(#[from] InvalidRpcScheme),
    #[error("Failed to resolve signer: {0}")]
    SignerResolve(#[from] SignerResolveError),
}

impl<P> ChainRegistry<P> {
    pub(crate) fn get(&self, network: Network) -> Option<&ChainRuntime<P>> {
        self.runtimes.get(&network)
    }

    pub(crate) fn get_required(
        &self,
        network: Network,
    ) -> Result<&ChainRuntime<P>, ChainRegistryError> {
        self.get(network).ok_or_else(|| {
            debug!(
                target: "startup",
                %network,
                "Chain registry lookup missed configured network"
            );
            ChainRegistryError::NetworkNotConfigured { network }
        })
    }

    pub(crate) fn base(&self) -> Result<&ChainRuntime<P>, ChainRegistryError> {
        self.get_required(Network::Base)
    }

    pub(crate) fn configured_networks(&self) -> ConfiguredNetworks {
        self.runtimes.keys().copied().collect()
    }

    pub(crate) fn clone_vault_services(
        &self,
    ) -> HashMap<Network, Arc<dyn VaultService>> {
        self.runtimes
            .iter()
            .map(|(network, runtime)| (*network, runtime.vault_service.clone()))
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
            warn!(
                target: "startup",
                network = %second.network,
                "Duplicate chain configuration for network"
            );
            return Err(ChainRegistryError::DuplicateNetwork {
                network: second.network,
            });
        }

        if first.chain_id == second.chain_id {
            warn!(
                target: "startup",
                chain_id = second.chain_id,
                first = %first.network,
                second = %second.network,
                "Duplicate chain_id across networks"
            );
            return Err(ChainRegistryError::DuplicateChainId {
                chain_id: second.chain_id,
                first: first.network,
                second: second.network,
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

    let runtimes = stream::iter(configs)
        .map(|config| build_chain_runtime(config, signer))
        .buffer_unordered(MAX_CHAIN_RUNTIME_BUILD_CONCURRENCY)
        .map_ok(|runtime| (runtime.network, runtime))
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

    let rpc_chain_id = http_provider.get_chain_id().await?;
    if rpc_chain_id != chain_id {
        return Err(ChainRegistryError::ChainIdMismatch {
            configured: chain_id,
            from_rpc: rpc_chain_id,
        });
    }

    let oa_schema_cache = Arc::new(OaSchemaCache::new(subgraph_url.clone())?);

    let resolved = match signer {
        SignerConfig::Local(key) => resolve_local_signer(key, chain_id)?,
        SignerConfig::Turnkey(env) => resolve_turnkey_signer(env, chain_id)?,
    };
    info!(
        target: "startup",
        signer_kind = ?resolved.kind,
        %network,
        "Signer backend resolved"
    );

    let signing_provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .with_gas_estimation()
        .filler(BlobGasFiller)
        .with_simple_nonce_management()
        .with_chain_id(chain_id)
        .wallet(resolved.wallet)
        .connect_http(wss_to_http(&rpc_url)?);

    let vault_service: Arc<dyn VaultService> =
        Arc::new(RealBlockchainService::new(signing_provider, oa_schema_cache));

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
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{
        AssetKey, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        UnderlyingSymbol,
    };
    use crate::vault::mock::MockVaultService;

    fn base_config(chain_id: u64) -> ChainConfig {
        ChainConfig {
            network: Network::Base,
            chain_id,
            rpc_url: Url::parse("wss://localhost:8545").unwrap(),
            subgraph_url: Url::parse("http://localhost:0/subgraph").unwrap(),
            backfill_start_block: 1,
        }
    }

    #[traced_test]
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

        assert!(
            matches!(
                validate_chain_configs(&configs),
                Err(ChainRegistryError::DuplicateNetwork { .. })
            ),
            "expected duplicate network rejection"
        );
        assert!(logs_contain_at!(
            Level::WARN,
            &["Duplicate chain configuration for network", "base"]
        ));
    }

    #[traced_test]
    #[test]
    fn get_required_errors_on_miss() {
        let registry: ChainRegistry<()> =
            ChainRegistry { runtimes: HashMap::new() };

        assert!(
            matches!(
                registry.get_required(Network::Base),
                Err(ChainRegistryError::NetworkNotConfigured { .. })
            ),
            "expected unconfigured network rejection"
        );
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Chain registry lookup missed configured network", "base"]
        ));
    }

    #[traced_test]
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
        assert!(logs_contain_at!(
            Level::INFO,
            &["Validated configured networks for all enabled assets"]
        ));
    }

    #[traced_test]
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

        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
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

        assert!(
            matches!(
                validate_configured_asset_networks(&pool, &registry).await,
                Err(AssetNetworkValidationError::ChainRegistry(
                    ChainRegistryError::NetworkNotConfigured { .. }
                ))
            ),
            "expected enabled asset on an unconfigured network to fail startup validation"
        );
        assert!(logs_contain_at!(
            Level::WARN,
            &["Enabled asset network is not configured", "AAPL", "base"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn validate_configured_asset_networks_passes_with_configured_asset() {
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

        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
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

        let base_runtime = ChainRuntime {
            network: Network::Base,
            chain_id: 8453,
            vault_service: Arc::new(MockVaultService::new_success())
                as Arc<dyn VaultService>,
            http_provider: (),
            subgraph_url: Url::parse("http://localhost:0/subgraph").unwrap(),
            backfill_start_block: 1,
        };
        let registry = ChainRegistry {
            runtimes: HashMap::from([(Network::Base, base_runtime)]),
        };

        validate_configured_asset_networks(&pool, &registry).await.unwrap();
        assert!(logs_contain_at!(
            Level::INFO,
            &["Validated configured networks for all enabled assets"]
        ));
    }
}
