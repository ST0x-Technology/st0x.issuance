use std::time::Duration;

use alloy::primitives::{
    Address, B256, Bytes, Signature, SignatureError, TxHash, U256,
};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionReceipt;
use alloy::transports::{RpcError, TransportErrorKind};
use fireblocks_sdk::apis;
use fireblocks_sdk::apis::transactions_api::{
    CreateTransactionError, CreateTransactionParams,
    GetTransactionByExternalIdError, GetTransactionByExternalIdParams,
};
use fireblocks_sdk::apis::whitelisted_contracts_api::GetContractsError;
use fireblocks_sdk::models::signed_message_signature::V as SignatureParity;
use fireblocks_sdk::models::{self, TransactionStatus};
use fireblocks_sdk::{Client, ClientBuilder};
use tracing::{debug, warn};

use super::config::{
    AssetId, ChainAssetIds, ContractWalletId, Environment, FireblocksConfig,
    FireblocksVaultAccountId,
};

/// Fireblocks-specific errors that can occur during vault operations.
#[derive(Debug, thiserror::Error)]
pub enum FireblocksVaultError {
    #[error("Fireblocks SDK error: {0}")]
    Fireblocks(#[from] fireblocks_sdk::FireblocksError),
    #[error("Fireblocks API error: {0}")]
    Api(#[from] fireblocks_sdk::apis::Error<CreateTransactionError>),
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("no deposit address found for vault {}, asset {}", vault_id.as_str(), asset_id.as_str())]
    NoAddress { vault_id: FireblocksVaultAccountId, asset_id: AssetId },
    #[error("invalid address from Fireblocks: {0}")]
    FromHex(#[from] alloy::hex::FromHexError),
    #[error("Fireblocks response did not return a transaction ID")]
    MissingTransactionId,
    #[error(
        "Fireblocks transaction {tx_id} reached terminal status: {status:?}"
    )]
    TransactionFailed { tx_id: String, status: TransactionStatus },
    #[error(
        "Fireblocks transaction {tx_id} is still {status:?} after the polling \
         window — it may yet complete (console approval can take longer). \
         Approve or reject it in the Fireblocks console, then re-run this \
         command: the deterministic externalTxId resumes the same transaction \
         instead of submitting a second one."
    )]
    PollTimedOut { tx_id: String, status: TransactionStatus },
    #[error(
        "Fireblocks transaction {tx_id} did not include a transaction hash"
    )]
    MissingTxHash { tx_id: String },
    #[error("invalid transaction hash from Fireblocks: {hash}")]
    InvalidTxHash {
        hash: String,
        #[source]
        source: alloy::hex::FromHexError,
    },
    #[error("Fireblocks contracts API error: {0}")]
    ContractsApi(#[from] fireblocks_sdk::apis::Error<GetContractsError>),
    #[error("contract {contract} is not whitelisted in Fireblocks")]
    ContractNotWhitelisted { contract: Address },
    #[error("no asset ID configured for chain {chain_id}")]
    UnknownChain { chain_id: u64 },
    #[error("transaction {tx_hash} has no receipt after confirmation")]
    MissingReceipt { tx_hash: TxHash },
    #[error(
        "failed to look up existing transaction by externalTxId: {external_tx_id}"
    )]
    ExternalTxIdLookupFailed {
        external_tx_id: String,
        #[source]
        source: Box<apis::Error<GetTransactionByExternalIdError>>,
    },
    #[error(
        "every submission attempt under {base_external_tx_id} (and its \
         -retry-N successors, {attempts} in total) resolved to a terminally \
         failed prior transaction; something re-fails this transfer faster \
         than the ids can be walked — investigate the failures in the \
         Fireblocks console before retrying"
    )]
    RetryAttemptsExhausted { base_external_tx_id: String, attempts: u32 },
    #[error(
        "Fireblocks RAW signing transaction {tx_id} completed without a \
         signed message signature"
    )]
    MissingSignature { tx_id: String },
    #[error(
        "Fireblocks RAW signature for transaction {tx_id} is missing its r, \
         s, or v component"
    )]
    IncompleteSignature { tx_id: String },
    #[error("invalid hex in a Fireblocks RAW signature component: {0}")]
    SignatureComponent(#[from] alloy::primitives::ruint::ParseError),
    #[error("failed to recover signer address from RAW signature: {0}")]
    SignatureRecovery(#[from] SignatureError),
    #[error(
        "Fireblocks RAW signature recovers to {recovered}, expected \
         {expected} -- the vault account signed with an unexpected key or the \
         response is corrupted; do NOT broadcast"
    )]
    RawSignerMismatch { expected: Address, recovered: Address },
}

/// How [`FireblocksVaultService::submit_contract_call`] obtained its
/// transaction id: freshly created for the given `externalTxId`, or recovered
/// from a previous run that already claimed that id.
///
/// Callers deciding whether to retry under a fresh id need the distinction:
/// Fireblocks reserves an `externalTxId` permanently, so a *recovered*
/// transaction that turns out terminally failed belongs to a previous attempt
/// whose id is spent — only then is submitting under a successor id correct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Submission {
    Created { tx_id: String },
    Recovered { tx_id: String },
}

impl Submission {
    #[must_use]
    pub fn tx_id(&self) -> &str {
        match self {
            Self::Created { tx_id } | Self::Recovered { tx_id } => tx_id,
        }
    }
}

/// Upper bound on the `externalTxId` successors
/// [`FireblocksVaultService::submit_contract_call_to_completion`] walks
/// through. Each step means a previous attempt failed terminally after its
/// cause was supposedly fixed; several in a row is an operational problem no
/// amount of fresh ids will solve.
const MAX_SUBMISSION_ATTEMPTS: u32 = 5;

/// Fetches the vault account address from Fireblocks.
///
/// This is used to derive the bot wallet address from the Fireblocks configuration.
/// It builds a temporary client, fetches the deposit address for the default asset,
/// and returns the address.
///
/// # Errors
///
/// Returns an error if the client cannot be built, the API call fails, or no
/// address exists for the configured vault account and asset.
pub async fn fetch_vault_address(
    config: &FireblocksConfig,
) -> Result<Address, FireblocksVaultError> {
    let mut builder =
        ClientBuilder::new(config.api_user_id.as_str(), &config.secret);
    if config.environment == Environment::Sandbox {
        builder = builder.use_sandbox();
    }
    let client = builder.build()?;

    vault_address_via(
        &client,
        &config.vault_account_id,
        config.chain_asset_ids.default_asset_id(),
    )
    .await
}

/// Fetches and parses the vault account's deposit address for `asset_id`.
///
/// Split from [`fetch_vault_address`] so the address-selection and
/// empty-response handling can be exercised against a mock server.
async fn vault_address_via(
    client: &Client,
    vault_account_id: &FireblocksVaultAccountId,
    asset_id: &AssetId,
) -> Result<Address, FireblocksVaultError> {
    let addresses =
        client.addresses(vault_account_id.as_str(), asset_id.as_str()).await?;

    let address_str = addresses
        .first()
        .and_then(|account_address| account_address.address.as_deref())
        .ok_or_else(|| FireblocksVaultError::NoAddress {
            vault_id: vault_account_id.clone(),
            asset_id: asset_id.clone(),
        })?;

    Ok(address_str.parse::<Address>()?)
}

/// Narrow Fireblocks client for migration-time RAW signing and the legacy
/// CONTRACT_CALL smoke path.
///
/// RAW returns only a signature over a caller-built hash. CONTRACT_CALL lets
/// Fireblocks build and broadcast through its transaction engine and remains
/// only for the optional whitelisted-contract smoke test.
///
/// The service holds a read-only RPC provider for receipt verification; the
/// retired mint/burn view calls did not survive into this slice.
pub struct FireblocksVaultService<P> {
    client: Client,
    vault_account_id: String,
    chain_asset_ids: ChainAssetIds,
    read_provider: P,
    chain_id: u64,
}

impl<P: Provider + Clone> FireblocksVaultService<P> {
    /// Creates a new Fireblocks vault service.
    ///
    /// # Arguments
    ///
    /// * `config` - Fireblocks configuration (with secret already loaded)
    /// * `read_provider` - Read-only RPC provider for view calls and receipt fetching
    /// * `chain_id` - The chain ID for transaction routing
    ///
    /// # Errors
    ///
    /// Returns an error if the Fireblocks client cannot be built.
    pub fn new(
        config: &FireblocksConfig,
        read_provider: P,
        chain_id: u64,
    ) -> Result<Self, FireblocksVaultError> {
        let mut builder =
            ClientBuilder::new(config.api_user_id.as_str(), &config.secret);
        if config.environment == Environment::Sandbox {
            builder = builder.use_sandbox();
        }
        let client = builder.build()?;

        debug!(target: "fireblocks", vault_account_id = %config.vault_account_id.as_str(),
            chain_asset_ids = ?config.chain_asset_ids,
            %chain_id,
            "Fireblocks vault service initialized"
        );

        Ok(Self {
            client,
            vault_account_id: config.vault_account_id.as_str().to_string(),
            chain_asset_ids: config.chain_asset_ids.clone(),
            read_provider,
            chain_id,
        })
    }

    /// Resolves a contract address to its Fireblocks whitelisted wallet ID.
    ///
    /// Queries Fireblocks' `GET /contracts` API and finds the wallet whose
    /// asset entry matches both the configured chain asset ID and the given
    /// contract address. This ensures transactions go through TAP policy
    /// controls instead of bypassing them with `OneTimeAddress`.
    ///
    /// # Errors
    ///
    /// Returns an error if no asset is configured for this chain, the API call
    /// fails, or the contract is not whitelisted.
    pub async fn resolve_contract_wallet(
        &self,
        contract: Address,
    ) -> Result<ContractWalletId, FireblocksVaultError> {
        let contract_address = contract.to_string().to_lowercase();
        let asset_id = self.chain_asset_ids.get(self.chain_id).ok_or(
            FireblocksVaultError::UnknownChain { chain_id: self.chain_id },
        )?;
        let expected_asset_id = asset_id.as_str();

        self.client
            .wallet_contract_api()
            .get_contracts()
            .await?
            .into_iter()
            .find_map(|wallet| {
                wallet
                    .assets
                    .iter()
                    .any(|asset| {
                        asset
                            .id
                            .as_ref()
                            .is_some_and(|id| id == expected_asset_id)
                            && asset.address.as_ref().is_some_and(|addr| {
                                addr.to_lowercase() == contract_address
                            })
                    })
                    .then_some(ContractWalletId::from(wallet.id))
            })
            .ok_or(FireblocksVaultError::ContractNotWhitelisted { contract })
    }

    /// Submits a CONTRACT_CALL transaction to Fireblocks.
    ///
    /// # Arguments
    ///
    /// * `contract_address` - The target contract address
    /// * `calldata` - The encoded function calldata
    /// * `note` - A descriptive note for the transaction
    /// * `external_tx_id` - Deterministic ID for idempotency across retries
    ///
    /// # Returns
    ///
    /// The Fireblocks transaction ID.
    ///
    /// # Errors
    ///
    /// Returns an error if no asset is configured for this chain, the contract
    /// is not whitelisted, or the submission fails and cannot be recovered by
    /// `externalTxId` lookup.
    /// Submits a CONTRACT_CALL and polls it to completion, walking to a fresh
    /// `externalTxId` when a recovered previous attempt turns out terminally
    /// failed.
    ///
    /// Fireblocks reserves an `externalTxId` forever, including for
    /// transactions that ended rejected or reverted. Retrying after the
    /// operator fixes the failure's cause (a TAP rule, expired certification)
    /// must therefore not reuse the spent id: the base id resumes only a
    /// still-pending or completed transaction, and each terminally failed
    /// prior attempt shifts submission to `{base}-retry-{n}`. A fresh
    /// submission that itself fails terminally is a real failure and
    /// propagates; only inherited corpses are walked past.
    ///
    /// # Errors
    ///
    /// As [`FireblocksVaultService::submit_contract_call`] and
    /// [`FireblocksVaultService::wait_for_completion`], plus
    /// [`FireblocksVaultError::RetryAttemptsExhausted`] when every candidate
    /// id is already spent by a terminally failed prior attempt.
    pub async fn submit_contract_call_to_completion(
        &self,
        contract_address: Address,
        calldata: &Bytes,
        note: &str,
        base_external_tx_id: &str,
    ) -> Result<B256, FireblocksVaultError> {
        for attempt in 0..MAX_SUBMISSION_ATTEMPTS {
            let external_tx_id = if attempt == 0 {
                base_external_tx_id.to_string()
            } else {
                format!("{base_external_tx_id}-retry-{attempt}")
            };

            let submission = self
                .submit_contract_call(
                    contract_address,
                    calldata,
                    note,
                    &external_tx_id,
                )
                .await?;

            match self.wait_for_completion(submission.tx_id()).await {
                Ok(tx_hash) => return Ok(tx_hash),
                Err(FireblocksVaultError::TransactionFailed {
                    tx_id,
                    status,
                }) if matches!(submission, Submission::Recovered { .. }) => {
                    warn!(target: "fireblocks",
                        %external_tx_id,
                        fireblocks_tx_id = %tx_id,
                        ?status,
                        "Recovered transaction from a previous attempt is \
                         terminally failed and its externalTxId is spent; \
                         submitting under a fresh retry id"
                    );
                }
                Err(err) => return Err(err),
            }
        }

        Err(FireblocksVaultError::RetryAttemptsExhausted {
            base_external_tx_id: base_external_tx_id.to_string(),
            attempts: MAX_SUBMISSION_ATTEMPTS,
        })
    }

    /// Submits a single CONTRACT_CALL transaction to Fireblocks under
    /// `external_tx_id`, reporting whether the id created a fresh transaction
    /// or recovered one a previous run already claimed.
    ///
    /// # Errors
    ///
    /// Returns an error if no asset is configured for this chain, the
    /// contract is not whitelisted, or the submission fails and cannot be
    /// recovered by `externalTxId` lookup.
    pub async fn submit_contract_call(
        &self,
        contract_address: Address,
        calldata: &Bytes,
        note: &str,
        external_tx_id: &str,
    ) -> Result<Submission, FireblocksVaultError> {
        let asset_id = self.chain_asset_ids.get(self.chain_id).ok_or(
            FireblocksVaultError::UnknownChain { chain_id: self.chain_id },
        )?;

        let wallet_id = self.resolve_contract_wallet(contract_address).await?;

        let tx_request = build_contract_call_request(
            asset_id.as_str(),
            &self.vault_account_id,
            &wallet_id,
            calldata,
            note,
            external_tx_id,
        );

        let params = CreateTransactionParams::builder()
            .transaction_request(tx_request)
            .build();

        let create_response =
            self.client.transactions_api().create_transaction(params).await;

        match create_response {
            Ok(response) => response
                .id
                .map(|tx_id| Submission::Created { tx_id })
                .ok_or(FireblocksVaultError::MissingTransactionId),
            Err(ref err) if is_duplicate_external_tx_id_error(err) => {
                warn!(target: "fireblocks",
                    %external_tx_id,
                    original_error = ?err,
                    "Duplicate externalTxId — looking up existing transaction"
                );

                self.recover_by_external_tx_id(external_tx_id)
                    .await
                    .map(|tx_id| Submission::Recovered { tx_id })
            }
            Err(err) => {
                // The SDK's Display impl only shows the status code, discarding
                // the response body which contains the actual error message and
                // code. Debug preserves the full ResponseContent including the
                // body and typed error entity.
                warn!(target: "fireblocks", error = ?err,
                    %contract_address,
                    %external_tx_id,
                    "Fireblocks create_transaction failed"
                );
                Err(err.into())
            }
        }
    }

    /// Signs a 32-byte transaction hash with the vault account's secp256k1
    /// key via a RAW signing operation and polls to completion, walking to a
    /// fresh `externalTxId` when a recovered previous attempt turns out
    /// terminally failed — the same id discipline as
    /// [`Self::submit_contract_call_to_completion`].
    ///
    /// RAW signing never touches Fireblocks' transaction engine
    /// (build/broadcast/node infrastructure): the caller builds and
    /// broadcasts the transaction itself and Fireblocks only produces the
    /// signature. The returned signature is verified to recover to
    /// `expected_signer` before it is handed back, so success is proof the
    /// vault key signed the exact hash — never broadcast on an unverified
    /// signature.
    ///
    /// # Errors
    ///
    /// As [`Self::submit_contract_call_to_completion`], plus a completed
    /// signing operation that carries no signature, a malformed signature
    /// component, or a signature that recovers to the wrong address.
    pub async fn sign_raw_to_completion(
        &self,
        sighash: B256,
        expected_signer: Address,
        note: &str,
        base_external_tx_id: &str,
    ) -> Result<Signature, FireblocksVaultError> {
        for attempt in 0..MAX_SUBMISSION_ATTEMPTS {
            let external_tx_id = if attempt == 0 {
                base_external_tx_id.to_string()
            } else {
                format!("{base_external_tx_id}-retry-{attempt}")
            };

            let submission =
                self.submit_raw_signing(sighash, note, &external_tx_id).await?;

            match self
                .wait_for_signature(
                    submission.tx_id(),
                    sighash,
                    expected_signer,
                )
                .await
            {
                Ok(signature) => return Ok(signature),
                Err(FireblocksVaultError::TransactionFailed {
                    tx_id,
                    status,
                }) if matches!(submission, Submission::Recovered { .. }) => {
                    warn!(target: "fireblocks",
                        %external_tx_id,
                        fireblocks_tx_id = %tx_id,
                        ?status,
                        "Recovered RAW signing transaction from a previous \
                         attempt is terminally failed and its externalTxId \
                         is spent; submitting under a fresh retry id"
                    );
                }
                Err(err) => return Err(err),
            }
        }

        Err(FireblocksVaultError::RetryAttemptsExhausted {
            base_external_tx_id: base_external_tx_id.to_string(),
            attempts: MAX_SUBMISSION_ATTEMPTS,
        })
    }

    /// Submits a single RAW signing transaction under `external_tx_id`,
    /// reporting whether the id created a fresh transaction or recovered one
    /// a previous run already claimed.
    async fn submit_raw_signing(
        &self,
        sighash: B256,
        note: &str,
        external_tx_id: &str,
    ) -> Result<Submission, FireblocksVaultError> {
        let asset_id = self.chain_asset_ids.get(self.chain_id).ok_or(
            FireblocksVaultError::UnknownChain { chain_id: self.chain_id },
        )?;

        let tx_request = build_raw_signing_request(
            asset_id.as_str(),
            &self.vault_account_id,
            sighash,
            note,
            external_tx_id,
        );

        let params = CreateTransactionParams::builder()
            .transaction_request(tx_request)
            .build();

        let create_response =
            self.client.transactions_api().create_transaction(params).await;

        match create_response {
            Ok(response) => response
                .id
                .map(|tx_id| Submission::Created { tx_id })
                .ok_or(FireblocksVaultError::MissingTransactionId),
            Err(ref err) if is_duplicate_external_tx_id_error(err) => {
                warn!(target: "fireblocks",
                    %external_tx_id,
                    original_error = ?err,
                    "Duplicate externalTxId — looking up existing RAW \
                     signing transaction"
                );

                self.recover_by_external_tx_id(external_tx_id)
                    .await
                    .map(|tx_id| Submission::Recovered { tx_id })
            }
            Err(err) => {
                warn!(target: "fireblocks", error = ?err,
                    %external_tx_id,
                    "Fireblocks RAW signing create_transaction failed"
                );
                Err(err.into())
            }
        }
    }

    /// Polls a RAW signing transaction to completion and extracts its
    /// verified signature.
    async fn wait_for_signature(
        &self,
        tx_id: &str,
        sighash: B256,
        expected_signer: Address,
    ) -> Result<Signature, FireblocksVaultError> {
        debug!(target: "fireblocks", fireblocks_tx_id = %tx_id, "Polling Fireblocks RAW signing transaction...");

        let result = self
            .client
            .poll_transaction(
                tx_id,
                Duration::from_secs(600),
                Duration::from_millis(500),
                |tx| {
                    debug!(target: "fireblocks", fireblocks_tx_id = %tx_id,
                        status = ?tx.status,
                        "Polling Fireblocks transaction"
                    );
                },
            )
            .await?;

        if result.status != TransactionStatus::Completed {
            if is_still_pending(result.status) {
                warn!(target: "fireblocks", fireblocks_tx_id = %tx_id,
                    status = ?result.status,
                    "Polling timed out but the signing operation may still \
                     complete"
                );

                return Err(FireblocksVaultError::PollTimedOut {
                    tx_id: tx_id.to_string(),
                    status: result.status,
                });
            }

            return Err(FireblocksVaultError::TransactionFailed {
                tx_id: tx_id.to_string(),
                status: result.status,
            });
        }

        let signature = result
            .signed_messages
            .as_ref()
            .and_then(|messages| messages.first())
            .and_then(|message| message.signature.as_ref())
            .ok_or_else(|| FireblocksVaultError::MissingSignature {
                tx_id: tx_id.to_string(),
            })?;

        let (Some(r_hex), Some(s_hex), Some(v)) =
            (&signature.r, &signature.s, signature.v)
        else {
            return Err(FireblocksVaultError::IncompleteSignature {
                tx_id: tx_id.to_string(),
            });
        };

        let r = U256::from_str_radix(r_hex.trim_start_matches("0x"), 16)?;
        let s = U256::from_str_radix(s_hex.trim_start_matches("0x"), 16)?;
        let odd_y_parity = matches!(v, SignatureParity::Variant1);

        let signature = Signature::new(r, s, odd_y_parity);
        let recovered = signature.recover_address_from_prehash(&sighash)?;

        if recovered != expected_signer {
            return Err(FireblocksVaultError::RawSignerMismatch {
                expected: expected_signer,
                recovered,
            });
        }

        Ok(signature)
    }

    /// Recovers a Fireblocks transaction ID by looking up the `externalTxId`.
    ///
    /// Called when `create_transaction` returns a duplicate `externalTxId`
    /// rejection (HTTP 409/400). This means we previously submitted a
    /// transaction with this ID but lost track of it (e.g., crash before
    /// persisting the Fireblocks tx ID). We look up the existing transaction
    /// and return its Fireblocks ID so polling can resume.
    async fn recover_by_external_tx_id(
        &self,
        external_tx_id: &str,
    ) -> Result<String, FireblocksVaultError> {
        let params = GetTransactionByExternalIdParams {
            external_tx_id: external_tx_id.to_string(),
        };

        let tx = self
            .client
            .transactions_api()
            .get_transaction_by_external_id(params)
            .await
            .map_err(|err| {
                warn!(target: "fireblocks",
                    %external_tx_id,
                    error = ?err,
                    "Failed to look up transaction by externalTxId"
                );
                FireblocksVaultError::ExternalTxIdLookupFailed {
                    external_tx_id: external_tx_id.to_string(),
                    source: Box::new(err),
                }
            })?;

        let fireblocks_tx_id = tx.id;

        debug!(target: "fireblocks",
            %external_tx_id,
            %fireblocks_tx_id,
            "Recovered existing Fireblocks transaction"
        );

        Ok(fireblocks_tx_id)
    }

    /// Polls a Fireblocks transaction until completion.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The Fireblocks transaction ID to poll
    ///
    /// # Returns
    ///
    /// The on-chain transaction hash (B256).
    ///
    /// # Errors
    ///
    /// Returns an error if polling fails, the transaction reaches a
    /// non-completed terminal status, or the completed transaction carries no
    /// parseable hash.
    pub async fn wait_for_completion(
        &self,
        tx_id: &str,
    ) -> Result<B256, FireblocksVaultError> {
        debug!(target: "fireblocks", fireblocks_tx_id = %tx_id, "Polling Fireblocks CONTRACT_CALL transaction...");

        let result = self
            .client
            .poll_transaction(
                tx_id,
                Duration::from_secs(600),
                Duration::from_millis(500),
                |tx| {
                    debug!(target: "fireblocks", fireblocks_tx_id = %tx_id,
                        status = ?tx.status,
                        "Polling Fireblocks transaction"
                    );
                },
            )
            .await?;

        if result.status != TransactionStatus::Completed {
            // A still-pending status after the polling window is a timeout,
            // not a terminal failure — the transaction may complete once
            // console approval clears, and re-running resumes it via the
            // deterministic externalTxId.
            if is_still_pending(result.status) {
                warn!(target: "fireblocks", fireblocks_tx_id = %tx_id,
                    status = ?result.status,
                    "Polling timed out but transaction may still confirm on-chain"
                );

                return Err(FireblocksVaultError::PollTimedOut {
                    tx_id: tx_id.to_string(),
                    status: result.status,
                });
            }

            return Err(FireblocksVaultError::TransactionFailed {
                tx_id: tx_id.to_string(),
                status: result.status,
            });
        }

        let tx_hash_str = result.tx_hash.ok_or_else(|| {
            FireblocksVaultError::MissingTxHash { tx_id: tx_id.to_string() }
        })?;

        parse_tx_hash(&tx_hash_str)
    }

    /// The read-only provider this service fetches receipts through.
    pub const fn read_provider(&self) -> &P {
        &self.read_provider
    }

    /// Test constructor injecting a client pointed at a mock server.
    #[cfg(test)]
    pub(crate) fn for_tests(
        client: Client,
        read_provider: P,
        chain_id: u64,
        chain_asset_ids: ChainAssetIds,
    ) -> Self {
        Self {
            client,
            vault_account_id: "0".to_string(),
            chain_asset_ids,
            read_provider,
            chain_id,
        }
    }

    /// Fetches a transaction receipt from the RPC provider.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC call fails or no receipt exists for the
    /// hash.
    pub async fn fetch_receipt(
        &self,
        tx_hash: B256,
    ) -> Result<TransactionReceipt, FireblocksVaultError> {
        self.read_provider
            .get_transaction_receipt(tx_hash)
            .await?
            .ok_or(FireblocksVaultError::MissingReceipt { tx_hash })
    }
}

/// Builds a Fireblocks RAW signing transaction request for a 32-byte
/// prehashed message.
///
/// No destination and no amount: the operation only produces a secp256k1
/// signature over `sighash` with the vault account's key for the given
/// asset — nothing is built or broadcast on the Fireblocks side.
fn build_raw_signing_request(
    asset_id: &str,
    vault_account_id: &str,
    sighash: B256,
    note: &str,
    external_tx_id: &str,
) -> models::TransactionRequest {
    let raw_message_data = models::ExtraParametersRawMessageData {
        messages: Some(vec![models::UnsignedMessage::new(alloy::hex::encode(
            sighash,
        ))]),
        algorithm: Some(
            models::extra_parameters_raw_message_data::Algorithm::MpcEcdsaSecp256K1,
        ),
    };

    models::TransactionRequest {
        operation: Some(models::TransactionOperation::Raw),
        asset_id: Some(asset_id.to_string()),
        source: Some(models::SourceTransferPeerPath {
            r#type: models::TransferPeerPathType::VaultAccount,
            id: Some(vault_account_id.to_string()),
            sub_type: None,
            name: None,
            wallet_id: None,
            is_collateral: None,
        }),
        destination: None,
        amount: None,
        extra_parameters: Some(models::ExtraParameters {
            contract_call_data: None,
            raw_message_data: Some(raw_message_data),
            inputs_selection: None,
            node_controls: None,
            program_call_data: None,
        }),
        external_tx_id: Some(external_tx_id.to_string()),
        note: Some(note.to_string()),
        fee_level: None,
        destinations: None,
        treat_as_gross_amount: None,
        force_sweep: None,
        fee: None,
        priority_fee: None,
        fail_on_low_fee: None,
        max_fee: None,
        gas_limit: None,
        gas_price: None,
        network_fee: None,
        replace_tx_by_hash: None,
        customer_ref_id: None,
        auto_staking: None,
        network_staking: None,
        cpu_staking: None,
        use_gasless: None,
        travel_rule_message: None,
    }
}

/// Builds a Fireblocks CONTRACT_CALL transaction request.
///
/// Uses `ExternalWallet` destination type with the resolved whitelisted
/// contract wallet ID, enabling Fireblocks TAP policy enforcement.
fn build_contract_call_request(
    asset_id: &str,
    vault_account_id: &str,
    wallet_id: &ContractWalletId,
    calldata: &Bytes,
    note: &str,
    external_tx_id: &str,
) -> models::TransactionRequest {
    let extra_parameters = models::ExtraParameters {
        contract_call_data: Some(alloy::hex::encode(calldata)),
        raw_message_data: None,
        inputs_selection: None,
        node_controls: None,
        program_call_data: None,
    };

    models::TransactionRequest {
        operation: Some(models::TransactionOperation::ContractCall),
        asset_id: Some(asset_id.to_string()),
        source: Some(models::SourceTransferPeerPath {
            r#type: models::TransferPeerPathType::VaultAccount,
            id: Some(vault_account_id.to_string()),
            sub_type: None,
            name: None,
            wallet_id: None,
            is_collateral: None,
        }),
        destination: Some(models::DestinationTransferPeerPath {
            r#type: models::TransferPeerPathType::ExternalWallet,
            id: Some(wallet_id.as_str().to_string()),
            one_time_address: None,
            sub_type: None,
            name: None,
            wallet_id: None,
            is_collateral: None,
        }),
        // Amount is "0" for contract calls that don't transfer value
        amount: Some(models::TransactionRequestAmount::String("0".to_string())),
        extra_parameters: Some(extra_parameters),
        external_tx_id: Some(external_tx_id.to_string()),
        note: Some(note.to_string()),
        // Use default fee level (MEDIUM)
        fee_level: Some(models::transaction_request::FeeLevel::Medium),
        // Remaining fields default to None
        destinations: None,
        treat_as_gross_amount: None,
        force_sweep: None,
        fee: None,
        priority_fee: None,
        fail_on_low_fee: None,
        max_fee: None,
        gas_limit: None,
        gas_price: None,
        network_fee: None,
        replace_tx_by_hash: None,
        customer_ref_id: None,
        travel_rule_message: None,
        auto_staking: None,
        network_staking: None,
        cpu_staking: None,
        use_gasless: None,
    }
}

/// Returns true if the transaction status indicates the transaction is still
/// in progress and may eventually confirm on-chain. Used to distinguish
/// "polling timed out but tx might still land" from "tx definitively failed."
pub(crate) const fn is_still_pending(status: TransactionStatus) -> bool {
    use TransactionStatus::*;

    match status {
        Submitted
        | PendingAmlScreening
        | PendingEnrichment
        | PendingAuthorization
        | Queued
        | PendingSignature
        | Pending3RdPartyManualApproval
        | Pending3RdParty
        | Broadcasting
        | Confirming
        | Cancelling => true,

        Completed | Cancelled | Blocked | Rejected | Failed => false,
    }
}

/// Detects whether a `create_transaction` error is a duplicate `externalTxId`
/// rejection. Fireblocks returns HTTP 409 (or sometimes 400) when a transaction
/// with the same `externalTxId` already exists.
fn is_duplicate_external_tx_id_error(
    err: &apis::Error<CreateTransactionError>,
) -> bool {
    if let apis::Error::ResponseError(apis::ResponseContent {
        status,
        content,
        ..
    }) = err
    {
        // HTTP 409 Conflict is the canonical duplicate response.
        // HTTP 400 with "externalTxId" in the body is also observed.
        // Require BOTH an externalTxId mention AND explicit duplicate
        // semantics: an unrelated 409/400 that merely references the field
        // (e.g. a validation error) must not be misrouted into the
        // externalTxId recovery lookup, which would mask the real error.
        if status.as_u16() == 409 || status.as_u16() == 400 {
            let lower = content.to_lowercase();
            let mentions_external_id = lower.contains("externaltxid")
                || lower.contains("external_tx_id")
                || lower.contains("external tx id");
            let indicates_duplicate = lower.contains("already exists")
                || lower.contains("duplicate")
                || lower.contains(r#""code":1438"#);

            return mentions_external_id && indicates_duplicate;
        }
    }

    false
}

/// Parses a transaction hash string (with or without 0x prefix) into B256.
fn parse_tx_hash(tx_hash_str: &str) -> Result<B256, FireblocksVaultError> {
    let tx_hash_hex = tx_hash_str.strip_prefix("0x").unwrap_or(tx_hash_str);
    let tx_hash_bytes: [u8; 32] = alloy::hex::decode(tx_hash_hex)
        .map_err(|e| FireblocksVaultError::InvalidTxHash {
            hash: tx_hash_str.to_string(),
            source: e,
        })?
        .try_into()
        .map_err(|_| FireblocksVaultError::InvalidTxHash {
            hash: tx_hash_str.to_string(),
            source: alloy::hex::FromHexError::InvalidStringLength,
        })?;

    Ok(B256::from(tx_hash_bytes))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, LazyLock};

    use httpmock::MockServer;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::EncodePrivateKey;

    use super::*;
    use crate::fireblocks::config::parse_chain_asset_ids;

    static TEST_RSA_PEM: LazyLock<Vec<u8>> = LazyLock::new(|| {
        let mut rng = rand::thread_rng();
        let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .as_bytes()
            .to_vec()
    });

    fn mock_client(server: &MockServer) -> Client {
        ClientBuilder::new("test-api-user", &TEST_RSA_PEM)
            .with_url(&server.base_url())
            .build()
            .unwrap()
    }

    fn build_test_service(
        client: Client,
    ) -> FireblocksVaultService<impl Provider + Clone + 'static> {
        let chain_asset_ids =
            parse_chain_asset_ids("8453:BASECHAIN_ETH").unwrap();

        let read_provider = alloy::providers::RootProvider::new_http(
            "http://localhost:1".parse().unwrap(),
        );

        FireblocksVaultService {
            client,
            vault_account_id: "0".to_string(),
            chain_asset_ids,
            read_provider,
            chain_id: 8453,
        }
    }

    fn mock_whitelisted_contracts<'a>(
        server: &'a MockServer,
        contract_address: &str,
        asset_id: &str,
    ) -> httpmock::Mock<'a> {
        server.mock(|when, then| {
            when.method("GET").path("/contracts");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!([
                    {
                        "id": "contract-wallet-123",
                        "name": "Test Vault",
                        "assets": [
                            {
                                "id": asset_id,
                                "address": contract_address
                            }
                        ]
                    }
                ]));
        })
    }

    #[tokio::test]
    async fn resolve_contract_wallet_finds_whitelisted_contract() {
        let contract = "0x1234567890abcdef1234567890abcdef12345678"
            .parse::<Address>()
            .unwrap();

        let server = MockServer::start();
        let mock = mock_whitelisted_contracts(
            &server,
            &contract.to_string().to_lowercase(),
            "BASECHAIN_ETH",
        );

        let service = build_test_service(mock_client(&server));

        let wallet_id =
            service.resolve_contract_wallet(contract).await.unwrap();

        assert_eq!(wallet_id.as_str(), "contract-wallet-123");
        mock.assert();
    }

    #[tokio::test]
    async fn resolve_contract_wallet_rejects_unknown_contract() {
        let contract = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse::<Address>()
            .unwrap();

        let server = MockServer::start();
        // Mock returns a wallet with a different address
        let mock = mock_whitelisted_contracts(
            &server,
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "BASECHAIN_ETH",
        );

        let service = build_test_service(mock_client(&server));

        let result = service.resolve_contract_wallet(contract).await;

        assert!(
            matches!(
                result,
                Err(FireblocksVaultError::ContractNotWhitelisted { .. })
            ),
            "Expected ContractNotWhitelisted, got {result:?}"
        );
        mock.assert();
    }

    #[tokio::test]
    async fn resolve_contract_wallet_rejects_wrong_asset_id() {
        let contract = "0x1234567890abcdef1234567890abcdef12345678"
            .parse::<Address>()
            .unwrap();

        let server = MockServer::start();
        // Mock returns matching address but wrong asset ID
        let mock = mock_whitelisted_contracts(
            &server,
            &contract.to_string().to_lowercase(),
            "ETH_TEST",
        );

        let service = build_test_service(mock_client(&server));

        let result = service.resolve_contract_wallet(contract).await;

        assert!(
            matches!(
                result,
                Err(FireblocksVaultError::ContractNotWhitelisted { .. })
            ),
            "Expected ContractNotWhitelisted, got {result:?}"
        );
        mock.assert();
    }

    #[test]
    fn build_contract_call_request_has_correct_structure() {
        let asset_id = "BASECHAIN_ETH";
        let vault_account_id = "0";
        let wallet_id = ContractWalletId::from("wallet-abc".to_string());
        let calldata = Bytes::from(vec![0x12, 0x34, 0x56, 0x78]);
        let note = "Test transaction";

        let request = build_contract_call_request(
            asset_id,
            vault_account_id,
            &wallet_id,
            &calldata,
            note,
            "test-id",
        );

        assert_eq!(
            request.operation,
            Some(models::TransactionOperation::ContractCall)
        );
        assert_eq!(request.asset_id, Some(asset_id.to_string()));
        assert_eq!(
            request.amount,
            Some(models::TransactionRequestAmount::String("0".to_string()))
        );
        assert!(request.extra_parameters.is_some());

        let extra = request.extra_parameters.as_ref().unwrap();
        assert_eq!(extra.contract_call_data, Some("12345678".to_string()));
        assert!(extra.raw_message_data.is_none());

        let source = request.source.as_ref().unwrap();
        assert_eq!(source.r#type, models::TransferPeerPathType::VaultAccount);
        assert_eq!(source.id, Some(vault_account_id.to_string()));

        let dest = request.destination.as_ref().unwrap();
        assert_eq!(dest.r#type, models::TransferPeerPathType::ExternalWallet);
        assert_eq!(dest.id, Some("wallet-abc".to_string()));
        assert!(dest.one_time_address.is_none());
    }

    #[test]
    fn build_contract_call_request_encodes_calldata_as_hex() {
        let calldata = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]);
        let wallet_id = ContractWalletId::from("w".to_string());

        let request = build_contract_call_request(
            "ETH", "0", &wallet_id, &calldata, "test", "test-id",
        );

        let extra = request.extra_parameters.unwrap();
        assert_eq!(extra.contract_call_data, Some("deadbeef".to_string()));
    }

    #[test]
    fn build_contract_call_request_uses_medium_fee_level() {
        let wallet_id = ContractWalletId::from("w".to_string());

        let request = build_contract_call_request(
            "ETH",
            "0",
            &wallet_id,
            &Bytes::new(),
            "test",
            "test-id",
        );

        assert_eq!(
            request.fee_level,
            Some(models::transaction_request::FeeLevel::Medium)
        );
    }

    // ==================== Unit Tests for parse_tx_hash ====================

    #[test]
    fn parse_tx_hash_with_0x_prefix() {
        let hash_str = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let result = parse_tx_hash(hash_str).unwrap();

        assert_eq!(
            result,
            B256::from([
                0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd,
                0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12,
                0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56,
                0x78, 0x90
            ])
        );
    }

    #[test]
    fn parse_tx_hash_without_0x_prefix() {
        let hash_str =
            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let result = parse_tx_hash(hash_str).unwrap();

        assert_eq!(
            result,
            B256::from([
                0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd,
                0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12,
                0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56,
                0x78, 0x90
            ])
        );
    }

    #[test]
    fn parse_tx_hash_invalid_hex_characters() {
        let hash_str = "0xGGGGGG1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let result = parse_tx_hash(hash_str);

        assert!(
            matches!(result, Err(FireblocksVaultError::InvalidTxHash { .. })),
            "Expected InvalidTxHash error, got {result:?}"
        );
    }

    #[test]
    fn parse_tx_hash_too_short() {
        let hash_str = "0xabcdef";
        let result = parse_tx_hash(hash_str);

        assert!(
            matches!(result, Err(FireblocksVaultError::InvalidTxHash { .. })),
            "Expected InvalidTxHash error, got {result:?}"
        );
    }

    #[test]
    fn parse_tx_hash_too_long() {
        // 33 bytes instead of 32
        let hash_str = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890aa";
        let result = parse_tx_hash(hash_str);

        assert!(
            matches!(result, Err(FireblocksVaultError::InvalidTxHash { .. })),
            "Expected InvalidTxHash error, got {result:?}"
        );
    }

    #[test]
    fn parse_tx_hash_empty_string() {
        let result = parse_tx_hash("");

        assert!(
            matches!(result, Err(FireblocksVaultError::InvalidTxHash { .. })),
            "Expected InvalidTxHash error, got {result:?}"
        );
    }

    #[test]
    fn is_still_pending_true_for_in_progress_statuses() {
        use TransactionStatus::*;

        assert!(is_still_pending(Submitted));
        assert!(is_still_pending(PendingAmlScreening));
        assert!(is_still_pending(PendingEnrichment));
        assert!(is_still_pending(PendingAuthorization));
        assert!(is_still_pending(Queued));
        assert!(is_still_pending(PendingSignature));
        assert!(is_still_pending(Pending3RdPartyManualApproval));
        assert!(is_still_pending(Pending3RdParty));
        assert!(is_still_pending(Broadcasting));
        assert!(is_still_pending(Confirming));
        assert!(is_still_pending(Cancelling));
    }

    #[test]
    fn is_still_pending_false_for_terminal_statuses() {
        use TransactionStatus::*;

        assert!(!is_still_pending(Completed));
        assert!(!is_still_pending(Failed));
        assert!(!is_still_pending(Cancelled));
        assert!(!is_still_pending(Blocked));
        assert!(!is_still_pending(Rejected));
    }

    /// The Fireblocks SDK must treat `Confirming` as a non-terminal status and
    /// keep polling. A transaction that is `Confirming` will eventually become
    /// `Completed` — stopping early causes a spurious `TransactionFailed` error.
    ///
    /// This test simulates a transaction that reports `CONFIRMING` for its first
    /// two polls, then transitions to `COMPLETED`. With a correct SDK,
    /// `wait_for_completion` polls through the `Confirming` state and succeeds.
    /// With a buggy SDK that treats `Confirming` as terminal, the poll loop
    /// exits early and returns `TransactionFailed { status: Confirming }`.
    #[tokio::test]
    async fn wait_for_completion_polls_through_confirming_status() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let server = MockServer::start();
        let call_count = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&call_count);

        let expected_tx_hash = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";

        let tx_hash = expected_tx_hash.to_string();

        server.mock(|when, then| {
            when.method("GET").path("/transactions/tx-confirming-123");
            then.respond_with(move |_req: &httpmock::HttpMockRequest| {
                let call = counter.fetch_add(1, Ordering::SeqCst);

                let status = if call < 2 { "CONFIRMING" } else { "COMPLETED" };

                httpmock::HttpMockResponse::builder()
                    .status(200)
                    .header("content-type", "application/json")
                    .body(
                        serde_json::json!({
                            "id": "tx-confirming-123",
                            "status": status,
                            "txHash": tx_hash,
                        })
                        .to_string(),
                    )
                    .build()
            });
        });

        let service = build_test_service(mock_client(&server));

        let result = service.wait_for_completion("tx-confirming-123").await;

        assert!(
            result.is_ok(),
            "wait_for_completion should succeed after polling through \
             Confirming status, but got: {result:?}"
        );

        let hash = result.unwrap();
        assert_eq!(
            hash,
            expected_tx_hash.parse::<B256>().unwrap(),
            "Should return the on-chain transaction hash"
        );

        let total_calls = call_count.load(Ordering::SeqCst);
        assert!(
            total_calls >= 3,
            "SDK should have polled past Confirming status \
             (expected >= 3 calls, got {total_calls})"
        );
    }

    /// The custody wallet the whole migration verifies ownership against is
    /// derived here — the first returned deposit address, parsed.
    #[tokio::test]
    async fn vault_address_parses_the_first_deposit_address() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET").path_includes("/addresses");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "addresses": [
                        { "address":
                            "0x1111111111111111111111111111111111111111" },
                        { "address":
                            "0x2222222222222222222222222222222222222222" }
                    ]
                }));
        });

        let client = mock_client(&server);
        let vault_account_id = FireblocksVaultAccountId::from("0".to_string());
        let chain_asset_ids =
            parse_chain_asset_ids("8453:BASECHAIN_ETH").unwrap();

        let address = vault_address_via(
            &client,
            &vault_account_id,
            chain_asset_ids.default_asset_id(),
        )
        .await
        .unwrap();

        assert_eq!(
            address,
            "0x1111111111111111111111111111111111111111"
                .parse::<Address>()
                .unwrap()
        );
        mock.assert();
    }

    /// An empty address list is a configuration problem, not an address —
    /// it must surface as `NoAddress`, never as a parse error or a default.
    #[tokio::test]
    async fn vault_address_with_no_addresses_is_no_address() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method("GET").path_includes("/addresses");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({ "addresses": [] }));
        });

        let client = mock_client(&server);
        let vault_account_id = FireblocksVaultAccountId::from("7".to_string());
        let chain_asset_ids =
            parse_chain_asset_ids("8453:BASECHAIN_ETH").unwrap();

        let result = vault_address_via(
            &client,
            &vault_account_id,
            chain_asset_ids.default_asset_id(),
        )
        .await;

        assert!(matches!(
            result,
            Err(FireblocksVaultError::NoAddress { vault_id, asset_id })
                if vault_id.as_str() == "7"
                    && asset_id.as_str() == "BASECHAIN_ETH"
        ));
    }

    /// End-to-end duplicate recovery: a duplicate-externalTxId rejection from
    /// `create_transaction` must route into the external-id lookup and return
    /// the original transaction's ID — the crash-retry idempotency path.
    #[tokio::test]
    async fn submit_contract_call_recovers_duplicate_via_external_id_lookup() {
        let contract = "0x1234567890abcdef1234567890abcdef12345678"
            .parse::<Address>()
            .unwrap();

        let server = MockServer::start();
        mock_whitelisted_contracts(
            &server,
            &contract.to_string().to_lowercase(),
            "BASECHAIN_ETH",
        );
        server.mock(|when, then| {
            when.method("POST").path("/transactions");
            then.status(409)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "message":
                        "Transaction with this externalTxId already exists",
                    "code": 1438
                }));
        });
        let lookup = server.mock(|when, then| {
            when.method("GET").path_includes("ext-dup-1");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "recovered-tx-1",
                    "status": "SUBMITTED",
                    "txHash": ""
                }));
        });

        let service = build_test_service(mock_client(&server));

        let submission = service
            .submit_contract_call(
                contract,
                &Bytes::from(vec![0xde, 0xad]),
                "note",
                "ext-dup-1",
            )
            .await
            .unwrap();

        assert_eq!(
            submission,
            Submission::Recovered { tx_id: "recovered-tx-1".to_string() },
            "a duplicate id must be reported as recovered, not created"
        );
        lookup.assert();
    }

    /// When the recovery lookup itself fails, the caller must see the
    /// dedicated `ExternalTxIdLookupFailed` — not a generic API error that
    /// hides which step broke.
    #[tokio::test]
    async fn submit_contract_call_reports_failed_duplicate_lookup() {
        let contract = "0x1234567890abcdef1234567890abcdef12345678"
            .parse::<Address>()
            .unwrap();

        let server = MockServer::start();
        mock_whitelisted_contracts(
            &server,
            &contract.to_string().to_lowercase(),
            "BASECHAIN_ETH",
        );
        server.mock(|when, then| {
            when.method("POST").path("/transactions");
            then.status(409)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "message":
                        "Transaction with this externalTxId already exists",
                    "code": 1438
                }));
        });
        server.mock(|when, then| {
            when.method("GET").path_includes("ext-dup-2");
            then.status(500)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({ "message": "boom" }));
        });

        let service = build_test_service(mock_client(&server));

        let result = service
            .submit_contract_call(
                contract,
                &Bytes::from(vec![0xde, 0xad]),
                "note",
                "ext-dup-2",
            )
            .await;

        assert!(matches!(
            result,
            Err(FireblocksVaultError::ExternalTxIdLookupFailed {
                external_tx_id,
                ..
            }) if external_tx_id == "ext-dup-2"
        ));
    }

    /// A terminal failure permanently spends its externalTxId at Fireblocks,
    /// so a re-run after the operator fixes the cause (a TAP rule, expired
    /// certification) must not resume the corpse: the walk recovers the dead
    /// transaction under the base id, recognizes it as terminally failed, and
    /// submits fresh under `-retry-1`. This is the fix-and-retry path the
    /// migration runbook depends on.
    #[tokio::test]
    async fn a_terminally_failed_prior_attempt_walks_to_a_fresh_retry_id() {
        let contract = "0x1234567890abcdef1234567890abcdef12345678"
            .parse::<Address>()
            .unwrap();
        let landed_hash = "0x8888888888888888888888888888888888888888888888888888888888888888";

        let server = MockServer::start();
        mock_whitelisted_contracts(
            &server,
            &contract.to_string().to_lowercase(),
            "BASECHAIN_ETH",
        );
        // The base id is spent by a previous run.
        let base_create = server.mock(|when, then| {
            when.method("POST")
                .path("/transactions")
                .body_includes(r#""externalTxId":"ext-walk-1""#);
            then.status(409)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "message":
                        "Transaction with this externalTxId already exists",
                    "code": 1438
                }));
        });
        // The retry id is free and the fresh submission completes.
        let retry_create = server.mock(|when, then| {
            when.method("POST")
                .path("/transactions")
                .body_includes(r#""externalTxId":"ext-walk-1-retry-1""#);
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({ "id": "fb-fresh-1" }));
        });
        // Recovery under the base id finds the terminally failed corpse.
        server.mock(|when, then| {
            when.method("GET").path_includes("ext-walk-1");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "dead-tx-1",
                    "status": "FAILED",
                    "txHash": ""
                }));
        });
        server.mock(|when, then| {
            when.method("GET").path_includes("dead-tx-1");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "dead-tx-1",
                    "status": "FAILED",
                    "txHash": ""
                }));
        });
        server.mock(|when, then| {
            when.method("GET").path_includes("fb-fresh-1");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "fb-fresh-1",
                    "status": "COMPLETED",
                    "txHash": landed_hash
                }));
        });

        let service = build_test_service(mock_client(&server));

        let tx_hash = service
            .submit_contract_call_to_completion(
                contract,
                &Bytes::from(vec![0xde, 0xad]),
                "note",
                "ext-walk-1",
            )
            .await
            .unwrap();

        assert_eq!(tx_hash, landed_hash.parse::<B256>().unwrap());
        base_create.assert();
        retry_create.assert();
    }

    fn make_response_error(
        status: u16,
        body: &str,
    ) -> apis::Error<CreateTransactionError> {
        apis::Error::ResponseError(apis::ResponseContent {
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            content: body.to_string(),
            entity: None,
        })
    }

    #[test]
    fn duplicate_detection_http_409_with_keyword() {
        let err = make_response_error(
            409,
            r#"{"message": "Duplicate externalTxId"}"#,
        );
        assert!(
            is_duplicate_external_tx_id_error(&err),
            "HTTP 409 with 'externalTxId' should be detected as duplicate"
        );
    }

    #[test]
    fn duplicate_detection_http_409_without_keyword() {
        let err = make_response_error(409, "Some other conflict");
        assert!(
            !is_duplicate_external_tx_id_error(&err),
            "HTTP 409 without keyword should not be detected as duplicate"
        );
    }

    #[test]
    fn duplicate_detection_http_400_with_external_tx_id_keyword() {
        let err = make_response_error(
            400,
            r#"{"message": "A transaction with externalTxId already exists"}"#,
        );
        assert!(
            is_duplicate_external_tx_id_error(&err),
            "HTTP 400 with 'externalTxId' should be detected as duplicate"
        );
    }

    #[test]
    fn duplicate_detection_http_400_with_external_tx_id_snake_case() {
        let err = make_response_error(
            400,
            r#"{"message": "Duplicate external_tx_id"}"#,
        );
        assert!(
            is_duplicate_external_tx_id_error(&err),
            "HTTP 400 with 'external_tx_id' should be detected as duplicate"
        );
    }

    #[test]
    fn duplicate_detection_http_400_with_external_tx_id_words() {
        let err = make_response_error(
            400,
            r#"{"message":"The external tx id that was provided in the request, already exists","code":1438}"#,
        );
        assert!(
            is_duplicate_external_tx_id_error(&err),
            "HTTP 400 with 'external tx id' words should be detected as duplicate"
        );
    }

    #[test]
    fn duplicate_detection_http_400_without_keyword() {
        let err = make_response_error(400, r#"{"message": "Invalid amount"}"#);
        assert!(
            !is_duplicate_external_tx_id_error(&err),
            "HTTP 400 without keyword should not be detected as duplicate"
        );
    }

    #[test]
    fn duplicate_detection_http_400_external_tx_id_without_duplicate_semantics()
    {
        let err = make_response_error(
            400,
            r#"{"message": "externalTxId exceeds the maximum allowed length"}"#,
        );
        assert!(
            !is_duplicate_external_tx_id_error(&err),
            "HTTP 400 that mentions externalTxId but does not indicate a \
             duplicate must not be misrouted into the recovery lookup"
        );
    }

    #[test]
    fn duplicate_detection_http_500() {
        let err = make_response_error(500, "Internal server error");
        assert!(
            !is_duplicate_external_tx_id_error(&err),
            "HTTP 500 should not be detected as duplicate"
        );
    }

    #[test]
    fn duplicate_detection_non_response_error() {
        let err: apis::Error<CreateTransactionError> = apis::Error::Serde(
            serde_json::from_str::<String>("!!!").unwrap_err(),
        );
        assert!(
            !is_duplicate_external_tx_id_error(&err),
            "Non-ResponseError should not be detected as duplicate"
        );
    }

    fn mock_raw_signing_creation<'a>(
        server: &'a MockServer,
        sighash: B256,
        tx_id: &str,
    ) -> httpmock::Mock<'a> {
        let response = serde_json::json!({ "id": tx_id });
        server.mock(move |when, then| {
            when.method("POST")
                .path("/transactions")
                .body_includes(r#""operation":"RAW""#)
                .body_includes(alloy::hex::encode(sighash));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(response);
        })
    }

    /// RAW signing must return the signature Fireblocks produced, verified to
    /// recover to the expected signer. The mock replays a real secp256k1
    /// signature (numeric `v`, as production sends it) so recovery is
    /// exercised for real, not stubbed.
    #[tokio::test]
    async fn sign_raw_returns_signature_recovering_to_expected_signer() {
        use alloy::signers::SignerSync;
        use alloy::signers::local::PrivateKeySigner;

        let signer = PrivateKeySigner::random();
        let sighash = B256::random();
        let signature = signer.sign_hash_sync(&sighash).unwrap();

        let server = MockServer::start();
        let create_mock =
            mock_raw_signing_creation(&server, sighash, "raw-tx-1");

        let poll_body = serde_json::json!({
            "id": "raw-tx-1",
            "status": "COMPLETED",
            "signedMessages": [{
                "signature": {
                    "r": format!("{:064x}", signature.r()),
                    "s": format!("{:064x}", signature.s()),
                    "v": u8::from(signature.v()),
                }
            }]
        });
        let poll_mock = server.mock(|when, then| {
            when.method("GET").path("/transactions/raw-tx-1");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(poll_body);
        });

        let service = build_test_service(mock_client(&server));

        let returned = service
            .sign_raw_to_completion(
                sighash,
                signer.address(),
                "raw signing test",
                "raw-ext-1",
            )
            .await
            .unwrap();

        assert_eq!(
            returned.recover_address_from_prehash(&sighash).unwrap(),
            signer.address(),
            "returned signature must recover to the vault wallet address"
        );
        create_mock.assert();
        assert!(
            poll_mock.calls() >= 1,
            "the signing operation must have been polled"
        );
    }

    /// A signature that recovers to any address other than the expected
    /// signer must never be handed back to a caller that would broadcast it.
    #[tokio::test]
    async fn sign_raw_rejects_signature_from_wrong_signer() {
        use alloy::signers::SignerSync;
        use alloy::signers::local::PrivateKeySigner;

        let signer = PrivateKeySigner::random();
        let expected = Address::random();
        let sighash = B256::random();
        let signature = signer.sign_hash_sync(&sighash).unwrap();

        let server = MockServer::start();
        mock_raw_signing_creation(&server, sighash, "raw-tx-2");

        let poll_body = serde_json::json!({
            "id": "raw-tx-2",
            "status": "COMPLETED",
            "signedMessages": [{
                "signature": {
                    "r": format!("{:064x}", signature.r()),
                    "s": format!("{:064x}", signature.s()),
                    "v": u8::from(signature.v()),
                }
            }]
        });
        server.mock(|when, then| {
            when.method("GET").path("/transactions/raw-tx-2");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(poll_body);
        });

        let service = build_test_service(mock_client(&server));

        let result = service
            .sign_raw_to_completion(
                sighash,
                expected,
                "raw signing test",
                "raw-ext-2",
            )
            .await;

        assert!(
            matches!(
                result.unwrap_err(),
                FireblocksVaultError::RawSignerMismatch {
                    expected: reported_expected,
                    recovered,
                } if reported_expected == expected
                    && recovered == signer.address()
            ),
            "a wrong-signer signature must be rejected before any broadcast"
        );
    }

    /// A completed RAW signing operation with no signature payload is a
    /// malformed response, not a success.
    #[tokio::test]
    async fn sign_raw_completed_without_signature_is_an_error() {
        let sighash = B256::random();

        let server = MockServer::start();
        mock_raw_signing_creation(&server, sighash, "raw-tx-3");

        server.mock(|when, then| {
            when.method("GET").path("/transactions/raw-tx-3");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "raw-tx-3",
                    "status": "COMPLETED",
                }));
        });

        let service = build_test_service(mock_client(&server));

        let result = service
            .sign_raw_to_completion(
                sighash,
                Address::random(),
                "raw signing test",
                "raw-ext-3",
            )
            .await;

        assert!(
            matches!(
                result.unwrap_err(),
                FireblocksVaultError::MissingSignature { tx_id } if tx_id == "raw-tx-3"
            ),
            "completed RAW op without a signature must surface MissingSignature"
        );
    }

    /// The RAW path carries the same fix-and-retry discipline as the
    /// CONTRACT_CALL path: a terminally failed prior signing attempt spends
    /// its externalTxId, so the walk recovers the corpse under the base id,
    /// recognizes the terminal failure, and signs fresh under `-retry-1`.
    #[tokio::test]
    async fn raw_signing_walks_a_terminally_failed_attempt_to_a_fresh_id() {
        use alloy::signers::SignerSync;
        use alloy::signers::local::PrivateKeySigner;

        let signer = PrivateKeySigner::random();
        let sighash = B256::random();
        let signature = signer.sign_hash_sync(&sighash).unwrap();

        let server = MockServer::start();
        // The base id is spent by a previous run.
        let base_create = server.mock(|when, then| {
            when.method("POST")
                .path("/transactions")
                .body_includes(r#""externalTxId":"raw-walk-1""#);
            then.status(409)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "message":
                        "Transaction with this externalTxId already exists",
                    "code": 1438
                }));
        });
        // The retry id is free and the fresh signing completes.
        let retry_create = server.mock(|when, then| {
            when.method("POST")
                .path("/transactions")
                .body_includes(r#""externalTxId":"raw-walk-1-retry-1""#);
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({ "id": "raw-fresh-1" }));
        });
        // Recovery under the base id finds the terminally failed corpse.
        server.mock(|when, then| {
            when.method("GET").path_includes("raw-walk-1");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "raw-dead-1",
                    "status": "FAILED"
                }));
        });
        server.mock(|when, then| {
            when.method("GET").path_includes("raw-dead-1");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "raw-dead-1",
                    "status": "FAILED"
                }));
        });
        let fresh_poll = serde_json::json!({
            "id": "raw-fresh-1",
            "status": "COMPLETED",
            "signedMessages": [{
                "signature": {
                    "r": format!("{:064x}", signature.r()),
                    "s": format!("{:064x}", signature.s()),
                    "v": u8::from(signature.v()),
                }
            }]
        });
        server.mock(|when, then| {
            when.method("GET").path_includes("raw-fresh-1");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(fresh_poll);
        });

        let service = build_test_service(mock_client(&server));

        let returned = service
            .sign_raw_to_completion(
                sighash,
                signer.address(),
                "note",
                "raw-walk-1",
            )
            .await
            .unwrap();

        assert_eq!(
            returned.recover_address_from_prehash(&sighash).unwrap(),
            signer.address(),
            "the retry-walked signature must recover to the expected signer"
        );
        base_create.assert();
        retry_create.assert();
    }

    /// When every candidate id resolves to a terminally failed prior
    /// attempt, the walk must stop with RetryAttemptsExhausted instead of
    /// spinning forever or resuming a corpse.
    #[tokio::test]
    async fn raw_signing_exhausts_retry_ids_on_persistent_terminal_failures() {
        let sighash = B256::random();

        let server = MockServer::start();
        // Every submission attempt reports the id as spent.
        server.mock(|when, then| {
            when.method("POST").path("/transactions");
            then.status(409)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "message":
                        "Transaction with this externalTxId already exists",
                    "code": 1438
                }));
        });
        // Every recovery finds a terminally failed corpse.
        server.mock(|when, then| {
            when.method("GET").path_includes("raw-exhaust");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "raw-corpse",
                    "status": "FAILED"
                }));
        });
        server.mock(|when, then| {
            when.method("GET").path_includes("raw-corpse");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "raw-corpse",
                    "status": "FAILED"
                }));
        });

        let service = build_test_service(mock_client(&server));

        let result = service
            .sign_raw_to_completion(
                sighash,
                Address::random(),
                "note",
                "raw-exhaust",
            )
            .await;

        assert!(
            matches!(
                result.unwrap_err(),
                FireblocksVaultError::RetryAttemptsExhausted {
                    base_external_tx_id,
                    attempts: MAX_SUBMISSION_ATTEMPTS,
                } if base_external_tx_id == "raw-exhaust"
            ),
            "persistent terminal failures must exhaust the id walk"
        );
    }
}
