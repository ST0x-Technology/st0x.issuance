//! Pure proof helpers for burn-excess path selection and bind validation.
//!
//! Chain I/O lives in the engine (later tasks). These functions take already
//! fetched facts and refuse invalid binds without side effects.

use alloy::primitives::{Address, B256, Bytes, U256};

use super::{BurnExcess, BurnExcessPath, FundingTransferId};
use crate::mint::IssuerMintRequestId;
use crate::tokenized_asset::Network;
use crate::vault::ReceiptInformation;
use crate::vault::rain_meta;

/// Operator-selected CLI mode keyword (`internal` | `external`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BurnExcessMode {
    Internal,
    External,
}

impl BurnExcessMode {
    pub(crate) const fn as_path(self) -> BurnExcessPath {
        match self {
            Self::Internal => BurnExcessPath::Internal,
            Self::External => BurnExcessPath::External,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Internal => "internal",
            Self::External => "external",
        }
    }
}

impl std::fmt::Display for BurnExcessMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// How a CLI invocation should treat path after loading the aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathResolution {
    /// Fresh stream: path comes solely from the mode keyword.
    Start(BurnExcessPath),
    /// Resume an in-progress stream; path locked from history.
    Resume(BurnExcessPath),
    /// Terminal stream: report only; mode mismatch is not PathConflict.
    ReportOnly(BurnExcessPath),
}

/// Proven deposit bind facts (engine-fetched; pure-checked here).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DepositProof {
    pub(crate) receipt_id: U256,
    pub(crate) shares: U256,
    pub(crate) receipt_info: ReceiptInformation,
    pub(crate) receipt_info_bytes: Bytes,
    pub(crate) original_recipient: Address,
    pub(crate) vault: Address,
}

/// Expected funding Transfer shape for Path B.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FundingTransferExpectation {
    pub(crate) network: Network,
    pub(crate) vault: Address,
    pub(crate) tx_hash: B256,
    pub(crate) from: Address,
    pub(crate) to: Address,
    pub(crate) amount: U256,
}

/// One Transfer log candidate from a funding transaction receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FundingTransferCandidate {
    pub(crate) log_index: u64,
    pub(crate) vault: Address,
    pub(crate) from: Address,
    pub(crate) to: Address,
    pub(crate) amount: U256,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum BurnExcessProofError {
    #[error(
        "path conflict: stream is locked to {locked} but mode keyword is \
         {requested}; re-run with `burn-excess {locked}`"
    )]
    PathConflict { locked: BurnExcessPath, requested: BurnExcessMode },

    #[error(
        "funding tx hash {provided:?} does not match recorded funding \
         {recorded:?}"
    )]
    FundingMismatch { provided: B256, recorded: B256 },

    #[error(
        "issuer share balance {balance} is not exactly the excess amount \
         {amount}; move exact excess to the issuer, or use \
         `burn-excess external --funding-tx-hash …` after funding"
    )]
    IssuerShareBalanceNotExact { balance: U256, amount: U256 },

    #[error(
        "issuer receipt balance {balance} for receipt {receipt_id} is below \
         the excess amount {amount}"
    )]
    IssuerReceiptBalanceInsufficient {
        receipt_id: U256,
        balance: U256,
        amount: U256,
    },

    #[error("deposit receipt id {found} does not match expected {expected}")]
    DepositReceiptIdMismatch { expected: U256, found: U256 },

    #[error("deposit shares {found} do not match expected {expected}")]
    DepositSharesMismatch { expected: U256, found: U256 },

    #[error(
        "deposit receiptInformation issuer request {found} does not match \
         expected {expected}"
    )]
    DepositIssuerRequestMismatch {
        expected: IssuerMintRequestId,
        found: IssuerMintRequestId,
    },

    #[error("deposit receiptInformation is empty")]
    EmptyReceiptInformation,

    #[error("deposit receiptInformation is not Rain metadata v1")]
    ReceiptInformationNotRainMeta,

    #[error("failed to decode Rain receipt metadata: {message}")]
    ReceiptInformationDecode { message: String },

    #[error(
        "receiptInformation JSON is not valid ReceiptInformation: {message}"
    )]
    ReceiptInformationParse { message: String },

    #[error("no matching funding Transfer log in transaction {tx_hash:?}")]
    FundingTransferNotFound { tx_hash: B256 },

    #[error(
        "ambiguous funding Transfer: {count} logs match vault/from/to/amount \
         in transaction {tx_hash:?}"
    )]
    FundingTransferAmbiguous { tx_hash: B256, count: usize },

    #[error(
        "funding Transfer from {found:?} does not match original mint \
         recipient {expected:?}"
    )]
    FundingFromMismatch { expected: Address, found: Address },

    #[error(
        "funding Transfer to {found:?} does not match issuer wallet \
         {expected:?}"
    )]
    FundingToMismatch { expected: Address, found: Address },

    #[error(
        "funding Transfer amount {found} does not match excess amount \
         {expected}"
    )]
    FundingAmountMismatch { expected: U256, found: U256 },

    #[error(
        "funding Transfer vault {found:?} does not match expected vault \
         {expected:?}"
    )]
    FundingVaultMismatch { expected: Address, found: Address },

    #[error(
        "funding log already bound to a Redemption aggregate \
         (tx={tx_hash:#x} log_index={log_index}); refuse exclusion"
    )]
    FundingAlreadyRedeemed { tx_hash: B256, log_index: u64 },

    #[error(
        "funding transaction {tx_hash:#x} is already bound to a Redemption \
         aggregate; refuse exclusion"
    )]
    FundingAlreadyRedeemedTx { tx_hash: B256 },

    #[error(
        "funding transaction {tx_hash:#x} has a Transfer log without \
         log_index; refuse funding selection"
    )]
    FundingLogIndexMissing { tx_hash: B256 },

    #[error(
        "external mode requires --funding-tx-hash (missing on prove or execute)"
    )]
    FundingTxHashRequired,

    #[error(
        "internal mode requires deposit original recipient to be the issuer \
         wallet ({issuer_wallet:?}); found {original_recipient:?}. Shares were \
         minted to a non-issuer recipient — fund them back then use \
         `burn-excess external --funding-tx-hash …`"
    )]
    InternalRequiresIssuerAsRecipient {
        original_recipient: Address,
        issuer_wallet: Address,
    },
}

/// D0.3–D0.4: resolve path from mode keyword and optional loaded aggregate.
pub(crate) fn resolve_path(
    mode: BurnExcessMode,
    state: Option<&BurnExcess>,
) -> Result<PathResolution, BurnExcessProofError> {
    let requested = mode.as_path();

    match state {
        None => Ok(PathResolution::Start(requested)),
        Some(BurnExcess::FundingExcluded { .. }) => {
            require_path(mode, BurnExcessPath::External)?;
            Ok(PathResolution::Resume(BurnExcessPath::External))
        }
        Some(
            BurnExcess::Intended { path, .. }
            | BurnExcess::Submitted { path, .. },
        ) => {
            require_path(mode, *path)?;
            Ok(PathResolution::Resume(*path))
        }
        Some(
            BurnExcess::Completed { path, .. }
            | BurnExcess::Closed { path, .. },
        ) => Ok(PathResolution::ReportOnly(*path)),
    }
}

fn require_path(
    mode: BurnExcessMode,
    locked: BurnExcessPath,
) -> Result<(), BurnExcessProofError> {
    if mode.as_path() == locked {
        Ok(())
    } else {
        Err(BurnExcessProofError::PathConflict { locked, requested: mode })
    }
}

/// When resuming Path B, the CLI funding hash must match the recorded log.
pub(crate) fn require_funding_hash_match(
    provided: B256,
    recorded: &FundingTransferId,
) -> Result<(), BurnExcessProofError> {
    if provided == recorded.tx_hash {
        Ok(())
    } else {
        Err(BurnExcessProofError::FundingMismatch {
            provided,
            recorded: recorded.tx_hash,
        })
    }
}

/// Path A / Path B share balance gate: issuer must hold exactly `amount`.
pub(crate) fn require_exact_issuer_share_balance(
    balance: U256,
    amount: U256,
) -> Result<(), BurnExcessProofError> {
    if balance == amount {
        Ok(())
    } else {
        Err(BurnExcessProofError::IssuerShareBalanceNotExact {
            balance,
            amount,
        })
    }
}

/// Issuer receipt balance for the excess receipt must cover the burn.
pub(crate) fn require_issuer_receipt_balance(
    receipt_id: U256,
    balance: U256,
    amount: U256,
) -> Result<(), BurnExcessProofError> {
    if balance >= amount {
        Ok(())
    } else {
        Err(BurnExcessProofError::IssuerReceiptBalanceInsufficient {
            receipt_id,
            balance,
            amount,
        })
    }
}

/// Strict deposit `receiptInformation` decoder.
///
/// Unlike inventory `determine_source`, this refuses empty, non-Rain-meta, and
/// unparseable payloads — burn-excess must bind the excess to a known issuer
/// request, not forgive and classify as External.
pub(crate) fn decode_receipt_information_strict(
    receipt_information: &Bytes,
) -> Result<ReceiptInformation, BurnExcessProofError> {
    if receipt_information.is_empty() {
        return Err(BurnExcessProofError::EmptyReceiptInformation);
    }

    if !rain_meta::is_rain_meta(receipt_information) {
        return Err(BurnExcessProofError::ReceiptInformationNotRainMeta);
    }

    let json_bytes = rain_meta::decode_receipt_meta(receipt_information)
        .map_err(|error| BurnExcessProofError::ReceiptInformationDecode {
            message: error.to_string(),
        })?;

    serde_json::from_slice(&json_bytes).map_err(|error| {
        BurnExcessProofError::ReceiptInformationParse {
            message: error.to_string(),
        }
    })
}

/// Bind a fetched deposit proof to operator-supplied excess args.
pub(crate) fn bind_deposit_proof(
    expected_issuer_request_id: &IssuerMintRequestId,
    expected_receipt_id: U256,
    expected_shares: U256,
    proof: &DepositProof,
) -> Result<(), BurnExcessProofError> {
    if proof.receipt_id != expected_receipt_id {
        return Err(BurnExcessProofError::DepositReceiptIdMismatch {
            expected: expected_receipt_id,
            found: proof.receipt_id,
        });
    }

    if proof.shares != expected_shares {
        return Err(BurnExcessProofError::DepositSharesMismatch {
            expected: expected_shares,
            found: proof.shares,
        });
    }

    if &proof.receipt_info.issuer_request_id != expected_issuer_request_id {
        return Err(BurnExcessProofError::DepositIssuerRequestMismatch {
            expected: expected_issuer_request_id.clone(),
            found: proof.receipt_info.issuer_request_id.clone(),
        });
    }

    Ok(())
}

/// Select the unique funding Transfer log matching Path B expectations.
pub(crate) fn select_funding_transfer(
    expectation: &FundingTransferExpectation,
    candidates: &[FundingTransferCandidate],
) -> Result<FundingTransferId, BurnExcessProofError> {
    let matches: Vec<&FundingTransferCandidate> = candidates
        .iter()
        .filter(|candidate| {
            candidate.vault == expectation.vault
                && candidate.from == expectation.from
                && candidate.to == expectation.to
                && candidate.amount == expectation.amount
        })
        .collect();

    match matches.as_slice() {
        [] => {
            if let Some(mismatch) =
                first_shape_mismatch(expectation, candidates)
            {
                return Err(mismatch);
            }

            Err(BurnExcessProofError::FundingTransferNotFound {
                tx_hash: expectation.tx_hash,
            })
        }
        [only] => Ok(FundingTransferId {
            network: expectation.network,
            vault: only.vault,
            tx_hash: expectation.tx_hash,
            log_index: only.log_index,
            from: only.from,
            to: only.to,
            amount: only.amount,
        }),
        many => Err(BurnExcessProofError::FundingTransferAmbiguous {
            tx_hash: expectation.tx_hash,
            count: many.len(),
        }),
    }
}

fn first_shape_mismatch(
    expectation: &FundingTransferExpectation,
    candidates: &[FundingTransferCandidate],
) -> Option<BurnExcessProofError> {
    // Prefer a same-vault candidate so the first mismatch is useful; fall back
    // to the first log when none share the expected vault.
    let candidate = candidates
        .iter()
        .find(|candidate| candidate.vault == expectation.vault)
        .or_else(|| candidates.first())?;

    if candidate.vault != expectation.vault {
        return Some(BurnExcessProofError::FundingVaultMismatch {
            expected: expectation.vault,
            found: candidate.vault,
        });
    }

    if candidate.from != expectation.from {
        return Some(BurnExcessProofError::FundingFromMismatch {
            expected: expectation.from,
            found: candidate.from,
        });
    }

    if candidate.to != expectation.to {
        return Some(BurnExcessProofError::FundingToMismatch {
            expected: expectation.to,
            found: candidate.to,
        });
    }

    if candidate.amount != expectation.amount {
        return Some(BurnExcessProofError::FundingAmountMismatch {
            expected: expectation.amount,
            found: candidate.amount,
        });
    }

    None
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, Bytes, U256, address, b256};
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::Quantity;
    use crate::mint::{IssuerMintRequestId, TokenizationRequestId};
    use crate::tokenized_asset::{Network, UnderlyingSymbol};
    use crate::vault::ReceiptInformation;

    fn issuer_request() -> IssuerMintRequestId {
        IssuerMintRequestId::new(
            Uuid::parse_str("d3042b2f-4845-4acd-9a67-92d743e4e58c").unwrap(),
        )
    }

    fn other_issuer_request() -> IssuerMintRequestId {
        IssuerMintRequestId::new(Uuid::new_v4())
    }

    fn sample_receipt_info(
        issuer_request_id: IssuerMintRequestId,
    ) -> ReceiptInformation {
        ReceiptInformation::new(
            TokenizationRequestId::new("tok-1"),
            issuer_request_id,
            UnderlyingSymbol::new("PTY").unwrap(),
            Quantity::new(rust_decimal::Decimal::new(750, 3)),
            Utc::now(),
            None,
        )
    }

    fn sample_bind() -> super::super::ExcessBurnBind {
        super::super::ExcessBurnBind {
            issuer_request_id: issuer_request(),
            deposit_tx_hash: b256!(
                "0x1bb6afc590e58095099373a8fea2242017b31acc7940bcd0d6b68820ebeb8ebd"
            ),
            receipt_id: U256::from(7u64),
            shares: U256::from(750_000_000_000_000_000u64),
            original_recipient: address!(
                "0xA9C16673F65AE808688cB18952AFE3d9658C808f"
            ),
            vault: address!("0x1111111111111111111111111111111111111111"),
            network: Network::Base,
            issuer_wallet: address!(
                "0x3d0CD66EFA66c05d86c3d4316B03eAE87ab9E8aE"
            ),
        }
    }

    fn funding_id() -> FundingTransferId {
        FundingTransferId {
            network: Network::Base,
            vault: address!("0x1111111111111111111111111111111111111111"),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            log_index: 3,
            from: address!("0xA9C16673F65AE808688cB18952AFE3d9658C808f"),
            to: address!("0x3d0CD66EFA66c05d86c3d4316B03eAE87ab9E8aE"),
            amount: U256::from(750_000_000_000_000_000u64),
        }
    }

    #[test]
    fn resolve_path_not_started_uses_mode_keyword_only() {
        assert_eq!(
            resolve_path(BurnExcessMode::Internal, None).unwrap(),
            PathResolution::Start(BurnExcessPath::Internal)
        );
        assert_eq!(
            resolve_path(BurnExcessMode::External, None).unwrap(),
            PathResolution::Start(BurnExcessPath::External)
        );
    }

    #[test]
    fn resolve_path_funding_excluded_locks_external() {
        let state = BurnExcess::FundingExcluded {
            bind: sample_bind(),
            funding_log_id: funding_id(),
            reason: "dup".into(),
            incident_id: None,
            excluded_at: Utc::now(),
        };

        assert_eq!(
            resolve_path(BurnExcessMode::External, Some(&state)).unwrap(),
            PathResolution::Resume(BurnExcessPath::External)
        );

        let conflict =
            resolve_path(BurnExcessMode::Internal, Some(&state)).unwrap_err();
        assert!(matches!(
            conflict,
            BurnExcessProofError::PathConflict {
                locked: BurnExcessPath::External,
                requested: BurnExcessMode::Internal,
            }
        ));
    }

    #[test]
    fn resolve_path_intended_internal_rejects_external_mode() {
        let state = BurnExcess::Intended {
            bind: sample_bind(),
            path: BurnExcessPath::Internal,
            funding_log_id: None,
            reason: "dup".into(),
            incident_id: None,
            sendable_tx: crate::vault::SendableTxWithHash::default(),
            intended_at: Utc::now(),
        };

        let conflict =
            resolve_path(BurnExcessMode::External, Some(&state)).unwrap_err();
        assert!(matches!(
            conflict,
            BurnExcessProofError::PathConflict {
                locked: BurnExcessPath::Internal,
                requested: BurnExcessMode::External,
            }
        ));
    }

    #[test]
    fn resolve_path_completed_is_report_only_any_mode() {
        let state = BurnExcess::Completed {
            bind: sample_bind(),
            path: BurnExcessPath::External,
            funding_log_id: Some(funding_id()),
            burn_tx_hash: B256::ZERO,
            block_number: 1,
            completed_at: Utc::now(),
        };

        assert_eq!(
            resolve_path(BurnExcessMode::Internal, Some(&state)).unwrap(),
            PathResolution::ReportOnly(BurnExcessPath::External)
        );
        assert_eq!(
            resolve_path(BurnExcessMode::External, Some(&state)).unwrap(),
            PathResolution::ReportOnly(BurnExcessPath::External)
        );
    }

    #[test]
    fn funding_hash_match_and_mismatch() {
        let recorded = funding_id();
        require_funding_hash_match(recorded.tx_hash, &recorded).unwrap();
        let err =
            require_funding_hash_match(B256::ZERO, &recorded).unwrap_err();
        assert!(matches!(err, BurnExcessProofError::FundingMismatch { .. }));
    }

    #[test]
    fn exact_share_balance_gate() {
        let amount = U256::from(750_000_000_000_000_000u64);
        require_exact_issuer_share_balance(amount, amount).unwrap();
        let err = require_exact_issuer_share_balance(U256::from(1u64), amount)
            .unwrap_err();
        assert!(matches!(
            err,
            BurnExcessProofError::IssuerShareBalanceNotExact { .. }
        ));
    }

    #[test]
    fn receipt_balance_gate() {
        let receipt_id = U256::from(7u64);
        let amount = U256::from(10u64);
        require_issuer_receipt_balance(receipt_id, U256::from(10u64), amount)
            .unwrap();
        require_issuer_receipt_balance(receipt_id, U256::from(11u64), amount)
            .unwrap();
        let err = require_issuer_receipt_balance(
            receipt_id,
            U256::from(9u64),
            amount,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            BurnExcessProofError::IssuerReceiptBalanceInsufficient { .. }
        ));
    }

    #[test]
    fn strict_receipt_information_decoder_round_trip() {
        let info = sample_receipt_info(issuer_request());
        let encoded = info.encode(None).unwrap();
        let decoded = decode_receipt_information_strict(&encoded).unwrap();
        assert_eq!(decoded.issuer_request_id, info.issuer_request_id);
    }

    #[test]
    fn strict_receipt_information_refuses_empty_and_plain_json() {
        assert!(matches!(
            decode_receipt_information_strict(&Bytes::new()).unwrap_err(),
            BurnExcessProofError::EmptyReceiptInformation
        ));

        let plain = Bytes::from(
            serde_json::to_vec(&sample_receipt_info(issuer_request())).unwrap(),
        );
        assert!(matches!(
            decode_receipt_information_strict(&plain).unwrap_err(),
            BurnExcessProofError::ReceiptInformationNotRainMeta
        ));
    }

    #[test]
    fn bind_deposit_proof_refusal_matrix() {
        let issuer = issuer_request();
        let receipt_id = U256::from(7u64);
        let shares = U256::from(750_000_000_000_000_000u64);
        let info = sample_receipt_info(issuer.clone());
        let encoded = info.encode(None).unwrap();

        let ok = DepositProof {
            receipt_id,
            shares,
            receipt_info: info,
            receipt_info_bytes: encoded,
            original_recipient: Address::ZERO,
            vault: Address::ZERO,
        };
        bind_deposit_proof(&issuer, receipt_id, shares, &ok).unwrap();

        let wrong_receipt =
            DepositProof { receipt_id: U256::from(8u64), ..ok.clone() };
        assert!(matches!(
            bind_deposit_proof(&issuer, receipt_id, shares, &wrong_receipt)
                .unwrap_err(),
            BurnExcessProofError::DepositReceiptIdMismatch { .. }
        ));

        let wrong_shares =
            DepositProof { shares: U256::from(1u64), ..ok.clone() };
        assert!(matches!(
            bind_deposit_proof(&issuer, receipt_id, shares, &wrong_shares)
                .unwrap_err(),
            BurnExcessProofError::DepositSharesMismatch { .. }
        ));

        let wrong_issuer_info = sample_receipt_info(other_issuer_request());
        let wrong_issuer =
            DepositProof { receipt_info: wrong_issuer_info, ..ok };
        assert!(matches!(
            bind_deposit_proof(&issuer, receipt_id, shares, &wrong_issuer)
                .unwrap_err(),
            BurnExcessProofError::DepositIssuerRequestMismatch { .. }
        ));
    }

    #[test]
    fn select_funding_transfer_unique_match() {
        let expectation = FundingTransferExpectation {
            network: Network::Base,
            vault: address!("0x1111111111111111111111111111111111111111"),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            from: address!("0xA9C16673F65AE808688cB18952AFE3d9658C808f"),
            to: address!("0x3d0CD66EFA66c05d86c3d4316B03eAE87ab9E8aE"),
            amount: U256::from(750_000_000_000_000_000u64),
        };

        let candidates = vec![
            FundingTransferCandidate {
                log_index: 1,
                vault: expectation.vault,
                from: address!("0x2222222222222222222222222222222222222222"),
                to: expectation.to,
                amount: expectation.amount,
            },
            FundingTransferCandidate {
                log_index: 2,
                vault: expectation.vault,
                from: expectation.from,
                to: expectation.to,
                amount: expectation.amount,
            },
        ];

        let selected =
            select_funding_transfer(&expectation, &candidates).unwrap();
        assert_eq!(selected.log_index, 2);
        assert_eq!(selected.tx_hash, expectation.tx_hash);
    }

    #[test]
    fn select_funding_transfer_not_found_and_ambiguous() {
        let expectation = FundingTransferExpectation {
            network: Network::Base,
            vault: address!("0x1111111111111111111111111111111111111111"),
            tx_hash: B256::ZERO,
            from: address!("0xA9C16673F65AE808688cB18952AFE3d9658C808f"),
            to: address!("0x3d0CD66EFA66c05d86c3d4316B03eAE87ab9E8aE"),
            amount: U256::from(1u64),
        };

        let none = select_funding_transfer(&expectation, &[]).unwrap_err();
        assert!(matches!(
            none,
            BurnExcessProofError::FundingTransferNotFound { .. }
        ));

        let match_shape = FundingTransferCandidate {
            log_index: 0,
            vault: expectation.vault,
            from: expectation.from,
            to: expectation.to,
            amount: expectation.amount,
        };
        let ambiguous = select_funding_transfer(
            &expectation,
            &[
                match_shape.clone(),
                FundingTransferCandidate { log_index: 1, ..match_shape },
            ],
        )
        .unwrap_err();
        assert!(matches!(
            ambiguous,
            BurnExcessProofError::FundingTransferAmbiguous { count: 2, .. }
        ));
    }

    #[test]
    fn select_funding_reports_shape_mismatch_when_only_wrong_from() {
        let expectation = FundingTransferExpectation {
            network: Network::Base,
            vault: address!("0x1111111111111111111111111111111111111111"),
            tx_hash: B256::ZERO,
            from: address!("0xA9C16673F65AE808688cB18952AFE3d9658C808f"),
            to: address!("0x3d0CD66EFA66c05d86c3d4316B03eAE87ab9E8aE"),
            amount: U256::from(1u64),
        };
        let candidates = [FundingTransferCandidate {
            log_index: 0,
            vault: expectation.vault,
            from: address!("0x2222222222222222222222222222222222222222"),
            to: expectation.to,
            amount: expectation.amount,
        }];
        let err =
            select_funding_transfer(&expectation, &candidates).unwrap_err();
        assert!(matches!(
            err,
            BurnExcessProofError::FundingFromMismatch { .. }
        ));
    }

    #[test]
    fn funding_already_redeemed_error_is_constructible() {
        let err = BurnExcessProofError::FundingAlreadyRedeemed {
            tx_hash: B256::ZERO,
            log_index: 2,
        };
        let message = err.to_string();
        assert!(message.contains("Redemption"));
        assert!(message.contains("log_index=2"));
        assert!(message.contains("0x"));
    }

    #[test]
    fn funding_already_redeemed_tx_error_is_constructible() {
        let err = BurnExcessProofError::FundingAlreadyRedeemedTx {
            tx_hash: B256::ZERO,
        };
        let message = err.to_string();
        assert!(message.contains("Redemption"));
        assert!(message.contains("0x"));
    }

    #[test]
    fn first_shape_mismatch_prefers_same_vault_candidate() {
        let expectation = FundingTransferExpectation {
            network: Network::Base,
            vault: address!("0x1111111111111111111111111111111111111111"),
            tx_hash: B256::ZERO,
            from: address!("0x2222222222222222222222222222222222222222"),
            to: address!("0x3333333333333333333333333333333333333333"),
            amount: U256::from(1u64),
        };
        let other_vault =
            address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let candidates = vec![
            FundingTransferCandidate {
                log_index: 0,
                vault: other_vault,
                from: expectation.from,
                to: expectation.to,
                amount: expectation.amount,
            },
            FundingTransferCandidate {
                log_index: 1,
                vault: expectation.vault,
                from: address!("0x4444444444444444444444444444444444444444"),
                to: expectation.to,
                amount: expectation.amount,
            },
        ];
        let err =
            select_funding_transfer(&expectation, &candidates).unwrap_err();
        assert!(
            matches!(err, BurnExcessProofError::FundingFromMismatch { .. }),
            "same-vault candidate should drive the mismatch report, got {err:?}"
        );
    }

    /// The fallback the preference test cannot reach: with nothing on the
    /// expected vault, the operator has to be told the funding transaction
    /// touched the wrong vault rather than which field differed on it.
    #[test]
    fn first_shape_mismatch_reports_vault_when_no_candidate_matches_it() {
        let expectation = FundingTransferExpectation {
            network: Network::Base,
            vault: address!("0x1111111111111111111111111111111111111111"),
            tx_hash: B256::ZERO,
            from: address!("0x2222222222222222222222222222222222222222"),
            to: address!("0x3333333333333333333333333333333333333333"),
            amount: U256::from(1u64),
        };
        let other_vault =
            address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let candidates = vec![FundingTransferCandidate {
            log_index: 0,
            vault: other_vault,
            // Every other field matches, so only the vault check can fire.
            from: expectation.from,
            to: expectation.to,
            amount: expectation.amount,
        }];

        let err =
            select_funding_transfer(&expectation, &candidates).unwrap_err();
        assert!(
            matches!(
                err,
                BurnExcessProofError::FundingVaultMismatch { expected, found }
                    if expected == expectation.vault && found == other_vault
            ),
            "no same-vault candidate must report the vault, got {err:?}"
        );
    }
}
