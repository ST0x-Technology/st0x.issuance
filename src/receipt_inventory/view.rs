use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};

use crate::mint::{Mint, MintEvent};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiptInventoryViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
}

/// Tracks the lifecycle of ERC-1155 receipt tokens from minting through burning.
///
/// This view accumulates data across multiple Mint events to track receipt state:
/// - `Unavailable`: No mint initiated yet (initial state)
/// - `Pending`: Mint initiated, waiting for on-chain minting (has underlying/token)
/// - `Active`: Tokens minted, receipt exists with available balance
/// - `Depleted`: Receipt fully burned (balance = 0)
///
/// The view uses cross-aggregate listening, implementing both `View<Mint>` and
/// `View<Redemption>` to track receipts through their complete lifecycle. Currently
/// only Mint events are fully implemented; Redemption burn events will be added in
/// issue #25.
///
/// View ID: `issuer_request_id` (the Mint aggregate's aggregate_id)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum ReceiptInventoryView {
    Unavailable,
    Pending {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
    },
    Active {
        receipt_id: U256,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        initial_amount: U256,
        current_balance: U256,
        minted_at: DateTime<Utc>,
    },
    Depleted {
        receipt_id: U256,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        initial_amount: U256,
        depleted_at: DateTime<Utc>,
    },
}

impl Default for ReceiptInventoryView {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl ReceiptInventoryView {
    fn with_initiated_data(
        self,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
    ) -> Self {
        match self {
            Self::Unavailable => Self::Pending { underlying, token },
            other => other,
        }
    }

    fn with_tokens_minted(
        self,
        receipt_id: U256,
        shares_minted: U256,
        minted_at: DateTime<Utc>,
    ) -> Self {
        match self {
            Self::Pending { underlying, token } => Self::Active {
                receipt_id,
                underlying,
                token,
                initial_amount: shares_minted,
                current_balance: shares_minted,
                minted_at,
            },
            other => other,
        }
    }
}

impl View<Mint> for ReceiptInventoryView {
    fn update(&mut self, event: &EventEnvelope<Mint>) {
        match &event.payload {
            MintEvent::Initiated { underlying, token, .. } => {
                *self = self
                    .clone()
                    .with_initiated_data(underlying.clone(), token.clone());
            }
            MintEvent::TokensMinted {
                receipt_id,
                shares_minted,
                minted_at,
                ..
            } => {
                *self = self.clone().with_tokens_minted(
                    *receipt_id,
                    *shares_minted,
                    *minted_at,
                );
            }
            MintEvent::JournalConfirmed { .. }
            | MintEvent::JournalRejected { .. }
            | MintEvent::MintingStarted { .. }
            | MintEvent::MintingFailed { .. }
            | MintEvent::MintCompleted { .. }
            | MintEvent::ExistingMintRecovered { .. }
            | MintEvent::MintRetryStarted { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, uint};
    use chrono::Utc;
    use cqrs_es::EventEnvelope;
    use rust_decimal::Decimal;
    use std::collections::HashMap;
    use uuid::Uuid;

    use super::*;
    use crate::mint::{
        ClientId, IssuerRequestId, Mint, MintEvent, Network, Quantity,
        TokenizationRequestId,
    };

    #[test]
    fn test_unavailable_to_pending_on_mint_initiated() {
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");

        let event = MintEvent::Initiated {
            issuer_request_id: IssuerRequestId::new(Uuid::new_v4()),
            tokenization_request_id: TokenizationRequestId::new("alp-456"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: underlying.clone(),
            token: token.clone(),
            network: Network::new("base"),
            client_id: ClientId::new(),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            initiated_at: Utc::now(),
        };

        let envelope: EventEnvelope<Mint> = EventEnvelope {
            aggregate_id: "iss-123".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = ReceiptInventoryView::default();

        assert!(matches!(view, ReceiptInventoryView::Unavailable));

        view.update(&envelope);

        let ReceiptInventoryView::Pending {
            underlying: view_underlying,
            token: view_token,
        } = view
        else {
            panic!("Expected Pending variant, got {view:?}");
        };

        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
    }

    #[test]
    fn test_pending_to_active_on_tokens_minted() {
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let receipt_id = uint!(42_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let minted_at = Utc::now();

        let mut view = ReceiptInventoryView::Pending {
            underlying: underlying.clone(),
            token: token.clone(),
        };

        let event = MintEvent::TokensMinted {
            issuer_request_id: IssuerRequestId::new(Uuid::new_v4()),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            receipt_id,
            shares_minted,
            gas_used: 50000,
            block_number: 1000,
            minted_at,
        };

        let envelope: EventEnvelope<Mint> = EventEnvelope {
            aggregate_id: "iss-456".to_string(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let ReceiptInventoryView::Active {
            receipt_id: view_receipt_id,
            underlying: view_underlying,
            token: view_token,
            initial_amount,
            current_balance,
            minted_at: view_minted_at,
        } = view
        else {
            panic!("Expected Active variant, got {view:?}");
        };

        assert_eq!(view_receipt_id, receipt_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(initial_amount, shares_minted);
        assert_eq!(current_balance, shares_minted);
        assert_eq!(view_minted_at, minted_at);
    }

    #[test]
    fn test_view_stores_underlying_and_token_from_initiated() {
        let underlying = UnderlyingSymbol::new("NVDA");
        let token = TokenSymbol::new("tNVDA");

        let event = MintEvent::Initiated {
            issuer_request_id: IssuerRequestId::new(Uuid::new_v4()),
            tokenization_request_id: TokenizationRequestId::new("alp-999"),
            quantity: Quantity::new(Decimal::from(50)),
            underlying: underlying.clone(),
            token: token.clone(),
            network: Network::new("base"),
            client_id: ClientId::new(),
            wallet: address!("0xfedcbafedcbafedcbafedcbafedcbafedcbafedc"),
            initiated_at: Utc::now(),
        };

        let envelope: EventEnvelope<Mint> = EventEnvelope {
            aggregate_id: "iss-789".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = ReceiptInventoryView::default();
        view.update(&envelope);

        let ReceiptInventoryView::Pending {
            underlying: stored_underlying,
            token: stored_token,
        } = view
        else {
            panic!("Expected Pending variant, got {view:?}");
        };

        assert_eq!(stored_underlying, underlying);
        assert_eq!(stored_token, token);
    }

    #[test]
    fn test_view_combines_initiated_data_with_tokens_minted() {
        let underlying = UnderlyingSymbol::new("AMD");
        let token = TokenSymbol::new("tAMD");

        let mut view = ReceiptInventoryView::Pending {
            underlying: underlying.clone(),
            token: token.clone(),
        };

        let receipt_id = uint!(7_U256);
        let shares_minted = uint!(250_500000000000000000_U256);
        let minted_at = Utc::now();

        let event = MintEvent::TokensMinted {
            issuer_request_id: IssuerRequestId::new(Uuid::new_v4()),
            tx_hash: b256!(
                "0x1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff"
            ),
            receipt_id,
            shares_minted,
            gas_used: 75000,
            block_number: 2000,
            minted_at,
        };

        let envelope: EventEnvelope<Mint> = EventEnvelope {
            aggregate_id: "iss-combo".to_string(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let ReceiptInventoryView::Active {
            underlying: view_underlying,
            token: view_token,
            receipt_id: view_receipt_id,
            initial_amount,
            current_balance,
            minted_at: view_minted_at,
        } = view
        else {
            panic!("Expected Active variant, got {view:?}");
        };

        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_receipt_id, receipt_id);
        assert_eq!(initial_amount, shares_minted);
        assert_eq!(current_balance, shares_minted);
        assert_eq!(view_minted_at, minted_at);
    }

    #[test]
    fn test_multiple_mints_for_different_symbols() {
        let aapl_underlying = UnderlyingSymbol::new("AAPL");
        let aapl_token = TokenSymbol::new("tAAPL");

        let tsla_underlying = UnderlyingSymbol::new("TSLA");
        let tsla_token = TokenSymbol::new("tTSLA");

        let mut aapl_view = ReceiptInventoryView::default();
        let mut tsla_view = ReceiptInventoryView::default();

        let aapl_initiated = MintEvent::Initiated {
            issuer_request_id: IssuerRequestId::new(Uuid::new_v4()),
            tokenization_request_id: TokenizationRequestId::new("alp-aapl"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: aapl_underlying.clone(),
            token: aapl_token,
            network: Network::new("base"),
            client_id: ClientId::new(),
            wallet: address!("0x1111111111111111111111111111111111111111"),
            initiated_at: Utc::now(),
        };

        let tsla_initiated = MintEvent::Initiated {
            issuer_request_id: IssuerRequestId::new(Uuid::new_v4()),
            tokenization_request_id: TokenizationRequestId::new("alp-tsla"),
            quantity: Quantity::new(Decimal::from(50)),
            underlying: tsla_underlying.clone(),
            token: tsla_token,
            network: Network::new("base"),
            client_id: ClientId::new(),
            wallet: address!("0x2222222222222222222222222222222222222222"),
            initiated_at: Utc::now(),
        };

        aapl_view.update(&EventEnvelope::<Mint> {
            aggregate_id: "iss-aapl".to_string(),
            sequence: 1,
            payload: aapl_initiated,
            metadata: HashMap::new(),
        });

        tsla_view.update(&EventEnvelope::<Mint> {
            aggregate_id: "iss-tsla".to_string(),
            sequence: 1,
            payload: tsla_initiated,
            metadata: HashMap::new(),
        });

        assert!(matches!(aapl_view, ReceiptInventoryView::Pending { .. }));
        assert!(matches!(tsla_view, ReceiptInventoryView::Pending { .. }));

        if let ReceiptInventoryView::Pending { underlying, .. } = &aapl_view {
            assert_eq!(*underlying, aapl_underlying);
        }

        if let ReceiptInventoryView::Pending { underlying, .. } = &tsla_view {
            assert_eq!(*underlying, tsla_underlying);
        }
    }

    #[test]
    fn test_other_mint_events_do_not_change_state() {
        let mut view = ReceiptInventoryView::Pending {
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
        };
        let original_view = view.clone();

        let events = vec![
            MintEvent::JournalConfirmed {
                issuer_request_id: IssuerRequestId::new(Uuid::new_v4()),
                confirmed_at: Utc::now(),
            },
            MintEvent::JournalRejected {
                issuer_request_id: IssuerRequestId::new(Uuid::new_v4()),
                reason: "test".to_string(),
                rejected_at: Utc::now(),
            },
            MintEvent::MintingFailed {
                issuer_request_id: IssuerRequestId::new(Uuid::new_v4()),
                error: "test error".to_string(),
                failed_at: Utc::now(),
            },
            MintEvent::MintCompleted {
                issuer_request_id: IssuerRequestId::new(Uuid::new_v4()),
                completed_at: Utc::now(),
            },
        ];

        for event in events {
            let envelope: EventEnvelope<Mint> = EventEnvelope {
                aggregate_id: "iss-123".to_string(),
                sequence: 2,
                payload: event,
                metadata: HashMap::new(),
            };

            view.update(&envelope);

            assert_eq!(view, original_view);
        }
    }
}
