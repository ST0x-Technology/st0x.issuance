#![allow(dead_code)]

use httpmock::Mock;
use httpmock::prelude::*;
use serde_json::json;
use std::sync::{Arc, Mutex};

use st0x_issuance::test_utils::test_alpaca_legacy_auth;

pub fn setup_mint_mocks(mock_alpaca: &MockServer) -> Mock {
    let (basic_auth, api_key, api_secret) = test_alpaca_legacy_auth();

    mock_alpaca.mock(|when, then| {
        when.method(POST)
            .path("/v1/accounts/test-account/tokenization/callback/mint")
            .header("authorization", basic_auth)
            .header("APCA-API-KEY-ID", api_key)
            .header("APCA-API-SECRET-KEY", api_secret);
        then.status(200).body("");
    })
}

pub struct RedemptionState {
    pub issuer_request_id: String,
    pub tx_hash: String,
    pub qty: String,
    pub underlying_symbol: String,
    pub token_symbol: String,
    pub wallet_address: String,
}

pub fn setup_redemption_mocks(mock_alpaca: &MockServer) -> (Mock, Mock) {
    let (basic_auth, api_key, api_secret) = test_alpaca_legacy_auth();
    let shared_state = Arc::new(Mutex::new(Vec::<RedemptionState>::new()));

    let shared_state_redeem = Arc::clone(&shared_state);

    let redeem_mock = mock_alpaca.mock(|when, then| {
        when.method(POST)
            .path("/v1/accounts/test-account/tokenization/redeem")
            .header("authorization", &basic_auth)
            .header("APCA-API-KEY-ID", &api_key)
            .header("APCA-API-SECRET-KEY", &api_secret);

        then.status(200).respond_with(
            move |req: &httpmock::HttpMockRequest| {
                let body: serde_json::Value =
                    serde_json::from_slice(req.body().as_ref()).unwrap();
                let issuer_request_id =
                    body["issuer_request_id"].as_str().unwrap().to_string();
                let tx_hash =
                    body["tx_hash"].as_str().unwrap().to_string();
                let qty = body["qty"].as_str().unwrap().to_string();
                let underlying_symbol = body["underlying_symbol"]
                    .as_str()
                    .unwrap()
                    .to_string();
                let token_symbol =
                    body["token_symbol"].as_str().unwrap().to_string();
                let wallet_address = body["wallet_address"]
                    .as_str()
                    .unwrap()
                    .to_string();

                shared_state_redeem.lock().unwrap().push(
                    RedemptionState {
                        issuer_request_id: issuer_request_id.clone(),
                        tx_hash: tx_hash.clone(),
                        qty: qty.clone(),
                        underlying_symbol: underlying_symbol.clone(),
                        token_symbol: token_symbol.clone(),
                        wallet_address: wallet_address.clone(),
                    },
                );

                let response_body = serde_json::to_string(&json!({
                    "tokenization_request_id": format!("tok-redeem-{}", issuer_request_id),
                    "issuer_request_id": issuer_request_id,
                    "created_at": "2025-09-12T17:28:48.642437-04:00",
                    "type": "redeem",
                    "status": "pending",
                    "underlying_symbol": underlying_symbol,
                    "token_symbol": token_symbol,
                    "qty": qty,
                    "issuer": "test-issuer",
                    "network": "base",
                    "wallet_address": wallet_address,
                    "tx_hash": tx_hash,
                    "fees": "0.5"
                }))
                .unwrap();

                httpmock::HttpMockResponse {
                    status: Some(200),
                    headers: None,
                    body: Some(response_body.into()),
                }
            },
        );
    });

    let poll_mock = mock_alpaca.mock(|when, then| {
        when.method(GET)
            .path_matches(
                r"^/v1/accounts/test-account/tokenization/requests.*",
            )
            .header("authorization", &basic_auth)
            .header("APCA-API-KEY-ID", &api_key)
            .header("APCA-API-SECRET-KEY", &api_secret);

        then.status(200).respond_with(
            move |_req: &httpmock::HttpMockRequest| {
                let responses: Vec<_> = {
                    let states = shared_state.lock().unwrap();
                    states
                        .iter()
                        .map(|state| {
                            json!({
                                "tokenization_request_id": format!("tok-redeem-{}", state.issuer_request_id),
                                "issuer_request_id": state.issuer_request_id,
                                "created_at": "2025-09-12T17:28:48.642437-04:00",
                                "type": "redeem",
                                "status": "completed",
                                "underlying_symbol": state.underlying_symbol,
                                "token_symbol": state.token_symbol,
                                "qty": state.qty,
                                "issuer": "test-issuer",
                                "network": "base",
                                "wallet_address": state.wallet_address,
                                "tx_hash": state.tx_hash,
                                "fees": "0.5"
                            })
                        })
                        .collect()
                };

                let response_body =
                    serde_json::to_string(&responses).unwrap();

                httpmock::HttpMockResponse {
                    status: Some(200),
                    headers: None,
                    body: Some(response_body.into()),
                }
            },
        );
    });

    (redeem_mock, poll_mock)
}
