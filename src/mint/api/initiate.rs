use alloy::primitives::Address;
use rocket::post;
use rocket::serde::json::Json;
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::error;

use super::{
    MintApiError, MintResponse, validate_asset_exists, validate_client_eligible,
};
use crate::auth::IssuerAuth;
use crate::mint::{
    ClientId, IssuerRequestId, MintCommand, MintView, Network, Quantity,
    TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    view::find_by_issuer_request_id,
};

#[derive(Debug, Deserialize)]
pub(crate) struct MintRequest {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    #[serde(rename = "qty")]
    pub(crate) quantity: Decimal,
    #[serde(rename = "underlying_symbol")]
    pub(crate) underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    pub(crate) token: TokenSymbol,
    pub(crate) network: Network,
    pub(crate) client_id: ClientId,
    #[serde(rename = "wallet_address")]
    pub(crate) wallet: Address,
}

#[tracing::instrument(skip(_auth, cqrs, pool), fields(
    tokenization_request_id = %request.tokenization_request_id.0,
    underlying = %request.underlying.0,
    client_id = %request.client_id,
    quantity = %request.quantity
))]
#[post("/inkind/issuance", format = "json", data = "<request>")]
pub(crate) async fn initiate_mint(
    _auth: IssuerAuth,
    cqrs: &rocket::State<crate::MintCqrs>,
    pool: &rocket::State<sqlx::Pool<sqlx::Sqlite>>,
    request: Json<MintRequest>,
) -> Result<Json<MintResponse>, MintApiError> {
    let request = request.into_inner();

    if request.quantity <= Decimal::ZERO {
        return Err(MintApiError::InvalidQuantity);
    }

    validate_asset_exists(
        pool.inner(),
        &request.underlying,
        &request.token,
        &request.network,
    )
    .await?;

    validate_client_eligible(pool.inner(), &request.client_id, &request.wallet)
        .await?;

    let issuer_request_id =
        IssuerRequestId::new(uuid::Uuid::new_v4().to_string());

    let command = MintCommand::Initiate {
        issuer_request_id: issuer_request_id.clone(),
        tokenization_request_id: request.tokenization_request_id,
        quantity: Quantity::new(request.quantity),
        underlying: request.underlying,
        token: request.token,
        network: request.network,
        client_id: request.client_id,
        wallet: request.wallet,
    };

    cqrs.execute(&issuer_request_id.0, command).await.map_err(|e| {
        error!("Failed to execute mint command: {e}");
        MintApiError::CommandExecutionFailed(Box::new(e))
    })?;

    let mint_view = find_by_issuer_request_id(pool.inner(), &issuer_request_id)
        .await
        .map_err(|e| {
            error!("Failed to find mint by issuer_request_id: {e}");
            MintApiError::MintViewQueryFailed(e)
        })?
        .ok_or(MintApiError::MintViewNotFound)?;

    let MintView::Initiated { issuer_request_id, .. } = mint_view else {
        return Err(MintApiError::UnexpectedMintState);
    };

    Ok(Json(MintResponse { issuer_request_id, status: "created".to_string() }))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use rocket::http::{ContentType, Header, Status};
    use rocket::routes;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use tracing::debug;

    use super::initiate_mint;
    use crate::account::{AccountCommand, AlpacaAccountNumber, Email};
    use crate::auth::FailedAuthRateLimiter;
    use crate::mint::api::test_utils::{
        TestAccountAndAsset, TestHarness, test_config,
    };
    use crate::mint::api::{ErrorResponse, MintResponse};
    use crate::mint::{
        ClientId, IssuerRequestId, MintCommand, MintView, Network, Quantity,
        TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
        view::find_by_issuer_request_id,
    };
    use crate::tokenized_asset::TokenizedAssetCommand;

    #[tokio::test]
    async fn test_initiate_mint_returns_issuer_request_id() {
        let TestHarness {
            pool,
            account_cqrs,
            asset_cqrs: tokenized_asset_cqrs,
            mint_cqrs,
            ..
        } = TestHarness::new().await;

        let email = Email::new("test@placeholder.com".to_string())
            .expect("Valid email");
        let client_id = ClientId::new();

        let register_cmd =
            AccountCommand::Register { client_id, email: email.clone() };

        let aggregate_id = client_id.to_string();
        account_cqrs
            .execute(&aggregate_id, register_cmd)
            .await
            .expect("Failed to register account");

        let link_cmd = AccountCommand::LinkToAlpaca {
            alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
        };

        account_cqrs
            .execute(&aggregate_id, link_cmd)
            .await
            .expect("Failed to link account to Alpaca");

        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_cmd = AccountCommand::WhitelistWallet { wallet };

        account_cqrs
            .execute(&aggregate_id, whitelist_cmd)
            .await
            .expect("Failed to whitelist wallet");

        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        let asset_cmd = TokenizedAssetCommand::Add {
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            vault,
        };
        tokenized_asset_cqrs
            .execute(&underlying.0, asset_cmd)
            .await
            .expect("Failed to add asset");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "100.5",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "network": "base",
            "client_id": client_id,
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let mint_response: MintResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert!(!mint_response.issuer_request_id.0.is_empty());
        assert_eq!(mint_response.status, "created");
    }

    #[tokio::test]
    async fn test_initiate_mint_rejects_unknown_asset() {
        let TestHarness {
            pool,
            account_cqrs,
            asset_cqrs: tokenized_asset_cqrs,
            mint_cqrs,
            ..
        } = TestHarness::new().await;

        let email = Email::new("test@placeholder.com".to_string())
            .expect("Valid email");
        let client_id = ClientId::new();

        let register_cmd =
            AccountCommand::Register { client_id, email: email.clone() };

        let aggregate_id = client_id.to_string();
        account_cqrs
            .execute(&aggregate_id, register_cmd)
            .await
            .expect("Failed to register account");

        let link_cmd = AccountCommand::LinkToAlpaca {
            alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
        };

        account_cqrs
            .execute(&aggregate_id, link_cmd)
            .await
            .expect("Failed to link account to Alpaca");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "100.5",
            "underlying_symbol": "UNKNOWN",
            "token_symbol": "tUNKNOWN",
            "network": "base",
            "client_id": client_id,
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::BadRequest);

        let error_response: ErrorResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert_eq!(
            error_response.error,
            "Invalid Token: Token not available on the network"
        );
    }

    #[tokio::test]
    async fn test_initiate_mint_rejects_negative_quantity() {
        let TestHarness {
            pool,
            account_cqrs,
            asset_cqrs: tokenized_asset_cqrs,
            mint_cqrs,
            ..
        } = TestHarness::new().await;

        let client_id = ClientId::new();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "-10",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "network": "base",
            "client_id": client_id,
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::BadRequest);

        let error_response: ErrorResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert_eq!(
            error_response.error,
            "Failed Validation: Invalid data payload"
        );
    }

    #[tokio::test]
    async fn test_initiate_mint_rejects_unknown_client_id() {
        let TestHarness {
            pool,
            account_cqrs,
            asset_cqrs: tokenized_asset_cqrs,
            mint_cqrs,
            ..
        } = TestHarness::new().await;

        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        let asset_cmd = TokenizedAssetCommand::Add {
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            vault,
        };
        tokenized_asset_cqrs
            .execute(&underlying.0, asset_cmd)
            .await
            .expect("Failed to add asset");

        let nonexistent_client_id = ClientId::new();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "100.5",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "network": "base",
            "client_id": nonexistent_client_id,
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::BadRequest);

        let error_response: ErrorResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert_eq!(
            error_response.error,
            "Insufficient Eligibility: Client not eligible"
        );
    }

    #[tokio::test]
    async fn test_events_are_persisted_correctly() {
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness {
            pool,
            account_cqrs,
            asset_cqrs: tokenized_asset_cqrs,
            mint_cqrs,
            ..
        } = harness;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool.clone())
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-events-test",
            "qty": "50.0",
            "underlying_symbol": underlying.0,
            "token_symbol": token.0,
            "network": network.0,
            "client_id": client_id,
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let mint_response: MintResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        let all_events = sqlx::query!("SELECT COUNT(*) as count FROM events")
            .fetch_one(&pool)
            .await
            .expect("Failed to count events");

        debug!("Total events in database: {}", all_events.count);

        let events = sqlx::query!(
            r"
            SELECT aggregate_id, event_type, sequence
            FROM events
            WHERE aggregate_id = ? AND aggregate_type = 'Mint'
            ORDER BY sequence
            ",
            mint_response.issuer_request_id.0
        )
        .fetch_all(&pool)
        .await
        .expect("Failed to query events");

        debug!(
            "Events for mint {}: {}",
            mint_response.issuer_request_id.0,
            events.len()
        );

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].aggregate_id, mint_response.issuer_request_id.0);
        assert_eq!(events[0].event_type, "MintEvent::Initiated");
        assert_eq!(events[0].sequence, 1);
    }

    #[tokio::test]
    async fn test_views_are_updated_correctly() {
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness {
            pool,
            account_cqrs,
            asset_cqrs: tokenized_asset_cqrs,
            mint_cqrs,
            ..
        } = harness;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool.clone())
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let tokenization_request_id = "alp-view-test";

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id,
            "qty": "75.5",
            "underlying_symbol": underlying.0,
            "token_symbol": token.0,
            "network": network.0,
            "client_id": client_id,
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let mint_response: MintResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        let view =
            find_by_issuer_request_id(&pool, &mint_response.issuer_request_id)
                .await
                .expect("Failed to query view")
                .expect("View should exist");

        let MintView::Initiated {
            issuer_request_id: view_issuer_id,
            tokenization_request_id: view_tokenization_id,
            quantity: view_quantity,
            underlying: view_underlying,
            token: view_token,
            network: view_network,
            client_id: view_client_id,
            wallet: view_wallet,
            ..
        } = view
        else {
            panic!("Expected Initiated variant");
        };

        assert_eq!(view_issuer_id, mint_response.issuer_request_id);
        assert_eq!(view_tokenization_id.0, tokenization_request_id);
        assert_eq!(view_quantity.0, Decimal::from_str("75.5").unwrap());
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_network, network);
        assert_eq!(view_client_id, client_id);
        assert_eq!(
            view_wallet,
            address!("0x1234567890abcdef1234567890abcdef12345678")
        );
    }

    #[tokio::test]
    async fn test_initiate_mint_rejects_invalid_wallet_address() {
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness {
            pool,
            account_cqrs,
            asset_cqrs: tokenized_asset_cqrs,
            mint_cqrs,
            ..
        } = harness;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "100.5",
            "underlying_symbol": underlying.0,
            "token_symbol": token.0,
            "network": network.0,
            "client_id": client_id,
            "wallet_address": "invalid-address"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::UnprocessableEntity);
    }

    #[tokio::test]
    async fn test_initiate_mint_rejects_wrong_network() {
        let harness = TestHarness::new().await;
        let TestAccountAndAsset { client_id, underlying, token, .. } =
            harness.setup_account_and_asset().await;
        let TestHarness {
            pool,
            account_cqrs,
            asset_cqrs: tokenized_asset_cqrs,
            mint_cqrs,
            ..
        } = harness;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "100.5",
            "underlying_symbol": underlying.0,
            "token_symbol": token.0,
            "network": "ethereum",
            "client_id": client_id,
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::BadRequest);

        let error_response: ErrorResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert_eq!(
            error_response.error,
            "Invalid Token: Token not available on the network"
        );
    }

    #[tokio::test]
    async fn test_initiate_mint_with_duplicate_issuer_request_id() {
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness { mint_cqrs, .. } = harness;

        let issuer_request_id = "test-issuer-request-id";

        let command = MintCommand::Initiate {
            issuer_request_id: IssuerRequestId::new(issuer_request_id),
            tokenization_request_id: TokenizationRequestId::new("alp-123"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        };

        mint_cqrs
            .execute(issuer_request_id, command.clone())
            .await
            .expect("First execution should succeed");

        let result = mint_cqrs.execute(issuer_request_id, command).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_initiate_mint_without_auth_returns_401() {
        let TestHarness { pool, mint_cqrs, .. } = TestHarness::new().await;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "100.0",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "network": "base",
            "client_id": "client-456",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[tokio::test]
    async fn test_initiate_mint_rejects_non_whitelisted_wallet() {
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id,
            underlying,
            token,
            network,
            wallet: whitelisted_wallet,
        } = harness.setup_account_and_asset().await;
        let TestHarness {
            pool,
            account_cqrs,
            asset_cqrs: tokenized_asset_cqrs,
            mint_cqrs,
            ..
        } = harness;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let non_whitelisted_wallet =
            address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        assert_ne!(
            whitelisted_wallet, non_whitelisted_wallet,
            "Test requires different wallet addresses"
        );

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "100.5",
            "underlying_symbol": underlying.0,
            "token_symbol": token.0,
            "network": network.0,
            "client_id": client_id,
            "wallet_address": non_whitelisted_wallet.to_string()
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::BadRequest);

        let error_response: ErrorResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert_eq!(
            error_response.error,
            "Insufficient Eligibility: Wallet not whitelisted for client"
        );
    }
}
