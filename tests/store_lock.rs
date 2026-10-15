mod harness;

use httpmock::prelude::*;
use rocket::local::asynchronous::Client;

use st0x_issuance::initialize_rocket as initialize_service;
use st0x_issuance::test_utils::LocalEvm;

use crate::harness::initialize_rocket;

/// The running service holds the store lock: a second service on the same
/// store refuses to start, and a start after the first one stops succeeds. The
/// same lock makes `issuer move-receipts` and `issuer confirm-custody` refuse
/// while the service runs.
#[tracing_test::traced_test]
#[tokio::test]
async fn a_second_service_on_the_same_store_refuses_until_the_first_stops()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("store-lock.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
    let lock_path = format!("{}.lock", db_path.display());

    let first = initialize_rocket(harness::create_config_with_db(
        &db_url,
        &mock_alpaca,
        &evm,
    )?)
    .await?;
    let first_client = Client::tracked(first).await?;
    logs_assert(|lines: &[&str]| {
        lines
            .iter()
            .any(|line| {
                line.contains("INFO")
                    && line.contains(
                        "Holding the store lock while the service runs",
                    )
                    && line.contains(&lock_path)
            })
            .then_some(())
            .ok_or_else(|| "no INFO line names the held lock file".to_owned())
    });

    // Straight to the service initializer: the harness wrapper opens the store
    // and runs migrations first, before the service checks the lock.
    let second = initialize_service(harness::create_config_with_db(
        &db_url,
        &mock_alpaca,
        &evm,
    )?)
    .await;
    let Err(error) = second else {
        return Err("a second service on the same store must refuse".into());
    };
    assert!(
        error.to_string().contains("another process holds the store lock"),
        "the refusal must name the store lock, got {error}"
    );
    assert!(logs_contain("Refusing to start: cannot take the store lock"));

    first_client.terminate().await;

    let restarted = initialize_rocket(harness::create_config_with_db(
        &db_url,
        &mock_alpaca,
        &evm,
    )?)
    .await?;
    let _restarted_client = Client::tracked(restarted).await?;

    Ok(())
}
