//! Startup when the SQLite database file does not exist yet.
//!
//! Both tests call the application's own `initialize_rocket`, not the harness
//! wrapper, because the wrapper opens the database before the application does.

mod harness;

use httpmock::prelude::*;

use st0x_issuance::initialize_rocket;
use st0x_issuance::test_utils::LocalEvm;

/// A missing file means a bad path or an unmounted volume, so startup must
/// fail instead of serving from a new, empty database
/// (docs/nixos-provisioning.md, "Database").
#[tokio::test]
async fn startup_fails_without_creating_a_missing_database_file()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("issuance.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let config = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let error = initialize_rocket(config)
        .await
        .err()
        .ok_or("startup succeeded against a missing database file")?;

    assert!(
        format!("{error:?}").contains("unable to open database file"),
        "unexpected error: {error:?}"
    );
    assert!(!db_path.exists());
    Ok(())
}

/// `mode=rwc` in the URL is the explicit opt-in to create the file.
#[tokio::test]
async fn startup_creates_a_missing_database_file_with_mode_rwc()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("issuance.db");
    let db_url = format!("sqlite://{}?mode=rwc", db_path.display());

    let config = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    initialize_rocket(config).await?;

    assert!(db_path.exists());
    Ok(())
}
