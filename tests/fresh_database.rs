//! Startup on a fresh disk, where the SQLite database file does not exist yet.

mod harness;

use httpmock::prelude::*;

use st0x_issuance::initialize_rocket;
use st0x_issuance::test_utils::LocalEvm;

/// The production URL (`sqlite:///mnt/data/issuance.db`) has no `mode=rwc`,
/// so startup must create the file itself instead of failing with
/// `(code: 14) unable to open database file`. This calls the application's
/// own `initialize_rocket`, not the harness wrapper, because the wrapper opens
/// the database before the application does.
#[tokio::test]
async fn startup_creates_a_missing_database_file()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("issuance.db");
    let db_url = format!("sqlite://{}", db_path.display());
    assert!(!db_path.exists());

    let config = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    initialize_rocket(config).await?;

    assert!(db_path.exists());
    Ok(())
}
