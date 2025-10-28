#[macro_use]
extern crate rocket;

use clap::Parser;
use tracing::error;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[launch]
async fn rocket() -> _ {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("debug")),
        )
        .init();

    let config = st0x_issuance::Config::parse();

    match st0x_issuance::initialize_rocket(config).await {
        Ok(rocket) => rocket,
        Err(e) => {
            error!("Failed to initialize application: {e}");
            std::process::exit(1);
        }
    }
}
