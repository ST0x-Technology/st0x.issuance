#[macro_use]
extern crate rocket;

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

    match st0x_issuance::initialize_rocket().await {
        Ok(rocket) => rocket,
        Err(e) => {
            error!("Failed to initialize application: {e}");
            std::process::exit(1);
        }
    }
}
