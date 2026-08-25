use st0x_issuance::{Config, initialize_rocket, setup_tracing};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv_override().ok();
    let config = Config::parse()?;

    let telemetry_guard = if let Some(ref hyperdx) = config.hyperdx {
        match hyperdx.setup_telemetry() {
            Ok(guard) => Some(guard),
            Err(err) => {
                setup_tracing(&config.log_level, config.log_format);
                tracing::error!(
                    target: "startup",
                    error = %err,
                    "Telemetry setup failed; using local tracing"
                );
                None
            }
        }
    } else {
        setup_tracing(&config.log_level, config.log_format);
        None
    };

    let rocket = initialize_rocket(config).await?;
    let result = rocket.launch().await;

    drop(telemetry_guard);

    result?;
    Ok(())
}
