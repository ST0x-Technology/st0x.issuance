use st0x_issuance::{
    Config, initialize_rocket_with_notifications, setup_tracing,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv_override().ok();
    let (config, lifecycle_notifications) =
        Config::parse_with_lifecycle_notifications()?;

    let telemetry_guard = if let Some(ref hyperdx) = config.hyperdx {
        match hyperdx.setup_telemetry() {
            Ok(guard) => Some(guard),
            Err(err) => {
                setup_tracing(&config.log_level);
                tracing::error!(
                    target: "startup",
                    error = %err,
                    "Telemetry setup failed; using local tracing"
                );
                None
            }
        }
    } else {
        setup_tracing(&config.log_level);
        None
    };

    let rocket =
        initialize_rocket_with_notifications(config, lifecycle_notifications)
            .await?;
    let result = rocket.launch().await;

    drop(telemetry_guard);

    result?;
    Ok(())
}
