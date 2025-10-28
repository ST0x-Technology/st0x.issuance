use clap::Parser;

use st0x_issuance::{Env, TelemetryGuard, initialize_rocket, setup_tracing};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env = Env::parse();
    let config = env.into_config()?;

    let telemetry_guard = if let Some(ref hyperdx) = config.hyperdx {
        match hyperdx.setup_telemetry() {
            Ok(guard) => Some(guard),
            Err(e) => {
                eprintln!("Failed to setup telemetry: {e}");
                setup_tracing(&config.log_level);
                None
            }
        }
    } else {
        setup_tracing(&config.log_level);
        None
    };

    let result = initialize_rocket(config).await;

    drop(telemetry_guard);

    result?;
    Ok(())
}
