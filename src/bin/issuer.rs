#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // `dotenv` (not `dotenv_override`) so a stale `.env` on the issuer host can't
    // override host-provided env (e.g. DATABASE_URL) and point the CLI at the
    // wrong database.
    dotenvy::dotenv().ok();
    st0x_issuance::run_issuer_cli().await
}
