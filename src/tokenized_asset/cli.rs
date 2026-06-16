use clap::{Args, Parser, Subcommand};
use event_sorcery::{
    AggregateError, LifecycleError, ReconcileError, Store, StoreBuilder,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};
use std::io::{self, Write};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use super::view::{TokenizedAssetViewError, load_asset_by_underlying};
use super::{
    AssetStatus, TokenizedAsset, TokenizedAssetCommand, UnderlyingSymbol,
};
use crate::config::{
    DEFAULT_DATABASE_MAX_CONNECTIONS, DEFAULT_DATABASE_URL, LogLevel,
    setup_tracing,
};

/// Parses and runs the issuer-host CLI end to end. The `issuer` binary is a thin
/// wrapper over this entry point.
///
/// # Errors
///
/// Returns an error if argument parsing fails, the store cannot be opened, the
/// asset is not supported, the operator aborts a mutation, or the command
/// dispatch fails.
pub async fn run_issuer_cli() -> anyhow::Result<()> {
    setup_tracing(&LogLevel::Info);
    IssuerCli::parse().dispatch().await
}

#[derive(Parser)]
#[command(
    name = "issuer",
    version,
    about = "Issuer-host admin CLI for st0x.issuance"
)]
struct IssuerCli {
    #[command(subcommand)]
    command: IssuerCommand,
}

#[derive(Subcommand)]
enum IssuerCommand {
    /// Freeze an asset: reject new mints (in-flight redemptions still complete).
    Freeze(AssetArgs),
    /// Unfreeze an asset: resume accepting new mints.
    Unfreeze(AssetArgs),
    /// Print an asset's current freeze status.
    Status(AssetArgs),
}

#[derive(Args)]
struct AssetArgs {
    /// Underlying symbol, e.g. SGOV.
    #[arg(value_parser = parse_underlying)]
    underlying: UnderlyingSymbol,
    #[arg(
        long = "database-url",
        env = "DATABASE_URL",
        default_value = DEFAULT_DATABASE_URL,
        value_parser = parse_sqlite_url
    )]
    database_url: String,
    #[arg(
        long,
        env = "DATABASE_MAX_CONNECTIONS",
        default_value_t = DEFAULT_DATABASE_MAX_CONNECTIONS,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    database_max_connections: u32,
}

impl IssuerCli {
    async fn dispatch(self) -> anyhow::Result<()> {
        match self.command {
            IssuerCommand::Freeze(args) => {
                run_asset_command(AssetAction::Freeze, &args).await
            }
            IssuerCommand::Unfreeze(args) => {
                run_asset_command(AssetAction::Unfreeze, &args).await
            }
            IssuerCommand::Status(args) => {
                run_asset_command(AssetAction::Status, &args).await
            }
        }
    }
}

enum AssetAction {
    Freeze,
    Unfreeze,
    Status,
}

/// Connects to the store, prints the resolved database so the operator can
/// confirm they are acting on the intended store, and runs the action with the
/// real stdin confirmation prompt.
async fn run_asset_command(
    action: AssetAction,
    args: &AssetArgs,
) -> anyhow::Result<()> {
    println!("Using database: {}", args.database_url);

    let admin =
        AssetAdmin::connect(&args.database_url, args.database_max_connections)
            .await?;

    execute(&admin, action, &args.underlying, prompt_confirm).await
}

/// Orchestrates a single action against an already-connected admin. The
/// confirmation is injected so the abort/confirm branches are unit-testable
/// without driving real stdin. Aborting a mutation returns an error (non-zero
/// exit) so automation can distinguish "operator declined" from "done".
async fn execute(
    admin: &AssetAdmin,
    action: AssetAction,
    underlying: &UnderlyingSymbol,
    confirm: impl Fn(&str) -> io::Result<bool>,
) -> anyhow::Result<()> {
    // Display the current status for the operator to confirm against, and reject
    // an unknown asset up front. This snapshot only drives the prompt and the
    // not-found check — the freeze/unfreeze decision is NOT derived from it (see
    // `freeze`/`unfreeze`), so a concurrent write landing in the confirmation
    // window can never leave the asset in the wrong persisted state.
    let report = admin.status(underlying).await?.ok_or_else(|| {
        AssetAdminError::NotFound { underlying: underlying.clone() }
    })?;
    println!("{report}");

    match action {
        AssetAction::Status => Ok(()),
        AssetAction::Freeze => {
            if !confirm(&format!("Freeze {underlying}?"))? {
                anyhow::bail!("aborted by operator");
            }
            match admin.freeze(underlying).await? {
                FreezeOutcome::Froze => println!("Froze {underlying}."),
                FreezeOutcome::AlreadyFrozen => {
                    println!("{underlying} was already frozen.");
                }
            }
            Ok(())
        }
        AssetAction::Unfreeze => {
            if !confirm(&format!("Unfreeze {underlying}?"))? {
                anyhow::bail!("aborted by operator");
            }
            match admin.unfreeze(underlying).await? {
                UnfreezeOutcome::Unfroze => println!("Unfroze {underlying}."),
                UnfreezeOutcome::AlreadyEnabled => {
                    println!("{underlying} was already enabled.");
                }
            }
            Ok(())
        }
    }
}

/// Issuer-host admin for freezing/unfreezing supported assets.
///
/// Opens the same SQLite event store the server uses and dispatches the CQRS
/// `Freeze` / `Unfreeze` commands through the event-sorcery `Store` — never
/// writing the `events` table directly.
pub(crate) struct AssetAdmin {
    store: Arc<Store<TokenizedAsset>>,
    pool: Pool<Sqlite>,
}

/// Outcome of a freeze request, so the caller can report an idempotent no-op
/// distinctly from an actual state change. A missing asset is an
/// `AssetAdminError::NotFound`, not an outcome: `execute` rejects unknown assets
/// up front, so `freeze` only runs against an asset that exists.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FreezeOutcome {
    Froze,
    AlreadyFrozen,
}

/// Outcome of an unfreeze request. A missing asset is an
/// `AssetAdminError::NotFound`, not an outcome (see `FreezeOutcome`).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UnfreezeOutcome {
    Unfroze,
    AlreadyEnabled,
}

/// A supported asset's freeze status, formatted for the CLI.
#[derive(Debug)]
pub(crate) struct AssetStatusReport {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) status: AssetStatus,
}

impl std::fmt::Display for AssetStatusReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self.status {
            AssetStatus::Frozen => "frozen",
            AssetStatus::Enabled => "enabled",
        };
        write!(f, "{} is {state}", self.underlying)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AssetAdminError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("failed to read asset view: {0}")]
    View(#[from] TokenizedAssetViewError),
    #[error("event store reconcile error: {0}")]
    Reconcile(#[from] ReconcileError),
    #[error("aggregate error: {0}")]
    Aggregate(Box<AggregateError<LifecycleError<TokenizedAsset>>>),
    #[error("{underlying} is not a supported tokenized asset")]
    NotFound { underlying: UnderlyingSymbol },
}

// `Store::send` yields an un-boxed `AggregateError`; box it on conversion so the
// enum variant stays small (the error is large) while `?` still works at the
// call site without a hand-rolled `.map_err(Box::new)`.
impl From<AggregateError<LifecycleError<TokenizedAsset>>> for AssetAdminError {
    fn from(error: AggregateError<LifecycleError<TokenizedAsset>>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}

impl AssetAdmin {
    /// Connects to the SQLite store at `db`, applying migrations so the command
    /// can run standalone on the issuer host. The 5s busy timeout pins sqlx's
    /// default: it makes SQLite wait on `SQLITE_BUSY` while the server holds the
    /// write lock instead of failing immediately. It does NOT cover an
    /// event-sorcery optimistic-concurrency conflict (a UNIQUE collision on the
    /// events PK), which `Store::send` surfaces as an error the operator re-runs.
    pub(crate) async fn connect(
        db: &str,
        max_connections: u32,
    ) -> Result<Self, AssetAdminError> {
        let connect_options = SqliteConnectOptions::from_str(db)?
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_with(connect_options)
            .await?;

        sqlx::migrate!("./migrations").run(&pool).await?;

        let (store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone()).build(()).await?;

        Ok(Self { store, pool })
    }

    /// Reads the current freeze status, or `None` if the asset is unknown.
    pub(crate) async fn status(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<Option<AssetStatusReport>, AssetAdminError> {
        Ok(load_asset_by_underlying(&self.pool, underlying).await?.map(
            |view| AssetStatusReport {
                underlying: underlying.clone(),
                status: view.status,
            },
        ))
    }

    /// Freezes the asset. Always dispatches `Freeze` through the store so the
    /// aggregate — the source of truth — decides the final state; an
    /// already-frozen asset is a zero-event no-op there, so the asset is
    /// guaranteed frozen afterwards even if a concurrent writer changed it since
    /// the operator's status read. The returned `FreezeOutcome` only labels the
    /// message from a status read taken immediately before dispatch: it is
    /// best-effort under a concurrent write, but the persisted state is always
    /// correct. Deriving the label from the live store (not a snapshot passed in
    /// by the caller) is what closes the read-then-confirm-then-dispatch TOCTOU
    /// where a stale "already frozen" read would otherwise skip the dispatch.
    pub(crate) async fn freeze(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<FreezeOutcome, AssetAdminError> {
        let already_frozen = matches!(
            self.status(underlying).await?.map(|report| report.status),
            Some(AssetStatus::Frozen)
        );

        self.dispatch(underlying, TokenizedAssetCommand::Freeze).await?;

        Ok(if already_frozen {
            FreezeOutcome::AlreadyFrozen
        } else {
            FreezeOutcome::Froze
        })
    }

    /// Unfreezes the asset. Always dispatches `Unfreeze` through the store so the
    /// aggregate decides the final state; an already-enabled asset is a
    /// zero-event no-op there. The returned `UnfreezeOutcome` labels the message
    /// from a pre-dispatch status read (best-effort under a concurrent write);
    /// the persisted state is always correct. See `freeze` for why the label is
    /// derived from the live store rather than a caller-supplied snapshot.
    pub(crate) async fn unfreeze(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<UnfreezeOutcome, AssetAdminError> {
        let already_enabled = matches!(
            self.status(underlying).await?.map(|report| report.status),
            Some(AssetStatus::Enabled)
        );

        self.dispatch(underlying, TokenizedAssetCommand::Unfreeze).await?;

        Ok(if already_enabled {
            UnfreezeOutcome::AlreadyEnabled
        } else {
            UnfreezeOutcome::Unfroze
        })
    }

    async fn dispatch(
        &self,
        underlying: &UnderlyingSymbol,
        command: TokenizedAssetCommand,
    ) -> Result<(), AssetAdminError> {
        self.store.send(underlying, command).await?;
        Ok(())
    }
}

/// Rejects an empty or whitespace-only underlying at the CLI boundary so a typo
/// fails fast with a clear parse error instead of a later "not a supported
/// asset" lookup failure. Trims surrounding whitespace and upper-cases the
/// symbol so `" sgov "` resolves to the stored `SGOV` (assets are keyed by their
/// upper-case symbol) instead of silently missing the lookup.
fn parse_underlying(value: &str) -> Result<UnderlyingSymbol, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("underlying symbol must not be empty".to_string());
    }

    Ok(UnderlyingSymbol::new(trimmed.to_ascii_uppercase()))
}

/// Validates the database URL uses the `sqlite:` scheme so a wrong env value
/// (e.g. an `http://` URL) fails fast with a clear message rather than an opaque
/// driver error deep inside sqlx. Returns the string unchanged so both the CLI
/// and the server hand sqlx identical bytes.
fn parse_sqlite_url(value: &str) -> Result<String, String> {
    if value.starts_with("sqlite:") {
        Ok(value.to_string())
    } else {
        Err(format!("database URL must use the sqlite: scheme, got: {value}"))
    }
}

fn prompt_confirm(prompt: &str) -> io::Result<bool> {
    print!("{prompt} [y/N] ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    Ok(parse_confirmation(&input))
}

/// Confirmation accepts `y`/`yes` case-insensitively (after trimming);
/// everything else — including empty input and EOF — declines.
fn parse_confirmation(input: &str) -> bool {
    let trimmed = input.trim();
    trimmed.eq_ignore_ascii_case("y") || trimmed.eq_ignore_ascii_case("yes")
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use sqlx::sqlite::SqlitePoolOptions;
    use tracing_test::traced_test;

    use super::*;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{Network, TokenSymbol};

    async fn admin_with_asset(underlying: &str) -> AssetAdmin {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let (store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .expect("Failed to build tokenized asset store");

        let underlying = UnderlyingSymbol::new(underlying);
        store
            .send(
                &underlying,
                TokenizedAssetCommand::Add {
                    underlying: underlying.clone(),
                    token: TokenSymbol::new(format!("t{underlying}")),
                    network: Network::new("base"),
                    vault: address!(
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                },
            )
            .await
            .expect("Failed to add asset");

        AssetAdmin { store, pool }
    }

    #[traced_test]
    #[tokio::test]
    async fn freeze_then_unfreeze_round_trip() {
        let admin = admin_with_asset("SGOV").await;
        let underlying = UnderlyingSymbol::new("SGOV");

        let report =
            admin.status(&underlying).await.unwrap().expect("asset exists");
        assert_eq!(report.status, AssetStatus::Enabled);
        assert_eq!(format!("{report}"), "SGOV is enabled");

        assert_eq!(
            admin.freeze(&underlying).await.unwrap(),
            FreezeOutcome::Froze
        );
        let frozen = admin.status(&underlying).await.unwrap().expect("exists");
        assert_eq!(frozen.status, AssetStatus::Frozen);
        assert_eq!(format!("{frozen}"), "SGOV is frozen");
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Freezing tokenized asset", "SGOV"]
        ));

        assert_eq!(
            admin.unfreeze(&underlying).await.unwrap(),
            UnfreezeOutcome::Unfroze
        );
        assert_eq!(
            admin.status(&underlying).await.unwrap().expect("exists").status,
            AssetStatus::Enabled
        );
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Unfreezing tokenized asset", "SGOV"]
        ));
    }

    #[tokio::test]
    async fn freeze_and_unfreeze_report_idempotent_no_ops() {
        let admin = admin_with_asset("SGOV").await;
        let underlying = UnderlyingSymbol::new("SGOV");

        // A second freeze of an already-frozen asset (and a second unfreeze of an
        // already-enabled one) is a zero-event no-op the aggregate dedups, and is
        // reported as the AlreadyFrozen / AlreadyEnabled label.
        assert_eq!(
            admin.freeze(&underlying).await.unwrap(),
            FreezeOutcome::Froze
        );
        assert_eq!(
            admin.freeze(&underlying).await.unwrap(),
            FreezeOutcome::AlreadyFrozen
        );

        assert_eq!(
            admin.unfreeze(&underlying).await.unwrap(),
            UnfreezeOutcome::Unfroze
        );
        assert_eq!(
            admin.unfreeze(&underlying).await.unwrap(),
            UnfreezeOutcome::AlreadyEnabled
        );
    }

    #[tokio::test]
    async fn status_is_none_for_unknown_asset() {
        let admin = admin_with_asset("SGOV").await;
        assert!(
            admin
                .status(&UnderlyingSymbol::new("UNKNOWN"))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn execute_rejects_unknown_asset() {
        let admin = admin_with_asset("SGOV").await;
        let unknown = UnderlyingSymbol::new("UNKNOWN");

        // The not-found rejection is `execute`'s entry-point behavior for all
        // three subcommands; assert the operator-facing message.
        let err = execute(&admin, AssetAction::Freeze, &unknown, |_| Ok(true))
            .await
            .expect_err("an unknown asset must be rejected");
        assert!(
            err.to_string().contains("is not a supported tokenized asset"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn execute_freeze_aborts_without_dispatching_when_declined() {
        let admin = admin_with_asset("SGOV").await;
        let underlying = UnderlyingSymbol::new("SGOV");

        let result =
            execute(&admin, AssetAction::Freeze, &underlying, |_| Ok(false))
                .await;

        assert!(result.is_err(), "declined freeze must return an error");
        assert_eq!(
            admin.status(&underlying).await.unwrap().expect("exists").status,
            AssetStatus::Enabled,
            "a declined freeze must not change state"
        );
    }

    #[tokio::test]
    async fn execute_freeze_dispatches_when_confirmed() {
        let admin = admin_with_asset("SGOV").await;
        let underlying = UnderlyingSymbol::new("SGOV");

        execute(&admin, AssetAction::Freeze, &underlying, |_| Ok(true))
            .await
            .expect("confirmed freeze succeeds");

        assert_eq!(
            admin.status(&underlying).await.unwrap().expect("exists").status,
            AssetStatus::Frozen,
            "a confirmed freeze must change state"
        );
    }

    #[tokio::test]
    async fn execute_unfreeze_aborts_without_dispatching_when_declined() {
        let admin = admin_with_asset("SGOV").await;
        let underlying = UnderlyingSymbol::new("SGOV");
        admin.freeze(&underlying).await.expect("freeze succeeds");

        let result =
            execute(&admin, AssetAction::Unfreeze, &underlying, |_| Ok(false))
                .await;

        assert!(result.is_err(), "declined unfreeze must return an error");
        assert_eq!(
            admin.status(&underlying).await.unwrap().expect("exists").status,
            AssetStatus::Frozen,
            "a declined unfreeze must not change state"
        );
    }

    #[tokio::test]
    async fn execute_unfreeze_dispatches_when_confirmed() {
        let admin = admin_with_asset("SGOV").await;
        let underlying = UnderlyingSymbol::new("SGOV");
        admin.freeze(&underlying).await.expect("freeze succeeds");

        execute(&admin, AssetAction::Unfreeze, &underlying, |_| Ok(true))
            .await
            .expect("confirmed unfreeze succeeds");

        assert_eq!(
            admin.status(&underlying).await.unwrap().expect("exists").status,
            AssetStatus::Enabled,
            "a confirmed unfreeze must change state"
        );
    }

    #[tokio::test]
    async fn execute_status_never_prompts_or_mutates() {
        let admin = admin_with_asset("SGOV").await;
        let underlying = UnderlyingSymbol::new("SGOV");

        execute(&admin, AssetAction::Status, &underlying, |_| {
            panic!("status must not prompt for confirmation")
        })
        .await
        .expect("status succeeds");

        assert_eq!(
            admin.status(&underlying).await.unwrap().expect("exists").status,
            AssetStatus::Enabled
        );
    }

    #[test]
    fn parse_confirmation_accepts_yes_case_insensitively() {
        for affirmative in ["y", "Y", "yes", "Yes", "YES", " y ", "yEs\n"] {
            assert!(
                parse_confirmation(affirmative),
                "{affirmative:?} should confirm"
            );
        }

        for decline in ["", "n", "N", "no", "yep", "  ", "\n"] {
            assert!(!parse_confirmation(decline), "{decline:?} should decline");
        }
    }

    #[test]
    fn parse_underlying_trims_uppercases_and_rejects_blank() {
        assert_eq!(
            parse_underlying(" sgov ").unwrap(),
            UnderlyingSymbol::new("SGOV"),
            "input is trimmed and upper-cased to the stored symbol"
        );
        assert_eq!(
            parse_underlying("SGOV").unwrap(),
            UnderlyingSymbol::new("SGOV")
        );
        assert!(parse_underlying("").is_err(), "empty must be rejected");
        assert!(
            parse_underlying("   ").is_err(),
            "whitespace-only must be rejected"
        );
    }

    #[test]
    fn issuer_cli_rejects_empty_symbol_and_non_sqlite_url() {
        assert!(
            IssuerCli::try_parse_from(["issuer", "freeze", ""]).is_err(),
            "empty underlying must be rejected at parse time"
        );
        assert!(
            IssuerCli::try_parse_from([
                "issuer",
                "freeze",
                "SGOV",
                "--database-url",
                "http://example.com/db",
            ])
            .is_err(),
            "non-sqlite database URL must be rejected at parse time"
        );
    }
}
