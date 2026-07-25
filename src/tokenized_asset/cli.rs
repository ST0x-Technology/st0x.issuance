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

use super::UnderlyingSymbol;
use super::view::{TokenizedAssetViewError, underlying_has_listing};
use crate::config::{
    DEFAULT_DATABASE_MAX_CONNECTIONS, DEFAULT_DATABASE_URL, LogLevel,
    setup_tracing,
};
use crate::underlying::{
    AssetStatus, Underlying, UnderlyingCommand, UnderlyingViewError,
    load_freeze_status,
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

/// Freeze, unfreeze, and status address the underlying-keyed `Underlying`
/// aggregate: a corporate action applies to every listing of the underlying on
/// every network, so these subcommands deliberately take no network argument.
#[derive(Subcommand)]
enum IssuerCommand {
    /// Freeze an underlying on all networks: reject new mints (in-flight
    /// redemptions still complete).
    Freeze(AssetArgs),
    /// Unfreeze an underlying: resume accepting new mints on all networks.
    Unfreeze(AssetArgs),
    /// Print an underlying's current freeze status.
    Status(AssetArgs),
}

#[derive(Args)]
struct AssetArgs {
    /// Underlying symbol, e.g. SGOV. Upper-cased so `"sgov"` resolves to the
    /// stored `SGOV` (assets are keyed by their upper-case symbol). Whitespace
    /// trimming is handled by [`UnderlyingSymbol::new`].
    #[arg(value_parser = |value: &str| UnderlyingSymbol::new(value.to_ascii_uppercase()))]
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
    // an underlying with no listing up front. This snapshot only drives the
    // prompt and the not-found check — the freeze/unfreeze decision is NOT
    // derived from it (see `freeze`/`unfreeze`), so a concurrent write landing
    // in the confirmation window can never leave the underlying in the wrong
    // persisted state.
    let report = admin.status(underlying).await?.ok_or_else(|| {
        AssetAdminError::NotFound { underlying: underlying.clone() }
    })?;
    println!("{report}");

    match action {
        AssetAction::Status => Ok(()),
        AssetAction::Freeze => {
            if !confirm(&format!("Freeze {underlying} on all networks?"))? {
                anyhow::bail!("aborted by operator");
            }
            match admin.freeze(underlying).await? {
                FreezeOutcome::Froze => {
                    println!("Froze {underlying} on all networks.");
                }
                FreezeOutcome::AlreadyFrozen => {
                    println!("{underlying} was already frozen.");
                }
            }
            Ok(())
        }
        AssetAction::Unfreeze => {
            if !confirm(&format!("Unfreeze {underlying} on all networks?"))? {
                anyhow::bail!("aborted by operator");
            }
            match admin.unfreeze(underlying).await? {
                UnfreezeOutcome::Unfroze => {
                    println!("Unfroze {underlying} on all networks.");
                }
                UnfreezeOutcome::AlreadyEnabled => {
                    println!("{underlying} was already enabled.");
                }
            }
            Ok(())
        }
    }
}

/// Issuer-host admin for freezing/unfreezing supported underlyings.
///
/// Opens the same SQLite event store the server uses and dispatches the CQRS
/// `Freeze` / `Unfreeze` commands through the event-sorcery `Store` — never
/// writing the `events` table directly.
pub(crate) struct AssetAdmin {
    store: Arc<Store<Underlying>>,
    pool: Pool<Sqlite>,
}

/// Outcome of a freeze request, so the caller can report an idempotent no-op
/// distinctly from an actual state change. An underlying with no listing is an
/// `AssetAdminError::NotFound`, not an outcome: `execute` rejects unknown
/// underlyings up front, so `freeze` only runs against one that exists.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FreezeOutcome {
    Froze,
    AlreadyFrozen,
}

/// Outcome of an unfreeze request. An underlying with no listing is an
/// `AssetAdminError::NotFound`, not an outcome (see `FreezeOutcome`).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UnfreezeOutcome {
    Unfroze,
    AlreadyEnabled,
}

/// An underlying's freeze status, formatted for the CLI.
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
        write!(f, "{} is {state} (applies to all networks)", self.underlying)
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
    #[error("failed to read underlying freeze view: {0}")]
    UnderlyingView(#[from] UnderlyingViewError),
    #[error("event store reconcile error: {0}")]
    Reconcile(#[from] ReconcileError),
    #[error("aggregate error: {0}")]
    Aggregate(Box<AggregateError<LifecycleError<Underlying>>>),
    #[error("{underlying} is not a supported tokenized asset on any network")]
    NotFound { underlying: UnderlyingSymbol },
}

// `Store::send` yields an un-boxed `AggregateError`; box it on conversion so the
// enum variant stays small (the error is large) while `?` still works at the
// call site without a hand-rolled `.map_err(Box::new)`.
impl From<AggregateError<LifecycleError<Underlying>>> for AssetAdminError {
    fn from(error: AggregateError<LifecycleError<Underlying>>) -> Self {
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
            StoreBuilder::<Underlying>::new(pool.clone()).build(()).await?;

        Ok(Self { store, pool })
    }

    /// Reads the current freeze status, or `None` if the underlying has no
    /// listing on any network.
    pub(crate) async fn status(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<Option<AssetStatusReport>, AssetAdminError> {
        if !underlying_has_listing(&self.pool, underlying).await? {
            return Ok(None);
        }

        let status = load_freeze_status(&self.pool, underlying).await?;

        Ok(Some(AssetStatusReport { underlying: underlying.clone(), status }))
    }

    /// Freezes the underlying on all networks. Always dispatches `Freeze`
    /// through the store so the aggregate — the source of truth — decides the
    /// final state; an already-frozen underlying is a zero-event no-op there,
    /// so it is guaranteed frozen afterwards even if a concurrent writer
    /// changed it since the operator's status read. The returned
    /// `FreezeOutcome` only labels the message from a status read taken
    /// immediately before dispatch: it is best-effort under a concurrent
    /// write, but the persisted state is always correct. Deriving the label
    /// from the live store (not a snapshot passed in by the caller) is what
    /// closes the read-then-confirm-then-dispatch TOCTOU where a stale
    /// "already frozen" read would otherwise skip the dispatch.
    pub(crate) async fn freeze(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<FreezeOutcome, AssetAdminError> {
        let already_frozen = matches!(
            self.status(underlying).await?.map(|report| report.status),
            Some(AssetStatus::Frozen)
        );

        self.store
            .send(
                underlying,
                UnderlyingCommand::Freeze { underlying: underlying.clone() },
            )
            .await?;

        Ok(if already_frozen {
            FreezeOutcome::AlreadyFrozen
        } else {
            FreezeOutcome::Froze
        })
    }

    /// Unfreezes the underlying. Always dispatches `Unfreeze` through the store
    /// so the aggregate decides the final state; an already-enabled underlying
    /// is a zero-event no-op there. The returned `UnfreezeOutcome` labels the
    /// message from a pre-dispatch status read (best-effort under a concurrent
    /// write); the persisted state is always correct. See `freeze` for why the
    /// label is derived from the live store rather than a caller-supplied
    /// snapshot.
    pub(crate) async fn unfreeze(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<UnfreezeOutcome, AssetAdminError> {
        let already_enabled = matches!(
            self.status(underlying).await?.map(|report| report.status),
            Some(AssetStatus::Enabled)
        );

        self.store
            .send(
                underlying,
                UnderlyingCommand::Unfreeze { underlying: underlying.clone() },
            )
            .await?;

        Ok(if already_enabled {
            UnfreezeOutcome::AlreadyEnabled
        } else {
            UnfreezeOutcome::Unfroze
        })
    }
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

/// Freezes an underlying on all networks.
///
/// Public so end-to-end tests can drive the real freeze operations the
/// operator runs, instead of asserting against state produced some other way.
///
/// # Errors
///
/// Returns an error if the store cannot be opened or the command dispatch
/// fails.
pub async fn freeze_underlying(
    database_url: &str,
    database_max_connections: u32,
    underlying: &UnderlyingSymbol,
) -> anyhow::Result<()> {
    let admin =
        AssetAdmin::connect(database_url, database_max_connections).await?;
    admin.freeze(underlying).await?;

    Ok(())
}

/// Unfreezes an underlying on all networks.
///
/// The counterpart to [`freeze_underlying`], so a caller driving the operator
/// sequence can leave the asset as it found it.
///
/// # Errors
///
/// Returns an error if the store cannot be opened or the command dispatch
/// fails.
pub async fn unfreeze_underlying(
    database_url: &str,
    database_max_connections: u32,
    underlying: &UnderlyingSymbol,
) -> anyhow::Result<()> {
    let admin =
        AssetAdmin::connect(database_url, database_max_connections).await?;
    admin.unfreeze(underlying).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use sqlx::sqlite::SqlitePoolOptions;
    use tracing_test::traced_test;

    use super::*;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{
        AssetKey, Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    };

    /// Seeds one listing per given network for `underlying`, then hands back an
    /// admin over the same pool — mirroring an issuer host where the server
    /// maintains the listing view and the CLI acts on the freeze store.
    async fn admin_with_asset(
        underlying: &str,
        networks: &[Network],
    ) -> AssetAdmin {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let (listing_store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .expect("Failed to build tokenized asset store");

        let underlying = UnderlyingSymbol::new(underlying).unwrap();
        for network in networks {
            let key = AssetKey::new(underlying.clone(), *network);
            listing_store
                .send(
                    &key,
                    TokenizedAssetCommand::Add {
                        underlying: underlying.clone(),
                        token: TokenSymbol::new(format!("t{underlying}")),
                        network: *network,
                        vault: address!(
                            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        ),
                    },
                )
                .await
                .expect("Failed to add asset");
        }

        let (store, _projection) =
            StoreBuilder::<Underlying>::new(pool.clone())
                .build(())
                .await
                .expect("Failed to build underlying store");

        AssetAdmin { store, pool }
    }

    #[traced_test]
    #[tokio::test]
    async fn freeze_then_unfreeze_round_trip() {
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();

        let report =
            admin.status(&underlying).await.unwrap().expect("asset exists");
        assert_eq!(report.status, AssetStatus::Enabled);
        assert_eq!(
            format!("{report}"),
            "SGOV is enabled (applies to all networks)"
        );

        assert_eq!(
            admin.freeze(&underlying).await.unwrap(),
            FreezeOutcome::Froze
        );
        let frozen = admin.status(&underlying).await.unwrap().expect("exists");
        assert_eq!(frozen.status, AssetStatus::Frozen);
        assert_eq!(
            format!("{frozen}"),
            "SGOV is frozen (applies to all networks)"
        );
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Freezing underlying across all networks", "SGOV"]
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
            &["Unfreezing underlying across all networks", "SGOV"]
        ));
    }

    // One freeze covers every listing of the underlying: with listings on two
    // networks, a single freeze is what the status (and the mint gate, which
    // reads the same view) reports for both.
    #[tokio::test]
    async fn freeze_covers_every_network_listing() {
        let admin =
            admin_with_asset("SGOV", &[Network::Base, Network::Ethereum]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();

        assert_eq!(
            admin.freeze(&underlying).await.unwrap(),
            FreezeOutcome::Froze
        );

        assert_eq!(
            load_freeze_status(&admin.pool, &underlying).await.unwrap(),
            AssetStatus::Frozen,
            "the underlying-scoped status applies to all network listings"
        );
    }

    #[tokio::test]
    async fn freeze_and_unfreeze_report_idempotent_no_ops() {
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();

        // A second freeze of an already-frozen underlying (and a second
        // unfreeze of an already-enabled one) is a zero-event no-op the
        // aggregate dedups, and is reported as the AlreadyFrozen /
        // AlreadyEnabled label.
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
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        assert!(
            admin
                .status(&UnderlyingSymbol::new("UNKNOWN").unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn execute_rejects_unknown_asset() {
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let unknown = UnderlyingSymbol::new("UNKNOWN").unwrap();

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
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();

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
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();

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
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();
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
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();
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
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();

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
    fn issuer_cli_uppercases_underlying_and_rejects_blank() {
        let IssuerCli { command: IssuerCommand::Freeze(args) } =
            IssuerCli::try_parse_from(["issuer", "freeze", " sgov "]).unwrap()
        else {
            panic!("expected freeze command");
        };
        assert_eq!(
            args.underlying,
            UnderlyingSymbol::new("SGOV").unwrap(),
            "input is trimmed and upper-cased to the stored symbol"
        );

        for blank in ["", "   "] {
            assert!(
                IssuerCli::try_parse_from(["issuer", "freeze", blank]).is_err(),
                "{blank:?} must be rejected at parse time"
            );
        }
    }

    #[test]
    fn issuer_cli_rejects_non_sqlite_url() {
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
