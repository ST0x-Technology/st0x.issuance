//! Command-line model. Every leaf verb maps to exactly one `/ops` route, and
//! arguments parse into the shared wire types the bot deserializes; the bot
//! still validates and decides everything.

use alloy_primitives::Address;
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use st0x_issuance_dto::{
    Email, Network, UnderlyingSymbol, UnderlyingSymbolError,
};

use crate::target::Env;

/// S01 issuance bot operations client. Calls the IAP-gated `/ops` API with the
/// current S01 Google login and prints the response as one JSON line.
#[derive(Debug, Parser)]
#[command(name = "st0x-issuance-client", version, about)]
pub(crate) struct Cli {
    /// Deployment to call; there is no default.
    #[arg(long, value_enum)]
    pub(crate) env: Env,
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Read-only diagnostics.
    #[command(subcommand)]
    Read(ReadCommand),
    /// Recovery and onboarding.
    #[command(subcommand)]
    Debug(DebugCommand),
    /// Operations that gate token supply or move funds.
    #[command(subcommand)]
    Capital(CapitalCommand),
}

#[derive(Debug, Subcommand)]
pub(crate) enum ReadCommand {
    /// Mints and redemptions stuck in a recoverable state.
    Stuck,
    /// Orchestrator deployment health and per-asset vault mode.
    OrchestratorHealth,
    /// Per-network poller, backfill, and gas telemetry.
    NetworkTelemetry,
    /// Detected wrapped-token transfers.
    WrappedTransfers(WrappedTransfersArgs),
    /// An underlying's freeze status.
    Status {
        #[arg(value_parser = uppercase_symbol)]
        underlying: UnderlyingSymbol,
    },
    /// Read-only on-chain readiness of a network's orchestrator.
    OrchestratorPreflight {
        network: Network,
        /// Asset to check; repeat for several.
        #[arg(long = "asset", value_parser = uppercase_symbol)]
        assets: Vec<UnderlyingSymbol>,
    },
}

/// Paging for `read wrapped-transfers`; omitted flags send nothing, so the
/// bot's defaults apply.
#[derive(Debug, Args)]
pub(crate) struct WrappedTransfersArgs {
    #[arg(long)]
    pub(crate) limit: Option<u32>,
    #[arg(long)]
    pub(crate) before_block: Option<u64>,
    #[arg(long)]
    pub(crate) before_log_index: Option<u64>,
    #[arg(long)]
    pub(crate) before_network: Option<Network>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum DebugCommand {
    /// Recover a stuck or failed redemption.
    RecoverRedemption { issuer_request_id: String },
    /// Reprocess a stuck or failed mint.
    ReprocessMint { issuer_request_id: String },
    /// Run the orchestrator signing verification for an asset.
    VerifyOrchestratorSigning {
        network: Network,
        #[arg(value_parser = uppercase_symbol)]
        underlying: UnderlyingSymbol,
    },
    /// Register an account for an email address.
    RegisterAccount {
        #[arg(long, value_parser = Email::new)]
        email: Email,
    },
    /// Whitelist a wallet for an account.
    WhitelistWallet { client_id: String, wallet: Address },
    /// Remove a wallet from an account's whitelist.
    UnwhitelistWallet { client_id: String, wallet: Address },
    /// Show an asset's listing on one network.
    TokenizedAsset {
        #[arg(value_parser = uppercase_symbol)]
        underlying: UnderlyingSymbol,
        #[arg(long)]
        network: Network,
    },
    /// List an asset on a network.
    AddTokenizedAsset {
        #[arg(long, value_parser = uppercase_symbol)]
        underlying: UnderlyingSymbol,
        #[arg(long)]
        token: String,
        #[arg(long)]
        network: Network,
        #[arg(long)]
        vault: Address,
    },
    /// Read an aggregate's cached snapshot.
    Snapshot { aggregate_type: String, aggregate_id: String },
}

#[derive(Debug, Subcommand)]
pub(crate) enum CapitalCommand {
    /// Freeze an underlying on every network.
    Freeze {
        #[arg(value_parser = uppercase_symbol)]
        underlying: UnderlyingSymbol,
    },
    /// Unfreeze an underlying.
    Unfreeze {
        #[arg(value_parser = uppercase_symbol)]
        underlying: UnderlyingSymbol,
    },
    /// Schedule a freeze window for a corporate action (RFC 3339 instants).
    ScheduleFreeze {
        #[arg(long, value_parser = uppercase_symbol)]
        underlying: UnderlyingSymbol,
        #[arg(long)]
        freeze_at: DateTime<Utc>,
        #[arg(long)]
        unfreeze_at: DateTime<Utc>,
    },
    /// Grant the orchestrator its one-time allowance for an asset.
    ApproveOrchestrator {
        network: Network,
        #[arg(value_parser = uppercase_symbol)]
        underlying: UnderlyingSymbol,
    },
}

/// Parses an underlying symbol upper-cased, as the offline `issuer` CLI does.
/// The bot keys a listing or freeze window by the symbol exactly as a request
/// body carries it, so `aapl` would otherwise address a different asset than
/// the `AAPL` every other path uses.
fn uppercase_symbol(
    value: &str,
) -> Result<UnderlyingSymbol, UnderlyingSymbolError> {
    UnderlyingSymbol::new(value.to_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use clap::error::ErrorKind;
    use st0x_issuance_dto::UnderlyingSymbol;

    use super::{Cli, Command, ReadCommand};
    use crate::target::Env;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(
            std::iter::once("st0x-issuance-client").chain(args.iter().copied()),
        )
    }

    #[test]
    fn the_environment_must_be_chosen_explicitly() {
        let error = parse(&["read", "stuck"]).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn repeated_assets_are_collected() {
        let cli = parse(&[
            "--env",
            "production",
            "read",
            "orchestrator-preflight",
            "base",
            "--asset",
            "AAPL",
            "--asset",
            "TSLA",
        ])
        .unwrap();

        assert_eq!(cli.env, Env::Production);
        let Command::Read(ReadCommand::OrchestratorPreflight {
            network,
            assets,
        }) = cli.command
        else {
            panic!("expected orchestrator-preflight, got {:?}", cli.command);
        };
        assert_eq!(network.as_str(), "base");
        let assets: Vec<&str> =
            assets.iter().map(UnderlyingSymbol::as_str).collect();
        assert_eq!(assets, ["AAPL", "TSLA"]);
    }

    #[test]
    fn malformed_wire_values_fail_at_parse_time() {
        for args in [
            ["--env", "staging", "read", "orchestrator-preflight", "solana"]
                .as_slice(),
            &[
                "--env",
                "staging",
                "debug",
                "register-account",
                "--email",
                "not-an-email",
            ],
            &[
                "--env",
                "staging",
                "debug",
                "whitelist-wallet",
                "client",
                "0xnothex",
            ],
            &[
                "--env",
                "staging",
                "capital",
                "schedule-freeze",
                "--underlying",
                "AAPL",
                "--freeze-at",
                "tomorrow",
                "--unfreeze-at",
                "2026-10-02T00:00:00Z",
            ],
        ] {
            let error = parse(args).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ValueValidation, "{args:?}");
        }
    }
}
