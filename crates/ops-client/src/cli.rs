//! Command-line model. Every leaf verb maps to exactly one `/ops` route, and
//! arguments parse into the shared wire types the bot deserializes; the bot
//! still validates and decides everything.

use alloy_primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use st0x_issuance_dto::{
    DecimalShares, Email, Network, UnderlyingSymbol, UnderlyingSymbolError,
};
use uuid::Uuid;

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
    /// Overrides of normal safety checks: force-complete, close, and excess
    /// burns.
    #[command(subcommand)]
    Breakglass(BreakglassCommand),
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

#[derive(Debug, Subcommand)]
pub(crate) enum BreakglassCommand {
    /// Terminalize a redemption whose burn already landed on-chain but was
    /// never recorded (a `Burning`, `BurnIntended`, or `BurnSubmitted`
    /// redemption); the bot verifies the burn before recording it. A `Failed`
    /// redemption needs the offline `issuer force-complete-redemption` instead.
    ForceCompleteRedemption {
        issuer_request_id: String,
        #[arg(long)]
        burn_tx_hash: B256,
        #[arg(long)]
        reason: String,
        /// The persisted signed burn's hash, when it differs from
        /// `--burn-tx-hash`.
        #[arg(long)]
        acknowledged_unresolved_burn_tx_hash: Option<B256>,
    },
    /// Close a redemption that cannot be recovered automatically.
    CloseRedemption {
        issuer_request_id: String,
        #[arg(long)]
        reason: String,
        /// Required when a signed burn is persisted: its exact hash.
        #[arg(long)]
        acknowledged_unresolved_burn_tx_hash: Option<B256>,
    },
    /// Close a mint that cannot be recovered automatically.
    CloseMint {
        issuer_request_id: String,
        #[arg(long)]
        reason: String,
        /// Required when the mint holds a prepared deposit: its exact hash.
        #[arg(long)]
        acknowledged_unresolved_mint_tx_hash: Option<B256>,
        /// Required only for a `NonceReplayUnresolved` mint: its persisted
        /// authorization nonce.
        #[arg(long)]
        acknowledged_unresolved_mint_nonce: Option<B256>,
    },
    /// Burn excess shares minted by a duplicate deposit. Dry-run unless
    /// `--execute`.
    #[command(subcommand)]
    BurnExcess(BurnExcessCommand),
}

#[derive(Debug, Subcommand)]
pub(crate) enum BurnExcessCommand {
    /// The excess shares already sit in the issuer wallet.
    Internal(BurnExcessArgs),
    /// First step when the excess shares must be sent back into the issuer
    /// wallet: run with `--execute` before that funding Transfer is
    /// broadcast, so the bot's redemption poller holds it instead of
    /// redeeming it. `--close` releases the hold if it will not be sent.
    ExpectFunding(BurnExcessArgs),
    /// The excess shares arrived through an on-chain Transfer into the issuer
    /// wallet. Needs `expect-funding --execute` recorded before that Transfer
    /// was broadcast; the bot refuses a stream without it.
    External {
        /// Funding Transfer that moved the excess shares into the wallet.
        #[arg(long)]
        funding_tx_hash: B256,
        #[command(flatten)]
        args: BurnExcessArgs,
    },
}

/// Flags shared by every burn-excess step, named as on the offline
/// `issuer burn-excess` CLI.
#[derive(Debug, Args)]
pub(crate) struct BurnExcessArgs {
    /// Mint whose deposit produced the excess.
    #[arg(long)]
    pub(crate) issuer_request_id: Uuid,
    /// Deposit transaction that created the excess receipt and shares.
    #[arg(long)]
    pub(crate) deposit_tx_hash: B256,
    /// Excess receipt id from the deposit.
    #[arg(long)]
    pub(crate) receipt_id: U256,
    /// Excess share amount as a decimal (18-decimal fixed point on chain),
    /// e.g. `0.750`.
    #[arg(long)]
    pub(crate) shares: DecimalShares,
    /// Why this recovery is being run; recorded on events.
    #[arg(long)]
    pub(crate) reason: String,
    /// Optional incident or ticket id for the audit trail.
    #[arg(long)]
    pub(crate) incident_id: Option<String>,
    #[arg(long)]
    pub(crate) network: Network,
    /// Must match the network's chain configured on the bot.
    #[arg(long)]
    pub(crate) chain_id: u64,
    /// Perform the mutation. Without it the bot returns the proven plan and
    /// changes nothing.
    #[arg(long)]
    pub(crate) execute: bool,
    /// Close the stream instead of burning: a dead intended or submitted
    /// burn, or an expectation whose funding Transfer will not be sent.
    #[arg(long)]
    pub(crate) close: bool,
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
            &burn_excess(["--shares", "0.0000000000000000001"]),
            &burn_excess(["--issuer-request-id", "not-a-uuid"]),
        ] {
            let error = parse(args).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ValueValidation, "{args:?}");
        }
    }

    /// A complete `breakglass burn-excess internal` invocation with `field`
    /// overridden, so only that one value can fail parsing.
    fn burn_excess(field: [&'static str; 2]) -> Vec<&'static str> {
        let mut args = vec![
            "--env",
            "staging",
            "breakglass",
            "burn-excess",
            "internal",
            "--issuer-request-id",
            "5f0c6c0e-8a4b-4c9e-9f3a-2b7d1e6a4c10",
            "--deposit-tx-hash",
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            "--receipt-id",
            "1",
            "--shares",
            "0.75",
            "--reason",
            "duplicate deposit",
            "--network",
            "base",
            "--chain-id",
            "8453",
        ];
        let position = args.iter().position(|arg| *arg == field[0]).unwrap();
        args[position + 1] = field[1];
        args
    }
}
