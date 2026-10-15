//! Command cases and a dispatch entry point for the bot's tests, which drive
//! the client against its real router. Compiled for this crate's own tests and
//! with `test-support`.

use clap::Parser;
use uuid::Uuid;

use crate::cli::Cli;
use crate::route;
pub use crate::transport::{Client, TransportError};

/// Failure to parse or send one command through a supplied client.
#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    /// The command line arguments are invalid.
    #[error(transparent)]
    Parse(clap::Error),
    /// The request failed.
    #[error(transparent)]
    Transport(TransportError),
}

/// Parses and sends one command without resolving a deployment, signing in,
/// or printing, and returns the response body.
///
/// `args` includes the program name and `--env`, as normal command line
/// arguments do. The supplied client decides the target and the token.
///
/// # Errors
///
/// Returns the parse error for invalid arguments and the transport error for
/// a failed request.
pub async fn dispatch_args(
    client: &Client,
    args: &[&str],
) -> Result<String, CommandError> {
    let cli = Cli::try_parse_from(args).map_err(CommandError::Parse)?;
    client
        .send(&route(&cli.command), Uuid::new_v4())
        .await
        .map_err(CommandError::Transport)
}

/// One complete command line used to prove client and server route agreement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandCase {
    /// The command group, which is also the `/ops/<tier>` prefix whose
    /// token the command needs.
    pub tier: &'static str,
    /// Full arguments including the program name and environment.
    pub args: Vec<&'static str>,
}

fn case(tier: &'static str, verb: &[&'static str]) -> CommandCase {
    let mut args = vec!["st0x-issuance-client", "--env", "staging", tier];
    args.extend_from_slice(verb);
    CommandCase { tier, args }
}

/// A mint, account, or aggregate id.
const ID: &str = "5f0c6c0e-8a4b-4c9e-9f3a-2b7d1e6a4c10";
const WALLET: &str = "0x1111111111111111111111111111111111111111";
/// A transaction hash, which is also how a redemption is identified.
const HASH: &str =
    "0x1111111111111111111111111111111111111111111111111111111111111111";

/// Every client command, with representative valid arguments.
#[must_use]
pub fn command_cases() -> Vec<CommandCase> {
    let mut cases = vec![
        case("read", &["stuck"]),
        case("read", &["orchestrator-health"]),
        case("read", &["network-telemetry"]),
        case("read", &["wrapped-transfers"]),
        case("read", &["status", "AAPL"]),
        case("read", &["orchestrator-preflight", "base", "--asset", "AAPL"]),
        case("debug", &["recover-redemption", HASH]),
        case("debug", &["reprocess-mint", ID]),
        case("debug", &["verify-orchestrator-signing", "base", "AAPL"]),
        case("debug", &["register-account", "--email", "ops@example.com"]),
        case("debug", &["whitelist-wallet", ID, WALLET]),
        case("debug", &["unwhitelist-wallet", ID, WALLET]),
        case("debug", &["tokenized-asset", "AAPL", "--network", "base"]),
        case(
            "debug",
            &[
                "add-tokenized-asset",
                "--underlying",
                "AAPL",
                "--token",
                "tAAPL",
                "--network",
                "base",
                "--vault",
                WALLET,
            ],
        ),
        case("debug", &["snapshot", "Mint", ID]),
        case("capital", &["freeze", "AAPL"]),
        case("capital", &["unfreeze", "AAPL"]),
        case(
            "capital",
            &[
                "schedule-freeze",
                "--underlying",
                "AAPL",
                "--freeze-at",
                "2026-10-01T13:30:00Z",
                "--unfreeze-at",
                "2026-10-02T13:30:00Z",
            ],
        ),
        case("capital", &["approve-orchestrator", "base", "AAPL"]),
        case(
            "breakglass",
            &[
                "force-complete-redemption",
                HASH,
                "--burn-tx-hash",
                HASH,
                "--reason",
                "burn landed",
            ],
        ),
        case("breakglass", &["close-redemption", HASH, "--reason", "dead"]),
        case("breakglass", &["close-mint", ID, "--reason", "dead"]),
    ];
    cases.extend(["internal", "expect-funding"].map(|step| {
        case(
            "breakglass",
            &[&["burn-excess", step], BURN_EXCESS_FLAGS].concat(),
        )
    }));
    cases.push(case(
        "breakglass",
        &[
            &["burn-excess", "external", "--funding-tx-hash", HASH],
            BURN_EXCESS_FLAGS,
        ]
        .concat(),
    ));
    cases
}

const BURN_EXCESS_FLAGS: &[&str] = &[
    "--issuer-request-id",
    ID,
    "--deposit-tx-hash",
    HASH,
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use clap::CommandFactory;

    use super::command_cases;
    use crate::cli::Cli;

    /// Collects the subcommand path of every command that takes no further
    /// subcommand.
    fn leaf_commands(
        command: &clap::Command,
        path: &mut Vec<String>,
        leaves: &mut BTreeSet<Vec<String>>,
    ) {
        let mut subcommands = command.get_subcommands().peekable();
        if subcommands.peek().is_none() {
            leaves.insert(path.clone());
            return;
        }
        for subcommand in subcommands {
            path.push(subcommand.get_name().to_owned());
            leaf_commands(subcommand, path, leaves);
            path.pop();
        }
    }

    /// A new client verb without a case would escape the bot's route tests,
    /// so every leaf of the clap tree must have one.
    #[test]
    fn command_cases_cover_every_leaf_command() {
        let mut leaves = BTreeSet::new();
        leaf_commands(&Cli::command(), &mut Vec::new(), &mut leaves);
        assert!(leaves.iter().all(|leaf| !leaf.is_empty()));

        let cases = command_cases();
        let uncovered = leaves
            .iter()
            .filter(|leaf| {
                !cases.iter().any(|case| {
                    case.args.get(3..).is_some_and(|verb| {
                        verb.len() >= leaf.len()
                            && leaf
                                .iter()
                                .zip(verb)
                                .all(|(name, arg)| name == arg)
                    })
                })
            })
            .collect::<Vec<_>>();
        assert!(uncovered.is_empty(), "commands without a case: {uncovered:?}");
    }
}
