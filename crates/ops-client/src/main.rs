//! `st0x-issuance-client`, the S01 issuance operations client. Resolves the
//! environment, obtains the S01 Google ID token, sends the command's one ops
//! route, and explains any failure with an exit code and, when the deployment
//! answered, a link to its Cloud Logging.

mod auth;
mod cli;
mod target;
mod transport;

use clap::Parser;
use reqwest::Method;
use st0x_issuance_dto::{
    AddTokenizedAssetRequest, RegisterAccountRequest,
    ScheduleFreezeWindowRequest, TokenSymbol, WhitelistWalletRequest,
};
use std::io::Write;
use std::process::ExitCode;

use crate::auth::AuthError;
use crate::cli::{
    CapitalCommand, Cli, Command, DebugCommand, ReadCommand,
    WrappedTransfersArgs,
};
use crate::target::{Env, TargetError};
use crate::transport::{
    Client, Route, RouteBody, Tier, TransportError, encode_segment,
};

/// Exit status for a setup or argument error (clap uses the same for its own).
const EXIT_SETUP: u8 = 2;
/// Exit status for an authentication or authorization failure, distinct from a
/// request the bot accepted and failed.
const EXIT_ACCESS_DENIED: u8 = 77;
const EXIT_FAILED: u8 = 1;

#[tokio::main]
async fn main() -> ExitCode {
    let Cli { env, command } = Cli::parse();
    let route = route(&command);

    match run(env, &route, |name| std::env::var(name).ok()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(failure) => {
            eprintln!("error: {failure}");
            if failure.reached_server() {
                eprintln!(
                    "\nS01 Cloud Logging: {}",
                    cloud_logging_url(env, log_search(&command))
                );
            }
            ExitCode::from(failure.exit_code())
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error(transparent)]
    Target(#[from] TargetError),
    #[error("could not build the HTTP client: {0}")]
    Http(#[from] reqwest::Error),
    #[error("could not obtain an S01 Google identity: {0}")]
    Auth(#[from] AuthError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(
        "the S01 ops API answered with success, but the response could not be \
         written to stdout: {0}\nThe request was completed: a read can simply \
         be re-run, but do not blindly retry a write."
    )]
    Output(#[source] std::io::Error),
}

impl Failure {
    fn exit_code(&self) -> u8 {
        match self {
            Self::Target(_) | Self::Http(_) => EXIT_SETUP,
            Self::Auth(error) if error.is_access_denied() => EXIT_ACCESS_DENIED,
            Self::Auth(error) if error.is_client_misconfigured() => EXIT_SETUP,
            Self::Transport(error) if error.is_access_denied() => {
                EXIT_ACCESS_DENIED
            }
            Self::Auth(_) | Self::Transport(_) | Self::Output(_) => EXIT_FAILED,
        }
    }

    const fn reached_server(&self) -> bool {
        match self {
            Self::Transport(error) => error.reached_server(),
            Self::Output(_) => true,
            Self::Target(_) | Self::Http(_) | Self::Auth(_) => false,
        }
    }
}

async fn run(
    env: Env,
    route: &Route,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<(), Failure> {
    let target = target::resolve(env, lookup)?;
    let token = auth::id_token(env, target.identity).await?;
    let client = Client::new(target.base_url, token)?;
    let body = client.send(route).await?;

    // A write failure here follows a success the bot already acted on, which
    // is what `Failure::Output` tells the operator.
    print_body(&body).map_err(Failure::Output)
}

/// Writes the bot's response body to stdout as one line.
fn print_body(body: &str) -> std::io::Result<()> {
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{}", body.trim_end())?;
    stdout.flush()
}

fn route(command: &Command) -> Route {
    match command {
        Command::Read(read) => read_route(read),
        Command::Debug(debug) => debug_route(debug),
        Command::Capital(capital) => capital_route(capital),
    }
}

fn read_route(command: &ReadCommand) -> Route {
    use ReadCommand::{
        NetworkTelemetry, OrchestratorHealth, OrchestratorPreflight, Status,
        Stuck, WrappedTransfers,
    };

    match command {
        Stuck => bare(Method::GET, Tier::Read, "/stuck".to_owned()),
        OrchestratorHealth => {
            bare(Method::GET, Tier::Read, "/orchestrator-health".to_owned())
        }
        NetworkTelemetry => {
            bare(Method::GET, Tier::Read, "/network-telemetry".to_owned())
        }
        WrappedTransfers(args) => Route {
            query: wrapped_transfers_query(args),
            ..bare(Method::GET, Tier::Read, "/wrapped-transfers".to_owned())
        },
        Status { underlying } => bare(
            Method::GET,
            Tier::Read,
            format!("/status/{}", encode_segment(underlying.as_str())),
        ),
        OrchestratorPreflight { network, assets } => Route {
            query: assets
                .iter()
                .map(|asset| ("asset", asset.as_str().to_owned()))
                .collect(),
            ..bare(
                Method::GET,
                Tier::Read,
                format!("/orchestrator-preflight/{}", network.as_str()),
            )
        },
    }
}

fn wrapped_transfers_query(
    args: &WrappedTransfersArgs,
) -> Vec<(&'static str, String)> {
    let WrappedTransfersArgs {
        limit,
        before_block,
        before_log_index,
        before_network,
    } = args;

    [
        ("limit", limit.map(|limit| limit.to_string())),
        ("before_block", before_block.map(|block| block.to_string())),
        ("before_log_index", before_log_index.map(|index| index.to_string())),
        (
            "before_network",
            before_network.map(|network| network.as_str().to_owned()),
        ),
    ]
    .into_iter()
    .filter_map(|(key, value)| value.map(|value| (key, value)))
    .collect()
}

fn debug_route(command: &DebugCommand) -> Route {
    use DebugCommand::{
        AddTokenizedAsset, RecoverRedemption, RegisterAccount, ReprocessMint,
        Snapshot, TokenizedAsset, UnwhitelistWallet, VerifyOrchestratorSigning,
        WhitelistWallet,
    };

    match command {
        RecoverRedemption { issuer_request_id } => bare(
            Method::POST,
            Tier::Debug,
            format!(
                "/recover/redemption/{}",
                encode_segment(issuer_request_id)
            ),
        ),
        ReprocessMint { issuer_request_id } => bare(
            Method::POST,
            Tier::Debug,
            format!("/reprocess/mint/{}", encode_segment(issuer_request_id)),
        ),
        VerifyOrchestratorSigning { network, underlying } => bare(
            Method::POST,
            Tier::Debug,
            format!(
                "/orchestrator-verify-signing/{}/{}",
                network.as_str(),
                encode_segment(underlying.as_str())
            ),
        ),
        RegisterAccount { email } => Route {
            body: Some(RouteBody::RegisterAccount(RegisterAccountRequest {
                email: email.clone(),
            })),
            ..bare(Method::POST, Tier::Debug, "/accounts".to_owned())
        },
        WhitelistWallet { client_id, wallet } => Route {
            body: Some(RouteBody::WhitelistWallet(WhitelistWalletRequest {
                wallet: *wallet,
            })),
            ..bare(
                Method::POST,
                Tier::Debug,
                format!("/accounts/{}/wallets", encode_segment(client_id)),
            )
        },
        UnwhitelistWallet { client_id, wallet } => bare(
            Method::DELETE,
            Tier::Debug,
            format!("/accounts/{}/wallets/{wallet}", encode_segment(client_id)),
        ),
        TokenizedAsset { underlying, network } => Route {
            query: vec![("network", network.as_str().to_owned())],
            ..bare(
                Method::GET,
                Tier::Debug,
                format!(
                    "/tokenized-assets/{}",
                    encode_segment(underlying.as_str())
                ),
            )
        },
        AddTokenizedAsset { underlying, token, network, vault } => Route {
            body: Some(RouteBody::AddTokenizedAsset(
                AddTokenizedAssetRequest {
                    underlying: underlying.clone(),
                    token: TokenSymbol::new(token.as_str()),
                    network: *network,
                    vault: *vault,
                },
            )),
            ..bare(Method::POST, Tier::Debug, "/tokenized-assets".to_owned())
        },
        Snapshot { aggregate_type, aggregate_id } => bare(
            Method::GET,
            Tier::Debug,
            format!(
                "/snapshots/{}/{}",
                encode_segment(aggregate_type),
                encode_segment(aggregate_id)
            ),
        ),
    }
}

fn capital_route(command: &CapitalCommand) -> Route {
    use CapitalCommand::{
        ApproveOrchestrator, Freeze, ScheduleFreeze, Unfreeze,
    };

    match command {
        Freeze { underlying } => bare(
            Method::POST,
            Tier::Capital,
            format!("/freeze/{}", encode_segment(underlying.as_str())),
        ),
        Unfreeze { underlying } => bare(
            Method::POST,
            Tier::Capital,
            format!("/unfreeze/{}", encode_segment(underlying.as_str())),
        ),
        ScheduleFreeze { underlying, freeze_at, unfreeze_at } => Route {
            body: Some(RouteBody::ScheduleFreezeWindow(
                ScheduleFreezeWindowRequest {
                    underlying: underlying.clone(),
                    freeze_at: *freeze_at,
                    unfreeze_at: *unfreeze_at,
                },
            )),
            ..bare(Method::POST, Tier::Capital, "/freeze-schedules".to_owned())
        },
        ApproveOrchestrator { network, underlying } => bare(
            Method::POST,
            Tier::Capital,
            format!(
                "/orchestrator-approve/{}/{}",
                network.as_str(),
                encode_segment(underlying.as_str())
            ),
        ),
    }
}

/// A route with no query and no body.
const fn bare(method: Method, tier: Tier, path: String) -> Route {
    Route { method, tier, path, query: Vec::new(), body: None }
}

/// The identifier a failed command's logs would mention: its aggregate or
/// client id, or its underlying. `None` for commands that name neither.
fn log_search(command: &Command) -> Option<&str> {
    match command {
        Command::Read(ReadCommand::Status { underlying }) => {
            Some(underlying.as_str())
        }
        Command::Read(_)
        | Command::Debug(DebugCommand::RegisterAccount { .. }) => None,
        Command::Debug(
            DebugCommand::RecoverRedemption { issuer_request_id }
            | DebugCommand::ReprocessMint { issuer_request_id },
        ) => Some(issuer_request_id),
        Command::Debug(
            DebugCommand::WhitelistWallet { client_id, .. }
            | DebugCommand::UnwhitelistWallet { client_id, .. },
        ) => Some(client_id),
        Command::Debug(DebugCommand::Snapshot { aggregate_id, .. }) => {
            Some(aggregate_id)
        }
        Command::Debug(
            DebugCommand::VerifyOrchestratorSigning { underlying, .. }
            | DebugCommand::TokenizedAsset { underlying, .. }
            | DebugCommand::AddTokenizedAsset { underlying, .. },
        )
        | Command::Capital(
            CapitalCommand::Freeze { underlying }
            | CapitalCommand::Unfreeze { underlying }
            | CapitalCommand::ScheduleFreeze { underlying, .. }
            | CapitalCommand::ApproveOrchestrator { underlying, .. },
        ) => Some(underlying.as_str()),
    }
}

/// A Logs Explorer link in the environment's project, searching for `search`
/// when the command names an identifier and for warnings otherwise.
fn cloud_logging_url(env: Env, search: Option<&str>) -> String {
    let query = search.map_or_else(
        || "severity>=WARNING".to_owned(),
        |term| format!("\"{}\"", term.replace('"', "\\\"")),
    );

    format!(
        "https://console.cloud.google.com/logs/query;query={}?project={}",
        encode_segment(&query),
        env.logging_project()
    )
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use reqwest::StatusCode;
    use serde_json::{Value, json};
    use url::Url;

    use super::{Failure, cloud_logging_url, log_search, route};
    use crate::auth::AuthError;
    use crate::cli::Cli;
    use crate::target::{Env, TargetError};
    use crate::transport::{Client, TransportError};

    fn command(args: &[&str]) -> crate::cli::Command {
        Cli::try_parse_from(
            ["st0x-issuance-client", "--env", "staging"]
                .into_iter()
                .chain(args.iter().copied()),
        )
        .unwrap()
        .command
    }

    /// Renders a command's route as `METHOD /path?query` plus its JSON body,
    /// through the same URL construction the transport sends.
    fn wire(args: &[&str]) -> (String, Option<Value>) {
        let route = route(&command(args));
        let client = Client::new(
            Url::parse("https://ops.example").unwrap(),
            "id-token".to_owned(),
        )
        .unwrap();
        let url = client.url(&route);
        let target = url.query().map_or_else(
            || format!("{} {}", route.method, url.path()),
            |query| format!("{} {}?{query}", route.method, url.path()),
        );

        (target, route.body.map(|body| serde_json::to_value(body).unwrap()))
    }

    fn bodiless(line: &str) -> (String, Option<Value>) {
        (line.to_owned(), None)
    }

    #[test]
    fn every_read_verb_maps_to_its_route() {
        assert_eq!(wire(&["read", "stuck"]), bodiless("GET /ops/read/stuck"));
        assert_eq!(
            wire(&["read", "orchestrator-health"]),
            bodiless("GET /ops/read/orchestrator-health")
        );
        assert_eq!(
            wire(&["read", "network-telemetry"]),
            bodiless("GET /ops/read/network-telemetry")
        );
        assert_eq!(
            wire(&["read", "wrapped-transfers"]),
            bodiless("GET /ops/read/wrapped-transfers")
        );
        assert_eq!(
            wire(&[
                "read",
                "wrapped-transfers",
                "--limit",
                "10",
                "--before-block",
                "7",
                "--before-log-index",
                "2",
                "--before-network",
                "base",
            ]),
            bodiless(
                "GET /ops/read/wrapped-transfers?limit=10&before_block=7\
                 &before_log_index=2&before_network=base"
            )
        );
        assert_eq!(
            wire(&["read", "status", "AAPL"]),
            bodiless("GET /ops/read/status/AAPL")
        );
        assert_eq!(
            wire(&[
                "read",
                "orchestrator-preflight",
                "ethereum",
                "--asset",
                "AAPL",
                "--asset",
                "TSLA",
            ]),
            bodiless(
                "GET /ops/read/orchestrator-preflight/ethereum?asset=AAPL&asset=TSLA"
            )
        );
    }

    #[test]
    fn every_debug_verb_maps_to_its_route() {
        assert_eq!(
            wire(&["debug", "recover-redemption", "0xabc"]),
            bodiless("POST /ops/debug/recover/redemption/0xabc")
        );
        assert_eq!(
            wire(&["debug", "reprocess-mint", "mint/1"]),
            bodiless("POST /ops/debug/reprocess/mint/mint%2F1")
        );
        assert_eq!(
            wire(&["debug", "verify-orchestrator-signing", "base", "AAPL"]),
            bodiless("POST /ops/debug/orchestrator-verify-signing/base/AAPL")
        );
        assert_eq!(
            wire(&[
                "debug",
                "register-account",
                "--email",
                " Ops@Example.com "
            ]),
            (
                "POST /ops/debug/accounts".to_owned(),
                Some(json!({ "email": "ops@example.com" }))
            )
        );
        assert_eq!(
            wire(&[
                "debug",
                "whitelist-wallet",
                "client-1",
                "0x1111111111111111111111111111111111111111",
            ]),
            (
                "POST /ops/debug/accounts/client-1/wallets".to_owned(),
                Some(json!({
                    "wallet": "0x1111111111111111111111111111111111111111"
                }))
            )
        );
        assert_eq!(
            wire(&[
                "debug",
                "unwhitelist-wallet",
                "client-1",
                "0x1111111111111111111111111111111111111111",
            ]),
            bodiless(
                "DELETE /ops/debug/accounts/client-1/wallets/\
                 0x1111111111111111111111111111111111111111"
            )
        );
        assert_eq!(
            wire(&["debug", "tokenized-asset", "AAPL", "--network", "base"]),
            bodiless("GET /ops/debug/tokenized-assets/AAPL?network=base")
        );
        assert_eq!(
            wire(&[
                "debug",
                "add-tokenized-asset",
                "--underlying",
                "AAPL",
                "--token",
                "tAAPL",
                "--network",
                "base",
                "--vault",
                "0x2222222222222222222222222222222222222222",
            ]),
            (
                "POST /ops/debug/tokenized-assets".to_owned(),
                Some(json!({
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "network": "base",
                    "vault": "0x2222222222222222222222222222222222222222"
                }))
            )
        );
        assert_eq!(
            wire(&["debug", "snapshot", "Mint", "some id"]),
            bodiless("GET /ops/debug/snapshots/Mint/some%20id")
        );
    }

    #[test]
    fn every_capital_verb_maps_to_its_route() {
        assert_eq!(
            wire(&["capital", "freeze", "AAPL"]),
            bodiless("POST /ops/capital/freeze/AAPL")
        );
        assert_eq!(
            wire(&["capital", "unfreeze", "AAPL"]),
            bodiless("POST /ops/capital/unfreeze/AAPL")
        );
        assert_eq!(
            wire(&[
                "capital",
                "schedule-freeze",
                "--underlying",
                "AAPL",
                "--freeze-at",
                "2026-10-01T13:30:00Z",
                "--unfreeze-at",
                "2026-10-02T13:30:00Z",
            ]),
            (
                "POST /ops/capital/freeze-schedules".to_owned(),
                Some(json!({
                    "underlying": "AAPL",
                    "freeze_at": "2026-10-01T13:30:00Z",
                    "unfreeze_at": "2026-10-02T13:30:00Z"
                }))
            )
        );
        assert_eq!(
            wire(&["capital", "approve-orchestrator", "base", "AAPL"]),
            bodiless("POST /ops/capital/orchestrator-approve/base/AAPL")
        );
    }

    /// `add-tokenized-asset` and `schedule-freeze` carry the symbol in a JSON
    /// body, which the bot keys exactly as sent, so the client upper-cases
    /// every symbol at parse time as the offline `issuer` CLI does.
    #[test]
    fn lowercase_symbols_are_sent_upper_cased() {
        let (_, add) = wire(&[
            "debug",
            "add-tokenized-asset",
            "--underlying",
            "aapl",
            "--token",
            "tAAPL",
            "--network",
            "base",
            "--vault",
            "0x2222222222222222222222222222222222222222",
        ]);
        assert_eq!(add.unwrap()["underlying"], "AAPL");

        let (_, schedule) = wire(&[
            "capital",
            "schedule-freeze",
            "--underlying",
            "aapl",
            "--freeze-at",
            "2026-10-01T13:30:00Z",
            "--unfreeze-at",
            "2026-10-02T13:30:00Z",
        ]);
        assert_eq!(schedule.unwrap()["underlying"], "AAPL");

        assert_eq!(
            wire(&["capital", "freeze", "aapl"]),
            bodiless("POST /ops/capital/freeze/AAPL")
        );
        assert_eq!(
            wire(&[
                "read",
                "orchestrator-preflight",
                "base",
                "--asset",
                "aapl"
            ]),
            bodiless("GET /ops/read/orchestrator-preflight/base?asset=AAPL")
        );
    }

    #[test]
    fn the_logging_link_searches_the_named_identifier_in_the_right_project() {
        let freeze = command(&["capital", "freeze", "AAPL"]);
        assert_eq!(
            cloud_logging_url(Env::Staging, log_search(&freeze)),
            "https://console.cloud.google.com/logs/query;query=%22AAPL%22\
             ?project=s01-issuance-staging"
        );

        let stuck = command(&["read", "stuck"]);
        assert_eq!(
            cloud_logging_url(Env::Production, log_search(&stuck)),
            "https://console.cloud.google.com/logs/query;\
             query=severity%3E%3DWARNING?project=s01-issuance"
        );
    }

    #[test]
    fn failures_map_to_setup_access_denied_and_failed_exit_codes() {
        let setup = Failure::Target(TargetError::Missing {
            variable: "S01_ISSUANCE_STAGING_URL".to_owned(),
            hint: "the url",
        });
        assert_eq!(setup.exit_code(), 2);
        assert!(!setup.reached_server());

        let denied = Failure::Transport(TransportError::Forbidden {
            body: String::new(),
        });
        assert_eq!(denied.exit_code(), 77);
        assert!(denied.reached_server());

        let failed = Failure::Transport(TransportError::NotFound {
            body: String::new(),
        });
        assert_eq!(failed.exit_code(), 1);
        assert!(failed.reached_server());

        let declined = Failure::Auth(AuthError::Authorization {
            code: "access_denied".to_owned(),
        });
        assert_eq!(declined.exit_code(), 77);

        let bad_scope = Failure::Auth(AuthError::Authorization {
            code: "invalid_scope".to_owned(),
        });
        assert_eq!(bad_scope.exit_code(), 2, "a client misconfiguration");

        let revoked = Failure::Auth(AuthError::TokenEndpoint {
            status: StatusCode::BAD_REQUEST,
            code: Some("invalid_grant".to_owned()),
            body: String::new(),
        });
        assert_eq!(revoked.exit_code(), 77);

        let bad_secret = Failure::Auth(AuthError::TokenEndpoint {
            status: StatusCode::UNAUTHORIZED,
            code: Some("invalid_client".to_owned()),
            body: String::new(),
        });
        assert_eq!(
            bad_secret.exit_code(),
            2,
            "a misconfigured client is setup"
        );

        let wrong_grant_type = Failure::Auth(AuthError::TokenEndpoint {
            status: StatusCode::BAD_REQUEST,
            code: Some("unauthorized_client".to_owned()),
            body: String::new(),
        });
        assert_eq!(
            wrong_grant_type.exit_code(),
            2,
            "a client misconfiguration"
        );

        let throttled = Failure::Auth(AuthError::TokenEndpoint {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: None,
            body: String::new(),
        });
        assert_eq!(throttled.exit_code(), 1, "a rate limit is not a denial");

        let google_down = Failure::Auth(AuthError::Authorization {
            code: "temporarily_unavailable".to_owned(),
        });
        assert_eq!(google_down.exit_code(), 1, "an outage is not a denial");
        assert!(!google_down.reached_server());

        let unwritten = Failure::Output(std::io::Error::other("broken pipe"));
        assert_eq!(unwritten.exit_code(), 1);
        assert!(unwritten.reached_server(), "the request was completed");
    }
}
