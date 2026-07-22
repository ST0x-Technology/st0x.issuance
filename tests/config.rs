//! Production configuration parsing at the `validate-config` binary boundary.

use alloy::signers::local::PrivateKeySigner;
use std::process::{Command, Output};

fn validate_config_command() -> Command {
    let private_key = PrivateKeySigner::random().to_bytes();
    let mut command = Command::new(env!("CARGO_BIN_EXE_validate-config"));
    command.env_clear().args([
        "--database-url",
        "sqlite::memory:",
        "--issuer-api-key",
        "test-key-12345678901234567890123456",
        "--alpaca-account-id",
        "account-id",
        "--alpaca-api-key",
        "api-key",
        "--alpaca-api-secret",
        "api-secret",
    ]);
    command
        .arg("--evm-private-key")
        .arg(format!("{private_key:#x}"))
        .args(["--backfill-start-block", "0"]);

    command
}

fn legacy_base_command() -> Command {
    let mut command = validate_config_command();
    command.args([
        "--rpc-url",
        "http://127.0.0.1:8545",
        "--subgraph-url",
        "http://127.0.0.1:8080/subgraph",
    ]);

    command
}

fn command_stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn base_only_deployment_config_is_valid() {
    let output = legacy_base_command().output().unwrap();

    assert!(output.status.success(), "{}", command_stderr(&output));
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "configuration valid\n"
    );
}

#[test]
fn explicit_base_environment_group_needs_no_legacy_duplicate() {
    let output = validate_config_command()
        .env("CHAIN_BASE_RPC_URL", "http://127.0.0.1:8545")
        .env("CHAIN_BASE_CHAIN_ID", "8453")
        .env("CHAIN_BASE_SUBGRAPH_URL", "http://127.0.0.1:8080/base-subgraph")
        .env("CHAIN_BASE_BACKFILL_START_BLOCK", "42000000")
        .output()
        .unwrap();

    assert!(output.status.success(), "{}", command_stderr(&output));
}

/// A Base group pointed at Base Sepolia is the realistic copy-paste error: the
/// RPC and the chain id agree with each other, so the startup cross-check
/// passes, and only the network label is wrong. It must be rejected at parse,
/// because the receipt inventory is keyed by chain id and a wrong one silently
/// orphans every existing aggregate for that network.
/// With several chains configured, an error that names only the legacy
/// `SUBGRAPH_URL` sends an operator to the wrong variable.
#[test]
fn subgraph_scheme_error_names_the_failing_chain_variable() {
    let output = legacy_base_command()
        .env("CHAIN_ETHEREUM_RPC_URL", "http://127.0.0.1:9545")
        .env("CHAIN_ETHEREUM_CHAIN_ID", "1")
        .env(
            "CHAIN_ETHEREUM_SUBGRAPH_URL",
            "wss://127.0.0.1:8080/ethereum-subgraph",
        )
        .env("CHAIN_ETHEREUM_BACKFILL_START_BLOCK", "100")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = command_stderr(&output);
    assert!(
        stderr.contains("CHAIN_ETHEREUM_SUBGRAPH_URL"),
        "the error must name the variable that actually failed, got: {stderr}"
    );
}

#[test]
fn base_group_bound_to_a_testnet_chain_id_fails_validation() {
    let output = validate_config_command()
        .env("CHAIN_BASE_RPC_URL", "http://127.0.0.1:8545")
        .env("CHAIN_BASE_CHAIN_ID", "84532")
        .env("CHAIN_BASE_SUBGRAPH_URL", "http://127.0.0.1:8080/base-subgraph")
        .env("CHAIN_BASE_BACKFILL_START_BLOCK", "42000000")
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "a Base label on chain 84532 must not validate"
    );
    let stderr = command_stderr(&output);
    assert!(
        stderr.contains("CHAIN_BASE_CHAIN_ID is 84532")
            && stderr.contains("is chain 8453;"),
        "the error must name both the configured and the expected chain, got: \
         {stderr}"
    );
}

/// The Ethereum group has the same failure mode, and shares no code path with
/// the Base group's legacy fallback.
#[test]
fn ethereum_group_bound_to_the_wrong_chain_id_fails_validation() {
    let output = legacy_base_command()
        .env("CHAIN_ETHEREUM_RPC_URL", "http://127.0.0.1:9545")
        .env("CHAIN_ETHEREUM_CHAIN_ID", "8453")
        .env(
            "CHAIN_ETHEREUM_SUBGRAPH_URL",
            "http://127.0.0.1:8080/ethereum-subgraph",
        )
        .env("CHAIN_ETHEREUM_BACKFILL_START_BLOCK", "100")
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "an Ethereum label on chain 8453 must not validate"
    );
}

#[test]
fn complete_ethereum_environment_group_is_valid() {
    let output = legacy_base_command()
        .env("CHAIN_ETHEREUM_RPC_URL", "http://127.0.0.1:9545")
        .env("CHAIN_ETHEREUM_CHAIN_ID", "1")
        .env(
            "CHAIN_ETHEREUM_SUBGRAPH_URL",
            "http://127.0.0.1:8080/ethereum-subgraph",
        )
        .env("CHAIN_ETHEREUM_BACKFILL_START_BLOCK", "100")
        .output()
        .unwrap();

    assert!(output.status.success(), "{}", command_stderr(&output));
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "configuration valid\n"
    );
}

#[test]
fn invalid_ethereum_subgraph_scheme_fails_validation() {
    let output = legacy_base_command()
        .env("CHAIN_ETHEREUM_RPC_URL", "http://127.0.0.1:9545")
        .env("CHAIN_ETHEREUM_CHAIN_ID", "1")
        .env(
            "CHAIN_ETHEREUM_SUBGRAPH_URL",
            "wss://127.0.0.1:8080/ethereum-subgraph",
        )
        .env("CHAIN_ETHEREUM_BACKFILL_START_BLOCK", "100")
        .output()
        .unwrap();

    assert!(!output.status.success(), "invalid config must fail");
    assert!(
        command_stderr(&output).contains(
            "CHAIN_ETHEREUM_SUBGRAPH_URL must use http or https scheme, \
             got: wss"
        ),
        "{}",
        command_stderr(&output)
    );
}

#[test]
fn partial_ethereum_environment_group_fails_closed() {
    let output = legacy_base_command()
        .env("CHAIN_ETHEREUM_RPC_URL", "http://127.0.0.1:9545")
        .output()
        .unwrap();

    assert!(!output.status.success(), "partial config must fail");
    assert!(
        command_stderr(&output).contains("--chain-ethereum-chain-id"),
        "{}",
        command_stderr(&output)
    );
}
