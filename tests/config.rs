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
    command.args(["--rpc-url", "http://127.0.0.1:8545"]);

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
        .env("CHAIN_BASE_BACKFILL_START_BLOCK", "42000000")
        .output()
        .unwrap();

    assert!(output.status.success(), "{}", command_stderr(&output));
}

#[test]
fn base_group_bound_to_a_testnet_chain_id_fails_validation() {
    let output = validate_config_command()
        .env("CHAIN_BASE_RPC_URL", "http://127.0.0.1:8545")
        .env("CHAIN_BASE_CHAIN_ID", "84532")
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
        .env("CHAIN_ETHEREUM_BACKFILL_START_BLOCK", "100")
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "an Ethereum label on chain 8453 must not validate"
    );
    let stderr = command_stderr(&output);
    assert!(
        stderr.contains("CHAIN_ETHEREUM_CHAIN_ID is 8453")
            && stderr.contains("is chain 1;"),
        "the error must name both the configured and the expected chain, got: \
         {stderr}"
    );
}

#[test]
fn complete_ethereum_environment_group_is_valid() {
    let output = legacy_base_command()
        .env("CHAIN_ETHEREUM_RPC_URL", "http://127.0.0.1:9545")
        .env("CHAIN_ETHEREUM_CHAIN_ID", "1")
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
fn complete_hyperevm_environment_group_is_valid() {
    let output = legacy_base_command()
        .env("CHAIN_HYPEREVM_RPC_URL", "http://127.0.0.1:10545")
        .env("CHAIN_HYPEREVM_CHAIN_ID", "999")
        .env("CHAIN_HYPEREVM_BACKFILL_START_BLOCK", "9000000")
        .output()
        .unwrap();

    assert!(output.status.success(), "{}", command_stderr(&output));
}

#[test]
fn hyperevm_group_bound_to_the_testnet_chain_id_fails_validation() {
    let output = legacy_base_command()
        .env("CHAIN_HYPEREVM_RPC_URL", "http://127.0.0.1:10545")
        .env("CHAIN_HYPEREVM_CHAIN_ID", "998")
        .env("CHAIN_HYPEREVM_BACKFILL_START_BLOCK", "9000000")
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "a HyperEVM label on chain 998 must not validate"
    );
    let stderr = command_stderr(&output);
    assert!(
        stderr.contains("CHAIN_HYPEREVM_CHAIN_ID is 998")
            && stderr.contains("is chain 999;"),
        "the error must name both the configured and the expected chain, \
         got: {stderr}"
    );
}

#[test]
fn complete_robinhood_environment_group_is_valid() {
    let output = legacy_base_command()
        .env("CHAIN_ROBINHOOD_RPC_URL", "http://127.0.0.1:11545")
        .env("CHAIN_ROBINHOOD_CHAIN_ID", "4663")
        .env("CHAIN_ROBINHOOD_BACKFILL_START_BLOCK", "1000")
        .output()
        .unwrap();

    assert!(output.status.success(), "{}", command_stderr(&output));
}

#[test]
fn robinhood_group_bound_to_the_wrong_chain_id_fails_validation() {
    let output = legacy_base_command()
        .env("CHAIN_ROBINHOOD_RPC_URL", "http://127.0.0.1:11545")
        .env("CHAIN_ROBINHOOD_CHAIN_ID", "42161")
        .env("CHAIN_ROBINHOOD_BACKFILL_START_BLOCK", "1000")
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "a Robinhood label on chain 42161 must not validate"
    );
    let stderr = command_stderr(&output);
    assert!(
        stderr.contains("CHAIN_ROBINHOOD_CHAIN_ID is 42161")
            && stderr.contains("is chain 4663;"),
        "the error must name both the configured and the expected chain, \
         got: {stderr}"
    );
}

/// BNB Smart Chain's group prefix follows the network's wire name, so the
/// variables read `CHAIN_BINANCE_*` rather than `CHAIN_BNB_*`.
#[test]
fn complete_binance_environment_group_is_valid() {
    let output = legacy_base_command()
        .env("CHAIN_BINANCE_RPC_URL", "http://127.0.0.1:12545")
        .env("CHAIN_BINANCE_CHAIN_ID", "56")
        .env("CHAIN_BINANCE_BACKFILL_START_BLOCK", "1000")
        .output()
        .unwrap();

    assert!(output.status.success(), "{}", command_stderr(&output));
}

#[test]
fn binance_group_bound_to_the_testnet_chain_id_fails_validation() {
    let output = legacy_base_command()
        .env("CHAIN_BINANCE_RPC_URL", "http://127.0.0.1:12545")
        .env("CHAIN_BINANCE_CHAIN_ID", "97")
        .env("CHAIN_BINANCE_BACKFILL_START_BLOCK", "1000")
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "a BNB Smart Chain label on chain 97 must not validate"
    );
    let stderr = command_stderr(&output);
    assert!(
        stderr.contains("CHAIN_BINANCE_CHAIN_ID is 97")
            && stderr.contains("is chain 56;"),
        "the error must name both the configured and the expected chain, \
         got: {stderr}"
    );
}

/// A lone `CHAIN_BASE_RPC_URL` must fail the group requirement rather than
/// silently falling back to the legacy flat Base variables.
#[test]
fn partial_base_environment_group_does_not_fall_back_to_legacy() {
    let output = legacy_base_command()
        .env("CHAIN_BASE_RPC_URL", "http://127.0.0.1:8545")
        .output()
        .unwrap();

    assert!(!output.status.success(), "partial Base config must fail");
    assert!(
        command_stderr(&output).contains("--chain-base-chain-id"),
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

/// The committed deploy configs, at the binary boundary the deployment unit
/// gates a restart on. `[wrapped_tokens.robinhood]` makes the Robinhood chain
/// group a hard startup requirement, so these two tests pin both sides of
/// that: valid with the group, rejected without it.
const DEPLOY_CONFIGS: [&str; 2] = ["config.prod.toml", "config.staging.toml"];

fn robinhood_command(config: &str) -> Command {
    let mut command = legacy_base_command();
    command
        .args(["--config", config])
        .env("CHAIN_ROBINHOOD_RPC_URL", "http://127.0.0.1:10545")
        .env("CHAIN_ROBINHOOD_CHAIN_ID", "4663")
        .env("CHAIN_ROBINHOOD_BACKFILL_START_BLOCK", "0");

    command
}

#[test]
fn deploy_configs_validate_with_the_robinhood_chain_group() {
    for config in DEPLOY_CONFIGS {
        let output = robinhood_command(config).output().unwrap();

        assert!(
            output.status.success(),
            "{config}: {}",
            command_stderr(&output)
        );
    }
}

/// Rolling the config out ahead of the `CHAIN_ROBINHOOD_*` deployment secrets
/// must abort the start rather than leave the listed wrappers unwatched,
/// which is the failure the watcher exists to prevent.
#[test]
fn deploy_configs_are_rejected_without_the_robinhood_chain_group() {
    for config in DEPLOY_CONFIGS {
        let output =
            legacy_base_command().args(["--config", config]).output().unwrap();

        assert!(
            !output.status.success(),
            "{config} must not validate without CHAIN_ROBINHOOD_*"
        );
        let stderr = command_stderr(&output);
        assert!(
            stderr.contains("[wrapped_tokens.robinhood] is configured")
                && stderr.contains("add the CHAIN_ROBINHOOD_* group"),
            "{config}: the error must name the missing chain group, got: \
             {stderr}"
        );
    }
}
