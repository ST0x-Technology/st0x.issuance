use alloy::sol;

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    OffchainAssetReceiptVault,
    env!("ST0X_OFFCHAIN_ASSET_RECEIPT_VAULT_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    Receipt,
    env!("ST0X_RECEIPT_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    CloneFactory,
    env!("ST0X_CLONE_FACTORY_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    OffchainAssetReceiptVaultAuthorizerV1,
    env!("ST0X_OFFCHAIN_ASSET_RECEIPT_VAULT_AUTHORIZER_V1_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IST0xOrchestratorV1,
    env!("IST0X_ORCHESTRATOR_V1_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    ST0xOrchestrator,
    env!("ST0X_ORCHESTRATOR_ABI")
);
