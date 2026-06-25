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
