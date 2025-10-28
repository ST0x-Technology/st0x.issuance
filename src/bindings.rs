use alloy::sol;

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    OffchainAssetReceiptVault,
    "lib/ethgild/out/OffchainAssetReceiptVault.sol/OffchainAssetReceiptVault.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    Receipt,
    "lib/ethgild/out/Receipt.sol/Receipt.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    CloneFactory,
    "lib/ethgild/out/CloneFactory.sol/CloneFactory.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    OffchainAssetReceiptVaultAuthorizerV1,
    "lib/ethgild/out/OffchainAssetReceiptVaultAuthorizerV1.sol/OffchainAssetReceiptVaultAuthorizerV1.json"
);
