use alloy::sol;

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    OffchainAssetReceiptVault,
    "lib/ethgild/out/OffchainAssetReceiptVault.sol/OffchainAssetReceiptVault.json"
);
