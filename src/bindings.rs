use alloy::sol;

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    OffchainAssetReceiptVault,
    "lib/ethgild/out/OffchainAssetReceiptVault.sol/OffchainAssetReceiptVault.json"
);
