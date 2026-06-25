{ mkAbi, src }:

let
  abi = mkAbi {
    pname = "ethgild-abis";
    inherit src;
    installPhase = ''
      runHook preInstall
      mkdir -p $out
      cp -r out $out/out
      runHook postInstall
    '';
  };
in
{
  inherit abi;
  abiEnv = {
    ST0X_OFFCHAIN_ASSET_RECEIPT_VAULT_ABI = "${abi}/out/OffchainAssetReceiptVault.sol/OffchainAssetReceiptVault.json";
    ST0X_RECEIPT_ABI = "${abi}/out/Receipt.sol/Receipt.json";
    ST0X_CLONE_FACTORY_ABI = "${abi}/out/CloneFactory.sol/CloneFactory.json";
    ST0X_OFFCHAIN_ASSET_RECEIPT_VAULT_AUTHORIZER_V1_ABI = "${abi}/out/OffchainAssetReceiptVaultAuthorizerV1.sol/OffchainAssetReceiptVaultAuthorizerV1.json";
  };
}
