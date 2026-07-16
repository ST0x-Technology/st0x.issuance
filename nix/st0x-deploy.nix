{ mkSoldeerAbi, src }:

let
  abi = mkSoldeerAbi {
    pname = "st0x-deploy-abis";
    inherit src;
    soldeerDepsHash = "sha256-CoY0rhBAA4Y1PdXzvBKyjfPRUV8+sl9Ydaz0iZrtTSU=";
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
    IST0X_ORCHESTRATOR_V1_ABI = "${abi}/out/IST0xOrchestratorV1.sol/IST0xOrchestratorV1.json";
    ST0X_ORCHESTRATOR_ABI = "${abi}/out/ST0xOrchestrator.sol/ST0xOrchestrator.json";
    ST0X_UPGRADEABLE_BEACON_ABI = "${abi}/out/UpgradeableBeacon.sol/UpgradeableBeacon.json";
    ST0X_BEACON_PROXY_ABI = "${abi}/out/BeaconProxy.sol/BeaconProxy.json";
    ST0X_STOX_RECEIPT_ABI = "${abi}/out/StoxReceipt.sol/StoxReceipt.json";
    ST0X_STOX_RECEIPT_VAULT_ABI = "${abi}/out/StoxReceiptVault.sol/StoxReceiptVault.json";
    ST0X_STOX_OARV_BEACON_SET_DEPLOYER_ABI = "${abi}/out/StoxOffchainAssetReceiptVaultBeaconSetDeployer.sol/StoxOffchainAssetReceiptVaultBeaconSetDeployer.json";
  };
}
