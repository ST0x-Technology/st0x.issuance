use crate::wallet::{ResolvedSigner, SignerResolveError, WalletKind};
use alloy::network::EthereumWallet;
use alloy::primitives::B256;
use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;

/// Resolve a local private key into a wallet.
///
/// The chain_id is set on the signer for transaction signing.
pub(crate) fn resolve_local_signer(
    key: &B256,
    chain_id: u64,
) -> Result<ResolvedSigner, SignerResolveError> {
    let mut signer = PrivateKeySigner::from_bytes(key)?;
    signer.set_chain_id(Some(chain_id));
    let wallet = EthereumWallet::from(signer);
    Ok(ResolvedSigner { wallet, kind: WalletKind::Local })
}
