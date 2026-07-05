/// Rain metadata v1 CBOR encoding/decoding for receipt information.
///
/// Implements the format described in the Rain metadata v1 spec:
/// https://github.com/rainlanguage/specs/blob/main/metadata-v1.md
///
/// Format: 8-byte magic prefix followed by an RFC 8742 CBOR sequence of maps.
/// Each CBOR map uses integer keys: 0=payload, 1=magic, 2=content-type,
/// 3=content-encoding. The OA_SCHEMA key (a magic number, not a small integer)
/// optionally references the IPFS CID of the JSON schema for the payload.
///
/// The OffchainAsset magic numbers are used by the gildlab/SFT and h20.market
/// tokenization frontends for encoding receipt information in
/// `OffchainAssetReceiptVault` deposits and withdrawals.
use alloy::eips::BlockNumberOrTag;
use alloy::primitives::Address;
use alloy::providers::{DynProvider, Provider};
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use ciborium::value::Value;
use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::warn;

use crate::bindings::OffchainAssetReceiptVault::ReceiptVaultInformation;

/// 8-byte prefix for every Rain metadata v1 document.
const RAIN_META_DOCUMENT_V1: [u8; 8] =
    0xff_0a_89_c6_74_ee_78_74_u64.to_be_bytes();

/// Magic number for structured offchain asset data (receipt information).
const OA_STRUCTURE: u64 = 0xff_c4_7a_62_99_e8_a9_11;

/// CBOR map key indices per the Rain metadata v1 spec.
const KEY_PAYLOAD: u64 = 0;
const KEY_MAGIC: u64 = 1;
const KEY_CONTENT_TYPE: u64 = 2;
const KEY_CONTENT_ENCODING: u64 = 3;

/// Magic number used as a CBOR map key to reference the JSON schema (IPFS CID)
/// that the payload conforms to. Used by h20.market and gildlab/SFT frontends.
/// Reference: h20liquidity/sft-tokenisation `MAGIC_NUMBERS.OA_SCHEMA`
const OA_SCHEMA: u64 = 0xff_a8_e8_a9_b9_cf_4a_31;

/// Magic number identifying an OA hash list CBOR item in a `receiptVaultInformation`
/// document. Key 0 of this item contains the IPFS CID of the schema.
/// Reference: h20liquidity/sft-tokenisation `MAGIC_NUMBERS.OA_HASH_LIST`
const OA_HASH_LIST: u64 = 0xff_9f_ae_3c_c6_45_f4_63;

/// Maximum decompressed payload size (1 MB). Prevents memory exhaustion from
/// malformed or hostile deflate streams in untrusted on-chain receipt metadata.
const MAX_INFLATE_SIZE: usize = 1_024 * 1_024;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RainMetaError {
    #[error("CBOR serialization error: {0}")]
    CborSerialize(#[from] ciborium::ser::Error<std::io::Error>),

    #[error("CBOR deserialization error: {0}")]
    CborDeserialize(#[from] ciborium::de::Error<std::io::Error>),

    #[error("deflate compression failed: {0}")]
    Deflate(std::io::Error),

    #[error("inflate decompression failed: {0}")]
    Inflate(std::io::Error),

    #[error("missing CBOR map key {key} in rain meta item")]
    MissingKey { key: u64 },

    #[error("unexpected CBOR value type for key {key}")]
    UnexpectedValueType { key: u64 },

    #[error("expected CBOR map at root level")]
    ExpectedMap,

    #[error(
        "data too short for rain meta: expected at least 9 bytes, got {len}"
    )]
    TooShort { len: usize },

    #[error("missing rain meta v1 prefix")]
    InvalidPrefix,

    #[error(
        "unexpected magic number: expected OA_STRUCTURE ({expected:#018x}), \
         got {actual:#018x}"
    )]
    UnexpectedMagic { expected: u64, actual: u64 },

    #[error(
        "inflated payload exceeds maximum size of {max} bytes (got {actual})"
    )]
    PayloadTooLarge { max: usize, actual: usize },
}

/// Returns `true` if the bytes start with the Rain metadata v1 magic prefix.
pub(crate) fn is_rain_meta(data: &[u8]) -> bool {
    data.starts_with(&RAIN_META_DOCUMENT_V1)
}

/// Encodes a JSON payload as a Rain metadata v1 document with OA_STRUCTURE magic.
///
/// Format: rain meta prefix + CBOR map {0: deflated_json, 1: OA_STRUCTURE,
/// 2: "application/json", 3: "deflate", OA_SCHEMA: schema_hash (if provided)}
///
/// The optional `oa_schema` parameter is an IPFS CID string identifying the
/// JSON schema the payload conforms to. When present, it is included under the
/// `OA_SCHEMA` magic number key, matching the encoding used by h20.market.
pub(crate) fn encode_receipt_meta(
    json_bytes: &[u8],
    oa_schema: Option<&str>,
) -> Result<Vec<u8>, RainMetaError> {
    let deflated = deflate(json_bytes)?;

    let mut entries = vec![
        (Value::Integer(KEY_PAYLOAD.into()), Value::Bytes(deflated)),
        (Value::Integer(KEY_MAGIC.into()), Value::Integer(OA_STRUCTURE.into())),
        (
            Value::Integer(KEY_CONTENT_TYPE.into()),
            Value::Text("application/json".to_string()),
        ),
        (
            Value::Integer(KEY_CONTENT_ENCODING.into()),
            Value::Text("deflate".to_string()),
        ),
    ];

    if let Some(schema_hash) = oa_schema {
        entries.push((
            Value::Integer(OA_SCHEMA.into()),
            Value::Text(schema_hash.to_string()),
        ));
    }

    let cbor_map = Value::Map(entries);

    let mut result = RAIN_META_DOCUMENT_V1.to_vec();

    let mut cbor_bytes = Vec::new();
    ciborium::into_writer(&cbor_map, &mut cbor_bytes)?;

    result.extend_from_slice(&cbor_bytes);
    Ok(result)
}

/// Decodes a Rain metadata v1 document and extracts the JSON payload.
///
/// Validates the rain meta prefix and OA_STRUCTURE magic number, then strips
/// the 8-byte prefix, decodes the CBOR map, and extracts the payload.
/// Handles deflate decompression if content-encoding is "deflate".
pub(crate) fn decode_receipt_meta(
    data: &[u8],
) -> Result<Vec<u8>, RainMetaError> {
    if data.len() <= 8 {
        return Err(RainMetaError::TooShort { len: data.len() });
    }

    if data[..8] != RAIN_META_DOCUMENT_V1 {
        return Err(RainMetaError::InvalidPrefix);
    }

    let cbor_data = &data[8..];

    let value: Value = ciborium::from_reader(cbor_data)?;

    let Value::Map(ref map) = value else {
        return Err(RainMetaError::ExpectedMap);
    };

    let magic = extract_integer(map, KEY_MAGIC)?;
    if magic != OA_STRUCTURE {
        return Err(RainMetaError::UnexpectedMagic {
            expected: OA_STRUCTURE,
            actual: magic,
        });
    }

    let payload = extract_bytes(map, KEY_PAYLOAD)?;

    let encoding = extract_optional_text(map, KEY_CONTENT_ENCODING);
    let is_deflated = encoding.as_deref() == Some("deflate");

    if is_deflated { inflate(&payload) } else { Ok(payload) }
}

fn extract_bytes(
    map: &[(Value, Value)],
    key: u64,
) -> Result<Vec<u8>, RainMetaError> {
    let key_value = Value::Integer(key.into());

    for (map_key, map_val) in map {
        if map_key == &key_value {
            return match map_val {
                Value::Bytes(bytes) => Ok(bytes.clone()),
                _ => Err(RainMetaError::UnexpectedValueType { key }),
            };
        }
    }

    Err(RainMetaError::MissingKey { key })
}

fn extract_integer(
    map: &[(Value, Value)],
    key: u64,
) -> Result<u64, RainMetaError> {
    let key_value = Value::Integer(key.into());

    for (map_key, map_val) in map {
        if map_key == &key_value {
            return match map_val {
                Value::Integer(int_val) => {
                    let raw: i128 = (*int_val).into();
                    u64::try_from(raw)
                        .map_err(|_| RainMetaError::UnexpectedValueType { key })
                }
                _ => Err(RainMetaError::UnexpectedValueType { key }),
            };
        }
    }

    Err(RainMetaError::MissingKey { key })
}

fn extract_optional_text(map: &[(Value, Value)], key: u64) -> Option<String> {
    let key_value = Value::Integer(key.into());

    for (map_key, map_val) in map {
        if map_key == &key_value
            && let Value::Text(text) = map_val
        {
            return Some(text.clone());
        }
    }

    None
}

/// Compresses data using zlib format (matching pako.deflate() in the h20
/// frontend). The zlib wrapper adds a 2-byte header (78 9c) and 4-byte adler32
/// checksum, which is what h20's `pako.deflate()` produces by default.
fn deflate(data: &[u8]) -> Result<Vec<u8>, RainMetaError> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).map_err(RainMetaError::Deflate)?;
    encoder.finish().map_err(RainMetaError::Deflate)
}

fn inflate(data: &[u8]) -> Result<Vec<u8>, RainMetaError> {
    let mut decoder = ZlibDecoder::new(data).take(MAX_INFLATE_SIZE as u64 + 1);
    let mut result = Vec::new();
    decoder.read_to_end(&mut result).map_err(RainMetaError::Inflate)?;

    if result.len() > MAX_INFLATE_SIZE {
        return Err(RainMetaError::PayloadTooLarge {
            max: MAX_INFLATE_SIZE,
            actual: result.len(),
        });
    }

    Ok(result)
}

/// Resolves the OA schema hash (IPFS CID) for receipt metadata encoding from
/// the vault's own on-chain `ReceiptVaultInformation` events. The schema hash
/// rarely changes for a vault, so successful results are cached for the
/// lifetime of the process. Transient failures are NOT cached, allowing
/// retries on subsequent calls.
pub(crate) struct OaSchemaCache {
    mode: OaSchemaCacheMode,
    /// Only positive results (`Some(hash)`) are cached. Absent key means not
    /// yet fetched, last fetch failed, or vault had no schema (all retried).
    cache: Arc<RwLock<HashMap<Address, Option<String>>>>,
}

enum OaSchemaCacheMode {
    /// Reads the vault's latest `ReceiptVaultInformation` event through the
    /// chain's own RPC on cache miss.
    Live { provider: DynProvider },

    /// Returns a fixed schema hash for any vault. Used in tests to exercise
    /// the full CBOR encoding path without requiring a chain.
    #[cfg(test)]
    Fixed(String),
}

impl OaSchemaCache {
    pub(crate) fn new(provider: DynProvider) -> Self {
        Self {
            mode: OaSchemaCacheMode::Live { provider },
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a cache that returns a fixed schema hash for any vault address.
    #[cfg(test)]
    pub(crate) fn fixed(schema_hash: &str) -> Self {
        Self {
            mode: OaSchemaCacheMode::Fixed(schema_hash.to_string()),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns the OA schema hash for the given vault, or `None` if unavailable.
    ///
    /// Fetches from the chain on first call per vault, then caches the result.
    /// Returns `None` (and logs a warning) if the log query fails or the vault
    /// has no registered schema.
    ///
    /// Only successful responses are cached. Transient failures (network errors,
    /// timeouts) are NOT cached so subsequent calls will retry.
    pub(crate) async fn get(&self, vault: Address) -> Option<String> {
        match &self.mode {
            #[cfg(test)]
            OaSchemaCacheMode::Fixed(schema) => return Some(schema.clone()),
            OaSchemaCacheMode::Live { .. } => {}
        }

        {
            let cache = self.cache.read().await;

            if let Some(cached) = cache.get(&vault) {
                return cached.clone();
            }
        }

        let result = self.fetch_schema_hash(vault).await;

        match result {
            Ok(Some(schema_hash)) => {
                self.cache
                    .write()
                    .await
                    .insert(vault, Some(schema_hash.clone()));
                Some(schema_hash)
            }

            Ok(None) => {
                // Don't cache negative results -- the vault's information may
                // not have been registered yet. Retry on next call.
                None
            }

            Err(error) => {
                // Don't cache transient failures -- allow retry on next call
                warn!(
                    target: "vault",
                    %vault,
                    %error,
                    "failed to fetch OA schema hash from vault information \
                     events, receipt metadata will omit OA_SCHEMA"
                );
                None
            }
        }
    }

    /// Reads the vault's `ReceiptVaultInformation` events and extracts the
    /// schema IPFS CID from the most recent document.
    ///
    /// The event payload is a Rain meta v1 document containing a CBOR
    /// sequence. We look for an item with `OA_HASH_LIST` magic and extract
    /// key 0 (the IPFS CID string).
    async fn fetch_schema_hash(
        &self,
        vault: Address,
    ) -> Result<Option<String>, OaSchemaFetchError> {
        let provider = match &self.mode {
            OaSchemaCacheMode::Live { provider } => provider,
            // Fixed mode is handled in get() before reaching here
            #[cfg(test)]
            OaSchemaCacheMode::Fixed(_) => return Ok(None),
        };

        // Vault-scoped, single-topic query: a vault emits this event only
        // when its metadata is (re-)registered, so the full block range stays
        // a handful of logs.
        let filter = Filter::new()
            .address(vault)
            .event_signature(ReceiptVaultInformation::SIGNATURE_HASH)
            .from_block(0)
            .to_block(BlockNumberOrTag::Latest);

        let logs = provider.get_logs(&filter).await?;

        // Logs arrive in ascending block order; the last one is the current
        // registration, mirroring the latest-first ordering the old subgraph
        // query used.
        let Some(latest) = logs.last() else {
            return Ok(None);
        };

        let event = ReceiptVaultInformation::decode_log(&latest.inner)?;

        Ok(parse_schema_hash_from_information(&event.vaultInformation))
    }
}

#[derive(Debug, thiserror::Error)]
enum OaSchemaFetchError {
    #[error("vault information log query failed: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),

    #[error("failed to decode ReceiptVaultInformation event: {0}")]
    SolTypes(#[from] alloy::sol_types::Error),
}

/// Parses the schema IPFS CID from a `receiptVaultInformation` document.
///
/// The document is a Rain meta v1 CBOR sequence. We iterate the items looking
/// for one whose magic (key 1) equals `OA_HASH_LIST`, then extract key 0 as
/// the schema hash string. Malformed documents yield `None` rather than an
/// error: the payload is untrusted on-chain data, and an unparseable
/// registration is equivalent to no registration.
fn parse_schema_hash_from_information(raw_bytes: &[u8]) -> Option<String> {
    if raw_bytes.len() <= 8 || raw_bytes[..8] != RAIN_META_DOCUMENT_V1 {
        return None;
    }

    let cbor_data = &raw_bytes[8..];

    // The CBOR sequence contains multiple maps; iterate them all
    let mut cursor = std::io::Cursor::new(cbor_data);

    while usize::try_from(cursor.position()).unwrap_or(usize::MAX)
        < cbor_data.len()
    {
        let item: Value = match ciborium::from_reader(&mut cursor) {
            Ok(value) => value,
            Err(_) => break,
        };

        let Value::Map(ref map) = item else {
            continue;
        };

        let Ok(magic) = extract_integer(map, KEY_MAGIC) else {
            continue;
        };

        if magic == OA_HASH_LIST {
            // Key 0 contains the IPFS CID string. The comma check filters
            // out multi-hash entries (comma-separated lists), matching the
            // behavior of h20liquidity/sft-tokenisation's getAssetClasses.
            if let Some(hash) = extract_optional_text(map, KEY_PAYLOAD)
                && !hash.is_empty()
                && !hash.contains(',')
            {
                return Some(hash);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use alloy::hex;
    use alloy::network::EthereumWallet;
    use alloy::providers::ProviderBuilder;
    use alloy::signers::local::PrivateKeySigner;

    use super::*;
    use crate::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
    use crate::test_utils::LocalEvm;

    /// A realistic serialized `ReceiptInformation` with fixed values for
    /// deterministic cross-encoding tests against the h20 TypeScript frontend.
    const TEST_RECEIPT_JSON: &str = r#"{"tokenization_request_id":"tok-abc-123","issuer_request_id":"iss-def-456","underlying":"AAPL","quantity":"100.5","timestamp":"2024-01-15T09:30:00Z","notes":"Mint for AP account ac-789"}"#;

    const TEST_SCHEMA_HASH: &str =
        "bafkreicecnx2gvntm6fbcrvnc336qze6st5u7qq7457igegamd3bzkx7ri";

    #[test]
    fn encode_produces_rain_meta_prefix() {
        let json = br#"{"test": "value"}"#;
        let encoded = encode_receipt_meta(json, None).unwrap();

        assert!(is_rain_meta(&encoded));
        assert_eq!(&encoded[..8], &RAIN_META_DOCUMENT_V1);
    }

    #[test]
    fn encode_then_decode_roundtrips() {
        let json = TEST_RECEIPT_JSON.as_bytes();

        let encoded = encode_receipt_meta(json, None).unwrap();
        let decoded = decode_receipt_meta(&encoded).unwrap();

        assert_eq!(decoded, json);
    }

    #[test]
    fn decode_extracts_deflated_payload() {
        let json = br#"{"hello":"world"}"#;
        let encoded = encode_receipt_meta(json, None).unwrap();
        let decoded = decode_receipt_meta(&encoded).unwrap();

        let parsed: serde_json::Value =
            serde_json::from_slice(&decoded).unwrap();
        assert_eq!(parsed["hello"], "world");
    }

    #[test]
    fn is_rain_meta_rejects_json() {
        let json = br#"{"test": "value"}"#;
        assert!(!is_rain_meta(json));
    }

    #[test]
    fn is_rain_meta_rejects_empty() {
        assert!(!is_rain_meta(&[]));
    }

    #[test]
    fn is_rain_meta_rejects_short() {
        assert!(!is_rain_meta(&[0xff, 0x0a, 0x89]));
    }

    #[test]
    fn decode_rejects_empty_input() {
        let result = decode_receipt_meta(&[]);
        assert!(
            matches!(result, Err(RainMetaError::TooShort { len: 0 })),
            "expected TooShort error, got {result:?}"
        );
    }

    #[test]
    fn decode_rejects_prefix_only_input() {
        let result = decode_receipt_meta(&RAIN_META_DOCUMENT_V1);
        assert!(
            matches!(result, Err(RainMetaError::TooShort { len: 8 })),
            "expected TooShort error, got {result:?}"
        );
    }

    #[test]
    fn decode_rejects_wrong_prefix() {
        let data = vec![0x00; 9];
        let result = decode_receipt_meta(&data);
        assert!(
            matches!(result, Err(RainMetaError::InvalidPrefix)),
            "expected InvalidPrefix error, got {result:?}"
        );
    }

    #[test]
    fn decode_rejects_wrong_magic_number() {
        let wrong_magic: u64 = 0xff_00_00_00_00_00_00_01;
        let deflated = deflate(b"{}").unwrap();

        let cbor_map = Value::Map(vec![
            (Value::Integer(KEY_PAYLOAD.into()), Value::Bytes(deflated)),
            (
                Value::Integer(KEY_MAGIC.into()),
                Value::Integer(wrong_magic.into()),
            ),
            (
                Value::Integer(KEY_CONTENT_TYPE.into()),
                Value::Text("application/json".to_string()),
            ),
            (
                Value::Integer(KEY_CONTENT_ENCODING.into()),
                Value::Text("deflate".to_string()),
            ),
        ]);

        let mut data = RAIN_META_DOCUMENT_V1.to_vec();
        ciborium::into_writer(&cbor_map, &mut data).unwrap();

        let result = decode_receipt_meta(&data);
        assert!(
            matches!(
                result,
                Err(RainMetaError::UnexpectedMagic {
                    expected: OA_STRUCTURE,
                    actual,
                }) if actual == wrong_magic
            ),
            "expected UnexpectedMagic error, got {result:?}"
        );
    }

    #[test]
    fn deflate_inflate_roundtrips() {
        // Use a realistic receipt payload — large enough that zlib compression
        // (2-byte header + 4-byte checksum overhead) still produces smaller
        // output than the original.
        let original = TEST_RECEIPT_JSON.as_bytes();
        let compressed = deflate(original).unwrap();
        let decompressed = inflate(&compressed).unwrap();

        assert_eq!(decompressed, original);
        assert!(
            compressed.len() < original.len(),
            "compressed {} bytes should be smaller than original {} bytes",
            compressed.len(),
            original.len(),
        );
    }

    #[test]
    fn inflate_rejects_oversized_payload() {
        // Deflate a payload larger than MAX_INFLATE_SIZE
        let oversized = vec![0u8; MAX_INFLATE_SIZE + 1];
        let compressed = deflate(&oversized).unwrap();

        let result = inflate(&compressed);
        assert!(
            matches!(result, Err(RainMetaError::PayloadTooLarge { .. })),
            "expected PayloadTooLarge error, got {result:?}"
        );
    }

    #[test]
    fn encode_with_oa_schema_includes_schema_key() {
        let json = br#"{"hello":"world"}"#;
        let schema_hash =
            "bafkreicecnx2gvntm6fbcrvnc336qze6st5u7qq7457igegamd3bzkx7ri";

        let encoded = encode_receipt_meta(json, Some(schema_hash)).unwrap();

        // Verify it still decodes correctly (decode ignores unknown keys)
        let decoded = decode_receipt_meta(&encoded).unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&decoded).unwrap();
        assert_eq!(parsed["hello"], "world");

        // Verify the OA_SCHEMA key is present in the CBOR map
        let cbor_data = &encoded[8..];
        let value: Value = ciborium::from_reader(cbor_data).unwrap();
        let Value::Map(ref map) = value else {
            panic!("expected CBOR map");
        };

        let schema_value = extract_optional_text(map, OA_SCHEMA);
        assert_eq!(
            schema_value.as_deref(),
            Some(schema_hash),
            "OA_SCHEMA key should contain the schema hash"
        );
    }

    /// Real `receiptVaultInformation` payload emitted for the TESTSTOX vault
    /// (0x9b117137aa839b53fd1aaf2f92fc4d78087326a7) on Base. Contains two
    /// CBOR items: OA_SCHEMA definition + OA_HASH_LIST with the CID.
    const REAL_VAULT_INFORMATION_HEX: &str = "ff0a89c674ee7874a40058d3789c858eb14ec33010865fc53a3aa6a462a9e407600321d10d315ce34b73ad639bf37788a2be7beda620c482179f3ffffefccfe038278fd32b8e04167694d5bc77038d689ea0817c1bc1ceb0fa1e61504db66d8f3986f5021fa31c5a27d8eb7ab36d17f6505e2babbf59e389c20b293a54ac7c4a15c7fd913a2d67748e956340ff263191285306dba3cfd480d0d799851cd80f08b56569358dfbe8e1b381f42b3f2fd765bffbb30a8743c93bca9d70aa5f14fc7cf6ded4a889bdd1818cd67a70f9f1fe6bd8717722314bfc8fa5ac2bb25f75f0011bffa8e8a9b9cf4a3102706170706c69636174696f6e2f6a736f6e03676465666c617465a200783b6261666b7265696365636e783267766e746d3666626372766e63333336717a6536737435753771713734353769676567616d6433627a6b78377269011bff9fae3cc645f463";

    #[test]
    fn parse_schema_hash_from_real_vault_information() {
        let info_bytes = hex::decode(REAL_VAULT_INFORMATION_HEX).unwrap();

        let result = parse_schema_hash_from_information(&info_bytes);

        assert_eq!(
            result.as_deref(),
            Some("bafkreicecnx2gvntm6fbcrvnc336qze6st5u7qq7457igegamd3bzkx7ri"),
            "should extract the IPFS CID from the OA_HASH_LIST item"
        );
    }

    #[test]
    fn parse_schema_hash_returns_none_for_non_rain_meta() {
        let result =
            parse_schema_hash_from_information(&[0xde, 0xad, 0xbe, 0xef]);
        assert!(result.is_none());
    }

    #[test]
    fn parse_schema_hash_returns_none_for_empty_input() {
        let result = parse_schema_hash_from_information(&[]);
        assert!(result.is_none());
    }

    /// Verifies our decoder can read data encoded by the h20 TypeScript frontend.
    ///
    /// h20 hex was generated using h20's cbor-web + pako with `TEST_RECEIPT_JSON`
    /// as input. See: https://gist.github.com/JuaniRios/e5ce3242c04d1e96813c2507c2853847
    #[test]
    fn rust_can_decode_h20_encoded_receipt_with_schema() {
        let h20_hex = "ff0a89c674ee7874a500589a789c558c3b0ec2301005af12b9c6689d0f1077e9414a41451319c7412b884dec75111077672929dfcce8bd0585bbf3f83284c10fd12dd9251a7014fa67a4b95aa9ca4a6c04a6945dfc2f98c9d14db26e765c643fbaf858d1df58755d7f64b664e3096965a200b60d23c2990fccfc645642594b505235676875051ae0c2890fe412eb137a2aa6108bae2f8cb521f33456ee0fadf87c019db83a9a011bffc47a6299e8a91102706170706c69636174696f6e2f6a736f6e03676465666c6174651bffa8e8a9b9cf4a31783b6261666b7265696365636e783267766e746d3666626372766e63333336717a6536737435753771713734353769676567616d6433627a6b78377269";
        let h20_bytes = hex::decode(h20_hex).unwrap();

        let decoded = decode_receipt_meta(&h20_bytes).unwrap();

        assert_eq!(String::from_utf8_lossy(&decoded), TEST_RECEIPT_JSON,);
    }

    #[test]
    fn rust_can_decode_h20_encoded_receipt_without_schema() {
        let h20_hex = "ff0a89c674ee7874a400589a789c558c3b0ec2301005af12b9c6689d0f1077e9414a41451319c7412b884dec75111077672929dfcce8bd0585bbf3f83284c10fd12dd9251a7014fa67a4b95aa9ca4a6c04a6945dfc2f98c9d14db26e765c643fbaf858d1df58755d7f64b664e3096965a200b60d23c2990fccfc645642594b505235676875051ae0c2890fe412eb137a2aa6108bae2f8cb521f33456ee0fadf87c019db83a9a011bffc47a6299e8a91102706170706c69636174696f6e2f6a736f6e03676465666c617465";
        let h20_bytes = hex::decode(h20_hex).unwrap();

        let decoded = decode_receipt_meta(&h20_bytes).unwrap();

        assert_eq!(String::from_utf8_lossy(&decoded), TEST_RECEIPT_JSON,);
    }

    /// Verifies our Rust encoding produces structurally identical CBOR to h20:
    /// same map keys, magic numbers, content-type, content-encoding, and the
    /// decompressed payload matches. Only the compressed bytes may differ
    /// between deflate implementations (miniz_oxide in Rust vs pako in JS).
    #[test]
    fn rust_encoding_structurally_matches_h20() {
        let json = TEST_RECEIPT_JSON.as_bytes();

        let rust_encoded =
            encode_receipt_meta(json, Some(TEST_SCHEMA_HASH)).unwrap();

        // Verify prefix
        assert_eq!(&rust_encoded[..8], &RAIN_META_DOCUMENT_V1);

        // Decode CBOR and verify all non-payload fields match h20's encoding
        let cbor_data = &rust_encoded[8..];
        let value: Value = ciborium::from_reader(cbor_data).unwrap();
        let Value::Map(ref map) = value else {
            panic!("expected CBOR map");
        };

        assert_eq!(
            extract_integer(map, KEY_MAGIC).unwrap(),
            OA_STRUCTURE,
            "magic must be OA_STRUCTURE"
        );
        assert_eq!(
            extract_optional_text(map, KEY_CONTENT_TYPE).as_deref(),
            Some("application/json"),
        );
        assert_eq!(
            extract_optional_text(map, KEY_CONTENT_ENCODING).as_deref(),
            Some("deflate"),
        );
        assert_eq!(
            extract_optional_text(map, OA_SCHEMA).as_deref(),
            Some(TEST_SCHEMA_HASH),
        );

        // Verify decompressed payload matches input
        let decoded = decode_receipt_meta(&rust_encoded).unwrap();
        assert_eq!(decoded, json);
    }

    /// Decodes a real receipt minted via the h20.market frontend on the
    /// TESTSTOX3 vault (Polygon). The hex was fetched from the live Goldsky
    /// subgraph at `sft-offchainassetvaulttest-polygon/1.0.21`.
    ///
    /// This proves our decoder handles production h20-encoded receipts,
    /// including the two-item CBOR sequence (OA_STRUCTURE + OA_HASH_LIST).
    #[test]
    fn decode_real_h20_receipt_from_subgraph() {
        let real_hex = "ff0a89c674ee7874a40058b6789c45cd416e83301085e1ab545e7741c0e0901d4a9651942a2718c6638cc01e648c8aa97af7baab6cbf27fdef47ac91717a25d7f32c2ea27b3eef1f95f814c86e019f1ee028332ccb4c59234fe4afec63008c9dd681d635cfc52e159aa69065a5dad34955127bd56853ab960a85e7be35c5994ad398dca01d2df8e13ffbe85eb7eeebfd76e78133f760a640a3a964729ac96ad4566db5d79e93c3501ea3320eecf79cdc71c036adf690711cc23e80f8fd03ee7b467e011bffc47a6299e8a91102706170706c69636174696f6e2f6a736f6e03676465666c617465a200783b6261666b72656966796269766e6779676232626862727532347567786f66737662776f6b6f763333666d747270796d647077356c696f6765326465011bff9fae3cc645f463";
        let real_bytes = hex::decode(real_hex).unwrap();

        let decoded_json = decode_receipt_meta(&real_bytes).unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&decoded_json).unwrap();

        assert_eq!(parsed["stockSymbol"], "APPL 3");
        assert_eq!(parsed["companyName"], "apple");
        assert_eq!(parsed["exchange"], "NASDAQ");
        assert_eq!(
            parsed["tokenContractAddress"],
            "0x47cf604237911734cb76df579e07c8b9f08e2f6f"
        );
    }

    /// End-to-end test of the full OA schema resolution + encoding pipeline
    /// against a local Anvil chain.
    ///
    /// 1. Deploys a vault and registers real vault information on-chain
    /// 2. Calls `get()` -- verifies the schema hash is read from the
    ///    `ReceiptVaultInformation` event via the chain provider
    /// 3. Encodes a receipt with that schema hash
    /// 4. Decodes the result and verifies roundtrip correctness
    #[tokio::test]
    async fn fetch_schema_and_encode_receipt_via_onchain_event() {
        let evm = LocalEvm::new().await.unwrap();

        let signer = PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let vault_instance = OffchainAssetReceiptVaultInstance::new(
            evm.vault_address,
            provider.clone(),
        );

        let info_bytes = hex::decode(REAL_VAULT_INFORMATION_HEX).unwrap();
        vault_instance
            .receiptVaultInformation(info_bytes.into())
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let cache = OaSchemaCache::new(provider.erased());

        // Step 1: Read the schema hash from the on-chain event
        let schema_hash = cache.get(evm.vault_address).await.expect(
            "schema hash should be read from the ReceiptVaultInformation \
             event",
        );
        assert_eq!(
            schema_hash,
            "bafkreicecnx2gvntm6fbcrvnc336qze6st5u7qq7457igegamd3bzkx7ri",
            "should extract the CID registered on-chain"
        );

        // Step 2: Encode a receipt using the fetched schema hash
        let json = TEST_RECEIPT_JSON.as_bytes();
        let encoded = encode_receipt_meta(json, Some(&schema_hash)).unwrap();

        // Step 3: Verify the output is valid Rain meta with correct structure
        assert!(is_rain_meta(&encoded));

        let cbor_data = &encoded[8..];
        let value: Value = ciborium::from_reader(cbor_data).unwrap();
        let Value::Map(ref map) = value else {
            panic!("expected CBOR map");
        };

        assert_eq!(
            extract_optional_text(map, OA_SCHEMA).as_deref(),
            Some(schema_hash.as_str()),
            "encoded OA_SCHEMA should match the hash from the chain"
        );

        // Step 4: Decode and verify roundtrip
        let decoded = decode_receipt_meta(&encoded).unwrap();
        assert_eq!(decoded, json);
    }

    #[test]
    fn encode_without_oa_schema_omits_schema_key() {
        let json = br#"{"hello":"world"}"#;

        let encoded = encode_receipt_meta(json, None).unwrap();

        let cbor_data = &encoded[8..];
        let value: Value = ciborium::from_reader(cbor_data).unwrap();
        let Value::Map(ref map) = value else {
            panic!("expected CBOR map");
        };

        let schema_value = extract_optional_text(map, OA_SCHEMA);
        assert!(
            schema_value.is_none(),
            "OA_SCHEMA key should not be present when no schema hash provided"
        );
    }
}
