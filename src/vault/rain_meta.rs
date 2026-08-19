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
use ciborium::value::Value;
use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use std::io::{Read, Write};

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
/// 2: "application/json", 3: "deflate"}.
pub(crate) fn encode_receipt_meta(
    json_bytes: &[u8],
) -> Result<Vec<u8>, RainMetaError> {
    let deflated = deflate(json_bytes)?;

    let entries = vec![
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A realistic serialized `ReceiptInformation` with fixed values for
    /// deterministic cross-encoding tests against the h20 TypeScript frontend.
    const TEST_RECEIPT_JSON: &str = r#"{"tokenization_request_id":"tok-abc-123","issuer_request_id":"iss-def-456","underlying":"AAPL","quantity":"100.5","timestamp":"2024-01-15T09:30:00Z","notes":"Mint for AP account ac-789"}"#;

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
    fn encode_omits_oa_schema_key() {
        const OA_SCHEMA_MAGIC: u64 = 0xff_a8_e8_a9_b9_cf_4a_31;
        let json = br#"{"hello":"world"}"#;

        let encoded = encode_receipt_meta(json).unwrap();

        let value: Value = ciborium::from_reader(&encoded[8..]).unwrap();
        let Value::Map(map) = value else {
            panic!("expected CBOR map");
        };
        let has_schema_key = map
            .iter()
            .any(|(key, _)| key == &Value::Integer(OA_SCHEMA_MAGIC.into()));
        assert!(!has_schema_key, "receipts must not carry the OA_SCHEMA key");
    }
}
