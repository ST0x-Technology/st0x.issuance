//! Renders one JSON document to stdout as a single compact line.

use std::io::Write;

#[derive(Debug, thiserror::Error)]
pub(crate) enum OutputError {
    #[error("the response could not be re-encoded as JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("could not write output: {0}")]
    Io(#[from] std::io::Error),
}

/// Writes one JSON document to stdout as a single compact line, so output is
/// stable and machine-readable.
pub(crate) fn print(value: &serde_json::Value) -> Result<(), OutputError> {
    write_json(&mut std::io::stdout().lock(), value)
}

/// Renders `value` to `writer` as a single compact line, then flushes. Split
/// from `print` so write and flush failures are testable without stdout.
fn write_json<W: Write>(
    writer: &mut W,
    value: &serde_json::Value,
) -> Result<(), OutputError> {
    let rendered = serde_json::to_string(value)?;
    writeln!(writer, "{rendered}")?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{OutputError, write_json};

    #[test]
    fn writes_compact_json_with_a_trailing_newline() {
        let mut buffer = Vec::new();

        write_json(&mut buffer, &serde_json::json!({ "a": 1, "b": [2, 3] }))
            .unwrap();

        assert_eq!(buffer, b"{\"a\":1,\"b\":[2,3]}\n");
    }

    struct FailWriter;

    impl std::io::Write for FailWriter {
        fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("write refused"))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::other("flush refused"))
        }
    }

    #[test]
    fn propagates_write_failures() {
        let result =
            write_json(&mut FailWriter, &serde_json::json!({ "a": 1 }));

        assert!(matches!(result, Err(OutputError::Io(_))));
    }
}
