use std::path::PathBuf;
use std::process::ExitCode;

/// Exports the issuance API TypeScript bindings for the dashboard.
///
/// Usage: `st0x-issuance-dto <OUT_DIR>`. Kept dependency-free (no clap) so the
/// `st0x-issuance-dto` library stays minimal for external consumers — see the
/// crate docs.
fn main() -> ExitCode {
    let Some(out_dir) = std::env::args_os().nth(1) else {
        eprintln!("usage: st0x-issuance-dto <OUT_DIR>");
        return ExitCode::FAILURE;
    };

    match st0x_issuance_dto::export_bindings(&PathBuf::from(out_dir)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("failed to export TypeScript bindings: {err}");
            ExitCode::FAILURE
        }
    }
}
