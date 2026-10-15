//! Binary entrypoint for `st0x-issuance-client`; the library runs the parsed
//! command.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    st0x_issuance_ops::run().await
}
