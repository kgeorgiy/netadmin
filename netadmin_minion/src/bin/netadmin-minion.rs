use std::process::ExitCode;

use clap::Parser;
use tracing::error;

use netadmin_minion::{log::Log, minion_main::Cli};

#[tokio::main]
#[allow(clippy::print_stderr)]
async fn main() -> ExitCode {
    let log = Log::local();
    let cli = Cli::parse();

    match cli.run(log).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let causes: String = error
                .chain()
                .skip(1)
                .map(|err| format!("\n    Caused by: {err}"))
                .collect();
            error!("error: {error}{causes}");
            eprintln!("error: {error}{causes}");
            ExitCode::FAILURE
        }
    }
}
