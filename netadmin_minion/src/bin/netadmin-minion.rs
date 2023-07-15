use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;
use tracing::error;

use netadmin_minion::{log::Log, Minion, MinionConfig};

#[derive(Parser)]
#[command(author, version, about, long_about = "NetAdmin certificate generator")]
struct Cli {
    /// Configuration file location
    #[clap(
        short,
        long,
        value_name = "FILE",
        default_value = "resources/netadmin-minion.yaml"
    )]
    config: PathBuf,
}

#[tokio::main]
#[allow(clippy::print_stderr)]
async fn main() -> ExitCode {
    let mut log = Log::local();
    let cli = Cli::parse();

    match run(&cli.config, &mut log).await {
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

async fn run(config_path: &Path, log: &mut Log) -> Result<()> {
    let mut config: MinionConfig = Log::load_config(config_path)?;
    config.resolve_paths(config_path.parent().expect("Has parent path"));

    for handle in Minion::create_and_serve(&config, log)? {
        handle.await?;
    }
    Ok(())
}
