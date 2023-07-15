use std::path::Path;
use std::process::ExitCode;

use anyhow::Result;
use tracing::error;

use netadmin_minion::{log::Log, Minion, MinionConfig};

#[tokio::main]
#[allow(clippy::print_stderr)]
async fn main() -> ExitCode {
    let config = Path::new("resources/netadmin-minion.yaml");

    let mut log = Log::local();

    match run(config, &mut log).await {
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

    for handle in Minion::create_and_serve(&config, log).await? {
        handle.await?;
    }
    Ok(())
}
