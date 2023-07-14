use std::fs::File;
use std::path::Path;
use std::process::ExitCode;

use anyhow::{Context, Result};
use tracing::error;

use netadmin_minion::{log::Log, Minion, MinionConfig};

#[tokio::main]
async fn main() -> ExitCode {
    let config = Path::new("resources/minion.yaml");

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
            ExitCode::FAILURE
        }
    }
}

async fn run(config_path: &Path, log: &mut Log) -> Result<()> {
    let mut config: MinionConfig = {
        let config_file = File::open(config_path).context("Failed to open config file")?;
        serde_yaml::from_reader(&config_file).context("Failed to parse config file")?
    };
    config.resolve_paths(config_path.parent().expect("Has parent path"));

    for handle in Minion::create_and_serve(&config, log).await? {
        handle.await?;
    }
    Ok(())
}
