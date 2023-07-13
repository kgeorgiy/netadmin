use std::fs::File;
use std::path::Path;
use std::process::ExitCode;

use anyhow::{Context, Result};

use netadmin_minion::{Minion, MinionConfig};

#[tokio::main]
#[allow(clippy::unwrap_used, clippy::print_stderr)]
async fn main() -> ExitCode {
    let config = Path::new("resources/minion.yaml");

    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let causes: String = error
                .chain()
                .skip(1)
                .map(|err| format!("\n    Caused by: {err}"))
                .collect();
            eprintln!("Minion error: {error}{causes}");
            ExitCode::FAILURE
        }
    }
}

async fn run(config_path: &Path) -> Result<()> {
    let mut config: MinionConfig = {
        let config_file = File::open(config_path).context("Failed to open config file")?;
        serde_yaml::from_reader(&config_file).context("Failed to parse config file")?
    };
    config.resolve_paths(config_path.parent().expect("Has parent path"));

    let handles = Minion::create_and_serve(&config).await?;
    for handle in handles {
        handle.await?;
    }
    Ok(())
}
