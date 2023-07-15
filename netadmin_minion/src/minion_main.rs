use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::task::JoinHandle;

use crate::{log::Log, Minion, MinionConfig};

#[derive(Parser)]
#[command(author, version, about, long_about = "NetAdmin Minion")]
pub struct Cli {
    /// Configuration file location
    #[clap(
        short,
        long,
        value_name = "FILE",
        default_value = "resources/netadmin-minion.yaml"
    )]
    config: PathBuf,
}

impl Cli {
    /// Creates minion and serve until all services done
    ///
    /// # Errors
    /// - Configuration error
    /// - Service aborted
    pub async fn run(&self, mut log: Log) -> Result<()> {
        Self::wait(self.create_and_serve(&mut log)?).await
    }

    /// Creates and serve minion
    ///
    /// # Errors
    /// - Configuration error
    pub fn create_and_serve(&self, log: &mut Log) -> Result<Vec<JoinHandle<()>>> {
        let config_path = &self.config;
        let mut config: MinionConfig = Log::load_config(config_path)?;
        config.resolve_paths(config_path.parent().context("Has parent path")?);

        Minion::create_and_serve(&config, log)
    }

    /// Waits for all handles to finish
    ///
    /// # Errors
    /// - Handle aborted
    pub async fn wait(handles: Vec<JoinHandle<()>>) -> Result<()> {
        for handle in handles {
            handle.await?;
        }
        Ok(())
    }
}
