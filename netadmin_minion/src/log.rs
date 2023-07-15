use core::{fmt::Debug, future::Future, str};
use std::env;
use std::fs::File;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use anyhow::{Context, Error, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing::{
    info, level_filters::LevelFilter, subscriber, subscriber::DefaultGuard, warn, Instrument,
    Level, Span,
};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_appender::rolling::RollingFileAppender;
use tracing_subscriber::fmt::{
    format::{DefaultFields, Format},
    Subscriber,
};

#[must_use]
#[non_exhaustive]
#[derive(Serialize, Deserialize, Debug)]
pub enum LevelFilterConfig {
    #[serde(alias = "all")]
    All,
    #[serde(alias = "trace")]
    Trace,
    #[serde(alias = "debug")]
    Debug,
    #[serde(alias = "off")]
    Info,
    #[serde(alias = "warn")]
    Warn,
    #[serde(alias = "error")]
    Error,
    #[serde(alias = "off")]
    Off,
}

impl LevelFilterConfig {
    #[must_use]
    pub fn get(&self) -> LevelFilter {
        match *self {
            LevelFilterConfig::All | LevelFilterConfig::Trace => LevelFilter::TRACE,
            LevelFilterConfig::Debug => LevelFilter::DEBUG,
            LevelFilterConfig::Info => LevelFilter::INFO,
            LevelFilterConfig::Warn => LevelFilter::WARN,
            LevelFilterConfig::Error => LevelFilter::ERROR,
            LevelFilterConfig::Off => LevelFilter::OFF,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogConfig {
    file: PathBuf,
    level: LevelFilterConfig,
}

impl LogConfig {
    /// Creates a new subscriber
    ///
    /// # Errors
    /// - Log path does not have either directory of file name
    pub fn subscriber(
        &self,
    ) -> Result<(
        WorkerGuard,
        Subscriber<DefaultFields, Format, LevelFilter, NonBlocking>,
    )> {
        let file_appender: RollingFileAppender = tracing_appender::rolling::daily(
            self.file.parent().context("log file parent exits")?,
            self.file.file_name().context("log file name exits")?,
        );
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(self.level.get())
            .with_target(false)
            .with_writer(non_blocking)
            .with_ansi(false)
            .finish();
        info!("Logging to {:?}", self.file);
        Ok((guard, subscriber))
    }
}

enum LogState {
    Local(DefaultGuard),
    Global(WorkerGuard),
}

#[must_use]
pub struct Log(Option<LogState>);

impl Log {
    /// Creates a new thread-local log
    pub fn local() -> Log {
        Log(Some(LogState::Local(subscriber::set_default(
            tracing_subscriber::fmt()
                .with_max_level(Level::INFO)
                .with_target(false)
                .with_ansi(std::io::stdout().is_terminal())
                .finish(),
        ))))
    }

    /// Sets global log configuration
    ///
    /// # Errors
    /// - Same as in [`LogConfig::subscriber`]
    #[allow(clippy::unwrap_in_result)]
    pub fn set_global(&mut self, config: &LogConfig) -> Result<()> {
        self.0 = Some(match self.0.take().expect("impossible") {
            LogState::Local(local_guard) => {
                let (global_guard, subscriber) = config.subscriber()?;
                drop(local_guard);
                subscriber::set_global_default(subscriber)?;
                LogState::Global(global_guard)
            }
            state @ LogState::Global(_) => state,
        });
        Ok(())
    }

    /// Runs `future` within `span` and logs its result
    ///
    /// # Errors
    /// Original `future` errors
    pub async fn report<T: Debug, F>(span: Span, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>> + Send + 'static,
    {
        async {
            let result = future.await;
            Self::log_result(&result);
            result
        }
        .instrument(span)
        .await
    }

    pub fn log_result<T: Debug>(result: &Result<T>) {
        match *result {
            Ok(ref value) => {
                let value = format!("{value:?}");
                if value == "()" {
                    info!("Ok");
                } else {
                    info!("Ok {value}");
                }
            }
            Err(ref error) => Self::log_error(error),
        };
    }

    pub fn log_error(error: &Error) {
        let causes: String = error
            .chain()
            .skip(1)
            .map(|err| format!("\n    Caused by: {err}"))
            .collect();
        warn!("Error {error}{causes}");
    }

    pub fn spawn<T, F>(span: Span, future: F) -> JoinHandle<()>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Debug + 'static,
    {
        tokio::spawn(async {
            Self::report(span, future).await.map_or((), |_| ());
        })
    }

    #[must_use]
    pub fn str_or_bin(data: &[u8]) -> String {
        str::from_utf8(data).map_or_else(|_| format!("{data:?}"), |s| format!("\"{}\"", s.trim()))
    }

    /// Load main configuration file
    ///
    /// # Errors
    /// - Specified path does not exists
    /// - Configuration format error
    pub fn load_config<C: DeserializeOwned>(path: &Path) -> Result<C> {
        let path = &env::current_dir()?.join(path);
        info!("Using configuration file {path:?}");
        let config_file = File::open(path).context("Failed to open configuration file")?;
        serde_yaml::from_reader::<_, C>(&config_file).context("Failed to parse configuration file")
    }
}
