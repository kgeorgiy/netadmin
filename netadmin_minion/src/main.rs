use core::str::FromStr;
use std::net::SocketAddr;
use std::path::Path;

use anyhow::Result;
use tokio::join;

use netadmin_minion::{net::TlsServerConfig, Minion};

#[tokio::main]
#[allow(clippy::unwrap_used)]
async fn main() -> Result<()> {
    let minion = Minion::new("test_minion");
    let tls_config = &TlsServerConfig::new(
        Path::new("__keys/minion.netadmin.test.key"),
        Path::new("__keys/minion.netadmin.test.crt"),
        Some(Path::new("__keys/client.netadmin.test.crt")),
    );

    let _handles = join!(
        minion
            .serve_tls(&SocketAddr::from_str("0.0.0.0:6236").unwrap(), tls_config)
            .await?,
        minion
            .serve_legacy(&SocketAddr::from_str("0.0.0.0:12345").unwrap(), tls_config)
            .await?,
    );
    Ok(())
}
