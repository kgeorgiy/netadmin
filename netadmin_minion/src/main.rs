use core::str::FromStr;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;

use anyhow::Result;

use netadmin_minion::{Minion, TlsConfig};

#[tokio::main]
#[allow(clippy::unwrap_used)]
async fn main() -> Result<()> {
    let minion = Minion::new("test_minion".to_owned());
    let handle = minion
        .serve_tls(
            &SocketAddr::new(IpAddr::from_str("0.0.0.0").unwrap(), 6236),
            &TlsConfig {
                minion_certificates: Path::new("__keys/minion.netadmin.test.crt"),
                minion_key: Path::new("__keys/minion.netadmin.test.key"),
                client_certificates: Some(Path::new("__keys/client.netadmin.test.crt")),
            },
        )
        .await?;
    Ok(handle.await?)
}
