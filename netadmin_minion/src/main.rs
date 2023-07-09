use std::net::{IpAddr, SocketAddr};
use core::str::FromStr;

use anyhow::Result;

use netadmin_minion::Minion;

#[tokio::main]
#[allow(clippy::unwrap_used)]
async fn main() -> Result<()> {
    let minion = Minion::new("test_minion");
    Box::pin(minion.serve(&SocketAddr::new(IpAddr::from_str("0.0.0.0").unwrap(), 6236))).await
}
