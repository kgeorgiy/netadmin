#![allow(clippy::print_stdout)]

use core::fmt::Debug;
use std::env;
use std::net::SocketAddr;
use std::str;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;

#[derive(Serialize, Deserialize, Debug)]
#[must_use]
pub struct MinionResponse {
    minion_id: String,
    request_id: String,
    os: String,
    time_utc: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug)]
#[must_use]
pub struct MinionRequest {
    request_id: String,
}

impl MinionRequest {
    pub fn new(id: &str) -> MinionRequest {
        MinionRequest { request_id: id.to_owned() }
    }
}

#[must_use]
pub struct Minion {
    id: String,
}

impl Minion {
    #[must_use]
    pub fn new(id: &str) -> Arc<Minion> {
        Arc::new(Minion { id: id.to_owned() })
    }

    fn response(&self, request: &MinionRequest) -> MinionResponse {
        let id = self.id.clone();
        MinionResponse {
            minion_id: id,
            request_id: request.request_id.clone(),
            os: env::consts::OS.to_owned(),
            time_utc: Utc::now(),
        }
    }

    /// Serves minion information on UDP port
    ///
    /// # Errors
    /// Returns error if cannot bind to specified [`socket_address`]
    pub async fn serve(self: &Arc<Self>, socket_address: &SocketAddr) -> Result<()> {
        const MAX_PACKET_SIZE: usize = 0x1_0000;

        let mut buffer = [0; MAX_PACKET_SIZE];
        let socket = Arc::new(UdpSocket::bind(socket_address).await?);
        loop {
            let (len, addr) = socket.recv_from(&mut buffer).await?;
            let socket = Arc::clone(&socket);
            let this = Arc::clone(self);
            tokio::spawn(async move {
                #[allow(clippy::indexing_slicing)]
                if let Ok(request) = MinionRequest::from_bytes(&buffer[0..len]) {
                    let response = this.response(&request);
                    socket.send_to(&response.to_json().into_bytes(), addr).await
                } else {
                    // Invalid request
                    Ok(0)
                }
            });
        }
    }
}

pub trait Jsonable: Sized {
    /// Converts this value to JSON string
    fn to_json(&self) -> String;

    /// Restores value from JSON string
    //
    /// # Errors
    /// Returns error if
    /// - input is not valid JSON
    /// - input is not generated by [`Self::to_json`]
    fn from_json(json: &str) -> Result<Self>;

    /// Restores value from UTF-8 encoded JSON string
    //
    /// # Errors
    //
    /// Returns error if
    /// - input is not valid UTF-8 string
    /// - input is not valid JSON
    /// - input is not generated by [`Self::to_json`]
    fn from_bytes(json: &[u8]) -> Result<Self>;
}

impl<T> Jsonable for T where T: Serialize + for<'a> Deserialize<'a> {
    fn to_json(&self) -> String {
        serde_json::to_string(self).expect("never happens")
    }

    fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).context("Invalid JSON")
    }

    fn from_bytes(json: &[u8]) -> Result<Self> {
        Self::from_json(str::from_utf8(json).context("Invalid UTF-8")?)
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::net::{IpAddr, SocketAddr};
    use std::pin::Pin;
    use std::str::FromStr;

    use tokio::net::UdpSocket;

    use super::*;

    #[test]
    fn local_minion() {
        let minion_id = "test_minion";
        let minion = Minion::new(&minion_id);
        let request = MinionRequest::new(&"123");
        let response = minion.response(&request);

        check_response(minion_id, &request, &response);
    }

    #[test]
    fn serde() {
        let minion_id = "test_minion";
        let minion = Minion::new(minion_id);
        let request = MinionRequest::new(&"123");

        let serialized = minion.response(&request).to_json();
        println!("serialized = {}", serialized);
        let response = MinionResponse::from_json(&serialized).unwrap();
        println!("deserialized = {:?}", response);

        check_response(minion_id, &request, &response);
    }

    fn check_response(minion_name: &str, request: &MinionRequest, response: &MinionResponse) {
        assert_eq!(minion_name, response.minion_id);
        assert_eq!(request.request_id, response.request_id);
    }


    #[tokio::test]
    async fn test_udp() -> Result<()> {
        let server_address = SocketAddr::new(IpAddr::from_str("127.0.0.2").unwrap(), 16236);
        let minion_id = "test_minion";
        tokio::spawn(async move {
            Minion::new(minion_id).serve(&server_address).await
        });

        const TOTAL: usize = 5;
        (0..TOTAL).fold(
            Box::pin(async { Ok(()) as Result<()> }) as Pin<Box<dyn Future<Output = Result<()>>>>,
            |prev, i| Box::pin(async move {
                let next = async move {
                    let socket = UdpSocket::bind("0.0.0.0:0").await?;

                    let request = MinionRequest::new(&"12345");
                    socket.send_to(request.to_json().as_bytes(), &server_address).await?;
                    println!("Request {} sent", i + 1);

                    let mut buf = [0; 1024];
                    let (len, addr) = socket.recv_from(&mut buf).await?;

                    assert_eq!(server_address, addr);

                    let response = MinionResponse::from_bytes(&buf[0..len]).unwrap();

                    check_response(minion_id, &request, &response);
                    println!("Request {} OK", i + 1);
                    Ok(())
                };
                tokio::try_join!(prev, next).map(|_| ())
            })).await
    }
}
