use core::fmt::Debug;
use std::env;
use std::net::SocketAddr;
use std::path::Path;
use std::str;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;
use tokio_rustls::rustls::{Certificate, PrivateKey, ServerConfig};
use tokio_rustls::TlsAcceptor;

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
    pub fn new(id: String) -> MinionRequest {
        MinionRequest { request_id: id }
    }
}

#[must_use]
pub struct Minion {
    id: String,
}

impl Minion {
    pub const TLS_DOMAIN: &'static str = "netadmin_minion";

    const MAX_REQUEST_SIZE: usize = 0x100;

    #[must_use]
    pub fn new(id: String) -> Arc<Minion> {
        Arc::new(Minion { id })
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
    pub async fn serve_udp(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
    ) -> Result<JoinHandle<()>> {
        let socket = Arc::new(UdpSocket::bind(socket_address).await?);
        let this = Arc::clone(self);
        Ok(tokio::spawn(async move {
            let mut buffer = [0; Self::MAX_REQUEST_SIZE];
            while let Ok((len, addr)) = socket.recv_from(&mut buffer).await {
                let socket = Arc::clone(&socket);
                let this = Arc::clone(&this);
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
        }))
    }

    /// Serves minion information on TCP port
    ///
    /// # Errors
    /// Returns error if cannot bind to specified [`socket_address`]
    pub async fn serve_tcp(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
    ) -> Result<JoinHandle<()>> {
        let listener = Arc::new(TcpListener::bind(socket_address).await?);
        let this = Arc::clone(self);
        Ok(tokio::spawn(async move {
            loop {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let this = Arc::clone(&this);
                    tokio::spawn(async move {
                        let mut buffer = [0; Self::MAX_REQUEST_SIZE];
                        let len = stream.read(&mut buffer).await?;
                        #[allow(clippy::indexing_slicing)]
                        if let Ok(request) = MinionRequest::from_bytes(&buffer[0..len]) {
                            let response = this.response(&request);
                            stream.write_all(&response.to_json().into_bytes()).await
                        } else {
                            // Invalid request
                            Ok(())
                        }
                    });
                };
            }
        }))
    }

    /// Serves full minion
    ///
    /// # Errors
    /// Returns error if cannot bind to specified [`socket_address`]
    pub async fn serve_tls(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
        certificates_file: &Path,
        keys_file: &Path,
    ) -> Result<JoinHandle<()>> {
        let certificates = Self::load_certificates(certificates_file)?;
        let keys = Self::load_keys(keys_file)?;
        let key = keys.into_iter().next().context("Private key")?;
        let config = ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certificates, key)
            .context("Invalid private key")?;
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind(&socket_address).await?;

        let this = Arc::clone(self);
        Ok(tokio::spawn(async move {
            loop {
                if let Ok((stream, _)) = listener.accept().await {
                    let acceptor = acceptor.clone();
                    let this = Arc::clone(&this);
                    tokio::spawn(async move {
                        let mut stream = acceptor.accept(stream).await?;
                        // stream.write_all("Hello".as_bytes()).await
                        let mut buffer = [0; Self::MAX_REQUEST_SIZE];
                        let len = stream.read(&mut buffer).await?;
                        #[allow(clippy::indexing_slicing)]
                        if let Ok(request) = MinionRequest::from_bytes(&buffer[0..len]) {
                            let response = this.response(&request);
                            stream.write_all(&response.to_json().into_bytes()).await
                        } else {
                            // Invalid request
                            Ok(())
                        }
                    });
                }
            }
        }))
    }

    fn load_certificates(path: &Path) -> Result<Vec<Certificate>> {
        rustls_pemfile::certs(&mut Self::open(path).context("Certificates error")?)
            .context("Invalid certificates")
            .map(|certs| certs.into_iter().map(Certificate).collect())
    }

    fn load_keys(path: &Path) -> Result<Vec<PrivateKey>> {
        let rd = &mut Self::open(path)?;
        rustls_pemfile::pkcs8_private_keys(rd)
            .context("Invalid private keys")
            .map(|keys| keys.into_iter().map(PrivateKey).collect())
    }

    fn open(path: &Path) -> Result<std::io::BufReader<std::fs::File>> {
        let file = std::fs::File::open(path).context(format!("File {path:?} not found"))?;
        Ok(std::io::BufReader::new(file))
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

impl<T> Jsonable for T
where
    T: Serialize + for<'a> Deserialize<'a>,
{
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
#[allow(clippy::print_stdout)]
mod tests {
    use std::future::Future;
    use std::net::{IpAddr, SocketAddr};
    use std::pin::Pin;
    use std::str;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU16, Ordering};

    use tokio::net::{TcpStream, UdpSocket};
    use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerName};
    use tokio_rustls::TlsConnector;

    use super::*;

    static PORT: AtomicU16 = AtomicU16::new(16236);

    fn port() -> u16 {
        PORT.fetch_add(1, Ordering::Relaxed)
    }

    #[test]
    fn local_minion() {
        let minion_id = "test_minion".to_owned();
        let minion = Minion::new(minion_id.clone());
        let request = MinionRequest::new("123".to_owned());
        let response = minion.response(&request);

        check_response(minion_id, &request, &response);
    }

    #[test]
    fn serde() {
        let minion_id = "test_minion".to_owned();
        let minion = Minion::new(minion_id.clone());
        let request = MinionRequest::new("123".to_owned());

        let serialized = minion.response(&request).to_json();
        println!("serialized = {}", serialized);
        let response = MinionResponse::from_json(&serialized).unwrap();
        println!("deserialized = {:?}", response);

        check_response(minion_id, &request, &response);
    }

    fn check_response(minion_name: String, request: &MinionRequest, response: &MinionResponse) {
        assert_eq!(minion_name, response.minion_id);
        assert_eq!(request.request_id, response.request_id);
    }

    #[tokio::test]
    async fn test_udp_ipv4() -> Result<()> {
        test_udp("127.0.0.2", "0.0.0.0").await
    }

    #[tokio::test]
    async fn test_udp_ipv6() -> Result<()> {
        test_udp("::1", "::").await
    }

    async fn test_udp(host: &str, local: &str) -> Result<()> {
        let local = local.to_owned();
        let server_address = SocketAddr::new(IpAddr::from_str(host).unwrap(), port());
        parallel_requests(
            move |minion| async move { minion.serve_udp(&server_address).await },
            move |id, bytes| {
                let local = local.clone();
                async move {
                    let socket = UdpSocket::bind(format!("{local}:0")).await?;
                    socket.send_to(&bytes, &server_address).await?;
                    println!("UDP request {id} sent to {server_address}");

                    let mut buffer = [0; 1024];
                    let (len, addr) = socket.recv_from(&mut buffer).await?;
                    assert_eq!(server_address, addr);
                    Ok((buffer, len))
                }
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_tcp_ipv4() -> Result<()> {
        test_tcp("127.0.0.2").await
    }

    #[tokio::test]
    async fn test_tcp_ipv6() -> Result<()> {
        test_tcp("::1").await
    }

    async fn test_tcp(host: &str) -> Result<()> {
        let server_address = SocketAddr::new(IpAddr::from_str(host)?, port());
        parallel_requests(
            move |minion| async move { minion.serve_tcp(&server_address).await },
            move |id, bytes| async move {
                let mut stream = TcpStream::connect(server_address).await?;
                stream.write_all(&bytes).await?;
                AsyncWriteExt::shutdown(&mut stream).await?;
                println!("TCP request {id} sent to {server_address}");

                let mut buffer = [0; 1024];
                let len = stream.read(&mut buffer).await?;
                Ok((buffer, len))
            },
        )
        .await
    }

    async fn parallel_requests<S, SR, C, CR>(start: S, communicate: C) -> Result<()>
    where
        S: Fn(Arc<Minion>) -> SR,
        SR: Future<Output = Result<JoinHandle<()>>> + Send + 'static,
        C: Fn(String, Vec<u8>) -> CR + Clone + 'static,
        CR: Future<Output = Result<([u8; 1024], usize)>>,
    {
        let minion_id = "test_minion".to_owned();
        start(Minion::new(minion_id.clone())).await?;
        parallel(5, |i| {
            let communicate = communicate.clone();
            let minion_id = minion_id.clone();
            async move {
                let id = format!("12345_{i}");
                let request = MinionRequest::new(id.clone());
                let bytes = request.to_json().as_bytes().to_vec();

                let (buffer, read) = communicate(id.clone(), bytes).await?;
                let response = MinionResponse::from_bytes(&buffer[0..read]).unwrap();

                check_response(minion_id, &request, &response);
                println!("Request {id} OK");
                Ok(())
            }
        })
        .await?;
        Ok(())
    }

    type PBFR<T> = Pin<Box<dyn Future<Output = Result<T>>>>;

    fn parallel<T, F, FR>(n: usize, f: F) -> PBFR<Vec<T>>
    where
        T: 'static,
        F: FnMut(usize) -> FR,
        FR: Future<Output = Result<T>> + 'static,
    {
        (0..n).map(f).fold(
            Box::pin(async { Ok(vec![]) as Result<Vec<T>> }),
            |prev, next| {
                Box::pin(async {
                    tokio::try_join!(prev, next).map(|(mut p, n)| {
                        p.push(n);
                        p
                    })
                })
            },
        )
    }

    #[tokio::test]
    async fn test_tls() -> Result<()> {
        let host = "127.0.0.2";
        let server_address = SocketAddr::new(IpAddr::from_str(host)?, port());

        let mut root_cert_store = RootCertStore::empty();
        for certificate in Minion::load_certificates(Path::new("__keys/netadmin_minion.crt"))? {
            root_cert_store.add(&certificate)?;
        }
        let config = Arc::new(
            ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth(),
        );

        parallel_requests(
            move |minion| async move {
                minion
                    .serve_tls(
                        &server_address,
                        Path::new("__keys/netadmin_minion.crt"),
                        Path::new("__keys/netadmin_minion.key"),
                    )
                    .await
            },
            move |id, bytes| {
                let config = Arc::clone(&config);
                async move {
                    let connector = TlsConnector::from(config);
                    let domain = ServerName::try_from(Minion::TLS_DOMAIN)?;
                    let stream = TcpStream::connect(server_address).await?;
                    let mut stream = connector.connect(domain, stream).await?;
                    stream.write_all(&bytes).await?;
                    println!("TLS request {id} sent to {server_address}");

                    let mut buffer = [0; 1024];
                    let len = stream.read(&mut buffer).await?;
                    Ok((buffer, len))
                }
            },
        )
        .await
    }
}
