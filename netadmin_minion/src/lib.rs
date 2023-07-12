use core::{fmt::Debug, future::Future, time::Duration};
use std::{env, net::SocketAddr, path::Path, str, sync::Arc};

use anyhow::{anyhow, Context, Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_rustls::rustls::server::{AllowAnyAuthenticatedClient, NoClientAuth};
use tokio_rustls::rustls::{Certificate, PrivateKey, RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;
use net::{Message, MessageTransmitter, Packet, JsonTransmitter, UdpStream};

mod net;

//
// RequestHandler

#[async_trait]
trait RequestHandler {
    async fn handle<T: MessageTransmitter + Send>(
        self,
        minion: &Minion,
        transmitter: T,
    ) -> Result<()>;
}

//
// InfoRequest, InfoResponse

#[derive(Serialize, Deserialize, Debug)]
#[must_use]
pub struct InfoRequest {
    request_id: String,
}

impl InfoRequest {
    pub fn new(id: String) -> InfoRequest {
        InfoRequest { request_id: id }
    }

    fn response(&self, minion: &Minion) -> InfoResponse {
        InfoResponse {
            minion_id: minion.id.clone(),
            request_id: self.request_id.clone(),
            os: env::consts::OS.to_owned(),
            time_utc: Utc::now(),
        }
    }
}

impl Message for InfoRequest {
    const PACKET_TYPE: u32 = 0x49_4E_46_4F; // INFO
}

#[async_trait]
impl RequestHandler for InfoRequest {
    async fn handle<T: MessageTransmitter + Send>(
        self,
        minion: &Minion,
        mut transmitter: T,
    ) -> Result<()> {
        transmitter.send(self.response(minion)).await
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[must_use]
pub struct InfoResponse {
    minion_id: String,
    request_id: String,
    os: String,
    time_utc: DateTime<Utc>,
}

impl Message for InfoResponse {
    const PACKET_TYPE: u32 = 0x69_6E_66_6F; // info
}

//
// Minion

#[must_use]
pub struct Minion {
    id: String,
}

impl Minion {
    #[must_use]
    pub fn new(id: String) -> Arc<Minion> {
        Arc::new(Minion { id })
    }

    /// Serves minion information on UDP port
    ///
    /// # Errors
    /// Returns error if cannot bind to specified `socket_address`
    pub async fn serve_udp(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
    ) -> Result<JoinHandle<()>> {
        let socket = Arc::new(UdpSocket::bind(socket_address).await?);
        let this = Arc::clone(self);
        Ok(Self::spawn::<_, ()>("Serve UDP", async move {
            loop {
                match Box::pin(Packet::receive(&socket)).await {
                    Ok((request, address)) => {
                        let socket = Arc::clone(&socket);
                        let this = Arc::clone(&this);
                        Self::spawn("UDP packet", async move {
                            match InfoRequest::from_packet(&request) {
                                Ok(request) => {
                                    request
                                        .handle(
                                            &this,
                                            JsonTransmitter::new(UdpStream::new(&socket, &address)),
                                        )
                                        .await
                                }
                                Err(error) => Err(error),
                            }
                        });
                    }
                    Err(error) => Self::log_error("UDP packet", &error),
                }
            }
        }))
    }

    fn spawn<F, T>(context: &str, future: F) -> JoinHandle<()>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Debug,
    {
        let context = context.to_owned();
        tokio::spawn(async move {
            match future.await {
                Ok(value) => Self::log_success(&context, &value),
                Err(error) => Self::log_error(&context, &error),
            };
        })
    }

    #[allow(clippy::use_debug, clippy::print_stderr)]
    fn log_success<T: Debug>(context: &str, value: &T) {
        eprintln!("{context}: Success {value:?}");
    }

    #[allow(clippy::use_debug, clippy::print_stderr)]
    fn log_error(context: &str, error: &Error) {
        eprintln!("{context}: Error {error}");
    }

    /// Serves minion information on TCP port
    ///
    /// # Errors
    /// Returns error if cannot bind to specified `socket_address`
    pub async fn serve_tcp(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
    ) -> Result<JoinHandle<()>> {
        let listener = Arc::new(TcpListener::bind(socket_address).await?);
        let this = Arc::clone(self);
        Ok(Self::spawn::<_, ()>("Serve TCP", async move {
            loop {
                match listener.accept().await.context("accept") {
                    Ok((stream, _)) => {
                        let this = Arc::clone(&this);
                        Self::spawn(
                            "TCP connection",
                            async move { this.communicate(stream).await },
                        );
                    }
                    Err(error) => Self::log_error("TCP accept", &error),
                };
            }
        }))
    }

    /// Serves full minion
    ///
    /// # Errors
    /// Returns error if
    /// - cannot bind to specified `socket_address`
    /// - TLS `config` is invalid
    pub async fn serve_tls(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
        config: &TlsConfig<'_>,
    ) -> Result<JoinHandle<()>> {
        let acceptor = config.acceptor()?;
        let listener = TcpListener::bind(&socket_address).await?;
        let this = Arc::clone(self);
        Ok(Self::spawn::<_, ()>("Serve TLS", async move {
            loop {
                match listener.accept().await.context("accept") {
                    Ok((stream, _)) => {
                        let acceptor = acceptor.clone();
                        let this = Arc::clone(&this);
                        Self::spawn("TLS connection", async move {
                            this.communicate(acceptor.accept(stream).await?).await
                        });
                    }
                    Err(error) => Self::log_error("TLS accept", &error),
                }
            }
        }))
    }

    async fn communicate<T: AsyncRead + AsyncWrite + Send + Unpin + Sync>(
        self: &Arc<Self>,
        mut stream: T,
    ) -> Result<()> {
        match InfoRequest::from_packet(&Packet::read(&mut stream).await?) {
            Ok(request) => {
                request
                    .handle(
                        self,
                        JsonTransmitter::new(&mut stream),
                    )
                    .await?;
                AsyncWriteExt::shutdown(&mut stream).await?;

                // Java TLS compatibility
                sleep(Duration::from_millis(1)).await;

                Ok(())
            }
            Err(error) => Err(error),
        }
    }
}

/// TLS configuration
#[allow(clippy::exhaustive_structs)]
pub struct TlsConfig<'a> {
    pub minion_certificates: &'a Path,
    pub minion_key: &'a Path,
    pub client_certificates: Option<&'a Path>,
}

impl<'a> TlsConfig<'a> {
    /// Minion host name for TLS certificates
    pub const MINION_DOMAIN: &'static str = "minion.netadmin.test";

    /// Server host name for TLS certificates
    pub const CLIENT_DOMAIN: &'static str = "client.netadmin.test";

    pub(crate) fn acceptor(&self) -> Result<TlsAcceptor> {
        let certificates = Self::load_certificates(self.minion_certificates)?;
        let key = Self::load_key(self.minion_key)?;
        let config =
            ServerConfig::builder()
                .with_safe_defaults()
                .with_client_cert_verifier(self.client_certificates.map_or(
                    Ok(NoClientAuth::boxed()) as Result<_>,
                    |path| {
                        Ok(AllowAnyAuthenticatedClient::new(Self::load_root_cert(path)?).boxed())
                    },
                )?)
                .with_single_cert(certificates, key)
                .context("Invalid private key")?;
        Ok(TlsAcceptor::from(Arc::new(config)))
    }

    pub(crate) fn load_certificates(path: &Path) -> Result<Vec<Certificate>> {
        rustls_pemfile::certs(&mut Self::open(path, "Certificates file")?)
            .context(format!("Invalid certificates file {path:?}"))
            .and_then(|certs| match certs.len() {
                0 => Err(anyhow!("No certificates found in {path:?}")),
                _ => Ok(certs.into_iter().map(Certificate).collect()),
            })
    }

    pub(crate) fn load_key(path: &Path) -> Result<PrivateKey> {
        rustls_pemfile::pkcs8_private_keys(&mut Self::open(path, "Private key")?)
            .context(format!("Invalid key file {path:?}"))
            .and_then(|keys| match keys.len() {
                0 => Err(anyhow!("No private keys found in {path:?}")),
                1 => Ok(PrivateKey(keys.into_iter().next().expect("index checked"))),
                _ => Err(anyhow!("Multiple private keys found in {path:?}")),
            })
    }

    fn open(path: &Path, context: &str) -> Result<std::io::BufReader<std::fs::File>> {
        let file = std::fs::File::open(path).context(format!("{context} {path:?} not found"))?;
        Ok(std::io::BufReader::new(file))
    }

    pub(crate) fn load_root_cert(path: &Path) -> Result<RootCertStore> {
        let mut root_cert_store = RootCertStore::empty();
        for certificate in Self::load_certificates(path)? {
            root_cert_store.add(&certificate)?;
        }
        Ok(root_cert_store)
    }
}

#[cfg(test)]
#[allow(clippy::print_stdout)]
mod tests {
    use std::future::Future;
    use std::net::{IpAddr, SocketAddr};
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::str;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU16, Ordering};

    use tokio::net::{TcpStream, UdpSocket};
    use tokio_rustls::rustls::{ClientConfig, ServerName};
    use tokio_rustls::TlsConnector;
    use net::Message;

    use super::*;

    static PORT: AtomicU16 = AtomicU16::new(16236);

    fn port() -> u16 {
        PORT.fetch_add(1, Ordering::Relaxed)
    }

    #[test]
    fn local_minion() {
        let minion_id = "test_minion".to_owned();
        let minion = Minion::new(minion_id.clone());
        let request = InfoRequest::new("123".to_owned());
        let response = request.response(&minion);

        check_response(minion_id, &request, &response);
    }

    #[test]
    fn serde() {
        let minion_id = "test_minion".to_owned();
        let minion = Minion::new(minion_id.clone());
        let request = InfoRequest::new("123".to_owned());

        let serialized = serde_json::to_string(&request.response(&minion)).unwrap();
        println!("serialized = {}", serialized);
        let response = serde_json::from_str(&serialized).unwrap();
        println!("deserialized = {:?}", response);

        check_response(minion_id, &request, &response);
    }

    fn check_response(minion_name: String, request: &InfoRequest, response: &InfoResponse) {
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
            move |id, request| {
                let local = local.clone();
                async move {
                    let socket = UdpSocket::bind(format!("{local}:0")).await?;
                    request.send(&socket, &server_address).await?;

                    println!("UDP request {id} sent to {server_address}");

                    let (packet, addr) = Packet::receive(&socket).await?;
                    assert_eq!(server_address, addr);
                    Ok(packet)
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
            move |id, packet| async move {
                let mut stream = TcpStream::connect(server_address).await?;
                communicate("TCP", id, &server_address, packet, &mut stream).await
            },
        )
        .await
    }

    async fn parallel_requests<S, SR, C, CR>(start: S, communicate: C) -> Result<()>
    where
        S: Fn(Arc<Minion>) -> SR,
        SR: Future<Output = Result<JoinHandle<()>>> + Send + 'static,
        C: Fn(String, Packet) -> CR + Clone + 'static,
        CR: Future<Output = Result<Packet>>,
    {
        let minion_id = "test_minion".to_owned();
        start(Minion::new(minion_id.clone())).await?;
        parallel(5, |i| {
            let communicate = communicate.clone();
            let minion_id = minion_id.clone();
            async move {
                let id = format!("12345_{i}");
                let request = InfoRequest::new(id.clone());

                let response = communicate(id.clone(), request.to_packet()).await?;
                let response = InfoResponse::from_packet(&response)?;

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

    const KEYS_DIR: &'static str = "resources/test";

    fn tls_path(domain: &str, ext: &str) -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push(&format!("{KEYS_DIR}/{domain}.{ext}"));
        path
    }

    #[tokio::test]
    async fn test_tls_ipv4() -> Result<()> {
        test_tls("127.0.0.2", Some(()), true).await
    }

    #[tokio::test]
    async fn test_tls_ipv6() -> Result<()> {
        test_tls("::1", Some(()), true).await
    }

    #[tokio::test]
    async fn test_tls_no_auth() -> Result<()> {
        test_tls("127.0.0.2", None, false).await
    }

    #[tokio::test]
    #[should_panic(expected = "CertificateRequired")]
    async fn test_tls_missing_auth() {
        test_tls("127.0.0.2", Some(()), false).await.expect("panic")
    }

    async fn test_tls(host: &str, minion_auth: Option<()>, client_auth: bool) -> Result<()> {
        let server_address = SocketAddr::new(IpAddr::from_str(host)?, port());

        let builder = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(TlsConfig::load_root_cert(&tls_path(
                TlsConfig::MINION_DOMAIN,
                "crt",
            ))?);
        let config = Arc::new(if client_auth {
            builder.with_single_cert(
                TlsConfig::load_certificates(&tls_path(TlsConfig::CLIENT_DOMAIN, "crt"))?,
                TlsConfig::load_key(&tls_path(TlsConfig::CLIENT_DOMAIN, "key"))?,
            )?
        } else {
            builder.with_no_client_auth()
        });

        parallel_requests(
            move |minion| async move {
                let client_certificates_file =
                    minion_auth.map(|_| tls_path(TlsConfig::CLIENT_DOMAIN, "crt"));
                minion
                    .serve_tls(
                        &server_address,
                        &TlsConfig {
                            minion_certificates: &tls_path(TlsConfig::MINION_DOMAIN, "crt"),
                            minion_key: &tls_path(TlsConfig::MINION_DOMAIN, "key"),
                            client_certificates: client_certificates_file
                                .as_ref()
                                .map(PathBuf::as_path),
                        },
                    )
                    .await
            },
            move |id, packet| {
                let config = Arc::clone(&config);
                async move {
                    let connector = TlsConnector::from(config);
                    let domain = ServerName::try_from(TlsConfig::MINION_DOMAIN)?;
                    let stream = TcpStream::connect(server_address).await?;

                    let mut stream = connector.connect(domain, stream).await?;
                    communicate("TLS", id, &server_address, packet, &mut stream).await
                }
            },
        )
        .await
    }

    async fn communicate<T: AsyncRead + AsyncWrite + Unpin>(
        ty: &str,
        id: String,
        server_address: &SocketAddr,
        packet: Packet,
        stream: &mut T,
    ) -> Result<Packet> {
        packet.write(stream).await?;
        println!("{ty} request {id} sent to {server_address}");
        Ok(Packet::read(stream).await?)
    }
}
