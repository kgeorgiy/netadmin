use core::{fmt::Debug, future::Future, time::Duration};
use std::{env, net::SocketAddr, str, sync::Arc};

use anyhow::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use async_trait::async_trait;

use crate::net::{
    JsonTransmitter, Message, MessageTransmitter, Receiver, TcpReceiver, TlsReceiver,
    TlsServerConfig, Transmitter, UdpReceiver,
};

pub mod net;

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
    /// Minion host name for TLS certificates
    pub const MINION_DOMAIN: &'static str = "minion.netadmin.test";

    /// Server host name for TLS certificates
    pub const CLIENT_DOMAIN: &'static str = "client.netadmin.test";

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
        Ok(self.serve("UDP", UdpReceiver::new(socket_address).await?))
    }

    /// Serves minion information on TCP port
    ///
    /// # Errors
    /// Returns error if cannot bind to specified `socket_address`
    pub async fn serve_tcp(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
    ) -> Result<JoinHandle<()>> {
        Ok(self.serve("TCP", TcpReceiver::new(socket_address).await?))
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
        config: &TlsServerConfig<'_>,
    ) -> Result<JoinHandle<()>> {
        Ok(self.serve("TLS", TlsReceiver::new(socket_address, config).await?))
    }

    fn serve<T, R>(self: &Arc<Self>, protocol: &str, mut receiver: R) -> JoinHandle<()>
    where
        T: Transmitter + Unpin + Send + Sync + 'static,
        R: Receiver<T> + Send + 'static,
    {
        let protocol = protocol.to_owned();
        let this = Arc::clone(self);
        Self::spawn::<_, ()>(&format!("Serve {protocol}"), async move {
            loop {
                match receiver.receive().await {
                    Ok((packet, transmitter)) => {
                        let this = Arc::clone(&this);
                        Self::spawn(&format!("{protocol} connection"), async move {
                            match InfoRequest::from_packet(&packet) {
                                Ok(request) => {
                                    request
                                        .handle(&this, JsonTransmitter::new(transmitter))
                                        .await?;
                                    // AsyncWriteExt::shutdown(&mut stream).await?;

                                    // Java TLS compatibility
                                    sleep(Duration::from_millis(1)).await;

                                    Ok(())
                                }
                                Err(error) => Err(error),
                            }
                        });
                    }
                    Err(error) => Self::log_error(&format!("{protocol} accept"), &error),
                }
            }
        })
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
    use tokio_rustls::rustls::ServerName;
    use tokio_rustls::TlsConnector;

    use net::Message;

    use crate::net::{TlsAuth, TlsClientConfig, TlsServerConfig};

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

        let server_certificate = &tls_path(Minion::MINION_DOMAIN, "crt");
        let client_certificate = &tls_path(Minion::CLIENT_DOMAIN, "crt");
        let client_key = &tls_path(Minion::CLIENT_DOMAIN, "key");
        let config = TlsClientConfig::new(
            server_certificate,
            if client_auth {
                Some(TlsAuth::new(client_key, client_certificate))
            } else {
                None
            },
        );
        let config = Arc::new(config.config()?);

        parallel_requests(
            move |minion| async move {
                let client_certificates_file =
                    minion_auth.map(|_| tls_path(Minion::CLIENT_DOMAIN, "crt"));
                minion
                    .serve_tls(
                        &server_address,
                        &TlsServerConfig::new(
                            &tls_path(Minion::MINION_DOMAIN, "key"),
                            &tls_path(Minion::MINION_DOMAIN, "crt"),
                            client_certificates_file.as_ref().map(PathBuf::as_path),
                        ),
                    )
                    .await
            },
            move |id, packet| {
                let config = Arc::clone(&config);
                async move {
                    let connector = TlsConnector::from(config);
                    let domain = ServerName::try_from(Minion::MINION_DOMAIN)?;
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
