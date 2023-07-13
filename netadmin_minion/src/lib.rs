use core::{fmt::Debug, future::Future, mem, pin::Pin};
use std::path::Path;
use std::{env, net::SocketAddr, str, sync::Arc};

use anyhow::{anyhow, Context, Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::net::{
    JsonTransmitter, Message, MessageTransmitter, Packet, Receiver, TcpReceiver, TlsListener,
    TlsReceiver, TlsServerConfig, TlsTransmitter, Transmitter, UdpReceiver,
};

pub mod net;

//
// RequestHandler

type Handler<T> =
    Box<dyn FnOnce(Arc<Minion>, T) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send>;

#[async_trait]
trait RequestHandler {
    async fn handle<T: MessageTransmitter>(self, minion: Arc<Minion>, transmitter: T)
        -> Result<()>;

    fn handler<T: MessageTransmitter>(packet: &Packet) -> Option<Handler<T>>
    where
        Self: Message,
    {
        let request = Self::from_packet(packet).ok()?;
        Some(Box::new(|minion, transmitter| {
            request.handle(minion, transmitter)
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Service {
    #[serde(alias = "info")]
    Info,
    #[serde(alias = "exec")]
    Exec,
}

impl Service {
    fn handler<T: MessageTransmitter>(&self) -> fn(&Packet) -> Option<Handler<T>> {
        match *self {
            Service::Info => InfoRequest::handler,
            Service::Exec => ExecRequest::handler,
        }
    }
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
    const PACKET_TYPE: u32 = u32::from_be_bytes(*b"INFO");
}

#[async_trait]
impl RequestHandler for InfoRequest {
    async fn handle<T: MessageTransmitter>(
        self,
        minion: Arc<Minion>,
        transmitter: T,
    ) -> Result<()> {
        transmitter.send(self.response(&minion)).await
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
    const PACKET_TYPE: u32 = u32::from_be_bytes(*b"info");
}

// ExecRequest, ExecOutput, ExecResult

/// Command execution request
#[derive(Serialize, Deserialize, Debug)]
#[must_use]
pub struct ExecRequest {
    request_id: String,
    /// Command to execute
    command: String,
    /// Command arguments
    args: Vec<String>,
    /// Data to pass to `stdin`
    stdin: Option<Vec<u8>>,
    /// Redirect `stderr` to `stdout` (same as `2>&1`)
    join_stderr: bool,
}

impl Message for ExecRequest {
    const PACKET_TYPE: u32 = u32::from_be_bytes(*b"EXE?");
}

impl ExecRequest {
    fn copy<T: MessageTransmitter, R: AsyncRead + Send + Unpin + 'static>(
        self: Arc<Self>,
        context: &'static str,
        stream: &mut Option<R>,
        transmitter: T,
        stderr: bool,
    ) -> Result<()> {
        let mut stream = stream.take().context(context)?;
        Minion::spawn(&format!("Exec ({context})"), async move {
            let mut buffer = [0; 0x1000];
            loop {
                let n = stream.read(&mut buffer).await?;
                if n == 0 {
                    return Ok(());
                }
                #[allow(clippy::indexing_slicing)]
                let mut out = buffer[0..n].to_vec();
                let mut err = vec![];
                if stderr {
                    mem::swap(&mut out, &mut err);
                }
                transmitter
                    .send(ExecOutput {
                        request_id: self.request_id.clone(),
                        stdout: out,
                        stderr: err,
                    })
                    .await?;
            }
        });
        Ok(())
    }
}

#[async_trait]
impl RequestHandler for ExecRequest {
    async fn handle<T: MessageTransmitter>(self, _minion: Arc<Minion>, tx: T) -> Result<()> {
        use std::process::Stdio;
        use tokio::process::Command;

        let sin = self
            .stdin
            .as_ref()
            .map_or(Stdio::null(), |_| Stdio::piped());

        let mut child = Command::new(self.command.clone())
            .args(self.args.clone())
            .stdin(sin)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("failed to spawn command")?;

        let this = &Arc::new(self);
        Arc::clone(this).copy("stdout", &mut child.stdout, tx.clone(), false)?;
        Arc::clone(this).copy("stderr", &mut child.stderr, tx.clone(), !this.join_stderr)?;

        if let Some(ref data) = this.stdin {
            let mut stdin = child.stdin.take().expect("piped stdin");
            stdin.write_all(data).await?;
            stdin.flush().await?;
            drop(stdin);
        }

        let status = child
            .wait()
            .await
            .expect("child process encountered an error");
        tx.send(ExecResult {
            request_id: this.request_id.clone(),
            exit_code: status.code().unwrap_or(ExecResult::UNDEFINED_EXIT_CODE),
            exit_comment: format!("{status}"),
        })
        .await
    }
}

/// Command output
#[derive(Serialize, Deserialize, Debug)]
#[must_use]
pub struct ExecOutput {
    request_id: String,
    /// New data from `stdout`
    stdout: Vec<u8>,
    /// New data from `stderr`
    /// Empty if [`ExecRequest::join_stderr`] was `true`,
    stderr: Vec<u8>,
}

impl Message for ExecOutput {
    const PACKET_TYPE: u32 = u32::from_be_bytes(*b"exeo");
}

/// Command execution result
#[derive(Serialize, Deserialize, Debug)]
#[must_use]
pub struct ExecResult {
    request_id: String,
    /// Command exit code
    exit_code: i32,
    /// Command exit comment
    exit_comment: String,
}

impl ExecResult {
    // Returned when actual exit code unavailable, e.g. due to signal
    #[allow(overflowing_literals)]
    const UNDEFINED_EXIT_CODE: i32 = 0xDEAD_BEEF_i32;
}

impl Message for ExecResult {
    const PACKET_TYPE: u32 = u32::from_be_bytes(*b"exer");
}

//
// Legacy

struct LegacyReceiver {
    listener: TlsListener,
}

impl LegacyReceiver {
    async fn new(socket_address: &SocketAddr, config: &TlsServerConfig) -> Result<Self> {
        Ok(Self {
            listener: TlsListener::new(socket_address, config).await?,
        })
    }
}

#[async_trait]
impl Receiver<Arc<Mutex<LegacyTransmitter>>> for LegacyReceiver {
    async fn receive(&mut self) -> Result<(Packet, Arc<Mutex<LegacyTransmitter>>)> {
        let (mut transmitter, _addr) = self.listener.accept().await?;

        let request = {
            let stream = transmitter.stream();
            let mut buffer = vec![0; stream.read_u32().await? as usize];
            stream.read_exact(&mut buffer).await?;
            ExecRequest {
                request_id: "legacy".to_owned(),
                command: if env::consts::OS == "windows" {
                    "bash"
                } else {
                    "/bin/bash"
                }
                .to_owned(),
                args: vec!["-c".to_owned(), String::from_utf8(buffer)?],
                stdin: None,
                join_stderr: true,
            }
        };

        Ok((
            request.to_packet(),
            Arc::new(Mutex::new(LegacyTransmitter { transmitter })),
        ))
    }
}

struct LegacyTransmitter {
    transmitter: TlsTransmitter,
}

impl LegacyTransmitter {
    const OUTPUT: u32 = 0x1234_5678;
    const RESULT: u32 = 0;
}

#[async_trait]
impl Transmitter for Arc<Mutex<LegacyTransmitter>> {
    async fn send(&self, packet: Packet) -> Result<()> {
        let mut guard = self.lock().await;
        let stream = guard.transmitter.stream();
        match packet.ty() {
            ExecOutput::PACKET_TYPE => {
                let output = ExecOutput::from_packet(&packet)?.stdout;
                stream.write_u32(LegacyTransmitter::OUTPUT).await?;
                stream.write_u32(output.len() as u32).await?;
                stream.write_all(&output).await?;
            }
            ExecResult::PACKET_TYPE => {
                stream.write_u32(LegacyTransmitter::RESULT).await?;
                stream
                    .write_i32(ExecResult::from_packet(&packet)?.exit_code)
                    .await?;
            }
            _ => return Err(anyhow!("Unknown packet type {}", packet.ty())),
        };
        stream.flush().await?;
        Ok(())
    }
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
    pub fn new(id: &str) -> Arc<Minion> {
        Arc::new(Minion { id: id.to_owned() })
    }

    /// Serves minion information on UDP port
    ///
    /// # Errors
    /// Returns error if cannot bind to specified `socket_address`
    pub async fn serve_udp(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
        services: &[Service],
    ) -> Result<JoinHandle<()>> {
        Ok(self.serve_proto("UDP", services, UdpReceiver::new(socket_address).await?))
    }

    /// Serves minion information on TCP port
    ///
    /// # Errors
    /// Returns error if cannot bind to specified `socket_address`
    pub async fn serve_tcp(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
        services: &[Service],
    ) -> Result<JoinHandle<()>> {
        Ok(self.serve_proto("TCP", services, TcpReceiver::new(socket_address).await?))
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
        config: &TlsServerConfig,
        services: &[Service],
    ) -> Result<JoinHandle<()>> {
        Ok(self.serve_proto(
            "TLS",
            services,
            TlsReceiver::new(socket_address, config).await?,
        ))
    }

    /// Serves legacy (Scala) compatible minion
    ///
    /// # Errors
    /// Returns error if
    /// - cannot bind to specified `socket_address`
    /// - TLS `config` is invalid
    pub async fn serve_legacy(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
        config: &TlsServerConfig,
        services: &[Service],
    ) -> Result<JoinHandle<()>> {
        Ok(self.serve_proto(
            "Legacy",
            services,
            LegacyReceiver::new(socket_address, config).await?,
        ))
    }

    fn serve_proto<T, R>(
        self: &Arc<Self>,
        protocol: &str,
        services: &[Service],
        mut receiver: R,
    ) -> JoinHandle<()>
    where
        T: Transmitter,
        R: Receiver<T>,
    {
        let handlers = services.iter().map(Service::handler).collect::<Vec<_>>();

        let protocol = protocol.to_owned();
        let this = Arc::clone(self);
        Self::spawn::<_, ()>(&format!("Serve {protocol}"), async move {
            loop {
                match receiver.receive().await {
                    Ok((packet, transmitter)) => {
                        let this = Arc::clone(&this);
                        let handlers = handlers.clone();
                        Self::spawn(&format!("{protocol} connection"), async move {
                            for handler in &handlers {
                                if let Some(request) = handler(&packet) {
                                    return request(this, JsonTransmitter::new(transmitter)).await;
                                }
                            }

                            Err(anyhow!("Unknown or forbidden packet type"))
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
        let causes: String = error
            .chain()
            .skip(1)
            .map(|err| format!("\n    Caused by: {err}"))
            .collect();
        eprintln!("{context}: Error {error}{causes}");
    }

    /// Creates minion that serves specified ports and protocols
    ///
    /// # Errors
    /// - Duplicate ports
    /// - Invalid TLS keys or certificates
    pub async fn create_and_serve(config: &MinionConfig) -> Result<Vec<JoinHandle<()>>> {
        Minion::new(&config.id).serve(&config.serve).await
    }

    /// Serves specified ports and protocols
    ///
    /// # Errors
    /// - Duplicate ports
    /// - Invalid TLS keys or certificates
    pub async fn serve(self: &Arc<Minion>, config: &ServeConfig) -> Result<Vec<JoinHandle<()>>> {
        let mut handles = vec![];
        for udp in config.udp.iter().flatten() {
            for addr in &udp.bind {
                handles.push(self.serve_udp(addr, &udp.services).await?);
            }
        }
        for tcp in config.tcp.iter().flatten() {
            for addr in &tcp.bind {
                handles.push(self.serve_tcp(addr, &tcp.services).await?);
            }
        }
        for tls in config.tls.iter().flatten() {
            for addr in &tls.bind.bind {
                handles.push(self.serve_tls(addr, &tls.tls, &tls.bind.services).await?);
            }
        }
        for legacy in config.legacy.iter().flatten() {
            let services = &legacy.bind.services;
            for addr in &legacy.bind.bind {
                handles.push(self.serve_legacy(addr, &legacy.tls, services).await?);
            }
        }
        Ok(handles)
    }
}

//
// *Config

#[derive(Debug, Serialize, Deserialize)]
pub struct BindConfig {
    bind: Vec<SocketAddr>,
    services: Vec<Service>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(flatten)]
    bind: BindConfig,
    #[serde(flatten)]
    tls: TlsServerConfig,
}

impl TlsConfig {
    pub fn resolve_paths(&mut self, base: &Path) {
        self.tls.resolve_paths(base);
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServeConfig {
    tcp: Option<Vec<BindConfig>>,
    udp: Option<Vec<BindConfig>>,
    tls: Option<Vec<TlsConfig>>,
    legacy: Option<Vec<TlsConfig>>,
}

impl ServeConfig {
    pub fn resolve_paths(&mut self, base: &Path) {
        Self::resolve(base, &mut self.tls);
        Self::resolve(base, &mut self.legacy);
    }

    fn resolve(base: &Path, opt_configs: &mut Option<Vec<TlsConfig>>) {
        opt_configs
            .iter_mut()
            .for_each(|configs| configs.iter_mut().for_each(|c| c.resolve_paths(base)));
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MinionConfig {
    id: String,
    serve: ServeConfig,
}

impl MinionConfig {
    pub fn resolve_paths(&mut self, base: &Path) {
        self.serve.resolve_paths(base);
    }
}

//
// Tests

#[cfg(test)]
#[allow(
    clippy::print_stdout,
    clippy::use_debug,
    clippy::std_instead_of_core,
    clippy::panic,
    clippy::unwrap_used
)]
mod tests {
    use std::any::Any;
    use std::future::Future;
    use std::net::{IpAddr, SocketAddr};
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::str;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::sync::Mutex;

    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::net::{TcpStream, UdpSocket};
    use tokio_rustls::client::TlsStream;
    use tokio_rustls::rustls::{ClientConfig, ServerName};
    use tokio_rustls::TlsConnector;

    use net::Message;

    use crate::net::{Packet, TlsAuth, TlsClientConfig, TlsServerConfig};

    use super::*;

    fn new_socket_addr(host: &str) -> Result<SocketAddr, Error> {
        static PORT: AtomicU16 = AtomicU16::new(16236);
        Ok(SocketAddr::new(
            IpAddr::from_str(host)?,
            PORT.fetch_add(1, Ordering::Relaxed),
        ))
    }

    #[test]
    fn local_minion() {
        let minion_id = "test_minion";
        let minion = Minion::new(minion_id);
        let request = InfoRequest::new("123".to_owned());
        let response = request.response(&minion);

        check_response(minion_id, &request, &response);
    }

    #[test]
    fn serde() {
        let minion_id = "test_minion";
        let minion = Minion::new(minion_id);
        let request = InfoRequest::new("123".to_owned());

        let serialized = serde_json::to_string(&request.response(&minion)).expect("always succeed");
        println!("serialized = {serialized}");
        let response = serde_json::from_str(&serialized).expect("valid JSON");
        println!("deserialized = {response:?}");

        check_response(minion_id, &request, &response);
    }

    fn check_response(minion_name: &str, request: &InfoRequest, response: &InfoResponse) {
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
        let server_address = new_socket_addr(host)?;
        parallel_requests(
            move |minion| async move { minion.serve_udp(&server_address, &[Service::Info]).await },
            move |id, request| test_packet_udp(id, server_address, local.clone(), request),
        )
        .await
    }

    async fn test_packet_udp(
        id: String,
        server_address: SocketAddr,
        client_addr: String,
        request: Packet,
    ) -> Result<Packet, Error> {
        let socket = UdpSocket::bind(format!("{client_addr}:0")).await?;
        request.send(&socket, &server_address).await?;

        println!("UDP request {id} sent to {server_address}");

        let (packet, addr) = Box::pin(Packet::receive(&socket)).await?;
        assert_eq!(server_address, addr);
        Ok(packet)
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
        let server_address = new_socket_addr(host)?;
        parallel_requests(
            move |minion| async move { minion.serve_tcp(&server_address, &[Service::Info]).await },
            move |id, packet| test_packet_tcp(id, server_address, packet),
        )
        .await
    }

    async fn test_packet_tcp(
        id: String,
        server_address: SocketAddr,
        packet: Packet,
    ) -> Result<Packet> {
        let mut stream = TcpStream::connect(server_address).await?;
        communicate("TCP", id, server_address, packet, &mut stream).await
    }

    async fn parallel_requests<S, SR, C, CR>(start: S, communicate: C) -> Result<()>
    where
        S: Fn(Arc<Minion>) -> SR,
        SR: Future<Output = Result<JoinHandle<()>>> + Send + 'static,
        C: Fn(String, Packet) -> CR + Clone + 'static,
        CR: Future<Output = Result<Packet>>,
    {
        let minion_id = "test_minion";
        start(Minion::new(minion_id)).await?;
        parallel(5, |i| {
            let communicate = communicate.clone();
            let minion_id = minion_id.to_owned();
            async move {
                let id = format!("12345_{i}");
                let request = InfoRequest::new(id.clone());

                let response = communicate(id.clone(), request.to_packet()).await?;
                let response = InfoResponse::from_packet(&response)?;

                check_response(&minion_id, &request, &response);
                println!("Request {id} OK");
                Ok(())
            }
        })
        .await?;
        Ok(())
    }

    type Pbfr<T> = Pin<Box<dyn Future<Output = Result<T>>>>;

    fn parallel<T, F, FR>(n: usize, f: F) -> Pbfr<Vec<T>>
    where
        T: 'static,
        F: FnMut(usize) -> FR,
        FR: Future<Output = Result<T>> + 'static,
    {
        (0..n).map(f).fold(
            Box::pin(async { Ok(vec![]) as Result<Vec<T>> }),
            |prev, next| {
                Box::pin(async {
                    tokio::try_join!(prev, next).map(|(mut results, result)| {
                        results.push(result);
                        results
                    })
                })
            },
        )
    }

    const KEYS_DIR: &str = "resources/test";

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
        test_tls("127.0.0.2", Some(()), false).await.expect("panic");
    }

    async fn test_tls(host: &str, minion_auth: Option<()>, client_auth: bool) -> Result<()> {
        let config = tls_client_config(client_auth)?;

        let server_address = new_socket_addr(host)?;
        parallel_requests(
            move |minion| async move {
                minion
                    .serve_tls(
                        &server_address,
                        &tls_minion_config(minion_auth),
                        &[Service::Info],
                    )
                    .await
            },
            move |id, packet| {
                let config = Arc::clone(&config);
                async move {
                    let mut stream = tls_connect(server_address, config).await?;
                    communicate("TLS", id, server_address, packet, &mut stream).await
                }
            },
        )
        .await
    }

    async fn tls_connect(
        server_address: SocketAddr,
        config: Arc<ClientConfig>,
    ) -> Result<TlsStream<TcpStream>> {
        let connector = TlsConnector::from(config);
        let domain = ServerName::try_from(Minion::MINION_DOMAIN)?;
        let stream = TcpStream::connect(server_address).await?;
        Ok(connector.connect(domain, stream).await?)
    }

    fn tls_minion_config(minion_auth: Option<()>) -> TlsServerConfig {
        let client_certificates_file = minion_auth.map(|_| tls_path(Minion::CLIENT_DOMAIN, "crt"));
        TlsServerConfig::new(
            &tls_path(Minion::MINION_DOMAIN, "key"),
            &tls_path(Minion::MINION_DOMAIN, "crt"),
            client_certificates_file.as_deref(),
        )
    }

    fn tls_client_config(client_auth: bool) -> Result<Arc<ClientConfig>, Error> {
        let client_certificate = &tls_path(Minion::CLIENT_DOMAIN, "crt");
        let client_key = &tls_path(Minion::CLIENT_DOMAIN, "key");
        let config = TlsClientConfig::new(
            &tls_path(Minion::MINION_DOMAIN, "crt"),
            client_auth.then(|| TlsAuth::new(client_key, client_certificate)),
        );
        Ok(Arc::new(config.config()?))
    }

    async fn communicate<T: AsyncRead + AsyncWrite + Unpin>(
        ty: &str,
        id: String,
        server_address: SocketAddr,
        packet: Packet,
        stream: &mut T,
    ) -> Result<Packet> {
        packet.write(stream).await?;
        println!("{ty} request {id} sent to {server_address}");
        Packet::read(stream).await
    }

    struct ExecTest {
        request_id: String,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        result: Option<ExecResult>,
    }

    impl ExecTest {
        fn new(request_id: &str) -> Arc<Mutex<Self>> {
            Arc::new(Mutex::new(Self {
                request_id: request_id.to_owned(),
                stdout: vec![],
                stderr: vec![],
                result: None,
            }))
        }
    }

    #[async_trait]
    impl MessageTransmitter for Arc<Mutex<ExecTest>> {
        async fn send<M: Message + 'static>(&self, mut message: M) -> Result<()> {
            let this = &mut *self.lock().unwrap();
            let message = &mut message as &mut dyn Any;
            if let Some(message) = message.downcast_mut::<ExecOutput>() {
                assert_eq!(this.request_id, message.request_id);
                println!("    OUT: {:?}", str::from_utf8(&message.stdout));
                println!("    ERR: {:?}", str::from_utf8(&message.stderr));
                this.stdout.extend_from_slice(&message.stdout);
                this.stderr.extend_from_slice(&message.stderr);
            } else if let Some(message) = message.downcast_mut::<ExecResult>() {
                assert_eq!(this.request_id, message.request_id);
                this.result = Some(ExecResult {
                    request_id: message.request_id.clone(),
                    exit_code: message.exit_code,
                    exit_comment: message.exit_comment.clone(),
                });
            } else {
                panic!("Unexpected packet type");
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_exec_echo() -> Result<()> {
        test_exec("echo hello\n", 0, &["hello"], &[]).await
    }

    #[tokio::test]
    async fn test_exec_multi_echo() -> Result<()> {
        test_exec(
            "
                echo one
                echo two
                echo three
                echo with spaces
            ",
            0,
            &["one", "two", "three", "with spaces"],
            &[],
        )
        .await
    }

    #[tokio::test]
    async fn test_exec_echo_redirect() -> Result<()> {
        test_exec(
            "
                echo one
                echo two >&2
                echo three
                echo with spaces >&2
            ",
            0,
            &["one", "three"],
            &["two", "with spaces"],
        )
        .await
    }

    #[tokio::test]
    async fn test_exec_cat() -> Result<()> {
        test_exec(
            "
                echo one | cat
                echo two | cat
                echo three | cat
                echo with spaces | cat
            ",
            0,
            &["one", "two", "three", "with spaces"],
            &[],
        )
        .await
    }

    #[tokio::test]
    async fn test_loop() -> Result<()> {
        test_exec_cb(
            "for %a in (1 2 3) do (echo %a & echo %a %a >&2)",
            "
                for a in 1 2 3 ; do
                    echo $a
                    echo $a $a >&2
                done
            ",
            0,
            &["1", "2", "3"],
            &["1 1", "2 2", "3 3"],
        )
        .await
    }

    async fn test_exec(
        stdin: &str,
        exit_code: i32,
        stdout: &[&str],
        stderr: &[&str],
    ) -> Result<()> {
        test_exec_cb(stdin, stdin, exit_code, stdout, stderr).await
    }

    async fn test_exec_cb(
        cmd: &str,
        bash: &str,
        exit_code: i32,
        stdout: &[&str],
        stderr: &[&str],
    ) -> Result<()> {
        let request = exec_request(cmd, bash);
        let output = &ExecTest::new(&request.request_id);
        request
            .handle(Minion::new("test_minion"), Arc::clone(output))
            .await?;

        check_exec(output, exit_code, stdout, stderr);
        Ok(())
    }

    fn check_exec(test: &Arc<Mutex<ExecTest>>, exit_code: i32, stdout: &[&str], stderr: &[&str]) {
        let output = &*test.lock().unwrap();
        check_out("stderr", stderr, &output.stderr);
        check_out("stdout", stdout, &output.stdout);

        let result = output.result.as_ref().unwrap();
        assert_eq!(exit_code, result.exit_code);
        println!("Exit comment: {}", result.exit_comment);
    }

    fn check_out(stream: &str, expected: &[&str], actual: &[u8]) {
        let actual =
            str::from_utf8(actual).map(|out| out.lines().map(str::trim).collect::<Vec<_>>());
        assert_eq!(Ok(expected.to_vec()), actual, "{stream}");
    }

    fn exec_request(cmd: &str, bash: &str) -> ExecRequest {
        match env::consts::OS {
            "windows" => ExecRequest {
                request_id: "test".to_owned(),
                command: "cmd".to_owned(),
                args: vec!["/q", "/k", "@echo off"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                stdin: Some(
                    format!("{}\r\n", cmd.trim().replace('\n', "\r\n"))
                        .as_bytes()
                        .to_vec(),
                ),
                join_stderr: false,
            },
            "linux" | "macos" => ExecRequest {
                request_id: "test".to_owned(),
                command: "bash".to_owned(),
                args: vec![],
                stdin: Some(bash.as_bytes().to_vec()),
                join_stderr: false,
            },
            _ => panic!("Unsupported OS"),
        }
    }

    #[tokio::test]
    async fn test_exec_echo_net() -> Result<()> {
        let minion = Minion::new("test");
        let minion_addr = new_socket_addr("127.0.0.2")?;
        minion.serve_tcp(&minion_addr, &[Service::Exec]).await?;
        let stream = &mut TcpStream::connect(minion_addr).await?;

        let request = exec_request("echo 123", "echo 123");
        request.to_packet().write(stream).await?;

        let test = &ExecTest::new(&request.request_id);
        while let Ok(packet) = Packet::read(stream).await {
            match packet.ty() {
                ExecOutput::PACKET_TYPE => test.send(ExecOutput::from_packet(&packet)?).await?,
                ExecResult::PACKET_TYPE => test.send(ExecResult::from_packet(&packet)?).await?,
                _ => panic!("Unknown packet type"),
            };
        }
        check_exec(test, 0, &["123"], &[]);
        Ok(())
    }

    #[tokio::test]
    async fn test_exec_echo_legacy() -> Result<()> {
        let minion = Minion::new("test");
        let minion_addr = new_socket_addr("127.0.0.2")?;
        minion
            .serve_legacy(&minion_addr, &tls_minion_config(None), &[Service::Exec])
            .await?;
        let stream = &mut tls_connect(minion_addr, tls_client_config(true)?).await?;

        let command = b"echo 123";
        stream.write_u32(command.len() as u32).await?;
        stream.write_all(command).await?;

        let request_id = "test".to_owned();
        let test = &ExecTest::new(&request_id);
        loop {
            let ty = stream.read_u32().await?;
            match ty {
                LegacyTransmitter::OUTPUT => {
                    let mut stdout = vec![0; stream.read_u32().await? as usize];
                    stream.read_exact(&mut stdout).await?;
                    test.send(ExecOutput {
                        request_id: request_id.clone(),
                        stdout,
                        stderr: vec![],
                    })
                    .await?;
                }
                LegacyTransmitter::RESULT => {
                    test.send(ExecResult {
                        request_id: request_id.clone(),
                        exit_code: stream.read_i32().await?,
                        exit_comment: String::new(),
                    })
                    .await?;
                    break;
                }
                _ => panic!("Unknown packet type"),
            };
        }
        check_exec(test, 0, &["123"], &[]);
        Ok(())
    }

    #[tokio::test]
    async fn test_serve_info_tcp_udp_distinct() -> Result<()> {
        test_serve(&[Service::Info], &three_addresses()?, &three_addresses()?).await
    }

    #[tokio::test]
    async fn test_serve_info_tcp_udp_same() -> Result<()> {
        let addresses = three_addresses()?;
        test_serve(&[Service::Info], &addresses, &addresses).await
    }

    #[tokio::test]
    #[should_panic(expected = "unexpected end of file")]
    async fn test_serve_no_info_tcp() {
        test_serve(&[Service::Exec], &three_addresses().expect("valid"), &[])
            .await
            .expect("panic");
    }

    fn three_addresses() -> Result<Vec<SocketAddr>, Error> {
        let tcp_udp_addrs = (1..=3)
            .map(|i| new_socket_addr(&format!("127.0.0.{i}")))
            .collect::<Result<Vec<SocketAddr>>>()?;
        Ok(tcp_udp_addrs)
    }

    async fn test_serve(
        services: &[Service],
        tcp_addrs: &[SocketAddr],
        udp_addrs: &[SocketAddr],
    ) -> Result<()> {
        let minion = Minion::new("test_serve_info");
        let _: Vec<JoinHandle<()>> = minion
            .serve(&ServeConfig {
                tcp: tcp_udp_config(services, tcp_addrs),
                udp: tcp_udp_config(services, udp_addrs),
                tls: None,
                legacy: None,
            })
            .await?;
        for addr in tcp_addrs {
            test_info_tcp(&minion.id, *addr).await?;
        }
        for addr in udp_addrs {
            test_info_udp(&minion.id, *addr).await?;
        }
        Ok(())
    }

    #[allow(clippy::indexing_slicing)]
    fn tcp_udp_config(services: &[Service], addrs: &[SocketAddr]) -> Option<Vec<BindConfig>> {
        (addrs.len() == 3).then(|| {
            vec![
                BindConfig {
                    bind: vec![addrs[0], addrs[1]],
                    services: services.to_vec(),
                },
                BindConfig {
                    bind: vec![addrs[2]],
                    services: services.to_vec(),
                },
            ]
        })
    }

    async fn test_info_tcp(minion_id: &str, server_address: SocketAddr) -> Result<()> {
        let request = InfoRequest {
            request_id: format!("{minion_id}_{server_address}"),
        };
        let response = InfoResponse::from_packet(
            &test_packet_tcp(
                request.request_id.clone(),
                server_address,
                request.to_packet(),
            )
            .await?,
        )?;
        check_response(minion_id, &request, &response);
        Ok(())
    }

    async fn test_info_udp(minion_id: &str, server_address: SocketAddr) -> Result<()> {
        let request = InfoRequest {
            request_id: format!("{minion_id}_{server_address}"),
        };
        let response = InfoResponse::from_packet(
            &test_packet_udp(
                request.request_id.clone(),
                server_address,
                "127.0.0.1".to_owned(),
                request.to_packet(),
            )
            .await?,
        )?;
        check_response(minion_id, &request, &response);
        Ok(())
    }
}
