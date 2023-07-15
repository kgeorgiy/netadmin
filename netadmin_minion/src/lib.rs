use core::{
    fmt::{Debug, Formatter},
    future::Future,
    mem,
    pin::Pin,
};
use std::{env, net::SocketAddr, path::Path, str, sync::Arc};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
    task::JoinHandle,
};
use tracing::{info, info_span};

use crate::{
    log::{Log, LogConfig},
    net::{
        JsonTransmitter, Message, MessageTransmitter, Packet, Receiver, TcpReceiver, TlsListener,
        TlsReceiver, TlsServerConfig, TlsTransmitter, Transmitter, UdpReceiver,
    },
};

pub mod log;
pub mod net;
mod tests;

//
// RequestHandler

type Handler<T> =
    Box<dyn FnOnce(Arc<Minion>, T) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send>;

#[async_trait]
trait RequestHandler {
    const SERVICE: Service;

    async fn handle<T: MessageTransmitter>(self, minion: Arc<Minion>, transmitter: T)
        -> Result<()>;

    fn handler<T: MessageTransmitter>(packet: &Packet) -> Option<Handler<T>>
    where
        Self: Message,
    {
        let service = format!("{:?}", Self::SERVICE);
        let request = Self::from_packet(packet).ok()?;
        Some(Box::new(move |minion, transmitter| {
            let span = info_span!("handle", service);
            let future = async {
                info!("Got {request:?}");
                request.handle(minion, transmitter).await
            };
            Box::pin(Log::report(span, future))
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
    const SERVICE: Service = Service::Info;

    async fn handle<T: MessageTransmitter>(
        self,
        minion: Arc<Minion>,
        transmitter: T,
    ) -> Result<()> {
        let result = transmitter.send(self.response(&minion));
        result.await
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
        name: &'static str,
        stream: &mut Option<R>,
        transmitter: T,
        stderr: bool,
    ) -> Result<()> {
        let mut stream = stream.take().context(name)?;
        Log::spawn(info_span!("copy", stream = name), async move {
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
                let result = transmitter.send(ExecOutput {
                    request_id: self.request_id.clone(),
                    stdout: out,
                    stderr: err,
                });
                result.await?;
            }
        });
        Ok(())
    }
}

#[async_trait]
impl RequestHandler for ExecRequest {
    const SERVICE: Service = Service::Exec;

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
        let result = tx.send(ExecResult {
            request_id: this.request_id.clone(),
            exit_code: status.code().unwrap_or(ExecResult::UNDEFINED_EXIT_CODE),
            exit_comment: format!("{status}"),
        });
        result.await
    }
}

/// Command output
#[derive(Serialize, Deserialize)]
#[must_use]
pub struct ExecOutput {
    request_id: String,
    /// New data from `stdout`
    stdout: Vec<u8>,
    /// New data from `stderr`
    /// Empty if [`ExecRequest::join_stderr`] was `true`,
    stderr: Vec<u8>,
}

impl Debug for ExecOutput {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.write_fmt(format_args!(
            "ExecOutput {{ request_id: \"{}\", stdout: {}, stderr: {} }}",
            self.request_id,
            Log::str_or_bin(&self.stdout),
            Log::str_or_bin(&self.stderr),
        ))
    }
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
    async fn new(socket_address: SocketAddr, config: TlsServerConfig) -> Result<Self> {
        Ok(Self {
            listener: TlsListener::new(socket_address, config).await?,
        })
    }
}

#[async_trait]
impl Receiver<Arc<Mutex<LegacyTransmitter>>> for LegacyReceiver {
    async fn receive(&mut self) -> Result<(Packet, Arc<Mutex<LegacyTransmitter>>, SocketAddr)> {
        let (mut transmitter, addr) = self.listener.accept().await?;

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
            addr,
        ))
    }
}

#[derive(Debug)]
struct LegacyTransmitter {
    transmitter: TlsTransmitter,
}

impl LegacyTransmitter {
    const OUTPUT: u32 = 0x1234_5678;
    const RESULT: u32 = 0;
}

#[async_trait]
impl Transmitter for Arc<Mutex<LegacyTransmitter>> {
    async fn send_silent(&self, packet: Packet) -> Result<()> {
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

#[derive(Debug)]
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
    pub fn serve_udp(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
        services: &[Service],
    ) -> Result<JoinHandle<()>> {
        Ok(self.serve_proto(
            "UDP",
            services,
            UdpReceiver::new(*socket_address),
            *socket_address,
        ))
    }

    /// Serves minion information on TCP port
    ///
    /// # Errors
    /// Returns error if cannot bind to specified `socket_address`
    pub fn serve_tcp(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
        services: &[Service],
    ) -> Result<JoinHandle<()>> {
        Ok(self.serve_proto(
            "TCP",
            services,
            TcpReceiver::new(*socket_address),
            *socket_address,
        ))
    }

    /// Serves full minion
    ///
    /// # Errors
    /// Returns error if
    /// - cannot bind to specified `socket_address`
    /// - TLS `config` is invalid
    pub fn serve_tls(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
        config: &TlsServerConfig,
        services: &[Service],
    ) -> Result<JoinHandle<()>> {
        Ok(self.serve_proto(
            "TLS",
            services,
            TlsReceiver::new(*socket_address, config.clone()),
            *socket_address,
        ))
    }

    /// Serves legacy (Scala) compatible minion
    ///
    /// # Errors
    /// Returns error if
    /// - cannot bind to specified `socket_address`
    /// - TLS `config` is invalid
    pub fn serve_legacy(
        self: &Arc<Self>,
        socket_address: &SocketAddr,
        config: &TlsServerConfig,
        services: &[Service],
    ) -> Result<JoinHandle<()>> {
        Ok(self.serve_proto(
            "Legacy",
            services,
            LegacyReceiver::new(*socket_address, config.clone()),
            *socket_address,
        ))
    }

    fn serve_proto<T, R, F>(
        self: &Arc<Self>,
        proto: &str,
        services: &[Service],
        receiver: F,
        addr: SocketAddr,
    ) -> JoinHandle<()>
    where
        T: Transmitter,
        R: Receiver<T>,
        F: Future<Output = Result<R>> + Send + 'static,
    {
        let handlers = services.iter().map(Service::handler).collect::<Vec<_>>();

        let this = Arc::clone(self);
        Log::spawn::<(), _>(info_span!("serve", %proto, %addr), async move {
            let mut receiver = receiver.await?;
            info!("Listening");
            loop {
                match receiver.receive().await {
                    Ok((packet, tx, client_addr)) => {
                        let this = Arc::clone(&this);
                        let handlers = handlers.clone();
                        Log::spawn(
                            info_span!("client", addr = format!("{client_addr:?}")),
                            async move {
                                for handler in &handlers {
                                    if let Some(request) = handler(&packet) {
                                        return request(this, JsonTransmitter::new(tx)).await;
                                    }
                                }

                                Err(anyhow!("Unknown or forbidden packet type"))
                            },
                        );
                    }
                    Err(error) => Log::log_error(&error),
                }
            }
        })
    }

    /// Creates minion that serves specified ports and protocols
    ///
    /// # Errors
    /// - Duplicate ports
    /// - Invalid TLS keys or certificates
    pub fn create_and_serve(config: &MinionConfig, log: &mut Log) -> Result<Vec<JoinHandle<()>>> {
        log.set_global(&config.log)?;
        Minion::new(&config.id).serve(&config.serve)
    }

    /// Serves specified ports and protocols
    ///
    /// # Errors
    /// - Duplicate ports
    /// - Invalid TLS keys or certificates
    pub fn serve(self: &Arc<Minion>, config: &ServeConfig) -> Result<Vec<JoinHandle<()>>> {
        let mut handles = vec![];
        for udp in config.udp.iter().flatten() {
            for addr in &udp.bind {
                handles.push(self.serve_udp(addr, &udp.services)?);
            }
        }
        for tcp in config.tcp.iter().flatten() {
            for addr in &tcp.bind {
                handles.push(self.serve_tcp(addr, &tcp.services)?);
            }
        }
        for tls in config.tls.iter().flatten() {
            for addr in &tls.bind.bind {
                handles.push(self.serve_tls(addr, &tls.tls, &tls.bind.services)?);
            }
        }
        for legacy in config.legacy.iter().flatten() {
            let services = &legacy.bind.services;
            for addr in &legacy.bind.bind {
                handles.push(self.serve_legacy(addr, &legacy.tls, services)?);
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
    log: LogConfig,
    serve: ServeConfig,
}

impl MinionConfig {
    pub fn resolve_paths(&mut self, base: &Path) {
        self.serve.resolve_paths(base);
    }
}
