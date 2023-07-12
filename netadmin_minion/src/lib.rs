use core::{fmt::Debug, future::Future, mem, pin::Pin, time::Duration};
use std::{env, net::SocketAddr, str, sync::Arc};

use anyhow::{anyhow, Context, Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::{task::JoinHandle, time::sleep};

use crate::net::{
    JsonTransmitter, Message, MessageTransmitter, Packet, Receiver, TcpReceiver, TlsReceiver,
    TlsServerConfig, Transmitter, UdpReceiver,
};

pub mod net;

//
// RequestHandler

#[async_trait]
trait RequestHandler {
    async fn handle<T: MessageTransmitter>(self, minion: Arc<Minion>, transmitter: T)
        -> Result<()>;

    fn handler<T: MessageTransmitter>(
        packet: &Packet,
    ) -> Result<
        Box<dyn FnOnce(Arc<Minion>, T) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send>,
    >
    where
        Self: Message,
    {
        let request = Self::from_packet(packet)?;
        Ok(Box::new(|minion, transmitter| {
            request.handle(minion, transmitter)
        }))
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
        mut transmitter: T,
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
    fn transmit_std<T: MessageTransmitter, R: AsyncRead + Send + Unpin + 'static>(
        self: Arc<Self>,
        mut stream: R,
        mut transmitter: T,
        stderr: bool,
    ) {
        Minion::spawn("Exec", async move {
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
    }
}

#[async_trait]
impl RequestHandler for ExecRequest {
    async fn handle<T: MessageTransmitter>(
        self,
        _minion: Arc<Minion>,
        mut transmitter: T,
    ) -> Result<()> {
        use std::process::Stdio;
        use tokio::process::Command;

        let stdout = self
            .stdin
            .as_ref()
            .map_or(Stdio::null(), |_| Stdio::piped());

        let mut child = Command::new(self.command.clone())
            .args(self.args.clone())
            .stdout(stdout)
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("failed to spawn command")?;

        let this = &Arc::new(self);
        Arc::clone(this).transmit_std(
            child.stdout.take().context("stdout")?,
            transmitter.clone(),
            false,
        );
        Arc::clone(this).transmit_std(
            child.stderr.take().context("stderr")?,
            transmitter.clone(),
            !this.join_stderr,
        );

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
        transmitter
            .send(ExecResult {
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
        T: Transmitter,
        R: Receiver<T>,
    {
        let handlers = vec![InfoRequest::handler, ExecRequest::handler];

        let protocol = protocol.to_owned();
        let this = Arc::clone(self);
        Self::spawn::<_, ()>(&format!("Serve {protocol}"), async move {
            loop {
                match receiver.receive().await {
                    Ok((packet, transmitter)) => {
                        let this = Arc::clone(&this);
                        let handlers = handlers.clone();
                        Self::spawn(&format!("{protocol} connection"), async move {
                            let transmitter = JsonTransmitter::new(transmitter);
                            for handler in &handlers {
                                if let Ok(request) = handler(&packet) {
                                    request(this, transmitter).await?;
                                    // AsyncWriteExt::shutdown(&mut stream).await?;

                                    // Java TLS compatibility
                                    sleep(Duration::from_millis(1)).await;
                                    return Ok(());
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
        eprintln!("{context}: Error {error}");
    }
}

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
    use tokio_rustls::rustls::ServerName;
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
            move |minion| async move { minion.serve_udp(&server_address).await },
            move |id, request| {
                let local = local.clone();
                async move {
                    let socket = UdpSocket::bind(format!("{local}:0")).await?;
                    request.send(&socket, &server_address).await?;

                    println!("UDP request {id} sent to {server_address}");

                    let (packet, addr) = Box::pin(Packet::receive(&socket)).await?;
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
        let server_address = new_socket_addr(host)?;
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
        let server_certificate = &tls_path(Minion::MINION_DOMAIN, "crt");
        let client_certificate = &tls_path(Minion::CLIENT_DOMAIN, "crt");
        let client_key = &tls_path(Minion::CLIENT_DOMAIN, "key");
        let config = TlsClientConfig::new(
            server_certificate,
            client_auth.then(|| TlsAuth::new(client_key, client_certificate)),
        );
        let config = Arc::new(config.config()?);

        let server_address = new_socket_addr(host)?;
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
                            client_certificates_file.as_deref(),
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
        Packet::read(stream).await
    }

    struct ExecTransmitter {
        request_id: String,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        result: Option<ExecResult>,
    }

    impl ExecTransmitter {
        fn new(request_id: String) -> Self {
            Self {
                request_id,
                stdout: vec![],
                stderr: vec![],
                result: None,
            }
        }
    }

    #[async_trait]
    impl MessageTransmitter for Arc<Mutex<ExecTransmitter>> {
        async fn send<M: Message + 'static>(&mut self, mut message: M) -> Result<()> {
            let this = &mut *self.lock().unwrap();
            let message = &mut message as &mut dyn Any;
            if let Some(message) = message.downcast_mut::<ExecOutput>() {
                assert_eq!(this.request_id, message.request_id);
                println!("OUT: {:?}", str::from_utf8(&message.stdout));
                println!("ERR: {:?}", str::from_utf8(&message.stderr));
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
        let output = &Arc::new(Mutex::new(ExecTransmitter::new(request.request_id.clone())));
        request
            .handle(Minion::new("test_minion"), Arc::clone(output))
            .await?;

        let output = &*output.lock().unwrap();
        check_out(stderr, &output.stderr);
        check_out(stdout, &output.stdout);

        let result = output.result.as_ref().unwrap();
        assert_eq!(exit_code, result.exit_code);
        println!("Exit comment: {}", result.exit_comment);

        Ok(())
    }

    fn check_out(expected: &[&str], actual: &Vec<u8>) {
        let actual =
            str::from_utf8(actual).map(|out| out.lines().map(str::trim).collect::<Vec<_>>());
        assert_eq!(Ok(expected.to_vec()), actual);
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
        minion.serve_tcp(&minion_addr).await?;
        let stream = &mut TcpStream::connect(minion_addr).await?;
        exec_request("echo 123", "echo 123")
            .to_packet()
            .write(stream)
            .await?;
        let packet = Packet::read(stream).await?;
        println!("{}", packet.as_str()?);

        test_exec("echo hello\n", 0, &["hello"], &[]).await
    }
}
