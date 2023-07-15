#![cfg(test)]
#![allow(
    clippy::print_stdout,
    clippy::use_debug,
    clippy::std_instead_of_core,
    clippy::panic,
    clippy::unwrap_used,
    clippy::str_to_string
)]

use core::{future::Future, pin::Pin};
use std::{
    any::Any,
    env,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    str,
    str::FromStr,
    sync::atomic::{AtomicU16, Ordering},
    sync::Arc,
    sync::Mutex,
};

use anyhow::Result;
use async_trait::async_trait;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
    task::JoinHandle,
};
use tokio_rustls::{
    client::TlsStream,
    rustls::{ClientConfig, ServerName},
    TlsConnector,
};
use tracing_test::traced_test;

use super::{
    net::{Message, MessageTransmitter, Packet, TlsAuth, TlsClientConfig, TlsServerConfig},
    BindConfig, ExecOutput, ExecRequest, ExecResult, InfoRequest, InfoResponse, LegacyTransmitter,
    Minion, RequestHandler, ServeConfig, Service,
};

fn new_socket_addr(host: &str) -> Result<SocketAddr> {
    static PORT: AtomicU16 = AtomicU16::new(16236);
    Ok(SocketAddr::new(
        IpAddr::from_str(host)?,
        PORT.fetch_add(1, Ordering::Relaxed),
    ))
}

#[test]
#[traced_test]
fn local_minion() {
    let minion_id = "test_minion";
    let minion = Minion::new(minion_id);
    let request = InfoRequest::new("123".to_owned());
    let response = request.response(&minion);

    check_response(minion_id, &request, &response);
}

#[test]
#[traced_test]
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
#[traced_test]
async fn test_udp_ipv4() -> Result<()> {
    test_udp("127.0.0.2", "0.0.0.0").await
}

#[tokio::test]
#[traced_test]
async fn test_udp_ipv6() -> Result<()> {
    test_udp("::1", "::").await
}

async fn test_udp(host: &str, local: &str) -> Result<()> {
    let local = local.to_owned();
    let server_address = new_socket_addr(host)?;
    parallel_requests(
        move |minion| minion.serve_udp(&server_address, &[Service::Info]),
        move |id, request| test_packet_udp(id, server_address, local.clone(), request),
    )
    .await
}

async fn test_packet_udp(
    id: String,
    server_address: SocketAddr,
    client_addr: String,
    request: Packet,
) -> Result<Packet> {
    let socket = UdpSocket::bind(format!("{client_addr}:0")).await?;
    request.send(&socket, &server_address).await?;

    println!("UDP request {id} sent to {server_address}");

    let (packet, addr) = Box::pin(Packet::receive(&socket)).await?;
    assert_eq!(server_address, addr);
    Ok(packet)
}

#[tokio::test]
#[traced_test]
async fn test_tcp_ipv4() -> Result<()> {
    test_tcp("127.0.0.2").await
}

#[tokio::test]
#[traced_test]
async fn test_tcp_ipv6() -> Result<()> {
    test_tcp("::1").await
}

async fn test_tcp(host: &str) -> Result<()> {
    let server_address = new_socket_addr(host)?;
    parallel_requests(
        |minion| minion.serve_tcp(&server_address, &[Service::Info]),
        move |id, packet| test_packet_tcp(id, server_address, packet),
    )
    .await
}

async fn test_packet_tcp(id: String, server_address: SocketAddr, packet: Packet) -> Result<Packet> {
    let mut stream = TcpStream::connect(server_address).await?;
    communicate("TCP", id, server_address, packet, &mut stream).await
}

async fn parallel_requests<S, C, CR>(start: S, communicate: C) -> Result<()>
where
    S: Fn(Arc<Minion>) -> Result<JoinHandle<()>>,
    C: Fn(String, Packet) -> CR + Clone + 'static,
    CR: Future<Output = Result<Packet>>,
{
    let minion_id = "test_minion";
    start(Minion::new(minion_id))?;
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
#[traced_test]
async fn test_tls_ipv4() -> Result<()> {
    test_tls("127.0.0.2", Some(()), true).await
}

#[tokio::test]
#[traced_test]
async fn test_tls_ipv6() -> Result<()> {
    test_tls("::1", Some(()), true).await
}

#[tokio::test]
#[traced_test]
async fn test_tls_no_auth() -> Result<()> {
    test_tls("127.0.0.2", None, false).await
}

#[tokio::test]
#[traced_test]
#[should_panic(expected = "CertificateRequired")]
async fn test_tls_missing_auth() {
    test_tls("127.0.0.2", Some(()), false).await.expect("panic");
}

async fn test_tls(host: &str, minion_auth: Option<()>, client_auth: bool) -> Result<()> {
    let config = tls_client_config(client_auth)?;

    let server_address = new_socket_addr(host)?;
    parallel_requests(
        |minion| {
            minion.serve_tls(
                &server_address,
                &tls_minion_config(minion_auth),
                &[Service::Info],
            )
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

fn tls_client_config(client_auth: bool) -> Result<Arc<ClientConfig>> {
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

#[derive(Debug)]
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
    async fn send_silent<M: Message + 'static>(&self, mut message: M) -> Result<()> {
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
#[traced_test]
async fn test_exec_echo() -> Result<()> {
    test_exec("echo hello\n", 0, &["hello"], &[]).await
}

#[tokio::test]
#[traced_test]
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
#[traced_test]
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
#[traced_test]
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
#[traced_test]
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

async fn test_exec(stdin: &str, exit_code: i32, stdout: &[&str], stderr: &[&str]) -> Result<()> {
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
    let actual = str::from_utf8(actual).map(|out| out.lines().map(str::trim).collect::<Vec<_>>());
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
#[traced_test]
async fn test_exec_echo_net() -> Result<()> {
    let minion = Minion::new("test");
    let minion_addr = new_socket_addr("127.0.0.2")?;
    minion.serve_tcp(&minion_addr, &[Service::Exec])?;
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
#[traced_test]
async fn test_exec_echo_legacy() -> Result<()> {
    let minion = Minion::new("test");
    let minion_addr = new_socket_addr("127.0.0.2")?;
    minion.serve_legacy(&minion_addr, &tls_minion_config(None), &[Service::Exec])?;
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
#[traced_test]
async fn test_serve_info_tcp_udp_distinct() -> Result<()> {
    test_serve(&[Service::Info], &three_addresses()?, &three_addresses()?).await
}

#[tokio::test]
#[traced_test]
async fn test_serve_info_tcp_udp_same() -> Result<()> {
    let addresses = three_addresses()?;
    test_serve(&[Service::Info], &addresses, &addresses).await
}

#[tokio::test]
#[traced_test]
#[should_panic(expected = "unexpected end of file")]
async fn test_serve_no_info_tcp() {
    test_serve(&[Service::Exec], &three_addresses().expect("valid"), &[])
        .await
        .expect("panic");
}

fn three_addresses() -> Result<Vec<SocketAddr>> {
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
    let _: Vec<JoinHandle<()>> = minion.serve(&ServeConfig {
        tcp: tcp_udp_config(services, tcp_addrs),
        udp: tcp_udp_config(services, udp_addrs),
        tls: None,
        legacy: None,
    })?;
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
