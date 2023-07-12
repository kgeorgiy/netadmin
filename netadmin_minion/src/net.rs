use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::Mutex;
use tokio_rustls::rustls::server::{AllowAnyAuthenticatedClient, NoClientAuth};
use tokio_rustls::rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore, ServerConfig};
use tokio_rustls::{server::TlsStream, TlsAcceptor};

//
// Message

/// Network message
pub trait Message: Serialize + for<'a> Deserialize<'a> + Send + Sync + 'static {
    const PACKET_TYPE: u32;

    /// Converts to packet containing UTF-8 encoded JSON string
    fn to_packet(&self) -> Packet {
        let payload = serde_json::to_string(self)
            .expect("never happens")
            .as_bytes()
            .to_vec();
        Packet::new(Self::PACKET_TYPE, payload)
    }

    /// Interprets packet payload as UTF-8 encoded JSON string
    //
    /// # Errors
    /// - payload is not valid UTF-8 string
    /// - payload is not valid JSON
    /// - payload is not generated by [`Message::to_packet`]
    fn from_packet(packet: &Packet) -> Result<Self> {
        if packet.ty == Self::PACKET_TYPE {
            serde_json::from_str(packet.as_str()?).context("Invalid JSON")
        } else {
            Err(anyhow!("Unknown packet type"))
        }
    }
}

//
// MessageTransmitter, JsonTransmitter

/// Sends [`Message`]s
#[async_trait]
pub trait MessageTransmitter: Send + Clone + 'static {
    async fn send<M: Message>(&mut self, message: M) -> Result<()>;
}

/// Sends [`Message`]s in JSON
#[must_use]
#[derive(Clone)]
pub struct JsonTransmitter<T: Transmitter> {
    transmitter: T,
}

impl<T: Transmitter> JsonTransmitter<T> {
    pub fn new(transmitter: T) -> JsonTransmitter<T> {
        JsonTransmitter { transmitter }
    }
}

#[async_trait]
impl<T: Transmitter> MessageTransmitter for JsonTransmitter<T> {
    async fn send<M: Message>(&mut self, message: M) -> Result<()> {
        self.transmitter.send(message.to_packet()).await
    }
}

//
// Packet

/// Network packet
#[must_use]
pub struct Packet {
    ty: u32,
    payload: Vec<u8>,
}

impl Packet {
    const MAX_SIZE: usize = 0x10000 - 4 * 2;

    /// Writes packet to `target`
    ///
    /// # Errors
    /// - Target write error
    pub async fn write<T: AsyncWrite + Unpin>(&self, target: &mut T) -> Result<()> {
        target.write_u32(self.payload.len() as u32).await?;
        target.write_u32(self.ty).await?;
        target.write_all(&self.payload).await?;
        target.flush().await?;
        Ok(())
    }

    /// Reads packet from `source`
    ///
    /// # Errors
    /// - Source read error
    /// - Invalid packet layout
    pub async fn read<S: AsyncRead + Unpin>(source: &mut S) -> Result<Packet> {
        let size = source.read_u32().await? as usize;
        if size > Self::MAX_SIZE {
            return Err(anyhow!("Packet too large"));
        }

        let ty = source.read_u32().await?;

        let mut payload = vec![0; size];
        source.read_exact(&mut payload).await?;

        Ok(Self::new(ty, payload))
    }

    fn new(ty: u32, payload: Vec<u8>) -> Packet {
        Packet { ty, payload }
    }

    /// Receives packet via UDP
    ///
    /// # Errors
    /// - UDP receive error
    /// - Invalid packet layout
    #[allow(clippy::indexing_slicing)]
    pub async fn receive(socket: &UdpSocket) -> Result<(Packet, SocketAddr)> {
        let mut buffer = [0; Packet::MAX_SIZE + 8];
        let (len, address) = socket.recv_from(&mut buffer).await?;
        if len >= 8 && len - 8 == (&buffer[..4]).get_u32() as usize {
            let packet = Self::new((&buffer[4..8]).get_u32(), buffer[8..len].to_vec());
            Ok((packet, address))
        } else {
            Err(anyhow!("Size mismatch"))
        }
    }

    /// Sends packet via UDP
    ///
    /// # Errors
    /// - UDP send error
    pub async fn send(&self, socket: &UdpSocket, addr: &SocketAddr) -> Result<()> {
        let mut data = Vec::with_capacity(8 + self.payload.len());
        data.put_u32(self.payload.len() as u32);
        data.put_u32(self.ty);
        data.put_slice(&self.payload);
        socket.send_to(&data, addr).await?;
        Ok(())
    }

    /// Interprets payload as UTF-8 encoded string
    //
    /// # Errors
    /// - input is not valid UTF-8 string
    pub fn as_str(&self) -> Result<&str> {
        str::from_utf8(&self.payload).context("Invalid UTF-8")
    }
}

//
// Receiver, Transmitter

/// Receives [`Packet`]s
#[async_trait]
pub trait Receiver<T: Transmitter>: Send + 'static {
    async fn receive(&mut self) -> Result<(Packet, T)>;
}

/// Sends [`Packet`]s
#[async_trait]
pub trait Transmitter: Send + Sync + Unpin + Clone + 'static {
    async fn send(&mut self, packet: Packet) -> Result<()>;
}

#[async_trait]
impl<T: AsyncWrite + Send + Unpin + 'static> Transmitter for Arc<Mutex<T>> {
    async fn send(&mut self, packet: Packet) -> Result<()> {
        packet.write(&mut *self.lock().await).await
    }
}

//
// UdpStream, UdpReceiver

/// UDP pseudo-stream
#[derive(Clone)]
#[must_use]
pub struct UdpStream {
    socket: Arc<UdpSocket>,
    address: SocketAddr,
}

impl UdpStream {
    fn new(socket: Arc<UdpSocket>, address: SocketAddr) -> UdpStream {
        UdpStream { socket, address }
    }
}

#[async_trait]
impl Transmitter for UdpStream {
    async fn send(&mut self, packet: Packet) -> Result<()> {
        packet.send(&self.socket, &self.address).await
    }
}

/// Receives [`Packet`]s over UDP
#[must_use]
pub struct UdpReceiver {
    socket: Arc<UdpSocket>,
}

impl UdpReceiver {
    /// Creates an UDP receiver
    ///
    /// # Errors
    /// - Cannot bind to `socket_address`
    pub async fn new(socket_address: &SocketAddr) -> Result<Self> {
        Ok(Self {
            socket: Arc::new(UdpSocket::bind(socket_address).await?),
        })
    }
}

#[async_trait]
impl Receiver<UdpStream> for UdpReceiver {
    async fn receive(&mut self) -> Result<(Packet, UdpStream)> {
        Box::pin(Packet::receive(&self.socket))
            .await
            .map(|(packet, address)| (packet, UdpStream::new(Arc::clone(&self.socket), address)))
    }
}

//
// TcpReceiver

/// Receives [`Packet`]s over TCP
#[must_use]
pub struct TcpReceiver {
    listener: Arc<TcpListener>,
}

impl TcpReceiver {
    /// Creates TCP receiver
    ///
    /// # Errors
    /// - Cannot bind to `socket_address`
    pub async fn new(socket_address: &SocketAddr) -> Result<Self> {
        Ok(Self {
            listener: Arc::new(TcpListener::bind(socket_address).await?),
        })
    }
}

#[async_trait]
impl Receiver<Arc<Mutex<TcpStream>>> for TcpReceiver {
    async fn receive(&mut self) -> Result<(Packet, Arc<Mutex<TcpStream>>)> {
        match self.listener.accept().await.context("accept") {
            Ok((mut stream, _)) => Ok((
                Packet::read(&mut stream).await?,
                Arc::new(Mutex::new(stream)),
            )),
            // Self::spawn(
            //     "TCP connection",
            //     async move { this.communicate(stream).await },
            // );
            Err(error) => Err(error),
        }
    }
}

//
// TlsAuth, TlsServiceConfig, TlsClientConfig, TlsReceiver

/// Authentication pair: key and certificate chain
#[must_use]
pub struct TlsAuth<'a> {
    key: &'a Path,
    certificates: &'a Path,
}

impl<'a> TlsAuth<'a> {
    pub fn new(key: &'a PathBuf, certificates: &'a PathBuf) -> Self {
        Self { key, certificates }
    }
}

impl<'a> TlsAuth<'a> {
    fn load_key(&self) -> Result<PrivateKey> {
        TlsUtil::load_key(self.key)
    }
}

/// TLS server configuration
#[must_use]
pub struct TlsServerConfig<'a> {
    server_auth: TlsAuth<'a>,
    client_certificates: Option<&'a Path>,
}

impl<'a> TlsServerConfig<'a> {
    pub fn new(
        server_key: &'a Path,
        server_certificates: &'a Path,
        client_auth: Option<&'a Path>,
    ) -> Self {
        TlsServerConfig {
            server_auth: TlsAuth {
                key: server_key,
                certificates: server_certificates,
            },
            client_certificates: client_auth,
        }
    }

    fn acceptor(&self) -> Result<TlsAcceptor> {
        Ok(TlsAcceptor::from(Arc::new(self.config()?)))
    }

    fn config(&self) -> Result<ServerConfig> {
        let certificates = TlsUtil::load_certificates(self.server_auth.certificates)?;
        let key = self.server_auth.load_key()?;
        ServerConfig::builder()
            .with_safe_defaults()
            .with_client_cert_verifier(match self.client_certificates {
                None => Ok(NoClientAuth::boxed()) as Result<_>,
                Some(path) => {
                    Ok(AllowAnyAuthenticatedClient::new(TlsUtil::load_root_cert(path)?).boxed())
                }
            }?)
            .with_single_cert(certificates, key)
            .context("Invalid private key")
    }
}

/// TLS client configuration
#[must_use]
pub struct TlsClientConfig<'a> {
    server_certificates: &'a Path,
    client_auth: Option<TlsAuth<'a>>,
}

impl<'a> TlsClientConfig<'a> {
    pub fn new(server_certificates: &'a PathBuf, client_auth: Option<TlsAuth<'a>>) -> Self {
        Self {
            server_certificates,
            client_auth,
        }
    }
}

impl<'a> TlsClientConfig<'a> {
    /// Build [`ClientConfig`]
    ///
    /// # Errors
    /// - Cannot load server certificates
    /// - Cannot load client auth key or certificates
    pub fn config(&self) -> Result<ClientConfig> {
        let builder = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(TlsUtil::load_root_cert(self.server_certificates)?);
        match self.client_auth {
            None => Ok(builder.with_no_client_auth()),
            Some(ref auth) => builder
                .with_single_cert(
                    TlsUtil::load_certificates(auth.certificates)?,
                    auth.load_key()?,
                )
                .context("Client auth"),
        }
    }
}

struct TlsUtil;

impl TlsUtil {
    fn load_key(path: &Path) -> Result<PrivateKey> {
        rustls_pemfile::pkcs8_private_keys(&mut Self::open(path, "Private key")?)
            .context(format!("Invalid key file {path:?}"))
            .and_then(|keys| match keys.len() {
                0 => Err(anyhow!("No private keys found in {path:?}")),
                1 => Ok(PrivateKey(keys.into_iter().next().expect("index checked"))),
                _ => Err(anyhow!("Multiple private keys found in {path:?}")),
            })
    }

    fn load_certificates(path: &Path) -> Result<Vec<Certificate>> {
        rustls_pemfile::certs(&mut Self::open(path, "Certificates file")?)
            .context(format!("Invalid certificates file {path:?}"))
            .and_then(|certs| match certs.len() {
                0 => Err(anyhow!("No certificates found in {path:?}")),
                _ => Ok(certs.into_iter().map(Certificate).collect()),
            })
    }

    fn open(path: &Path, context: &str) -> Result<std::io::BufReader<std::fs::File>> {
        let file = std::fs::File::open(path).context(format!("{context} {path:?} not found"))?;
        Ok(std::io::BufReader::new(file))
    }

    fn load_root_cert(path: &Path) -> Result<RootCertStore> {
        let mut root_cert_store = RootCertStore::empty();
        for certificate in Self::load_certificates(path)? {
            root_cert_store.add(&certificate)?;
        }
        Ok(root_cert_store)
    }
}

/// Receives [`Packet`]s over TLS

#[must_use]
pub struct TlsReceiver {
    acceptor: TlsAcceptor,
    listener: TcpListener,
}

impl TlsReceiver {
    /// Creates a new TLS receiver
    ///
    /// # Errors
    /// Error error if
    /// - `config` is invalid
    /// - cannot bind to specified `socket_address`
    pub async fn new(socket_address: &SocketAddr, config: &TlsServerConfig<'_>) -> Result<Self> {
        Ok(Self {
            acceptor: config.acceptor()?,
            listener: TcpListener::bind(&socket_address).await?,
        })
    }
}

#[async_trait]
impl Receiver<Arc<Mutex<TlsStream<TcpStream>>>> for TlsReceiver {
    async fn receive(&mut self) -> Result<(Packet, Arc<Mutex<TlsStream<TcpStream>>>)> {
        match self.listener.accept().await.context("accept") {
            Ok((tcp_stream, _)) => {
                let acceptor = self.acceptor.clone();
                let mut tls_stream: TlsStream<TcpStream> = acceptor.accept(tcp_stream).await?;
                Ok((
                    Packet::read(&mut tls_stream).await?,
                    Arc::new(Mutex::new(tls_stream)),
                ))
            }
            // Self::spawn(
            //     "TLS connection",
            //     async move { this.communicate(stream).await },
            // );
            Err(error) => Err(error),
        }
    }
}
