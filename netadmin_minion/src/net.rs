use std::net::SocketAddr;
use std::str;

use anyhow::{anyhow, Context, Result};
use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UdpSocket;

use async_trait::async_trait;

//
// Message
pub trait Message: Serialize + for<'a> Deserialize<'a> + Send + Sync {
    const PACKET_TYPE: u32;

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
    //
    /// Returns error if
    /// - payload is not valid UTF-8 string
    /// - payload is not valid JSON
    /// - payload is not generated by [`PacketType::json`]
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

#[async_trait]
pub trait MessageTransmitter {
    async fn send<M: Message>(&mut self, message: M) -> Result<()>;
}

pub struct JsonTransmitter<T: Transmitter> {
    transmitter: T,
}

impl<T: Transmitter> JsonTransmitter<T> {
    pub fn new(transmitter: T) -> JsonTransmitter<T> {
        JsonTransmitter { transmitter }
    }
}

#[async_trait]
impl<T: Transmitter + Send + Sync> MessageTransmitter for JsonTransmitter<T> {
    async fn send<M: Message>(&mut self, message: M) -> Result<()> {
        self.transmitter.send(message.to_packet()).await
    }
}

//
// Packet

pub struct Packet {
    ty: u32,
    payload: Vec<u8>,
}

impl Packet {
    const MAX_SIZE: usize = 0x10000 - 4 * 2;

    pub async fn write<T: AsyncWrite + Unpin>(&self, stream: &mut T) -> Result<()> {
        stream.write_u32(self.payload.len() as u32).await?;
        stream.write_u32(self.ty).await?;
        stream.write_all(&self.payload).await?;
        stream.flush().await?;
        Ok(())
    }

    pub async fn read<T: AsyncRead + Unpin>(stream: &mut T) -> Result<Packet> {
        let size = stream.read_u32().await? as usize;
        if size > Self::MAX_SIZE {
            return Err(anyhow!("Packet too large"));
        }

        let ty = stream.read_u32().await?;

        let mut payload = vec![0; size];
        stream.read_exact(&mut payload).await?;

        Ok(Self::new(ty, payload))
    }

    fn new(ty: u32, payload: Vec<u8>) -> Packet {
        Packet { ty, payload }
    }

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

    pub async fn send(&self, socket: &UdpSocket, addr: &SocketAddr) -> Result<()> {
        let mut data = Vec::with_capacity(8 + self.payload.len());
        data.put_u32(self.payload.len() as u32);
        data.put_u32(self.ty);
        data.put_slice(&self.payload);
        socket.send_to(&data, addr).await?;
        Ok(())
    }

    /// Interprets payload as UTF-8 encoded JSON string
    //
    /// # Errors
    /// Returns payload if input is not valid UTF-8 string
    fn as_str(&self) -> Result<&str> {
        str::from_utf8(&self.payload).context("Invalid UTF-8")
    }
}

//
// Transmitter

#[async_trait]
pub trait Transmitter {
    async fn send(&mut self, packet: Packet) -> Result<()>;
}

#[async_trait]
impl<T: AsyncWrite + Send + Unpin> Transmitter for T {
    async fn send(&mut self, packet: Packet) -> Result<()> {
        packet.write(self).await
    }
}

pub struct UdpStream<'a> {
    socket: &'a UdpSocket,
    address: &'a SocketAddr,
}

impl<'a> UdpStream<'a> {
    pub fn new(socket: &'a UdpSocket, address: &'a SocketAddr) -> UdpStream<'a> {
        UdpStream { socket, address }
    }
}

#[async_trait]
impl<'a> Transmitter for UdpStream<'a> {
    async fn send(&mut self, packet: Packet) -> Result<()> {
        packet.send(self.socket, self.address).await
    }
}
