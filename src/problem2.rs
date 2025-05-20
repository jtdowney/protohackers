use std::{collections::BTreeMap, net::SocketAddr};

use anyhow::bail;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use futures_util::{SinkExt, StreamExt};
use nom::{Finish, IResult, Parser, branch::alt, bytes::complete::tag, number::complete::be_i32};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::server::ConnectionHandler;

const PACKET_SIZE: usize = 9;

#[derive(Debug)]
enum Packet {
    Insert { timestamp: i32, price: i32 },
    Query { start: i32, end: i32 },
}

fn packet(input: &[u8]) -> IResult<&[u8], Packet> {
    let mut packet_type_parser = alt((tag(&b"I"[..]), tag(&b"Q"[..])));
    let (input, packet_type) = packet_type_parser.parse(input)?;
    let mut first_parser = be_i32;
    let (input, first) = first_parser.parse(input)?;
    let mut second_parser = be_i32;
    let (input, second) = second_parser.parse(input)?;
    let packet = match packet_type {
        b"I" => Packet::Insert {
            timestamp: first,
            price: second,
        },
        b"Q" => Packet::Query {
            start: first,
            end: second,
        },
        _ => unreachable!(),
    };

    Ok((input, packet))
}

struct PacketCodec;

impl Decoder for PacketCodec {
    type Item = Packet;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < PACKET_SIZE {
            return Ok(None);
        }

        let buffer = src.split_to(PACKET_SIZE);
        match packet(&buffer).finish() {
            Ok((_, p)) => Ok(Some(p)),
            Err(e) => bail!("failed to parse {:?}: {:?}", buffer, e),
        }
    }
}

impl Encoder<i32> for PacketCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: i32, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.put_i32(item);
        Ok(())
    }
}

pub struct Handler;

#[async_trait]
impl ConnectionHandler for Handler {
    type State = ();

    async fn handle_connection(
        stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        _addr: SocketAddr,
        _state: Self::State,
    ) -> anyhow::Result<()> {
        let mut data = BTreeMap::new();
        let mut framed = Framed::new(stream, PacketCodec);
        while let Some(Ok(packet)) = framed.next().await {
            match packet {
                Packet::Insert { timestamp, price } => {
                    data.insert(timestamp, price);
                }
                Packet::Query { start, end } => {
                    if start > end {
                        framed.send(0).await?;
                        continue;
                    }

                    let items = data
                        .range(start..=end)
                        .map(|(_, &p)| p as isize)
                        .collect::<Vec<isize>>();
                    if items.is_empty() {
                        framed.send(0).await?;
                        continue;
                    }

                    let total = items.iter().sum::<isize>();
                    let mean = total / items.len() as isize;
                    framed.send(mean as i32).await?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn example_session() -> anyhow::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"I\x00\x00\x30\x39\x00\x00\x00\x65")
            .read(b"I\x00\x00\x30\x3a\x00\x00\x00\x66")
            .read(b"I\x00\x00\x30\x3b\x00\x00\x00\x64")
            .read(b"I\x00\x00\xa0\x00\x00\x00\x00\x05")
            .read(b"Q\x00\x00\x30\x00\x00\x00\x40\x00")
            .write(b"\x00\x00\x00\x65")
            .build();

        let _ = Handler::handle_connection(stream, "127.0.0.1:1024".parse()?, ()).await;

        Ok(())
    }

    #[tokio::test]
    async fn zero_items() -> anyhow::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"Q\x00\x00\x30\x00\x00\x00\x40\x00")
            .write(b"\x00\x00\x00\x00")
            .build();

        let _ = Handler::handle_connection(stream, "127.0.0.1:1024".parse()?, ()).await;

        Ok(())
    }

    #[tokio::test]
    async fn bad_range() -> anyhow::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"I\x00\x00\x30\x39\x00\x00\x00\x65")
            .read(b"I\x00\x00\x30\x3a\x00\x00\x00\x66")
            .read(b"I\x00\x00\x30\x3b\x00\x00\x00\x64")
            .read(b"I\x00\x00\xa0\x00\x00\x00\x00\x05")
            .read(b"Q\x00\x00\x40\x00\x00\x00\x30\x00")
            .write(b"\x00\x00\x00\x00")
            .build();

        let _ = Handler::handle_connection(stream, "127.0.0.1:1024".parse()?, ()).await;

        Ok(())
    }
}
