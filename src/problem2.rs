use std::collections::BTreeMap;

use nom::{branch::alt, bytes::complete::tag, number::complete::be_i32, Finish, IResult};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use tracing::{debug, error, info, warn};

use crate::proxy::read_proxy_proto;

pub async fn start(port: u16) -> eyre::Result<()> {
    let bind = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&bind).await?;
    info!("listening on on {bind}");

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let (reader, writer) = socket.split();
            let mut reader = BufReader::new(reader);

            match read_proxy_proto(&mut reader).await {
                Ok(addr) => info!("connection from {addr}"),
                Err(e) => {
                    debug!(error = ?e, "failed to read proxy protocol");
                    return;
                }
            }

            if let Err(e) = serve(reader, writer).await {
                warn!(error = ?e, "error serving client");
            }
        });
    }
}

#[derive(Debug)]
enum Packet {
    Insert { timestamp: i32, price: i32 },
    Query { start: i32, end: i32 },
}

fn packet(input: &[u8]) -> IResult<&[u8], Packet> {
    let (input, packet_type) = alt((tag(b"I"), tag(b"Q")))(input)?;
    let (input, first) = be_i32(input)?;
    let (input, second) = be_i32(input)?;
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

async fn serve<R, W>(mut reader: R, mut writer: W) -> eyre::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = [0; 9];
    let mut data = BTreeMap::new();
    loop {
        if let Err(e) = reader.read_exact(&mut buf).await {
            error!(error = ?e, "failed to read from socket");
            break;
        }

        match packet(&buf).finish() {
            Ok((_, Packet::Insert { timestamp, price })) => {
                data.insert(timestamp, price);
            }
            Ok((_, Packet::Query { start, end })) => {
                if start > end {
                    writer.write_i32(0).await?;
                    continue;
                }

                let items = data
                    .range(start..=end)
                    .map(|(_, &p)| p as isize)
                    .collect::<Vec<isize>>();
                if items.is_empty() {
                    writer.write_i32(0).await?;
                    continue;
                }

                let total = items.iter().sum::<isize>();
                let mean = total / items.len() as isize;
                writer.write_i32(mean as i32).await?;
            }
            Err(e) => {
                warn!(error = ?e, data = ?buf, "failed to parse");
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn example_session() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"I\x00\x00\x30\x39\x00\x00\x00\x65")
            .read(b"I\x00\x00\x30\x3a\x00\x00\x00\x66")
            .read(b"I\x00\x00\x30\x3b\x00\x00\x00\x64")
            .read(b"I\x00\x00\xa0\x00\x00\x00\x00\x05")
            .read(b"Q\x00\x00\x30\x00\x00\x00\x40\x00")
            .write(b"\x00\x00\x00\x65")
            .build();
        let (reader, writer) = tokio::io::split(stream);
        let reader = BufReader::new(reader);

        serve(reader, writer).await?;

        Ok(())
    }

    #[tokio::test]
    async fn zero_items() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"Q\x00\x00\x30\x00\x00\x00\x40\x00")
            .write(b"\x00\x00\x00\x00")
            .build();
        let (reader, writer) = tokio::io::split(stream);
        let reader = BufReader::new(reader);

        serve(reader, writer).await?;

        Ok(())
    }

    #[tokio::test]
    async fn bad_range() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"I\x00\x00\x30\x39\x00\x00\x00\x65")
            .read(b"I\x00\x00\x30\x3a\x00\x00\x00\x66")
            .read(b"I\x00\x00\x30\x3b\x00\x00\x00\x64")
            .read(b"I\x00\x00\xa0\x00\x00\x00\x00\x05")
            .read(b"Q\x00\x00\x40\x00\x00\x00\x30\x00")
            .write(b"\x00\x00\x00\x00")
            .build();
        let (reader, writer) = tokio::io::split(stream);
        let reader = BufReader::new(reader);

        serve(reader, writer).await?;

        Ok(())
    }
}
