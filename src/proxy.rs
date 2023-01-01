use std::net::{IpAddr, SocketAddr};

use eyre::{bail, eyre, ContextCompat};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

pub async fn read_proxy_proto<R: AsyncBufRead + Unpin>(reader: &mut R) -> eyre::Result<SocketAddr> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    let mut parts = line.split_ascii_whitespace();
    let magic = parts.next().context("magic is missing")?;
    if magic != "PROXY" {
        bail!("magic is incorrect, got: {magic:?}")
    }

    let protocol = parts.next().context("protocol is missing")?;
    let protocol = match protocol {
        "TCP4" | "TCP6" => protocol,
        other => bail!("protocol is incorrect, got {other:?}"),
    };

    let source_address: IpAddr =
        parts
            .next()
            .context("source address is missing")
            .and_then(|addr| {
                addr.parse()
                    .map_err(|_| eyre!("source address is incorrect, got {addr:?}"))
            })?;

    match (protocol, source_address) {
        ("TCP4", IpAddr::V4(_)) | ("TCP6", IpAddr::V6(_)) => {}
        _ => bail!("source address protocol mismatch, expected {protocol} got {source_address}"),
    }

    let destination_address: IpAddr = parts
        .next()
        .context("destination address is missing")
        .and_then(|addr| {
            addr.parse()
                .map_err(|_| eyre!("destination address is incorrect, got {addr:?}"))
        })?;

    match (protocol, destination_address) {
        ("TCP4", IpAddr::V4(_)) | ("TCP6", IpAddr::V6(_)) => {}
        _ => {
            bail!("destination address protocol mismatch, expected {protocol} got {source_address}")
        }
    }

    let source_port: u16 = parts
        .next()
        .context("source port is missing")
        .and_then(|port| {
            port.parse()
                .map_err(|_| eyre!("source port is incorrect, got {port:?}"))
        })?;

    let _destination_port: u16 = parts
        .next()
        .context("destination port is missing")
        .and_then(|port| {
            port.parse()
                .map_err(|_| eyre!("destination port is incorrect, got {port:?}"))
        })?;

    Ok(SocketAddr::new(source_address, source_port))
}
