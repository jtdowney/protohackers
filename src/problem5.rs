use std::{net::SocketAddr, str};

use anyhow::bail;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_util::codec::Framed;
use trust_dns_resolver::{
    config::{ResolverConfig, ResolverOpts},
    AsyncResolver,
};

use crate::codec::StrictLinesCodec;

const UPSTREAM_HOST: &str = "chat.protohackers.com";
const UPSTREAM_PORT: u16 = 16963;
const REPLACEMENT_ADDRESS: &str = "7YWHMfk9JZe0LM0g1ZauHuiSxhI";

pub async fn handle<T>(stream: T, _addr: SocketAddr, _state: ()) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let resolver = AsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default())?;
    let response = resolver.lookup_ip(UPSTREAM_HOST).await?;

    let upstream_addrs = response
        .iter()
        .map(|ip| SocketAddr::new(ip, UPSTREAM_PORT))
        .collect::<Vec<_>>();
    let upstream = TcpStream::connect(upstream_addrs.as_slice()).await?;

    let mut upstream = Framed::new(upstream, StrictLinesCodec::default());
    let mut client = Framed::new(stream, StrictLinesCodec::default());

    loop {
        tokio::select! {
            result = client.next() => match result {
                Some(Ok(message)) => {
                    let message = rewrite_message(&message);
                    upstream.send(message).await?;
                }
                Some(Err(e)) => {
                    bail!("an error occurred while processing client message; error = {:?}", e);
                }
                None => break,
            },
            result = upstream.next() => match result {
                Some(Ok(message)) => {
                    let message = rewrite_message(&message);
                    client.send(message).await?;
                }
                Some(Err(e)) => {
                    bail!("an error occurred while processing upstream message; error = {:?}", e);
                }
                None => break,
            },
        }
    }

    Ok(())
}

fn rewrite_message(message: &str) -> String {
    message
        .split(' ')
        .map(|part| {
            if is_boguscoin_address(part) {
                REPLACEMENT_ADDRESS
            } else {
                part
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_boguscoin_address(s: &str) -> bool {
    s.len() >= 26
        && s.len() <= 35
        && s.starts_with('7')
        && s.chars().all(|c| c.is_ascii_alphanumeric())
}
