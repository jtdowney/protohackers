use std::net::SocketAddr;

use futures_util::StreamExt;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    try_join,
};
use tokio_util::codec::Framed;

use crate::codec::StrictLinesCodec;

const UPSTREAM_ADDR: &str = "chat.protohackers.com:16963";
const REPLACEMENT_ADDRESS: &str = "7YWHMfk9JZe0LM0g1ZauHuiSxhI";

pub async fn handle<T>(stream: T, _addr: SocketAddr, _state: ()) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite,
{
    let server = TcpStream::connect(UPSTREAM_ADDR).await?;
    let (server_tx, server_rx) = Framed::new(server, StrictLinesCodec::default()).split();
    let (client_tx, client_rx) = Framed::new(stream, StrictLinesCodec::default()).split();

    let server_to_client = server_rx
        .map(|msg| msg.map(rewrite_message))
        .forward(client_tx);
    let client_to_server = client_rx
        .map(|msg| msg.map(rewrite_message))
        .forward(server_tx);

    try_join!(server_to_client, client_to_server)?;

    Ok(())
}

fn rewrite_message<S: AsRef<str>>(message: S) -> String {
    message
        .as_ref()
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
