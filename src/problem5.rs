use std::net::SocketAddr;

use async_trait::async_trait;
use futures_util::StreamExt;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    try_join,
};
use tokio_util::codec::Framed;

use crate::{codec::StrictLinesCodec, server::ConnectionHandler};

const UPSTREAM_ADDR: &str = "chat.protohackers.com:16963";
const REPLACEMENT_ADDRESS: &str = "7YWHMfk9JZe0LM0g1ZauHuiSxhI";

pub struct Handler;

#[async_trait]
impl ConnectionHandler for Handler {
    type State = ();

    async fn handle_connection(
        stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        _addr: SocketAddr,
        _state: Self::State,
    ) -> anyhow::Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boguscoin_address_detection() {
        // Valid addresses
        assert!(is_boguscoin_address("7F1u3wSD5RbOHQmupo9nx4TnhQ"));
        assert!(is_boguscoin_address("7jgvRVooHVDzKEz3ZSLwipYSXj"));

        // Too short
        assert!(!is_boguscoin_address("7F1u3wSD"));

        // Too long
        assert!(!is_boguscoin_address(
            "7F1u3wSD5RbOHQmupo9nx4TnhQabcdefghijklmnop"
        ));

        // Doesn't start with 7
        assert!(!is_boguscoin_address("8F1u3wSD5RbOHQmupo9nx4TnhQ"));

        // Has non-alphanumeric characters
        assert!(!is_boguscoin_address("7F1u3wSD5RbOHQmupo9nx4-TnhQ"));
    }
}
