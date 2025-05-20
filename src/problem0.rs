use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::server::ConnectionHandler;

pub struct Handler;

#[async_trait]
impl ConnectionHandler for Handler {
    type State = ();

    async fn handle_connection(
        stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        _addr: SocketAddr,
        _state: Self::State,
    ) -> anyhow::Result<()> {
        let (mut rd, mut wr) = tokio::io::split(stream);
        tokio::io::copy(&mut rd, &mut wr).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn echo() -> anyhow::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"test123")
            .write(b"test123")
            .read(b"foobar")
            .write(b"foobar")
            .build();

        let _ = Handler::handle_connection(stream, "127.0.0.1:1024".parse()?, ()).await;

        Ok(())
    }
}
