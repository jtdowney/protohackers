use std::net::{Ipv4Addr, SocketAddr};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{info, trace, warn};

use crate::{
    problem7::socket::LrcpListener,
    server::{ConnectionHandler, Server},
};

mod codec;
mod socket;

pub struct LrcpHandler;

#[async_trait]
impl ConnectionHandler for LrcpHandler {
    type State = ();

    async fn handle_connection(
        stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        _addr: SocketAddr,
        _state: Self::State,
    ) -> anyhow::Result<()> {
        let mut framed = Framed::new(stream, LinesCodec::new_with_max_length(10000));
        while let Some(frame) = framed.next().await {
            match frame {
                Ok(line) => {
                    let reversed = line.chars().rev().collect::<String>();
                    trace!(?line, ?reversed, "received data");

                    framed.send(reversed).await?;
                }
                Err(e) => {
                    warn!("error reading application frame: {}", e);
                    continue;
                }
            }
        }

        Ok(())
    }
}

pub struct LrcpServer;

impl Server for LrcpServer {
    fn start(
        port: u16,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + std::marker::Send + 'static {
        async move {
            let bind = (Ipv4Addr::UNSPECIFIED, port);
            let mut listener = LrcpListener::bind(bind).await?;

            loop {
                let (stream, addr) = listener.accept().await?;
                info!("connection from {addr}");

                tokio::spawn(async move {
                    if let Err(e) = LrcpHandler::handle_connection(stream, addr, ()).await {
                        warn!(error = ?e, "error handling client");
                    }
                });
            }
        }
    }
}
