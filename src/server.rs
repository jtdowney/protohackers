use std::net::{Ipv4Addr, SocketAddr};

use async_trait::async_trait;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
};
use tracing::{info, warn};

#[async_trait]
pub trait ConnectionHandler: Send + Sync + 'static {
    type State: Default + Send + Sync + Clone + 'static;

    async fn handle_connection(
        stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        addr: SocketAddr,
        state: Self::State,
    ) -> anyhow::Result<()>;
}

pub trait Server {
    async fn start(port: u16) -> anyhow::Result<()>;
}

pub struct TcpServer<H: ConnectionHandler>(std::marker::PhantomData<H>);

impl<H: ConnectionHandler> Server for TcpServer<H> {
    async fn start(port: u16) -> anyhow::Result<()> {
        let bind = (Ipv4Addr::UNSPECIFIED, port);
        let listener = TcpListener::bind(bind).await?;
        info!("listening on {bind:?}");

        let state = H::State::default();

        loop {
            let (stream, addr) = listener.accept().await?;
            info!("connection from {addr}");

            let state = state.clone();
            tokio::spawn(async move {
                if let Err(e) = H::handle_connection(stream, addr, state).await {
                    warn!(error = ?e, "error handling client");
                }
            });
        }
    }
}
