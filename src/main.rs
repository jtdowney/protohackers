mod problem0;
mod problem1;
mod problem2;
mod problem3;
mod problem4;

use std::{
    future::Future,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use argh::FromArgs;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tracing::{info, warn};

#[derive(FromArgs)]
/// Protohackers
struct Args {
    /// start port
    #[argh(option, short = 'p', default = "10000")]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let Args { port } = argh::from_env();

    let problem0 = tokio::spawn(create_server(port, problem0::handle));
    let problem1 = tokio::spawn(create_server(port + 1, problem1::handle));
    let problem2 = tokio::spawn(create_server(port + 2, problem2::handle));
    let problem3 = tokio::spawn(create_server(port + 3, problem3::handle));
    let problem4 = tokio::spawn(create_udp_server(port + 4, problem4::handle));

    let _ = tokio::join!(problem0, problem1, problem2, problem3, problem4);

    Ok(())
}

async fn create_server<F, Fut, S>(port: u16, handle: F) -> anyhow::Result<()>
where
    F: Fn(TcpStream, SocketAddr, S) -> Fut + Send + Sync + Copy + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    S: Default + Send + Sync + Clone + 'static,
{
    let bind = (Ipv4Addr::UNSPECIFIED, port);
    let listener = TcpListener::bind(bind).await?;
    info!("listening on on {bind:?}");

    let state = S::default();

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("connection from {addr}");

        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, addr, state).await {
                warn!(error = ?e, "error handling client");
            }
        });
    }
}

async fn create_udp_server<F, Fut, S>(port: u16, handle: F) -> anyhow::Result<()>
where
    F: Fn(Arc<UdpSocket>, Vec<u8>, SocketAddr, S) -> Fut + Send + Sync + Copy + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    S: Default + Send + Sync + Clone + 'static,
{
    let bind = (Ipv4Addr::UNSPECIFIED, port);
    let socket = Arc::new(UdpSocket::bind(bind).await?);
    info!("listening on on {bind:?}");

    let state = S::default();

    loop {
        let socket = socket.clone();
        let mut buffer = [0; 1000];
        let (n, addr) = socket.recv_from(&mut buffer).await?;
        let data = buffer[0..n].to_vec();

        let state = state.clone();
        tokio::spawn(async move {
            info!("{n} byte datagram from {addr}");

            if let Err(e) = handle(socket, data, addr, state).await {
                warn!(error = ?e, "error handling client");
            }
        });
    }
}
