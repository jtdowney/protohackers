mod problem0;
mod problem1;
mod problem2;
mod problem3;
mod problem4;
mod proxy;

use std::{
    future::Future,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use argh::FromArgs;
use tokio::{
    io::BufReader,
    net::{TcpListener, TcpStream, UdpSocket},
};
use tracing::{debug, info, warn};

use crate::proxy::read_proxy_proto;

#[derive(FromArgs)]
/// Protohackers
struct Args {
    /// read PROXY protocol header
    #[argh(switch)]
    proxy_protocol: bool,

    /// start port
    #[argh(option, short = 'p', default = "10000")]
    port: u16,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();
    stable_eyre::install()?;

    let Args {
        port,
        proxy_protocol,
    } = argh::from_env();

    let problem0 = tokio::spawn(create_server(port, proxy_protocol, problem0::handle));
    let problem1 = tokio::spawn(create_server(port + 1, proxy_protocol, problem1::handle));
    let problem2 = tokio::spawn(create_server(port + 2, proxy_protocol, problem2::handle));
    let problem3 = tokio::spawn(create_server(port + 3, proxy_protocol, problem3::handle));
    let problem4 = tokio::spawn(create_udp_server(port + 4, problem4::handle));

    let _ = tokio::join!(problem0, problem1, problem2, problem3, problem4);

    Ok(())
}

async fn create_server<F, Fut, S>(port: u16, proxy_protocol: bool, handle: F) -> eyre::Result<()>
where
    F: Fn(BufReader<TcpStream>, SocketAddr, S) -> Fut + Send + Sync + Copy + 'static,
    Fut: Future<Output = eyre::Result<()>> + Send + 'static,
    S: Default + Send + Sync + Clone + 'static,
{
    let bind = (Ipv4Addr::UNSPECIFIED, port);
    let listener = TcpListener::bind(bind).await?;
    info!("listening on on {bind:?}");

    let state = S::default();

    loop {
        let (stream, mut addr) = listener.accept().await?;

        let state = state.clone();
        tokio::spawn(async move {
            let mut stream = BufReader::new(stream);

            if proxy_protocol {
                match read_proxy_proto(&mut stream).await {
                    Ok(a) => addr = a,
                    Err(proxy::Error::EmptyHeader) => return,
                    Err(e) => {
                        debug!(error = ?e, "failed to read proxy protocol");
                        return;
                    }
                }
            }

            info!("connection from {addr}");

            if let Err(e) = handle(stream, addr, state).await {
                warn!(error = ?e, "error handling client");
            }
        });
    }
}

async fn create_udp_server<F, Fut, S>(port: u16, handle: F) -> eyre::Result<()>
where
    F: Fn(Arc<UdpSocket>, Vec<u8>, SocketAddr, S) -> Fut + Send + Sync + Copy + 'static,
    Fut: Future<Output = eyre::Result<()>> + Send + 'static,
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
