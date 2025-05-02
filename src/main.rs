mod codec;
mod problem0;
mod problem1;
mod problem10;
mod problem2;
mod problem3;
mod problem4;
mod problem5;
mod problem6;
mod problem7;
mod problem8;
mod problem9;

use std::{
    future::Future,
    net::{Ipv4Addr, SocketAddr},
};

use argh::FromArgs;
use tokio::net::{TcpListener, TcpStream};
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

    let server0 = tokio::spawn(create_server(port, problem0::handle));
    let server1 = tokio::spawn(create_server(port + 1, problem1::handle));
    let server2 = tokio::spawn(create_server(port + 2, problem2::handle));
    let server3 = tokio::spawn(create_server(port + 3, problem3::handle));
    let server4 = tokio::spawn(problem4::start(port + 4));
    let server5 = tokio::spawn(create_server(port + 5, problem5::handle));
    let server6 = tokio::spawn(create_server(port + 6, problem6::handle));
    let server7 = tokio::spawn(problem7::start(port + 7));
    let server8 = tokio::spawn(create_server(port + 8, problem8::handle));
    let server9 = tokio::spawn(create_server(port + 9, problem9::handle));
    let server10 = tokio::spawn(create_server(port + 10, problem10::handle));

    let _ = tokio::join!(
        server0, server1, server2, server3, server4, server5, server6, server7, server8, server9,
        server10
    );

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
    info!("listening on {bind:?}");

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
