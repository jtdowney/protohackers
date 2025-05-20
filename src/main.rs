use argh::FromArgs;

use crate::server::{Server, TcpServer};

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
mod server;

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

    let server0 = tokio::spawn(TcpServer::<problem0::Handler>::start(port));
    let server1 = tokio::spawn(TcpServer::<problem1::Handler>::start(port + 1));
    let server2 = tokio::spawn(TcpServer::<problem2::Handler>::start(port + 2));
    let server3 = tokio::spawn(TcpServer::<problem3::Handler>::start(port + 3));
    let server4 = tokio::spawn(problem4::KvStoreServer::start(port + 4));
    let server5 = tokio::spawn(TcpServer::<problem5::Handler>::start(port + 5));
    let server6 = tokio::spawn(TcpServer::<problem6::Handler>::start(port + 6));
    let server7 = tokio::spawn(problem7::LrcpServer::start(port + 7));
    let server8 = tokio::spawn(TcpServer::<problem8::Handler>::start(port + 8));
    let server9 = tokio::spawn(TcpServer::<problem9::Handler>::start(port + 9));
    let server10 = tokio::spawn(TcpServer::<problem10::Handler>::start(port + 10));

    let _ = tokio::join!(
        server0, server1, server2, server3, server4, server5, server6, server7, server8, server9,
        server10
    );

    Ok(())
}
