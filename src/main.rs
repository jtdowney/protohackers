mod problem0;
mod problem1;
mod problem2;
mod proxy;

use std::{future::Future, net::Ipv4Addr};

use argh::FromArgs;
use tokio::{
    io::BufReader,
    net::{TcpListener, TcpStream},
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

    let _ = tokio::join!(problem0, problem1, problem2);

    Ok(())
}

async fn create_server<F, Fut>(port: u16, proxy_protocol: bool, handle: F) -> eyre::Result<()>
where
    F: Fn(BufReader<TcpStream>) -> Fut + Send + Sync + Copy + 'static,
    Fut: Future<Output = eyre::Result<()>> + Send + 'static,
{
    let bind = (Ipv4Addr::UNSPECIFIED, port);
    let listener = TcpListener::bind(bind).await?;
    info!("listening on on {bind:?}");

    loop {
        let (stream, addr) = listener.accept().await?;

        tokio::spawn(async move {
            let mut stream = BufReader::new(stream);

            if proxy_protocol {
                match read_proxy_proto(&mut stream).await {
                    Ok(addr) => info!("connection from {addr}"),
                    Err(proxy::Error::EmptyHeader) => return,
                    Err(e) => {
                        debug!(error = ?e, "failed to read proxy protocol");
                        return;
                    }
                }
            } else {
                info!("connection from {addr}");
            }

            if let Err(e) = handle(stream).await {
                warn!(error = ?e, "error handling client");
            }
        });
    }
}
