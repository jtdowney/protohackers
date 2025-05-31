use std::{future::Future, pin::Pin};

use argh::FromArgs;
use futures_util::future::join_all;
use tokio::task::JoinHandle;
use tracing::info;

use crate::server::{Server, TcpServer};

mod codec;
mod problem0;
mod problem1;
mod problem10;
mod problem11;
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

type ServerFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'static>>;

/// Configuration for a server instance
struct ServerConfig {
    name: &'static str,
    port_offset: u16,
    starter: fn(u16) -> ServerFuture,
}

impl ServerConfig {
    fn new<S: Server>(name: &'static str, port_offset: u16) -> Self {
        Self {
            name,
            port_offset,
            starter: |port| Box::pin(S::start(port)),
        }
    }
}

/// Creates all the servers to start
fn create_servers() -> Vec<ServerConfig> {
    vec![
        ServerConfig::new::<TcpServer<problem0::Handler>>("problem0", 0),
        ServerConfig::new::<TcpServer<problem1::Handler>>("problem1", 1),
        ServerConfig::new::<TcpServer<problem2::Handler>>("problem2", 2),
        ServerConfig::new::<TcpServer<problem3::Handler>>("problem3", 3),
        ServerConfig::new::<problem4::KvStoreServer>("problem4", 4),
        ServerConfig::new::<TcpServer<problem5::Handler>>("problem5", 5),
        ServerConfig::new::<TcpServer<problem6::Handler>>("problem6", 6),
        ServerConfig::new::<problem7::LrcpServer>("problem7", 7),
        ServerConfig::new::<TcpServer<problem8::Handler>>("problem8", 8),
        ServerConfig::new::<TcpServer<problem9::Handler>>("problem9", 9),
        ServerConfig::new::<TcpServer<problem10::Handler>>("problem10", 10),
        ServerConfig::new::<TcpServer<problem11::Handler>>("problem11", 11),
    ]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let Args { port } = argh::from_env();

    let servers = create_servers();
    let mut handles: Vec<JoinHandle<anyhow::Result<()>>> = Vec::with_capacity(servers.len());

    for server_config in servers.iter() {
        let server_port = port + server_config.port_offset;
        info!("starting {} on port {}", server_config.name, server_port);

        let handle = tokio::spawn((server_config.starter)(server_port));
        handles.push(handle);
    }

    // Wait for all servers to complete concurrently (they shouldn't under normal circumstances)
    let results = join_all(handles).await;
    for (i, result) in results.into_iter().enumerate() {
        if let Err(e) = result {
            tracing::error!("server {} panicked: {:?}", servers[i].name, e);
        }
    }

    Ok(())
}
