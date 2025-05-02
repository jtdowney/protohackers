use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use tokio::net::UdpSocket;
use tracing::{info, trace, warn};

type State = lockfree::map::Map<String, String>;
type SharedState = Arc<State>;

const VERSION: &str = "jtdowney protohackers";

pub async fn start(port: u16) -> anyhow::Result<()> {
    let bind = (Ipv4Addr::UNSPECIFIED, port);
    let socket = Arc::new(UdpSocket::bind(bind).await?);
    info!("listening on {bind:?}");

    let state = SharedState::default();

    loop {
        let socket = socket.clone();
        let mut buffer = [0; 1000];
        let (n, addr) = socket.recv_from(&mut buffer).await?;
        let data = buffer[0..n].to_vec();

        let state = state.clone();
        tokio::spawn(async move {
            trace!("{n} byte datagram from {addr}");

            if let Err(e) = handle(socket, data, addr, state).await {
                warn!(error = ?e, "error handling client");
            }
        });
    }
}

async fn handle(
    socket: Arc<UdpSocket>,
    data: Vec<u8>,
    addr: SocketAddr,
    state: SharedState,
) -> anyhow::Result<()> {
    let data = String::from_utf8(data)?;

    match (data.as_str(), data.split_once('=')) {
        (_, Some((key, value))) => {
            let key = key.to_owned();
            let value = value.to_owned();
            state.insert(key, value);
        }
        ("version", None) => {
            let response = format!("version={VERSION}");
            socket.send_to(response.as_bytes(), addr).await?;
        }
        (key, None) => {
            let entry = state.get(key).map(|guard| guard.val().to_owned());
            if let Some(value) = entry {
                let response = format!("{key}={value}");
                socket.send_to(response.as_bytes(), addr).await?;
            }
        }
    }

    Ok(())
}
