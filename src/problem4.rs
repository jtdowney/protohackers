use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::{net::UdpSocket, sync::Mutex};

const VERSION: &str = "jtdowney protohackers";

pub async fn handle(
    socket: Arc<UdpSocket>,
    data: Vec<u8>,
    addr: SocketAddr,
    state: Arc<Mutex<HashMap<String, String>>>,
) -> eyre::Result<()> {
    let data = String::from_utf8(data)?;

    match (data.as_str(), data.split_once('=')) {
        (_, Some((key, value))) => {
            let key = key.to_owned();
            let value = value.to_owned();
            let mut state = state.lock().await;
            state.insert(key, value);
        }
        ("version", None) => {
            let response = format!("version={VERSION}");
            socket.send_to(response.as_bytes(), addr).await?;
        }
        (key, None) => {
            let entry = {
                let state = state.lock().await;
                state.get(key).cloned()
            };

            if let Some(value) = entry {
                let response = format!("{key}={value}");
                socket.send_to(response.as_bytes(), addr).await?;
            }
        }
    }

    Ok(())
}
