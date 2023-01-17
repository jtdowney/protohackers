mod codec;
mod socket;

use std::net::{Ipv4Addr, SocketAddr};

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{info, trace, warn};

use crate::problem7::socket::LrcpListener;

pub async fn start(port: u16) -> anyhow::Result<()> {
    let bind = (Ipv4Addr::UNSPECIFIED, port);
    let mut listener = LrcpListener::bind(bind).await?;
    info!("listening on on {bind:?}");

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("connection from {addr}");

        tokio::spawn(async move {
            if let Err(e) = handle(stream, addr).await {
                warn!(error = ?e, "error handling client");
            }
        });
    }
}

async fn handle<T>(stream: T, _addr: SocketAddr) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut framed = Framed::new(stream, LinesCodec::new_with_max_length(10000));
    loop {
        match framed.next().await {
            Some(Ok(line)) => {
                let reversed = line.chars().rev().collect::<String>();
                trace!(?line, ?reversed, "received data");

                framed.send(reversed).await?;
            }
            Some(Err(e)) => {
                warn!("error reading application frame: {}", e);
                continue;
            }
            None => break,
        }
    }

    Ok(())
}
