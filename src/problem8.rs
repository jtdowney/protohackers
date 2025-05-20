use std::net::SocketAddr;

use anyhow::bail;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, FramedParts, LinesCodec};
use tracing::{debug, trace, warn};

use crate::{
    problem8::cipher::{Cipher, CipherSpecCodec, CipherStream},
    server::ConnectionHandler,
};

mod cipher;

pub struct Handler;

#[async_trait]
impl ConnectionHandler for Handler {
    type State = ();

    async fn handle_connection(
        stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        _addr: SocketAddr,
        _state: Self::State,
    ) -> anyhow::Result<()> {
        let mut framed = Framed::new(stream, CipherSpecCodec::default());
        let spec = match framed.next().await {
            Some(Ok(s)) => s,
            Some(Err(e)) => {
                bail!("error reading spec: {:?}", e);
            }
            None => bail!("unexpected EOF while reading cipher spec"),
        };

        trace!(?spec, "read cipher spec");

        let FramedParts {
            io,
            read_buf,
            write_buf,
            ..
        } = framed.into_parts();

        let mut cipher = Cipher::from_iter(spec);
        if cipher.is_noop() {
            warn!("noop cipher detected, disconnecting");
            return Ok(());
        }

        let stream = CipherStream::new(io, cipher);
        let mut parts = FramedParts::new::<String>(stream, LinesCodec::new());
        parts.read_buf = read_buf;
        parts.write_buf = write_buf;

        let mut framed = Framed::from_parts(parts);
        while let Some(frame) = framed.next().await {
            match frame {
                Ok(line) => {
                    debug!(line, "got line");
                    let toys = line.split(',');
                    let max_toy = toys.max_by_key(|toy| {
                        toy.split('x')
                            .next()
                            .and_then(|s| s.parse::<u32>().ok())
                            .unwrap_or_default()
                    });

                    if let Some(toy) = max_toy {
                        debug!(toy, "sending");
                        framed.send(toy).await?;
                    } else {
                        warn!("no max toy, exiting");
                        break;
                    }
                }
                Err(e) => {
                    warn!(error = ?e, "received error while reading frame");
                    break;
                }
            }
        }

        Ok(())
    }
}
