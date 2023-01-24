mod cipher;

use std::net::SocketAddr;

use anyhow::bail;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, FramedParts, LinesCodec};
use tracing::{debug, trace, warn};

use crate::problem8::cipher::{Cipher, CipherSpecCodec, CipherStream};

pub async fn handle<T>(stream: T, _addr: SocketAddr, _state: ()) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut framed = Framed::new(stream, CipherSpecCodec::default());
    let spec = match framed.next().await {
        Some(Ok(s)) => s,
        Some(Err(e)) => {
            bail!("error reading spec: {:?}", e);
        }
        None => todo!(),
    };

    trace!(?spec, "read cipher spec");

    let FramedParts {
        io,
        read_buf,
        write_buf,
        ..
    } = framed.into_parts();

    let mut cipher = Cipher::new(spec);
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
