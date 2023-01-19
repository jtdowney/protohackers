mod cipher;

use std::net::SocketAddr;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader};
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{debug, trace, warn};

use crate::problem8::cipher::Cipher;

use self::cipher::CipherStream;

pub async fn handle<T>(stream: T, _addr: SocketAddr, _state: ()) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut spec = vec![];
    let mut stream = BufReader::new(stream);
    stream.read_until(0, &mut spec).await?;
    trace!(?spec, "read cipher spec");

    let mut cipher = Cipher::from_spec(&spec)?;
    if cipher.is_noop() {
        warn!("noop cipher detected, disconnecting");
        return Ok(());
    }

    let stream = CipherStream::new(stream, cipher);
    let mut framed = Framed::new(stream, LinesCodec::new());

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
