use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use tracing::{debug, info, warn};

use crate::proxy::{self, read_proxy_proto};

pub async fn start(port: u16) -> eyre::Result<()> {
    let bind = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&bind).await?;
    info!("listening on on {bind}");

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let (reader, writer) = socket.split();
            let mut reader = BufReader::new(reader);

            match read_proxy_proto(&mut reader).await {
                Ok(addr) => info!("connection from {addr}"),
                Err(proxy::Error::EmptyHeader) => return,
                Err(e) => {
                    debug!(error = ?e, "failed to read proxy protocol");
                    return;
                }
            }

            if let Err(e) = serve(reader, writer).await {
                warn!(error = ?e, "error serving client");
            }
        });
    }
}

async fn serve<R, W>(mut reader: R, mut writer: W) -> eyre::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = [0; 1024];
    loop {
        let n = reader.read(&mut buffer).await?;
        if n == 0 {
            break;
        }

        writer.write_all(&buffer[0..n]).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn echo() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"test123")
            .write(b"test123")
            .read(b"foobar")
            .write(b"foobar")
            .build();
        let (reader, writer) = tokio::io::split(stream);

        serve(reader, writer).await?;

        Ok(())
    }
}
