use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use tracing::{debug, error, info};

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

            serve(reader, writer).await;
        });
    }
}

async fn serve<R, W>(mut reader: R, mut writer: W)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = [0; 1024];
    loop {
        let n = match reader.read(&mut buf).await {
            Ok(n) if n == 0 => return,
            Ok(n) => n,
            Err(e) => {
                error!(error = ?e, "failed to read from socket");
                return;
            }
        };

        if let Err(e) = writer.write_all(&buf[0..n]).await {
            error!(error = ?e, "failed to write to socket");
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use tracing_test::traced_test;

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

        serve(reader, writer).await;

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn read_error() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read_error(io::ErrorKind::ConnectionReset.into())
            .build();
        let (reader, writer) = tokio::io::split(stream);

        serve(reader, writer).await;

        assert!(logs_contain(
            "failed to read from socket error=Kind(ConnectionReset)"
        ));

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn write_error() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"test123")
            .write_error(io::ErrorKind::ConnectionReset.into())
            .build();
        let (reader, writer) = tokio::io::split(stream);

        serve(reader, writer).await;

        assert!(logs_contain(
            "failed to write to socket error=Kind(ConnectionReset)"
        ));

        Ok(())
    }
}
