use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use tracing::{debug, info, warn};

use crate::proxy::{self, read_proxy_proto};

#[derive(Deserialize, Debug)]
struct Request {
    method: String,
    number: serde_json::Number,
}

#[derive(Serialize)]
struct Response {
    method: String,
    prime: bool,
}

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

async fn handle_malformed_request<W>(mut writer: W) -> eyre::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(b"malformed request\n").await?;
    Ok(())
}

async fn serve<R, W>(reader: R, mut writer: W) -> eyre::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        let Request { method, number } = match serde_json::from_str(&line) {
            Ok(Request { method, .. }) if method != "isPrime" => {
                return handle_malformed_request(writer).await
            }
            Err(_) => return handle_malformed_request(writer).await,
            Ok(r) => r,
        };

        let prime = if let Some(n) = number.as_u64() {
            primes::is_prime(n)
        } else {
            false
        };

        let response = Response { method, prime };
        let data = serde_json::to_vec(&response)?;
        writer.write_all(&data).await?;
        writer.write_all(b"\n").await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn successful_request() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":13}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":true}\n")
            .build();
        let (reader, writer) = tokio::io::split(stream);
        let reader = BufReader::new(reader);

        serve(reader, writer).await?;

        Ok(())
    }

    #[tokio::test]
    async fn unsuccessful_request() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":123}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":false}\n")
            .build();
        let (reader, writer) = tokio::io::split(stream);
        let reader = BufReader::new(reader);

        serve(reader, writer).await?;

        Ok(())
    }

    #[tokio::test]
    async fn bad_json() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\"\",\"number\":123}\n")
            .write(b"malformed request\n")
            .build();
        let (reader, writer) = tokio::io::split(stream);
        let reader = BufReader::new(reader);

        serve(reader, writer).await?;

        Ok(())
    }

    #[tokio::test]
    async fn bad_method() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"test\",\"number\":123}\n")
            .write(b"malformed request\n")
            .build();
        let (reader, writer) = tokio::io::split(stream);
        let reader = BufReader::new(reader);

        serve(reader, writer).await?;

        Ok(())
    }

    #[tokio::test]
    async fn bad_number() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":\"123\"}\n")
            .write(b"malformed request\n")
            .build();
        let (reader, writer) = tokio::io::split(stream);
        let reader = BufReader::new(reader);

        serve(reader, writer).await?;

        Ok(())
    }

    #[tokio::test]
    async fn float() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":3.14}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":false}\n")
            .build();
        let (reader, writer) = tokio::io::split(stream);
        let reader = BufReader::new(reader);

        serve(reader, writer).await?;

        Ok(())
    }

    #[tokio::test]
    async fn ignores_extra_fields() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":13,\"extra\":true}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":true}\n")
            .build();
        let (reader, writer) = tokio::io::split(stream);
        let reader = BufReader::new(reader);

        serve(reader, writer).await?;

        Ok(())
    }
}
