use std::net::SocketAddr;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use crate::codec::JsonLinesCodec;

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

async fn malformed_request<T>(
    framed: &mut Framed<T, JsonLinesCodec<Request, Response>>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    framed
        .send(Response {
            method: "error".into(),
            prime: false,
        })
        .await?;
    Ok(())
}

pub async fn handle<T>(stream: T, _addr: SocketAddr, _state: ()) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut framed = Framed::new(stream, JsonLinesCodec::<Request, Response>::default());
    loop {
        let Request { method, number } = match framed.next().await {
            Some(Ok(Request { method, .. })) if method != "isPrime" => {
                return malformed_request(&mut framed).await
            }
            Some(Err(_)) => return malformed_request(&mut framed).await,
            Some(Ok(r)) => r,
            None => break,
        };

        let prime = if let Some(n) = number.as_u64() {
            primes::is_prime(n)
        } else {
            false
        };

        let response = Response { method, prime };
        framed.send(response).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::io::BufReader;

    use super::*;

    #[tokio::test]
    async fn successful_request() -> anyhow::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":13}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":true}\n")
            .build();
        let stream = BufReader::new(stream);

        let _ = handle(stream, "127.0.0.1:1024".parse()?, ()).await;

        Ok(())
    }

    #[tokio::test]
    async fn unsuccessful_request() -> anyhow::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":123}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":false}\n")
            .build();
        let stream = BufReader::new(stream);

        let _ = handle(stream, "127.0.0.1:1024".parse()?, ()).await;

        Ok(())
    }

    #[tokio::test]
    async fn bad_json() -> anyhow::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\"\",\"number\":123}\n")
            .write(b"{\"method\":\"error\",\"prime\":false}\n")
            .build();
        let stream = BufReader::new(stream);

        let _ = handle(stream, "127.0.0.1:1024".parse()?, ()).await;

        Ok(())
    }

    #[tokio::test]
    async fn bad_method() -> anyhow::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"test\",\"number\":123}\n")
            .write(b"{\"method\":\"error\",\"prime\":false}\n")
            .build();
        let stream = BufReader::new(stream);

        let _ = handle(stream, "127.0.0.1:1024".parse()?, ()).await;

        Ok(())
    }

    #[tokio::test]
    async fn bad_number() -> anyhow::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":\"123\"}\n")
            .write(b"{\"method\":\"error\",\"prime\":false}\n")
            .build();
        let stream = BufReader::new(stream);

        let _ = handle(stream, "127.0.0.1:1024".parse()?, ()).await;

        Ok(())
    }

    #[tokio::test]
    async fn float() -> anyhow::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":3.14}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":false}\n")
            .build();
        let stream = BufReader::new(stream);

        let _ = handle(stream, "127.0.0.1:1024".parse()?, ()).await;

        Ok(())
    }

    #[tokio::test]
    async fn ignores_extra_fields() -> anyhow::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":13,\"extra\":true}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":true}\n")
            .build();
        let stream = BufReader::new(stream);

        let _ = handle(stream, "127.0.0.1:1024".parse()?, ()).await;

        Ok(())
    }
}
