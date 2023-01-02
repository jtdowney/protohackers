use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

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

async fn handle_malformed_request<W>(writer: &mut W) -> eyre::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(b"malformed request\n").await?;
    Ok(())
}

pub async fn handle<T>(mut stream: T) -> eyre::Result<()>
where
    T: AsyncBufRead + AsyncWrite + Unpin,
{
    let mut buffer = String::new();
    loop {
        stream.read_line(&mut buffer).await?;
        let Request { method, number } = match serde_json::from_str(&buffer) {
            Ok(Request { method, .. }) if method != "isPrime" => {
                return handle_malformed_request(&mut stream).await
            }
            Err(_) => return handle_malformed_request(&mut stream).await,
            Ok(r) => r,
        };

        let prime = if let Some(n) = number.as_u64() {
            primes::is_prime(n)
        } else {
            false
        };

        let response = Response { method, prime };
        let data = serde_json::to_vec(&response)?;
        stream.write_all(&data).await?;
        stream.write_all(b"\n").await?;
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::BufReader;

    use super::*;

    #[tokio::test]
    async fn successful_request() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":13}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":true}\n")
            .build();
        let stream = BufReader::new(stream);

        let _ = handle(stream).await;

        Ok(())
    }

    #[tokio::test]
    async fn unsuccessful_request() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":123}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":false}\n")
            .build();
        let stream = BufReader::new(stream);

        let _ = handle(stream).await;

        Ok(())
    }

    #[tokio::test]
    async fn bad_json() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\"\",\"number\":123}\n")
            .write(b"malformed request\n")
            .build();
        let stream = BufReader::new(stream);

        handle(stream).await?;

        Ok(())
    }

    #[tokio::test]
    async fn bad_method() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"test\",\"number\":123}\n")
            .write(b"malformed request\n")
            .build();
        let stream = BufReader::new(stream);

        handle(stream).await?;

        Ok(())
    }

    #[tokio::test]
    async fn bad_number() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":\"123\"}\n")
            .write(b"malformed request\n")
            .build();
        let stream = BufReader::new(stream);

        let _ = handle(stream).await;

        Ok(())
    }

    #[tokio::test]
    async fn float() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":3.14}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":false}\n")
            .build();
        let stream = BufReader::new(stream);

        let _ = handle(stream).await;

        Ok(())
    }

    #[tokio::test]
    async fn ignores_extra_fields() -> eyre::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"{\"method\":\"isPrime\",\"number\":13,\"extra\":true}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":true}\n")
            .build();
        let stream = BufReader::new(stream);

        let _ = handle(stream).await;

        Ok(())
    }
}
