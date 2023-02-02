use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncWrite};

pub async fn handle<T>(stream: T, _addr: SocketAddr, _state: ()) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite,
{
    let (mut rd, mut wr) = tokio::io::split(stream);
    tokio::io::copy(&mut rd, &mut wr).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn echo() -> anyhow::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .read(b"test123")
            .write(b"test123")
            .read(b"foobar")
            .write(b"foobar")
            .build();

        let _ = handle(stream, "127.0.0.1:1024".parse()?, ()).await;

        Ok(())
    }
}
