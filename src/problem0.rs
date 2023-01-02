use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub async fn handle<T>(mut stream: T) -> eyre::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut buffer = [0; 1024];
    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            break;
        }

        stream.write_all(&buffer[0..n]).await?;
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

        let _ = handle(stream).await;

        Ok(())
    }
}
