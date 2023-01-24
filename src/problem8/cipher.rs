use std::{
    io,
    pin::Pin,
    task::{self, ready, Poll},
};

use anyhow::bail;
use nom::{
    branch::alt,
    bytes::complete::tag,
    combinator::{map, value},
    multi::fold_many0,
    number::complete::be_u8,
    sequence::preceded,
    IResult,
};
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::codec::Decoder;

fn cipher_operation(input: &[u8]) -> IResult<&[u8], CipherOperation> {
    let reversebits = value(CipherOperation::ReverseBits, tag(b"\x01"));
    let xor = map(preceded(tag(b"\x02"), be_u8), CipherOperation::Xor);
    let xor_position = value(CipherOperation::XorPosition, tag(b"\x03"));
    let add = map(preceded(tag(b"\x04"), be_u8), CipherOperation::Add);
    let add_position = value(CipherOperation::AddPosition, tag(b"\x05"));
    alt((reversebits, xor, xor_position, add, add_position))(input)
}

fn cipher_spec(input: &[u8]) -> IResult<&[u8], Vec<CipherOperation>> {
    fold_many0(cipher_operation, Vec::new, |mut acc, operation| {
        acc.push(operation);
        acc
    })(input)
}

#[derive(Default)]
pub struct CipherSpecCodec {
    next_index: usize,
}

impl Decoder for CipherSpecCodec {
    type Item = Vec<CipherOperation>;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let read_to = src.len();
        let null_offset = src[self.next_index..read_to].iter().position(|&b| b == 0);

        match null_offset {
            Some(offset) => {
                let newline_index = offset + self.next_index;
                self.next_index = 0;
                let input = src.split_to(newline_index + 1);
                let input = &input[..input.len() - 1];
                match cipher_spec(input) {
                    Ok((_, spec)) => Ok(Some(spec)),
                    Err(e) => bail!("error reading spec: {:?}", e),
                }
            }
            None => {
                self.next_index = read_to;
                Ok(None)
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CipherOperation {
    ReverseBits,
    Xor(u8),
    XorPosition,
    Add(u8),
    AddPosition,
}

impl CipherOperation {
    fn forward(&self, byte: u8, position: u8) -> u8 {
        match self {
            CipherOperation::ReverseBits => byte.reverse_bits(),
            CipherOperation::Xor(n) => byte ^ n,
            CipherOperation::XorPosition => byte ^ position,
            CipherOperation::Add(n) => byte.wrapping_add(*n),
            CipherOperation::AddPosition => byte.wrapping_add(position),
        }
    }

    fn reverse(&self, byte: u8, position: u8) -> u8 {
        match self {
            CipherOperation::ReverseBits => byte.reverse_bits(),
            CipherOperation::Xor(n) => byte ^ n,
            CipherOperation::XorPosition => byte ^ position,
            CipherOperation::Add(n) => byte.wrapping_sub(*n),
            CipherOperation::AddPosition => byte.wrapping_sub(position),
        }
    }
}

#[derive(Clone)]
pub struct Cipher {
    spec: Vec<CipherOperation>,
    position: usize,
}

impl Cipher {
    pub fn new(spec: Vec<CipherOperation>) -> Self {
        Self { spec, position: 0 }
    }

    #[cfg(test)]
    pub fn from_spec(buffer: &[u8]) -> anyhow::Result<Self> {
        use nom::Finish;

        let spec = match cipher_spec(buffer).finish() {
            Ok((_, s)) => s,
            Err(e) => bail!("error parsing spec: {:?}", e),
        };

        Ok(Self { spec, position: 0 })
    }

    pub fn is_noop(&mut self) -> bool {
        let saved_position = self.position;
        let mut data = [0; 16];
        self.seal_in_place(&mut data);
        self.position = saved_position;
        data == [0; 16]
    }

    pub fn seal_in_place(&mut self, buffer: &mut [u8]) {
        for byte in buffer.iter_mut() {
            for op in &self.spec {
                *byte = op.forward(*byte, (self.position % 256) as u8);
            }

            self.position += 1;
        }
    }

    pub fn open_in_place(&mut self, buffer: &mut [u8]) {
        for byte in buffer.iter_mut() {
            for op in self.spec.iter().rev() {
                *byte = op.reverse(*byte, (self.position % 256) as u8);
            }

            self.position += 1;
        }
    }
}

#[pin_project]
pub struct CipherStream<T: AsyncRead + AsyncWrite> {
    #[pin]
    stream: T,
    reader_cipher: Cipher,
    writer_cipher: Cipher,
}

impl<T: AsyncRead + AsyncWrite> CipherStream<T> {
    pub fn new(stream: T, cipher: Cipher) -> Self {
        Self {
            stream,
            reader_cipher: cipher.clone(),
            writer_cipher: cipher,
        }
    }
}

impl<T: AsyncRead + AsyncWrite> AsyncRead for CipherStream<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.project();
        ready!(this.stream.poll_read(cx, buf))?;

        this.reader_cipher.open_in_place(buf.filled_mut());

        Poll::Ready(Ok(()))
    }
}

impl<T: AsyncRead + AsyncWrite> AsyncWrite for CipherStream<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.project();
        let mut buffer = buf.to_vec();
        this.writer_cipher.seal_in_place(&mut buffer);
        this.stream.poll_write(cx, &buffer)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Result<(), io::Error>> {
        let this = self.project();
        this.stream.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let this = self.project();
        this.stream.poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[test]
    fn cipher_encryption() -> anyhow::Result<()> {
        let mut cipher = Cipher::from_spec(b"\x02\x01\x01\x00")?;
        let mut buffer = b"hello".to_vec();
        cipher.seal_in_place(&mut buffer);
        assert_eq!(b"\x96\x26\xb6\xb6\x76", buffer.as_slice());

        let mut cipher = Cipher::from_spec(b"\x05\x05\x00")?;
        let mut buffer = b"hello".to_vec();
        cipher.seal_in_place(&mut buffer);
        assert_eq!(b"\x68\x67\x70\x72\x77", buffer.as_slice());

        Ok(())
    }

    #[test]
    fn encryption_and_decryption() -> anyhow::Result<()> {
        let spec = b"\x02\x32\x01\x05\x04\x80\x03\x01\x00";
        let mut cipher = Cipher::from_spec(spec)?;
        let plaintext = b"\xc6\xd3\xa7\x38\x1a\xbd\x54\x2a\xff\x13\x1f\xa5\x68\xa1\x22\x3c";
        let mut buffer = plaintext.to_vec();

        cipher.seal_in_place(&mut buffer);
        assert_ne!(plaintext, buffer.as_slice());

        let mut cipher = Cipher::from_spec(spec)?;
        cipher.open_in_place(&mut buffer);
        assert_eq!(plaintext, buffer.as_slice());

        Ok(())
    }

    #[tokio::test]
    async fn stream_read() -> anyhow::Result<()> {
        let buffer = Cursor::new(b"\x68\x67\x70\x72\x77".to_vec());
        let spec = b"\x05\x05\x00";
        let cipher = Cipher::from_spec(spec)?;
        let mut stream = CipherStream::new(buffer, cipher);
        let mut data = [0; 5];
        stream.read_exact(&mut data).await?;

        assert_eq!(b"hello", &data);

        Ok(())
    }

    #[tokio::test]
    async fn stream_write() -> anyhow::Result<()> {
        let mut raw_data = vec![];
        let buffer = Cursor::new(&mut raw_data);
        let spec = b"\x05\x05\x00";
        let cipher = Cipher::from_spec(spec)?;
        let mut stream = CipherStream::new(buffer, cipher);
        stream.write_all(b"hello").await?;

        assert_eq!(b"\x68\x67\x70\x72\x77", raw_data.as_slice());

        Ok(())
    }

    #[test]
    fn check_noop() -> anyhow::Result<()> {
        let mut cipher = Cipher::from_spec(b"\x05\x05\x00")?;
        assert!(!cipher.is_noop());

        let mut cipher = Cipher::from_spec(b"\x00")?;
        assert!(cipher.is_noop());

        let mut cipher = Cipher::from_spec(b"\x02\x00\x00")?;
        assert!(cipher.is_noop());

        Ok(())
    }
}
