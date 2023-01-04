use std::str;

use bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Default)]
pub struct StrictLinesCodec {
    next_index: usize,
}

impl Decoder for StrictLinesCodec {
    type Item = String;
    type Error = anyhow::Error;

    fn decode(&mut self, buf: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let read_to = buf.len();
        let newline_offset = buf[self.next_index..read_to]
            .iter()
            .position(|b| *b == b'\n');

        match newline_offset {
            Some(offset) => {
                let newline_index = offset + self.next_index;
                self.next_index = 0;
                let line = buf.split_to(newline_index + 1);
                let line = &line[..line.len() - 1];
                let line = str::from_utf8(line)?;
                Ok(Some(line.to_string()))
            }
            None => {
                self.next_index = read_to;
                Ok(None)
            }
        }
    }

    fn decode_eof(&mut self, buf: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode(buf)
    }
}

impl<T> Encoder<T> for StrictLinesCodec
where
    T: AsRef<str>,
{
    type Error = anyhow::Error;

    fn encode(&mut self, line: T, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let line = line.as_ref();
        buf.reserve(line.len() + 1);
        buf.put(line.as_bytes());
        buf.put_u8(b'\n');
        Ok(())
    }
}
