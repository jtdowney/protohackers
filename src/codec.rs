use std::{marker::PhantomData, str};

use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
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

pub struct JsonLinesCodec<Req: for<'a> Deserialize<'a>, Resp: Serialize> {
    next_index: usize,
    _req: PhantomData<Req>,
    _resp: PhantomData<Resp>,
}

impl<Req: for<'a> Deserialize<'a>, Resp: Serialize> Default for JsonLinesCodec<Req, Resp> {
    fn default() -> Self {
        Self {
            next_index: 0,
            _req: Default::default(),
            _resp: Default::default(),
        }
    }
}

impl<Req: for<'a> Deserialize<'a>, Resp: Serialize> Decoder for JsonLinesCodec<Req, Resp> {
    type Item = Req;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let read_to = src.len();
        let newline_offset = src[self.next_index..read_to]
            .iter()
            .position(|b| *b == b'\n');

        match newline_offset {
            Some(offset) => {
                let newline_index = offset + self.next_index;
                self.next_index = 0;
                let line = src.split_to(newline_index + 1);
                let line = &line[..line.len() - 1];
                let request = serde_json::from_slice(line)?;
                Ok(Some(request))
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

impl<Req: for<'a> Deserialize<'a>, Resp: Serialize> Encoder<Resp> for JsonLinesCodec<Req, Resp> {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Resp, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let response = serde_json::to_vec(&item)?;
        dst.reserve(response.len() + 1);
        dst.put(response.as_slice());
        dst.put_u8(b'\n');
        Ok(())
    }
}
