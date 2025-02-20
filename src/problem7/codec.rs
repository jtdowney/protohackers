use std::{fmt::Write, str};

use anyhow::{ensure, format_err};
use nom::{
    Finish, IResult,
    branch::alt,
    bytes::complete::{escaped_transform, tag},
    character::complete::none_of,
    combinator::{all_consuming, map, value, verify},
    sequence::tuple,
};
use tokio_util::codec::{Decoder, Encoder};
use tracing::{trace, warn};

#[derive(Debug, PartialEq)]
pub enum Message {
    Connect {
        session_id: usize,
    },
    Ack {
        session_id: usize,
        length: usize,
    },
    Data {
        session_id: usize,
        position: usize,
        payload: String,
    },
    Close {
        session_id: usize,
    },
}

fn positive_number(input: &str) -> IResult<&str, usize> {
    map(verify(nom::character::complete::i32, |&n| n >= 0), |n| {
        n as usize
    })(input)
}

pub fn message(input: &str) -> IResult<&str, Message> {
    let connect = map(
        tuple((tag("/connect/"), positive_number, tag("/"))),
        |(_, session_id, _)| Message::Connect { session_id },
    );
    let ack = map(
        tuple((
            tag("/ack/"),
            positive_number,
            tag("/"),
            positive_number,
            tag("/"),
        )),
        |(_, session_id, _, length, _)| Message::Ack { session_id, length },
    );
    let data = map(
        tuple((
            tag("/data/"),
            positive_number,
            tag("/"),
            positive_number,
            tag("/"),
            escaped_transform(
                none_of(r"\/"),
                '\\',
                alt((value("\\", tag("\\")), value("/", tag("/")))),
            ),
            tag("/"),
        )),
        |(_, session_id, _, position, _, payload, _)| Message::Data {
            session_id,
            position,
            payload,
        },
    );
    let close = map(
        tuple((tag("/close/"), positive_number, tag("/"))),
        |(_, session_id, _)| Message::Close { session_id },
    );

    alt((connect, ack, data, close))(input)
}

pub struct LrcpCodec;

impl Decoder for LrcpCodec {
    type Item = Message;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        if src.len() > 1000 {
            warn!(len = src.len(), "packet is over a 1000 bytes");
            // TODO: ignore for being too long
        }

        let buffer = str::from_utf8(src)?.to_owned();
        let result = match all_consuming(message)(&buffer).finish() {
            Ok((_, message)) => {
                trace!(?buffer, "parsed message");
                Ok(Some(message))
            }
            Err(e) => {
                let e = format_err!("error parsing {:?}: {}", src, e);
                Err(e)
            }
        };

        src.clear();

        result
    }

    fn decode_eof(&mut self, buf: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.decode(buf)? {
            Some(frame) => Ok(Some(frame)),
            None => {
                if buf.is_empty() {
                    Ok(None)
                } else {
                    let buffer = str::from_utf8(buf)?.to_owned();
                    warn!(?buffer, "bytes remaining on stream");
                    Ok(None)
                }
            }
        }
    }
}

fn encode_payload<S: AsRef<str>>(payload: S) -> String {
    payload.as_ref().replace('\\', r"\\").replace('/', r"\/")
}

impl Encoder<Message> for LrcpCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Message, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let response = match item {
            Message::Connect { session_id } => format!("/connect/{session_id}/"),
            Message::Ack { session_id, length } => format!("/ack/{session_id}/{length}/"),
            Message::Data {
                session_id,
                position,
                payload,
            } => {
                let payload = encode_payload(payload);
                format!("/data/{session_id}/{position}/{payload}/")
            }
            Message::Close { session_id } => format!("/close/{session_id}/"),
        };

        ensure!(response.len() <= 1000, "sending huge packet");

        trace!(?response, "sending");
        dst.write_str(&response)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nom::Finish;

    use super::*;

    #[test]
    fn connect() -> anyhow::Result<()> {
        let (_, message) = message(r"/connect/1234567/").finish()?;
        assert_eq!(
            Message::Connect {
                session_id: 1234567
            },
            message
        );

        Ok(())
    }

    #[test]
    fn positive_number_accepts_positive_number() -> anyhow::Result<()> {
        let (_, n) = positive_number("123").finish()?;
        assert_eq!(123, n);

        Ok(())
    }

    #[test]
    fn positive_number_rejects_negative_number() -> anyhow::Result<()> {
        let error = positive_number("-123").finish().unwrap_err();
        assert_eq!(
            nom::error::Error::new("-123", nom::error::ErrorKind::Verify),
            error
        );

        Ok(())
    }

    #[test]
    fn ack() -> anyhow::Result<()> {
        let (_, message) = message(r"/ack/1234567/89/").finish()?;
        assert_eq!(
            Message::Ack {
                session_id: 1234567,
                length: 89
            },
            message
        );

        Ok(())
    }

    #[test]
    fn simple_data() -> anyhow::Result<()> {
        let (_, message) = message(r"/data/1234567/89/testing/").finish()?;
        assert_eq!(
            Message::Data {
                session_id: 1234567,
                position: 89,
                payload: "testing".into()
            },
            message
        );

        Ok(())
    }

    #[test]
    fn data_with_escapes() -> anyhow::Result<()> {
        let (_, message) = message(r"/data/1234567/89/foo\/bar\\baz/").finish()?;
        assert_eq!(
            Message::Data {
                session_id: 1234567,
                position: 89,
                payload: r"foo/bar\baz".into()
            },
            message
        );

        Ok(())
    }

    #[test]
    fn data_with_newlines() -> anyhow::Result<()> {
        let (_, message) = message("/data/1234567/89/hello\nworld/").finish()?;
        assert_eq!(
            Message::Data {
                session_id: 1234567,
                position: 89,
                payload: "hello\nworld".into()
            },
            message
        );

        Ok(())
    }

    #[test]
    fn close() -> anyhow::Result<()> {
        let (_, message) = message(r"/close/1234567/").finish()?;
        assert_eq!(
            Message::Close {
                session_id: 1234567,
            },
            message
        );

        Ok(())
    }
}
