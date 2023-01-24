use std::collections::HashSet;
use std::io;
use std::{fmt::Write, str};

use bytes::{Buf, BufMut, Bytes};
use nom::character::streaming::space1;
use nom::combinator::verify;
use nom::multi::length_data;
use nom::sequence::terminated;
use nom::{
    branch::alt,
    bytes::streaming::{tag, tag_no_case, take_while1},
    character::streaming::newline,
    combinator::{consumed, map, map_res, opt},
    sequence::{preceded, tuple},
    IResult,
};
use once_cell::sync::Lazy;
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

static ILLEGAL_FILENAME_CHARACTERS: Lazy<HashSet<char>> =
    Lazy::new(|| r#"`~!@#$%^&*()+={}[]:;'",?\|"#.chars().collect());

#[derive(Debug)]
pub enum PlainCommand {
    Help,
    Get,
    Put,
    List,
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("command is malformed")]
    MalformedCommand(PlainCommand),
    #[error("unknown command")]
    UnknownCommand(String),
    #[error("network error")]
    Network(#[from] io::Error),
    #[error("data is not UTF8")]
    MalformedString,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Command {
    Get {
        file: String,
        revision: Option<usize>,
    },
    Put {
        file: String,
        data: Vec<u8>,
    },
    List {
        directory: String,
    },
}

fn revision(input: &[u8]) -> IResult<&[u8], usize> {
    map(preceded(tag("r"), nom::character::streaming::u32), |n| {
        n as usize
    })(input)
}

fn file_name(input: &[u8]) -> IResult<&[u8], String> {
    map(
        verify(
            map_res(take_while1(|b: u8| !b.is_ascii_whitespace()), |name| {
                str::from_utf8(name)
            }),
            |s: &str| {
                s.starts_with('/') && !s.chars().any(|c| ILLEGAL_FILENAME_CHARACTERS.contains(&c))
            },
        ),
        |s| s.to_owned(),
    )(input)
}

fn parse_get_command(input: &[u8]) -> IResult<&[u8], Command> {
    map(
        terminated(
            preceded(
                tuple((tag_no_case("get"), space1)),
                tuple((file_name, opt(map(tuple((space1, revision)), |(_, r)| r)))),
            ),
            newline,
        ),
        |(file, revision)| Command::Get { file, revision },
    )(input)
}

fn parse_put_command(input: &[u8]) -> IResult<&[u8], Command> {
    map(
        preceded(
            tuple((tag_no_case("put"), space1)),
            tuple((
                file_name,
                map(
                    verify(
                        length_data(terminated(
                            preceded(space1, nom::character::streaming::u32),
                            newline,
                        )),
                        |data: &[u8]| {
                            data.iter()
                                .all(|b| b.is_ascii_graphic() || b.is_ascii_whitespace())
                        },
                    ),
                    |data: &[u8]| data.to_vec(),
                ),
            )),
        ),
        |(file, data)| Command::Put { file, data },
    )(input)
}

fn parse_list_command(input: &[u8]) -> IResult<&[u8], Command> {
    map(
        terminated(
            preceded(tuple((tag_no_case("list"), space1)), file_name),
            newline,
        ),
        |directory| Command::List { directory },
    )(input)
}

fn parse_command(input: &[u8]) -> IResult<&[u8], Command> {
    alt((parse_get_command, parse_put_command, parse_list_command))(input)
}

pub struct VcsCodec;

impl Decoder for VcsCodec {
    type Item = Command;
    type Error = ParseError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let parsed = consumed(parse_command)(src).map(|(_, (r, c))| (r.len(), c));
        let result = match parsed {
            Ok((used_length, message)) => {
                src.advance(used_length);
                return Ok(Some(message));
            }
            Err(nom::Err::Incomplete(_)) => return Ok(None),
            Err(_) => {
                let data = if let Some(index) = src.iter().position(|&b| b.is_ascii_whitespace()) {
                    &src[0..index]
                } else {
                    src
                };

                let word = str::from_utf8(data).map_err(|_| ParseError::MalformedString)?;
                let error = match word.to_ascii_lowercase().as_str() {
                    "help" => ParseError::MalformedCommand(PlainCommand::Help),
                    "get" => ParseError::MalformedCommand(PlainCommand::Get),
                    "put" => ParseError::MalformedCommand(PlainCommand::Put),
                    "list" => ParseError::MalformedCommand(PlainCommand::List),
                    _ => ParseError::UnknownCommand(word.into()),
                };

                Err(error)
            }
        };

        // src.clear();
        result
    }
}

pub enum Reply {
    Ready,
    OkWithCount(usize),
    OkWithMessage(String),
    OkWithData(Bytes),
    Error(String),
}

impl Encoder<Reply> for VcsCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Reply, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        match item {
            Reply::Ready => dst.put_slice(b"READY\n"),
            Reply::OkWithCount(n) => writeln!(dst, "OK {n}")?,
            Reply::OkWithMessage(rev) => writeln!(dst, "OK {rev}")?,
            Reply::OkWithData(data) => {
                writeln!(dst, "OK {}", data.len())?;
                dst.put_slice(&data);
            }
            Reply::Error(message) => writeln!(dst, "ERR {message}")?,
        }

        Ok(())
    }
}

impl<S: AsRef<str>> Encoder<S> for VcsCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: S, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        dst.put_slice(item.as_ref().as_bytes());
        dst.put_u8(b'\n');

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nom::Finish;

    use super::*;

    #[test]
    fn parse_simple_get() {
        let (_, command) = parse_command(b"get /test\n").finish().unwrap();
        assert_eq!(
            Command::Get {
                file: "/test".into(),
                revision: None
            },
            command
        );
    }

    #[test]
    fn parse_get_with_revision() {
        let (_, command) = parse_command(b"get /test r3\n").finish().unwrap();
        assert_eq!(
            Command::Get {
                file: "/test".into(),
                revision: Some(3)
            },
            command
        );
    }

    #[test]
    fn parse_simple_put() {
        let (_, command) = parse_command(b"put /test 4\ntest").finish().unwrap();
        assert_eq!(
            Command::Put {
                file: "/test".into(),
                data: "test".into()
            },
            command
        );
    }

    #[test]
    fn parse_list() {
        let (_, command) = parse_command(b"list /test\n").finish().unwrap();
        assert_eq!(
            Command::List {
                directory: "/test".into(),
            },
            command
        );
    }
}
