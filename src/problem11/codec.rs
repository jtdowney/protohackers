use bytes::{BufMut, BytesMut};
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::streaming::{tag, take},
    combinator::complete,
    multi::count,
    number::streaming::{be_u8, be_u32},
};
use std::io;
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("invalid checksum")]
    InvalidChecksum,
    #[error("invalid message")]
    InvalidMessage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Cull,
    Conserve,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Population {
    pub species: String,
    pub count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetPopulation {
    pub species: String,
    pub min: u32,
    pub max: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Hello {
        protocol: String,
        version: u32,
    },
    Error {
        message: String,
    },
    Ok,
    DialAuthority {
        site: u32,
    },
    TargetPopulations {
        site: u32,
        populations: Vec<TargetPopulation>,
    },
    CreatePolicy {
        species: String,
        action: Action,
    },
    DeletePolicy {
        policy: u32,
    },
    PolicyResult {
        policy: u32,
    },
    SiteVisit {
        site: u32,
        populations: Vec<Population>,
    },
}

pub struct PestControlCodec;
fn parse_string(input: &[u8]) -> IResult<&[u8], String> {
    let (input, len) = be_u32(input)?;
    let (input, data) = take(len)(input)?;
    let string = std::str::from_utf8(data)
        .map_err(|_| nom::Err::Error(nom::error::make_error(input, nom::error::ErrorKind::Verify)))?
        .to_string();
    Ok((input, string))
}

fn parse_population(input: &[u8]) -> IResult<&[u8], Population> {
    let (input, species) = parse_string(input)?;
    let (input, count) = be_u32(input)?;
    Ok((input, Population { species, count }))
}

fn parse_target_population(input: &[u8]) -> IResult<&[u8], TargetPopulation> {
    let (input, species) = parse_string(input)?;
    let (input, min) = be_u32(input)?;
    let (input, max) = be_u32(input)?;
    Ok((input, TargetPopulation { species, min, max }))
}

fn parse_array<T, F>(input: &[u8], parser: F) -> IResult<&[u8], Vec<T>>
where
    F: Fn(&[u8]) -> IResult<&[u8], T>,
{
    let (input, len) = be_u32(input)?;
    count(parser, len as usize).parse(input)
}

fn parse_hello(input: &[u8]) -> IResult<&[u8], Message> {
    let (input, _) = tag(&[0x50][..])(input)?;
    let (input, _length) = be_u32(input)?;
    let (input, protocol) = parse_string(input)?;
    let (input, version) = be_u32(input)?;
    Ok((input, Message::Hello { protocol, version }))
}

fn parse_error(input: &[u8]) -> IResult<&[u8], Message> {
    let (input, _) = tag(&[0x51][..])(input)?;
    let (input, _length) = be_u32(input)?;
    let (input, message) = parse_string(input)?;
    Ok((input, Message::Error { message }))
}

fn parse_ok(input: &[u8]) -> IResult<&[u8], Message> {
    let (input, _) = tag(&[0x52][..])(input)?;
    let (input, _length) = be_u32(input)?;
    Ok((input, Message::Ok))
}

fn parse_dial_authority(input: &[u8]) -> IResult<&[u8], Message> {
    let (input, _) = tag(&[0x53][..])(input)?;
    let (input, _length) = be_u32(input)?;
    let (input, site) = be_u32(input)?;
    Ok((input, Message::DialAuthority { site }))
}

fn parse_target_populations(input: &[u8]) -> IResult<&[u8], Message> {
    let (input, _) = tag(&[0x54][..])(input)?;
    let (input, _length) = be_u32(input)?;
    let (input, site) = be_u32(input)?;
    let (input, populations) = parse_array(input, parse_target_population)?;
    Ok((input, Message::TargetPopulations { site, populations }))
}

fn parse_create_policy(input: &[u8]) -> IResult<&[u8], Message> {
    let (input, _) = tag(&[0x55][..])(input)?;
    let (input, _length) = be_u32(input)?;
    let (input, species) = parse_string(input)?;
    let (input, action_byte) = be_u8(input)?;
    let action = match action_byte {
        0x90 => Action::Cull,
        0xa0 => Action::Conserve,
        _ => {
            return Err(nom::Err::Error(nom::error::make_error(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }
    };
    Ok((input, Message::CreatePolicy { species, action }))
}

fn parse_delete_policy(input: &[u8]) -> IResult<&[u8], Message> {
    let (input, _) = tag(&[0x56][..])(input)?;
    let (input, _length) = be_u32(input)?;
    let (input, policy) = be_u32(input)?;
    Ok((input, Message::DeletePolicy { policy }))
}

fn parse_policy_result(input: &[u8]) -> IResult<&[u8], Message> {
    let (input, _) = tag(&[0x57][..])(input)?;
    let (input, _length) = be_u32(input)?;
    let (input, policy) = be_u32(input)?;
    Ok((input, Message::PolicyResult { policy }))
}

fn parse_site_visit(input: &[u8]) -> IResult<&[u8], Message> {
    let (input, _) = tag(&[0x58][..])(input)?;
    let (input, _length) = be_u32(input)?;
    let (input, site) = be_u32(input)?;
    let (input, populations) = parse_array(input, parse_population)?;
    Ok((input, Message::SiteVisit { site, populations }))
}

fn parse_message_content(input: &[u8]) -> IResult<&[u8], Message> {
    alt((
        parse_hello,
        parse_error,
        parse_ok,
        parse_dial_authority,
        parse_target_populations,
        parse_create_policy,
        parse_delete_policy,
        parse_policy_result,
        parse_site_visit,
    ))
    .parse(input)
}

fn calculate_checksum(data: &[u8]) -> u8 {
    let sum = data.iter().map(|&b| b as u32).sum::<u32>();
    (256 - (sum % 256)) as u8
}

impl Decoder for PestControlCodec {
    type Item = Message;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 5 {
            return Ok(None);
        }

        let length = u32::from_be_bytes([src[1], src[2], src[3], src[4]]) as usize;

        if src.len() < length {
            return Ok(None);
        }

        let data = src.split_to(length);
        let checksum_pos = data.len() - 1;
        let checksum = data[checksum_pos];
        let calculated = calculate_checksum(&data[..checksum_pos]);

        if checksum != calculated {
            return Err(CodecError::InvalidChecksum);
        }

        let content = &data[..checksum_pos];

        match complete(parse_message_content).parse(content) {
            Ok((remaining, message)) => {
                if !remaining.is_empty() {
                    return Err(CodecError::InvalidMessage);
                }
                Ok(Some(message))
            }
            Err(_) => Err(CodecError::Parse("Failed to parse message".to_string())),
        }
    }
}

fn encode_string(dst: &mut BytesMut, s: &str) {
    dst.put_u32(s.len() as u32);
    dst.put_slice(s.as_bytes());
}

fn encode_array<T, F>(dst: &mut BytesMut, items: &[T], encoder: F)
where
    F: Fn(&mut BytesMut, &T),
{
    dst.put_u32(items.len() as u32);
    for item in items {
        encoder(dst, item);
    }
}

impl Encoder<Message> for PestControlCodec {
    type Error = CodecError;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let start_len = dst.len();

        dst.put_u8(0);
        dst.put_u32(0);

        let (_msg_type, _) = match &item {
            Message::Hello { protocol, version } => {
                dst[start_len] = 0x50;
                encode_string(dst, protocol);
                dst.put_u32(*version);
                (0x50, ())
            }
            Message::Error { message } => {
                dst[start_len] = 0x51;
                encode_string(dst, message);
                (0x51, ())
            }
            Message::Ok => {
                dst[start_len] = 0x52;
                (0x52, ())
            }
            Message::DialAuthority { site } => {
                dst[start_len] = 0x53;
                dst.put_u32(*site);
                (0x53, ())
            }
            Message::TargetPopulations { site, populations } => {
                dst[start_len] = 0x54;
                dst.put_u32(*site);
                encode_array(dst, populations, |dst, pop| {
                    encode_string(dst, &pop.species);
                    dst.put_u32(pop.min);
                    dst.put_u32(pop.max);
                });
                (0x54, ())
            }
            Message::CreatePolicy { species, action } => {
                dst[start_len] = 0x55;
                encode_string(dst, species);
                let action_byte = match action {
                    Action::Cull => 0x90,
                    Action::Conserve => 0xa0,
                };
                dst.put_u8(action_byte);
                (0x55, ())
            }
            Message::DeletePolicy { policy } => {
                dst[start_len] = 0x56;
                dst.put_u32(*policy);
                (0x56, ())
            }
            Message::PolicyResult { policy } => {
                dst[start_len] = 0x57;
                dst.put_u32(*policy);
                (0x57, ())
            }
            Message::SiteVisit { site, populations } => {
                dst[start_len] = 0x58;
                dst.put_u32(*site);
                encode_array(dst, populations, |dst, pop| {
                    encode_string(dst, &pop.species);
                    dst.put_u32(pop.count);
                });
                (0x58, ())
            }
        };

        let length = (dst.len() - start_len + 1) as u32;
        dst[start_len + 1..start_len + 5].copy_from_slice(&length.to_be_bytes());
        let checksum = calculate_checksum(&dst[start_len..]);
        dst.put_u8(checksum);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    #[test]
    fn test_decode_hello() {
        let mut codec = PestControlCodec;
        let mut buf = BytesMut::new();

        // Hello message from the problem description
        buf.extend_from_slice(&[
            0x50, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x0b, 0x70, 0x65, 0x73, 0x74, 0x63,
            0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x00, 0x00, 0x00, 0x01, 0xce,
        ]);

        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(
            result,
            Some(Message::Hello {
                protocol: "pestcontrol".to_string(),
                version: 1
            })
        );
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_encode_hello() {
        let mut codec = PestControlCodec;
        let mut buf = BytesMut::new();

        let msg = Message::Hello {
            protocol: "pestcontrol".to_string(),
            version: 1,
        };

        codec.encode(msg, &mut buf).unwrap();

        let expected = vec![
            0x50, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x0b, 0x70, 0x65, 0x73, 0x74, 0x63,
            0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x00, 0x00, 0x00, 0x01, 0xce,
        ];

        assert_eq!(buf.to_vec(), expected);
    }

    #[test]
    fn test_decode_site_visit() {
        let mut codec = PestControlCodec;
        let mut buf = BytesMut::new();

        // SiteVisit message from the problem description
        buf.extend_from_slice(&[
            0x58, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x03, 0x64, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03,
            0x72, 0x61, 0x74, 0x00, 0x00, 0x00, 0x05, 0x8c,
        ]);

        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(
            result,
            Some(Message::SiteVisit {
                site: 12345,
                populations: vec![
                    Population {
                        species: "dog".to_string(),
                        count: 1,
                    },
                    Population {
                        species: "rat".to_string(),
                        count: 5,
                    },
                ],
            })
        );
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_decode_target_populations() {
        let mut codec = PestControlCodec;
        let mut buf = BytesMut::new();

        // TargetPopulations message from the problem description
        buf.extend_from_slice(&[
            0x54, // TargetPopulations
            0x00, 0x00, 0x00, 0x2c, // length 44
            0x00, 0x00, 0x30, 0x39, // site 12345
            0x00, 0x00, 0x00, 0x02, // populations length 2
            // First target population
            0x00, 0x00, 0x00, 0x03, // species length 3
            0x64, 0x6f, 0x67, // "dog"
            0x00, 0x00, 0x00, 0x01, // min 1
            0x00, 0x00, 0x00, 0x03, // max 3
            // Second target population
            0x00, 0x00, 0x00, 0x03, // species length 3
            0x72, 0x61, 0x74, // "rat"
            0x00, 0x00, 0x00, 0x00, // min 0
            0x00, 0x00, 0x00, 0x0a, // max 10
            0x80, // checksum
        ]);

        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(
            result,
            Some(Message::TargetPopulations {
                site: 12345,
                populations: vec![
                    TargetPopulation {
                        species: "dog".to_string(),
                        min: 1,
                        max: 3,
                    },
                    TargetPopulation {
                        species: "rat".to_string(),
                        min: 0,
                        max: 10,
                    },
                ],
            })
        );
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_encode_dial_authority() {
        let mut codec = PestControlCodec;
        let mut buf = BytesMut::new();

        let msg = Message::DialAuthority { site: 12345 };

        codec.encode(msg, &mut buf).unwrap();

        let expected = vec![
            0x53, // DialAuthority
            0x00, 0x00, 0x00, 0x0a, // length 10
            0x00, 0x00, 0x30, 0x39, // site 12345
            0x3a, // checksum
        ];

        assert_eq!(buf.to_vec(), expected);
    }

    #[test]
    fn test_encode_create_policy() {
        let mut codec = PestControlCodec;
        let mut buf = BytesMut::new();

        let msg = Message::CreatePolicy {
            species: "dog".to_string(),
            action: Action::Conserve,
        };

        codec.encode(msg, &mut buf).unwrap();

        let expected = vec![
            0x55, // CreatePolicy
            0x00, 0x00, 0x00, 0x0e, // length 14
            0x00, 0x00, 0x00, 0x03, // species length 3
            0x64, 0x6f, 0x67, // "dog"
            0xa0, // action: conserve
            0xc0, // checksum
        ];

        assert_eq!(buf.to_vec(), expected);
    }

    #[test]
    fn test_decode_error() {
        let mut codec = PestControlCodec;
        let mut buf = BytesMut::new();

        buf.extend_from_slice(&[
            0x51, // Error
            0x00, 0x00, 0x00, 0x0d, // length 13
            0x00, 0x00, 0x00, 0x03, // message length 3
            0x62, 0x61, 0x64, // "bad"
            0x78, // checksum
        ]);

        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(
            result,
            Some(Message::Error {
                message: "bad".to_string()
            })
        );
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_decode_ok() {
        let mut codec = PestControlCodec;
        let mut buf = BytesMut::new();

        buf.extend_from_slice(&[
            0x52, // OK
            0x00, 0x00, 0x00, 0x06, // length 6
            0xa8, // checksum
        ]);

        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, Some(Message::Ok));
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_decode_policy_result() {
        let mut codec = PestControlCodec;
        let mut buf = BytesMut::new();

        buf.extend_from_slice(&[
            0x57, // PolicyResult
            0x00, 0x00, 0x00, 0x0a, // length 10
            0x00, 0x00, 0x00, 0x7b, // policy 123
            0x24, // checksum
        ]);

        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, Some(Message::PolicyResult { policy: 123 }));
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_encode_delete_policy() {
        let mut codec = PestControlCodec;
        let mut buf = BytesMut::new();

        let msg = Message::DeletePolicy { policy: 123 };

        codec.encode(msg, &mut buf).unwrap();

        let expected = vec![
            0x56, // DeletePolicy
            0x00, 0x00, 0x00, 0x0a, // length 10
            0x00, 0x00, 0x00, 0x7b, // policy 123
            0x25, // checksum
        ];

        assert_eq!(buf.to_vec(), expected);
    }

    #[test]
    fn test_invalid_checksum() {
        let mut codec = PestControlCodec;
        let mut buf = BytesMut::new();

        buf.extend_from_slice(&[
            0x52, // OK
            0x00, 0x00, 0x00, 0x06, // length 6
            0xff, // wrong checksum
        ]);

        let result = codec.decode(&mut buf);
        assert!(matches!(result, Err(CodecError::InvalidChecksum)));
    }

    #[test]
    fn test_partial_message() {
        let mut codec = PestControlCodec;
        let mut buf = BytesMut::new();

        // Only partial message
        buf.extend_from_slice(&[
            0x52, // OK
            0x00, 0x00, // incomplete length
        ]);

        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, None);
        assert_eq!(buf.len(), 3);
    }

    #[test]
    fn test_decode_invalid_hello_protocol() {
        let mut codec = PestControlCodec;
        let mut buf = BytesMut::new();

        // Hello with wrong protocol "badproto"
        buf.extend_from_slice(&[
            0x50, // Hello
            0x00, 0x00, 0x00, 0x16, // length 22
            0x00, 0x00, 0x00, 0x08, // protocol length 8
            0x62, 0x61, 0x64, 0x70, 0x72, 0x6f, 0x74, 0x6f, // "badproto"
            0x00, 0x00, 0x00, 0x01, // version 1
            0x36, // checksum
        ]);

        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(
            result,
            Some(Message::Hello {
                protocol: "badproto".to_string(),
                version: 1
            })
        );
        assert_eq!(buf.len(), 0);
    }
}
