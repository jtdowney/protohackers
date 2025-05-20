use anyhow::bail;
use bytes::{Buf, BufMut, BytesMut};
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::streaming::tag,
    combinator::{consumed, map, map_res},
    multi::{length_count, length_data},
    number::streaming::{be_u8, be_u16, be_u32},
    sequence::preceded,
};
use tokio_util::codec::{Decoder, Encoder};

use super::{Road, Ticket, Timestamp};

pub struct ServerError<'a> {
    message: &'a str,
}

impl<'a> From<&'a str> for ServerError<'a> {
    fn from(message: &'a str) -> Self {
        ServerError { message }
    }
}

pub struct ServerHeartbeat;

pub enum ClientMessage {
    Plate { plate: String, timestamp: Timestamp },
    WantHeartbeat { interval: u32 },
    IAmCamera { road: Road, mile: u16, limit: u16 },
    IAmDispatcher { roads: Vec<Road> },
}

fn str(input: &[u8]) -> IResult<&[u8], String> {
    let mut parser = map_res(length_data(be_u8), |data| {
        std::str::from_utf8(data).map(String::from)
    });
    parser.parse(input)
}

fn client_message(input: &[u8]) -> IResult<&[u8], ClientMessage> {
    let plate = map(
        preceded(tag(&b"\x20"[..]), (str, be_u32)),
        |(plate, timestamp)| ClientMessage::Plate { plate, timestamp },
    );
    let want_heartbeat = map(preceded(tag(&b"\x40"[..]), be_u32), |interval| {
        ClientMessage::WantHeartbeat { interval }
    });
    let i_am_camera = map(
        preceded(tag(&b"\x80"[..]), (be_u16, be_u16, be_u16)),
        |(road, mile, limit)| ClientMessage::IAmCamera { road, mile, limit },
    );
    let i_am_dispatcher = map(
        preceded(tag(&b"\x81"[..]), length_count(be_u8, be_u16)),
        |roads| ClientMessage::IAmDispatcher { roads },
    );

    let mut parser = alt((plate, want_heartbeat, i_am_camera, i_am_dispatcher));
    parser.parse(input)
}

pub struct SpeedDaemonCodec;

impl Decoder for SpeedDaemonCodec {
    type Item = ClientMessage;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let used_length;
        let message = match consumed(client_message).parse(src) {
            Ok((_, (raw, message))) => {
                used_length = raw.len();
                message
            }
            Err(nom::Err::Incomplete(_)) => return Ok(None),
            Err(e) => bail!("error parsing {:?}: {}", src, e),
        };

        src.advance(used_length);
        Ok(Some(message))
    }
}

impl Encoder<Ticket> for SpeedDaemonCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        Ticket {
            plate,
            road,
            mile1,
            timestamp1,
            mile2,
            timestamp2,
            speed,
        }: Ticket,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        dst.put_u8(0x21);
        dst.put_u8(plate.len() as u8);
        dst.put_slice(plate.as_bytes());
        dst.put_u16(road);
        dst.put_u16(mile1);
        dst.put_u32(timestamp1);
        dst.put_u16(mile2);
        dst.put_u32(timestamp2);
        dst.put_u16(speed);

        Ok(())
    }
}

impl Encoder<ServerHeartbeat> for SpeedDaemonCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, _item: ServerHeartbeat, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.put_u8(0x41);

        Ok(())
    }
}

impl<'a> Encoder<ServerError<'a>> for SpeedDaemonCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        ServerError { message }: ServerError<'a>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        dst.put_u8(0x10);
        dst.put_u8(message.len() as u8);
        dst.put_slice(message.as_bytes());

        Ok(())
    }
}
