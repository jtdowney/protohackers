use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use anyhow::bail;
use bytes::{Buf, BufMut, BytesMut};
use futures_util::{SinkExt, StreamExt};
use nom::{
    branch::alt,
    bytes::streaming::tag,
    combinator::{consumed, map, map_res},
    multi::{length_count, length_data},
    number::streaming::{be_u16, be_u32, be_u8},
    sequence::{preceded, tuple},
    IResult,
};
use rand::prelude::*;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, Mutex, Semaphore},
    time::{Instant, Interval},
};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{debug, trace, warn};

type Road = u16;
type Timestamp = u32;

struct ServerError<'a> {
    message: &'a str,
}

struct ServerHeartbeat;

enum ClientMessage {
    Plate { plate: String, timestamp: Timestamp },
    WantHeartbeat { interval: u32 },
    IAmCamera { road: Road, mile: u16, limit: u16 },
    IAmDispatcher { roads: Vec<Road> },
}

#[derive(Copy, Clone, Debug)]
struct Camera {
    road: Road,
    mile: u16,
    limit: u16,
}

#[derive(Clone, Debug)]
struct Ticket {
    plate: String,
    road: Road,
    mile1: u16,
    timestamp1: Timestamp,
    mile2: u16,
    timestamp2: Timestamp,
    speed: u16,
}

#[derive(Debug, Default)]
pub struct State {
    dispatchers: HashMap<Road, Vec<(SocketAddr, mpsc::UnboundedSender<Ticket>)>>,
    pending_tickets: Vec<Ticket>,
    observations: HashMap<String, BTreeMap<Timestamp, Camera>>,
    existing_tickets: HashMap<String, HashSet<Timestamp>>,
    locked_plate: HashMap<String, Arc<Semaphore>>,
}

type SharedState = Arc<Mutex<State>>;

#[derive(Debug)]
enum Role {
    Unknown,
    Camera { road: Road, mile: u16, limit: u16 },
    Dispatcher { rx: mpsc::UnboundedReceiver<Ticket> },
}

pub async fn handle<T>(stream: T, addr: SocketAddr, state: SharedState) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut framed = Framed::new(stream, SpeedDaemonCodec);
    let mut heartbeat: Option<Interval> = None;
    let mut role = Role::Unknown;

    loop {
        tokio::select! {
            result = framed.next() => match result {
                Some(Ok(message)) => handle_client_message(message, addr, state.clone(), &mut framed, &mut role, &mut heartbeat).await?,
                Some(Err(_)) => framed.send(ServerError { message: "unable to parse message" }).await?,
                None => break,
            },
            maybe_ticket = dispatch_ticket(&mut role) => match maybe_ticket {
                Some(ticket) => framed.send(ticket).await?,
                None => continue,
            },
            _ = heartbeat_tick(heartbeat.as_mut()) => {
                framed.send(ServerHeartbeat).await?;
            }
        }
    }

    if let Role::Dispatcher { .. } = role {
        let mut state = state.lock().await;

        for (_, dispatchers) in state.dispatchers.iter_mut() {
            dispatchers.retain(|&(dispatcher_addr, _)| dispatcher_addr != addr)
        }

        state
            .dispatchers
            .retain(|_, dispatchers| !dispatchers.is_empty());
    }

    Ok(())
}

async fn dispatch_ticket(role: &mut Role) -> Option<Ticket> {
    if let Role::Dispatcher { rx, .. } = role {
        rx.recv().await
    } else {
        future::pending().await
    }
}

async fn handle_client_message<T>(
    message: ClientMessage,
    addr: SocketAddr,
    state: SharedState,
    framed: &mut Framed<T, SpeedDaemonCodec>,
    role: &mut Role,
    heartbeat: &mut Option<Interval>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    match (message, role) {
        (ClientMessage::Plate { plate, timestamp }, &mut Role::Camera { road, mile, limit }) => {
            trace!("[{}] {} seen at {}", addr, plate, timestamp);
            let camera = Camera { road, mile, limit };
            {
                let mut state = state.lock().await;
                state
                    .locked_plate
                    .entry(plate.clone())
                    .or_insert_with(|| Arc::new(Semaphore::new(1)));
                state
                    .observations
                    .entry(plate.clone())
                    .or_default()
                    .insert(timestamp, camera);
            }

            check_for_ticket(state, plate, timestamp, camera).await?;
        }
        (ClientMessage::Plate { .. }, role) => {
            warn!("[{}] with {:?} tried to send a plate", addr, role);
            framed
                .send(ServerError {
                    message: "only cameras can send plates",
                })
                .await?;
        }
        (ClientMessage::IAmCamera { road, mile, limit }, role @ Role::Unknown) => {
            *role = Role::Camera { road, mile, limit };
            debug!("[{}] registering camera ({:?})", addr, role);
        }
        (ClientMessage::IAmCamera { .. }, role) => {
            warn!("[{}] with {:?} tried to identify as a camera", addr, role);
            framed
                .send(ServerError {
                    message: "already identified",
                })
                .await?;
        }
        (ClientMessage::IAmDispatcher { roads }, role @ Role::Unknown) => {
            let (tx, rx) = mpsc::unbounded_channel();
            {
                let mut state = state.lock().await;
                for &road in &roads {
                    debug!("[{}] registering dispatcher for road {}", addr, road);
                    state
                        .dispatchers
                        .entry(road)
                        .or_default()
                        .push((addr, tx.clone()));
                }
            }

            let tickets = {
                let mut state = state.lock().await;
                let tickets = state
                    .pending_tickets
                    .iter()
                    .filter(|t| roads.contains(&t.road))
                    .cloned()
                    .collect::<Vec<_>>();
                state.pending_tickets.retain(|t| !roads.contains(&t.road));
                tickets
            };

            for ticket in tickets {
                framed.send(ticket).await?;
            }

            *role = Role::Dispatcher { rx };
        }
        (ClientMessage::IAmDispatcher { .. }, role) => {
            warn!(
                "[{}] with {:?} tried to identify as a dispatcher",
                addr, role
            );
            framed
                .send(ServerError {
                    message: "already identified",
                })
                .await?;
        }
        (ClientMessage::WantHeartbeat { interval }, _) => {
            if interval == 0 {
                trace!("[{}] disabled heartbeats", addr);
                *heartbeat = None;
            } else {
                trace!("[{}] enabled heartbeats at {}", addr, interval);
                let duration = Duration::from_millis(interval as u64 * 100);
                let interval = tokio::time::interval(duration);
                *heartbeat = Some(interval);
            }
        }
    }

    Ok(())
}

async fn check_for_ticket(
    state: SharedState,
    plate: String,
    timestamp: Timestamp,
    camera: Camera,
) -> anyhow::Result<()> {
    let semaphore = {
        let state = state.lock().await;
        state.locked_plate[&plate].clone()
    };

    let _permit = semaphore.acquire().await?;
    trace!("acquiring lock for {}", plate);

    let other_observations = {
        let state = state.lock().await;
        state.observations.get(&plate).map(|obs| {
            let prev = obs.range(..timestamp).map(|(&t, &c)| (t, c)).next_back();
            let next = obs.range((timestamp + 1)..).map(|(&t, &c)| (t, c)).next();
            (prev, next)
        })
    };

    let maybe_ticket = other_observations.and_then(|(prev, next)| {
        let cmph_limit = camera.limit as u32 * 100;
        let current = Some((timestamp, camera));
        let first = prev.zip(current);
        let second = current.zip(next);
        [first, second]
            .iter()
            .filter_map(|&pair| pair)
            .find_map(|((ta, ca), (tb, cb))| {
                let centimiles_traveled = (ca.mile as i32 - cb.mile as i32).unsigned_abs() * 100;
                let time_traveled = tb - ta;
                let cmph_traveled = (f64::from(centimiles_traveled) / f64::from(time_traveled)
                    * 60.0
                    * 60.0) as u32;

                trace!(
                    centimiles_traveled,
                    time_traveled,
                    cmph_traveled,
                    cmph_limit,
                    "checking"
                );

                if cmph_traveled > cmph_limit {
                    let ticket = Ticket {
                        plate: plate.clone(),
                        road: ca.road,
                        mile1: ca.mile,
                        timestamp1: ta,
                        mile2: cb.mile,
                        timestamp2: tb,
                        speed: cmph_traveled as u16,
                    };
                    Some(ticket)
                } else {
                    None
                }
            })
    });

    if let Some(ticket) = maybe_ticket {
        issue_ticket(state.clone(), ticket).await?
    }

    Ok(())
}

async fn issue_ticket(
    state: SharedState,
    ticket @ Ticket {
        road,
        timestamp1,
        timestamp2,
        ..
    }: Ticket,
) -> anyhow::Result<()> {
    let dates = [timestamp1, timestamp2].map(|t| (f64::from(t) / 86400.0) as u32);

    let existing_tickets = {
        let state = state.lock().await;
        state
            .existing_tickets
            .get(&ticket.plate)
            .map(|ticket_dates| {
                let [a, b] = dates;
                ticket_dates.contains(&a) || ticket_dates.contains(&b)
            })
            .unwrap_or_default()
    };

    trace!(?dates, "checking existing tickets for {}", ticket.plate);

    if existing_tickets {
        debug!(
            "skipping ticket for {} due to recent ticket on {:?}",
            ticket.plate, dates
        );
        return Ok(());
    }

    let dates = [timestamp1, timestamp2]
        .iter()
        .map(|&t| (f64::from(t) / 86400.0) as u32)
        .collect::<HashSet<_>>();
    debug!(?ticket, "issuing ticket for {:?}", dates);

    let maybe_dispatcher = {
        let mut state = state.lock().await;
        state
            .existing_tickets
            .entry(ticket.plate.clone())
            .or_default()
            .extend(dates);

        let mut rng = rand::thread_rng();
        state
            .dispatchers
            .get(&road)
            .and_then(|dispatchers| dispatchers.choose(&mut rng))
            .cloned()
    };

    if let Some((_, tx)) = maybe_dispatcher {
        trace!("sending ticket to a dispatcher");
        tx.send(ticket)?;
    } else {
        trace!(?ticket, "unable to dispatch ticket, saving");
        let mut state = state.lock().await;
        state.pending_tickets.push(ticket);
    }

    Ok(())
}

async fn heartbeat_tick(maybe_heartbeat: Option<&mut Interval>) -> Instant {
    if let Some(heartbeat) = maybe_heartbeat {
        heartbeat.tick().await
    } else {
        future::pending::<Instant>().await
    }
}

fn str(input: &[u8]) -> IResult<&[u8], String> {
    map_res(length_data(be_u8), |data| {
        std::str::from_utf8(data).map(String::from)
    })(input)
}

fn client_message(input: &[u8]) -> IResult<&[u8], ClientMessage> {
    let plate = map(
        preceded(tag(b"\x20"), tuple((str, be_u32))),
        |(plate, timestamp)| ClientMessage::Plate { plate, timestamp },
    );
    let want_heartbeat = map(preceded(tag(b"\x40"), be_u32), |interval| {
        ClientMessage::WantHeartbeat { interval }
    });
    let i_am_camera = map(
        preceded(tag(b"\x80"), tuple((be_u16, be_u16, be_u16))),
        |(road, mile, limit)| ClientMessage::IAmCamera { road, mile, limit },
    );
    let i_am_dispatcher = map(
        preceded(tag(b"\x81"), length_count(be_u8, be_u16)),
        |roads| ClientMessage::IAmDispatcher { roads },
    );

    alt((plate, want_heartbeat, i_am_camera, i_am_dispatcher))(input)
}

struct SpeedDaemonCodec;

impl Decoder for SpeedDaemonCodec {
    type Item = ClientMessage;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let used_length;
        let message = match consumed(client_message)(src) {
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
