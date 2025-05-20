use std::{
    collections::{HashMap, HashSet},
    future,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use itertools::Itertools;
use parking_lot::Mutex;
use rand::prelude::*;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Semaphore, mpsc},
    time::{Instant, Interval},
};
use tokio_util::codec::Framed;
use tracing::{debug, trace, warn};

use self::codec::{ClientMessage, SpeedDaemonCodec};
use crate::{
    problem6::codec::{ServerError, ServerHeartbeat},
    server::ConnectionHandler,
};

mod codec;

pub type Road = u16;
pub type Timestamp = u32;

#[derive(Copy, Clone, Debug)]
pub struct Camera {
    mile: u16,
    limit: u16,
}

#[derive(Clone, Debug)]
pub struct Ticket {
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
    observations: HashMap<(String, Road), Vec<(Timestamp, Camera)>>,
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

pub struct Handler;

#[async_trait]
impl ConnectionHandler for Handler {
    type State = SharedState;

    async fn handle_connection(
        stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        addr: SocketAddr,
        state: Self::State,
    ) -> anyhow::Result<()> {
        let mut framed = Framed::new(stream, SpeedDaemonCodec);
        let mut heartbeat: Option<Interval> = None;
        let mut role = Role::Unknown;

        loop {
            tokio::select! {
                result = framed.next() => match result {
                    Some(Ok(message)) => handle_client_message(message, addr, state.clone(), &mut framed, &mut role, &mut heartbeat).await?,
                    Some(Err(_)) => framed.send(ServerError::from("unable to parse message")).await?,
                    None => break,
                },
                ticket = dispatch_ticket(&mut role) => match ticket {
                    Some(ticket) => framed.send(ticket).await?,
                    None => continue,
                },
                _ = heartbeat_tick(heartbeat.as_mut()) => {
                    framed.send(ServerHeartbeat).await?;
                }
            }
        }

        if let Role::Dispatcher { .. } = role {
            let mut state = state.lock();

            for (_, dispatchers) in state.dispatchers.iter_mut() {
                dispatchers.retain(|&(dispatcher_addr, _)| dispatcher_addr != addr)
            }

            state
                .dispatchers
                .retain(|_, dispatchers| !dispatchers.is_empty());
        }

        Ok(())
    }
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
            let camera = Camera { mile, limit };
            {
                let mut state = state.lock();
                state
                    .locked_plate
                    .entry(plate.clone())
                    .or_insert_with(|| Arc::new(Semaphore::new(1)));
                state
                    .observations
                    .entry((plate.clone(), road))
                    .or_default()
                    .push((timestamp, camera));
            }

            check_for_ticket(state, plate, road).await?;
        }
        (ClientMessage::Plate { .. }, role) => {
            warn!("[{}] with {:?} tried to send a plate", addr, role);
            framed
                .send(ServerError::from("only cameras can send plates"))
                .await?;
        }
        (ClientMessage::IAmCamera { road, mile, limit }, role @ Role::Unknown) => {
            *role = Role::Camera { road, mile, limit };
            debug!("[{}] registering camera ({:?})", addr, role);
        }
        (ClientMessage::IAmCamera { .. }, role) => {
            warn!("[{}] with {:?} tried to identify as a camera", addr, role);
            framed.send(ServerError::from("already identified")).await?;
        }
        (ClientMessage::IAmDispatcher { roads }, role @ Role::Unknown) => {
            let (tx, rx) = mpsc::unbounded_channel();
            {
                let mut state = state.lock();
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
                let mut state = state.lock();
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
            framed.send(ServerError::from("already identified")).await?;
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

async fn check_for_ticket(state: SharedState, plate: String, road: Road) -> anyhow::Result<()> {
    let semaphore = {
        let state = state.lock();
        state.locked_plate[&plate].clone()
    };

    let _permit = semaphore.acquire().await?;
    trace!("acquiring lock for {}", plate);

    let (observations, existing_dates) = {
        let state = state.lock();
        let observations = state
            .observations
            .get(&(plate.clone(), road))
            .cloned()
            .unwrap_or_default();
        let existing_dates = state
            .existing_tickets
            .get(&plate)
            .cloned()
            .unwrap_or_default();
        (observations, existing_dates)
    };

    let speeding_ticket = observations
        .into_iter()
        .filter(|&(t, _)| {
            let date = timestamp_to_date(t);
            !existing_dates.contains(&date)
        })
        .tuple_combinations()
        .find_map(|((ta, ca), (tb, cb))| {
            let speed = check_speed(ta, ca, tb, cb)?;
            let (timestamp1, mile1, timestamp2, mile2) = if ta < tb {
                (ta, ca.mile, tb, cb.mile)
            } else {
                (tb, cb.mile, ta, ca.mile)
            };

            let ticket = Ticket {
                plate: plate.clone(),
                road,
                mile1,
                timestamp1,
                mile2,
                timestamp2,
                speed,
            };

            Some(ticket)
        });

    if let Some(ticket) = speeding_ticket {
        issue_ticket(state.clone(), ticket).await?
    }

    Ok(())
}

fn check_speed(
    timestamp1: Timestamp,
    camera1: Camera,
    timestamp2: Timestamp,
    camera2: Camera,
) -> Option<u16> {
    let cmph_limit = camera1.limit as u32 * 100;
    let centimiles_traveled = (camera1.mile as i32 - camera2.mile as i32).unsigned_abs() * 100;
    let time_traveled = (timestamp1 as i64 - timestamp2 as i64).unsigned_abs() as u32;
    let cmph_traveled =
        (f64::from(centimiles_traveled) / f64::from(time_traveled) * 60.0 * 60.0) as u32;

    trace!(
        centimiles_traveled,
        time_traveled, cmph_traveled, cmph_limit, "checking"
    );

    if cmph_traveled > cmph_limit {
        Some(cmph_traveled as u16)
    } else {
        None
    }
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
    let dates = [timestamp1, timestamp2].map(timestamp_to_date);

    debug!(?ticket, "issuing ticket for {:?}", dates);

    let dispatcher = {
        let mut state = state.lock();
        state
            .existing_tickets
            .entry(ticket.plate.clone())
            .or_default()
            .extend(dates);

        let mut rng = rand::rng();
        state
            .dispatchers
            .get(&road)
            .and_then(|dispatchers| dispatchers.choose(&mut rng))
            .cloned()
    };

    match dispatcher {
        Some((_, tx)) => {
            trace!("sending ticket to a dispatcher");
            tx.send(ticket)?;
        }
        _ => {
            trace!(?ticket, "unable to dispatch ticket, saving");
            let mut state = state.lock();
            state.pending_tickets.push(ticket);
        }
    }

    Ok(())
}

async fn heartbeat_tick(maybe_heartbeat: Option<&mut Interval>) -> Instant {
    if let Some(heartbeat) = maybe_heartbeat {
        heartbeat.tick().await
    } else {
        future::pending().await
    }
}

fn timestamp_to_date(timestamp: Timestamp) -> u32 {
    (f64::from(timestamp) / 86400.0).floor() as u32
}
