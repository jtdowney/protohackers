use std::{
    cmp::Ordering,
    collections::{HashMap, hash_map::Entry},
    future, io,
    net::SocketAddr,
    pin::Pin,
    str,
    sync::Arc,
    task::{self, Poll},
    time::Duration,
};

use anyhow::{Context, ensure};
use bytes::{Bytes, BytesMut};
use futures_util::{FutureExt, SinkExt, StreamExt, ready};
use parking_lot::Mutex;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{ToSocketAddrs, UdpSocket},
    sync::mpsc,
    time::{self, Instant},
};
use tokio_util::udp::UdpFramed;
use tracing::{debug, error, trace, warn};

use super::codec::{LrcpCodec, Message};

const SESSION_TIMEOUT: Duration = Duration::from_secs(60);
const RETRANSMISSION_TIMEOUT: Duration = Duration::from_secs(3);
const ACK_COALESCE_WINDOW: Duration = Duration::from_millis(500);
const CHUNK_SIZE: usize = 900;

struct Session {
    active_time: Instant,
    addr: SocketAddr,
    read_tx: mpsc::UnboundedSender<Bytes>,
    received_data: String,
    sent_data: String,
    confirmed_sent_length: usize,
    last_confirmation: Instant,
    misbehaving: bool,
    retransmit_queue: Vec<(Instant, usize)>,
}

type State = HashMap<usize, Session>;
type SharedState = Arc<Mutex<State>>;

#[derive(Debug)]
pub struct WriteRequest {
    session_id: usize,
    data: Vec<u8>,
}

#[derive(Debug)]
pub struct AcceptState {
    session_id: usize,
    addr: SocketAddr,
    read_rx: mpsc::UnboundedReceiver<Bytes>,
}

pub struct LrcpStream {
    session_id: usize,
    read_rx: mpsc::UnboundedReceiver<Bytes>,
    write_tx: mpsc::UnboundedSender<WriteRequest>,
    buffer: BytesMut,
}

pub struct LrcpListener {
    connections_rx: mpsc::UnboundedReceiver<AcceptState>,
    write_tx: mpsc::UnboundedSender<WriteRequest>,
}

impl LrcpListener {
    pub async fn bind<A>(addr: A) -> anyhow::Result<Self>
    where
        A: ToSocketAddrs,
    {
        let state = SharedState::default();
        let socket = Arc::new(UdpSocket::bind(addr).await?);
        let (connections_tx, connections_rx) = mpsc::unbounded_channel();
        let (write_tx, write_rx) = mpsc::unbounded_channel();

        {
            tokio::spawn(async move {
                if let Err(e) = message_loop(socket, state, connections_tx, write_rx).await {
                    warn!(error = ?e, "error while processing message loop")
                }
            });
        }

        Ok(Self {
            connections_rx,
            write_tx,
        })
    }

    pub async fn accept(&mut self) -> anyhow::Result<(LrcpStream, SocketAddr)> {
        let AcceptState {
            session_id,
            addr,
            read_rx,
        } = self.connections_rx.recv().await.context("channel closed")?;
        let stream = LrcpStream::new(session_id, read_rx, self.write_tx.clone());

        Ok((stream, addr))
    }
}

fn send_chunked(
    message_tx: mpsc::UnboundedSender<(Message, SocketAddr)>,
    session_id: usize,
    addr: SocketAddr,
    start: usize,
    payload: impl AsRef<str>,
) -> anyhow::Result<()> {
    let chars = payload.as_ref().chars().collect::<Vec<_>>();
    for (i, chunk) in chars.chunks(CHUNK_SIZE).enumerate() {
        let payload = chunk.iter().collect::<String>();

        let position = start + (i * CHUNK_SIZE);
        let message = Message::Data {
            session_id,
            position,
            payload,
        };
        message_tx.send((message, addr))?;
    }

    Ok(())
}

async fn message_loop(
    socket: Arc<UdpSocket>,
    state: SharedState,
    connections_tx: mpsc::UnboundedSender<AcceptState>,
    mut write_rx: mpsc::UnboundedReceiver<WriteRequest>,
) -> anyhow::Result<()> {
    let mut framed = UdpFramed::new(socket.clone(), LrcpCodec);
    let (message_tx, mut message_rx) = mpsc::unbounded_channel();
    loop {
        let state = state.clone();
        let message_tx = message_tx.clone();
        let connections_tx = connections_tx.clone();

        house_keeping(state.clone(), message_tx.clone()).await?;

        tokio::select! {
            biased;

            result = write_rx.recv() => match result {
                Some(request) => dispatch_write_request(message_tx, state, request).await?,
                None => {
                    error!("data write channel closed");
                    break
                },
            },
            result = message_rx.recv() => match result {
                Some(message) => {
                    framed.send(message).await?;
                }
                None => {
                    error!("message write channel closed");
                    break
                },
            },
            result = framed.next() => match result {
                Some(Ok((message, addr))) => {
                    trace!(?message, "dispatching message");
                    if let Err(e) = dispatch_message(message_tx, state, addr, message, connections_tx).await {
                        warn!("error dispatching: {}", e);
                    }
                }
                Some(Err(e)) => {
                    warn!("error reading frame: {}", e);
                    continue
                }
                None => continue,
            },
            (session_id, lengths @ (start, end)) = retransmission_timeouts(state.clone()) => {
                debug!(session_id, ?lengths, "retransmitting due to timeout");

                let mut state = state.lock();
                if let Some(Session { addr, sent_data, last_confirmation, .. }) = state.get_mut(&session_id) {
                    *last_confirmation = Instant::now();

                    send_chunked(message_tx, session_id, *addr, start, &sent_data[start..end])?;
                }
            },
            session_id = session_timeouts(state.clone()) => {
                debug!(session_id, "closing due to timeout");
                let mut state = state.lock();
                if let Some(Session { addr, .. }) = state.remove(&session_id) {
                    let message = Message::Close { session_id };
                    message_tx.send((message, addr))?;
                }
            },
        }
    }

    Ok(())
}

async fn house_keeping(
    state: SharedState,
    message_tx: mpsc::UnboundedSender<(Message, SocketAddr)>,
) -> anyhow::Result<()> {
    let mut state = state.lock();

    let misbehaving_sessions = state
        .iter()
        .filter(|(_, s)| s.misbehaving)
        .map(|(&id, _)| id)
        .collect::<Vec<_>>();
    for session_id in misbehaving_sessions {
        if let Some(Session { addr, .. }) = state.remove(&session_id) {
            let message = Message::Close { session_id };
            message_tx.send((message, addr))?;
        }
    }

    let retransmit_sessions = state
        .iter()
        .flat_map(|(&id, s)| {
            s.retransmit_queue
                .iter()
                .enumerate()
                .filter_map(move |(i, &(time, start))| {
                    let window = Instant::now().duration_since(time);
                    if window > ACK_COALESCE_WINDOW {
                        Some((id, i, start))
                    } else {
                        None
                    }
                })
        })
        .collect::<Vec<_>>();
    for (session_id, index, start) in retransmit_sessions {
        let message_tx = message_tx.clone();

        debug!(session_id, start, "retransmitting data");
        if let Some(Session {
            retransmit_queue,
            addr,
            sent_data,
            ..
        }) = state.get_mut(&session_id)
        {
            retransmit_queue.remove(index);
            send_chunked(message_tx, session_id, *addr, start, &sent_data[start..])?;
        }
    }

    Ok(())
}

async fn session_timeouts(state: SharedState) -> usize {
    let sleeps = {
        let state = state.lock();
        state
            .iter()
            .map(|(&id, s)| {
                Box::pin(time::sleep_until(s.active_time + SESSION_TIMEOUT).map(move |_| id))
            })
            .collect::<Vec<_>>()
    };

    if sleeps.is_empty() {
        return future::pending().await;
    }

    let sleeps = futures_util::future::select_all(sleeps);
    let (session_id, _, _) = sleeps.await;
    session_id
}

async fn retransmission_timeouts(state: SharedState) -> (usize, (usize, usize)) {
    let sleeps = {
        let state = state.lock();
        state
            .iter()
            .filter(|(_, s)| s.sent_data.len() > s.confirmed_sent_length)
            .map(|(&id, s)| {
                let start = s.confirmed_sent_length;
                let end = s.sent_data.len();
                Box::pin(
                    time::sleep_until(s.last_confirmation + RETRANSMISSION_TIMEOUT)
                        .map(move |_| (id, (start, end))),
                )
            })
            .collect::<Vec<_>>()
    };

    if sleeps.is_empty() {
        return future::pending().await;
    }

    let sleeps = futures_util::future::select_all(sleeps);
    let (result, _, _) = sleeps.await;
    result
}

async fn dispatch_write_request(
    message_tx: mpsc::UnboundedSender<(Message, SocketAddr)>,
    state: SharedState,
    WriteRequest { session_id, data }: WriteRequest,
) -> anyhow::Result<()> {
    let payload = str::from_utf8(&data)?.to_owned();
    ensure!(!payload.is_empty(), "payload is empty");

    let mut state = state.lock();
    if let Some(Session {
        addr, sent_data, ..
    }) = state.get_mut(&session_id)
    {
        let position = sent_data.len();
        sent_data.push_str(&payload);

        send_chunked(message_tx, session_id, *addr, position, payload)?;
    }

    Ok(())
}

async fn dispatch_message(
    message_tx: mpsc::UnboundedSender<(Message, SocketAddr)>,
    state: SharedState,
    addr: SocketAddr,
    message: Message,
    connections_tx: mpsc::UnboundedSender<AcceptState>,
) -> anyhow::Result<()> {
    match message {
        Message::Connect { session_id } => {
            let mut state = state.lock();
            if let Entry::Vacant(entry) = state.entry(session_id) {
                debug!(session_id, "opening new session");

                let (read_tx, read_rx) = mpsc::unbounded_channel();
                let session = Session {
                    addr,
                    read_tx,
                    active_time: Instant::now(),
                    received_data: String::new(),
                    sent_data: String::new(),
                    confirmed_sent_length: 0,
                    last_confirmation: Instant::now(),
                    misbehaving: false,
                    retransmit_queue: vec![],
                };

                entry.insert(session);

                let accept = AcceptState {
                    addr,
                    read_rx,
                    session_id,
                };

                connections_tx.send(accept)?;
            }

            let message = Message::Ack {
                session_id,
                length: 0,
            };
            message_tx.send((message, addr))?;
        }
        Message::Ack { session_id, length } => {
            let mut state = state.lock();
            if let Some(Session {
                active_time,
                sent_data,
                confirmed_sent_length,
                misbehaving,
                retransmit_queue,
                last_confirmation,
                ..
            }) = state.get_mut(&session_id)
            {
                debug!(session_id, length, "received ack");

                *active_time = Instant::now();

                if length < *confirmed_sent_length {
                    trace!(
                        session_id,
                        length, confirmed_sent_length, "ignoring, already seen this ack"
                    );
                    return Ok(());
                }

                retransmit_queue.retain(|&(_, start)| start > length);

                let unconfirmed_sent_length = sent_data.len();
                match length.cmp(&unconfirmed_sent_length) {
                    Ordering::Less => retransmit_queue.push((Instant::now(), length)),
                    Ordering::Equal => {
                        trace!(session_id, length, "setting confirmed length");
                        *confirmed_sent_length = length;
                        *last_confirmation = Instant::now();
                    }
                    Ordering::Greater => {
                        *misbehaving = true;
                    }
                }
            }
        }
        Message::Data {
            session_id,
            position,
            payload,
        } => {
            let mut state = state.lock();
            if let Some(Session {
                active_time,
                addr,
                received_data,
                read_tx,
                ..
            }) = state.get_mut(&session_id)
            {
                *active_time = Instant::now();
                if position == received_data.len() {
                    debug!(session_id, position, ?payload, "received data");
                    received_data.push_str(&payload);

                    let message = Message::Ack {
                        session_id,
                        length: received_data.len(),
                    };
                    message_tx.send((message, *addr))?;

                    read_tx.send(payload.into())?;
                } else {
                    let length = received_data.len();
                    debug!(
                        session_id,
                        length, "position doesn't match received, resending ack of last length"
                    );
                    let message = Message::Ack { session_id, length };
                    message_tx.send((message, *addr))?;
                }
            }
        }
        Message::Close { session_id } => {
            debug!(session_id, "closing");

            {
                let mut state = state.lock();
                state.remove(&session_id);
            }

            let message = Message::Close { session_id };
            message_tx.send((message, addr))?;
        }
    }

    Ok(())
}

impl LrcpStream {
    pub fn new(
        session_id: usize,
        read_rx: mpsc::UnboundedReceiver<Bytes>,
        write_tx: mpsc::UnboundedSender<WriteRequest>,
    ) -> Self {
        Self {
            session_id,
            read_rx,
            write_tx,
            buffer: BytesMut::new(),
        }
    }
}

impl AsyncRead for LrcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.buffer.is_empty() {
            let length = buf.remaining().min(self.buffer.len());
            if length > 0 {
                let data = self.buffer.split_to(length);
                buf.put_slice(&data);
                return Poll::Ready(Ok(()));
            }
        }

        match ready!(self.read_rx.poll_recv(cx)) {
            Some(mut data) => {
                if data.len() > buf.remaining() {
                    let remaining = data.split_off(buf.remaining());
                    self.buffer.extend_from_slice(&remaining);
                }

                buf.put_slice(&data);
                Poll::Ready(Ok(()))
            }
            None => Poll::Pending,
        }
    }
}

impl AsyncWrite for LrcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let request = WriteRequest {
            session_id: self.session_id,
            data: buf.to_vec(),
        };

        match self.write_tx.send(request) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(_) => Poll::Ready(Err(io::ErrorKind::Other.into())),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}
