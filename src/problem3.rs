use std::{net::SocketAddr, pin::pin, sync::Arc};

use anyhow::bail;
use futures_concurrency::stream::Merge;
use futures_util::{stream, SinkExt, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tokio_util::codec::Framed;

use crate::codec::StrictLinesCodec;

enum Action {
    SentMessage(String),
    ReceivedMessage(String),
    Disconnect,
}

pub struct Peer {
    tx: mpsc::UnboundedSender<String>,
    username: String,
}

#[derive(Default)]
pub struct State {
    peers: lockfree::map::Map<SocketAddr, Peer>,
}

type SharedState = Arc<State>;

impl State {
    fn broadcast(&self, sender: SocketAddr, message: &str) -> anyhow::Result<()> {
        for guard in &self.peers {
            let addr = *guard.key();
            let Peer { tx, .. } = guard.val();

            if addr != sender {
                tx.send(message.into())?;
            }
        }

        Ok(())
    }
}

pub async fn handle<T>(stream: T, addr: SocketAddr, state: SharedState) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let (mut framed_tx, mut framed_rx) = Framed::new(stream, StrictLinesCodec::default()).split();
    framed_tx
        .send("Welcome to budgetchat! What shall I call you?".into())
        .await?;

    let Some(Ok(username)) = framed_rx.next().await else {
        bail!("failed to get username");
    };

    if username.is_empty() || username.chars().any(|c| !c.is_alphanumeric()) {
        bail!("invalid username {username:?}")
    }

    let message = {
        let usernames = state
            .peers
            .iter()
            .map(|guard| guard.val().username.as_str().to_owned())
            .collect::<Vec<String>>();
        format!("* The room contains: {}", usernames.join(", "))
    };
    framed_tx.send(message).await?;

    let (tx, rx) = mpsc::unbounded_channel();
    state.peers.insert(
        addr,
        Peer {
            tx,
            username: username.clone(),
        },
    );

    let message = format!("* {username} has entered the chat");
    state.broadcast(addr, &message)?;

    let rx_stream = stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|message| (message, rx))
    })
    .map(|message| anyhow::Ok(Action::ReceivedMessage(message)));

    let tx_stream = framed_rx
        .map(|message| {
            message.map(|message| Action::SentMessage(format!("[{username}] {message}")))
        })
        .chain(stream::once(async { Ok(Action::Disconnect) }));

    let stream = (rx_stream, tx_stream).merge();
    let mut stream = pin!(stream);

    while let Some(action) = stream.next().await {
        match action {
            Ok(Action::ReceivedMessage(message)) => {
                framed_tx.send(message).await?;
            }
            Ok(Action::SentMessage(message)) => {
                state.broadcast(addr, &message)?;
            }
            Ok(Action::Disconnect) => break,
            Err(e) => bail!(
                "an error occurred while processing messages for {}; error = {:?}",
                username,
                e
            ),
        }
    }

    state.peers.remove(&addr);

    let message = format!("* {username} has left the chat");
    state.broadcast(addr, &message)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn single_user_session() -> anyhow::Result<()> {
        let stream = tokio_test::io::Builder::new()
            .write(b"Welcome to budgetchat! What shall I call you?\n")
            .read(b"user1\n")
            .write(b"* The room contains: \n")
            .build();

        let _ = handle(stream, "127.0.0.1:8080".parse()?, Default::default()).await;

        Ok(())
    }
}
