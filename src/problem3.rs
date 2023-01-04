use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use anyhow::bail;
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, Mutex},
};
use tokio_util::codec::{Framed, LinesCodec};

pub struct Peer {
    tx: mpsc::UnboundedSender<String>,
    username: String,
}

#[derive(Default)]
pub struct State {
    peers: HashMap<SocketAddr, Peer>,
}

impl State {
    async fn broadcast(&mut self, sender: SocketAddr, message: &str) -> anyhow::Result<()> {
        for (&addr, Peer { tx, .. }) in self.peers.iter_mut() {
            if addr != sender {
                tx.send(message.into())?;
            }
        }

        Ok(())
    }
}

pub async fn handle<T>(stream: T, addr: SocketAddr, state: Arc<Mutex<State>>) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut framed = Framed::new(stream, LinesCodec::new());
    framed
        .send("Welcome to budgetchat! What shall I call you?")
        .await?;

    let Some(Ok(username)) = framed.next().await else {
        bail!("failed to get username");
    };

    if username.is_empty() || username.chars().any(|c| !c.is_alphanumeric()) {
        bail!("invalid username {username:?}")
    }

    {
        let state = state.lock().await;
        let usernames = state
            .peers
            .values()
            .map(|Peer { username, .. }| username.as_str())
            .collect::<Vec<&str>>();
        let message = format!("* The room contains: {}", usernames.join(", "));
        framed.send(message).await?;
    }

    let (tx, mut rx) = mpsc::unbounded_channel();
    {
        let mut state = state.lock().await;
        let username = username.clone();
        state.peers.insert(addr, Peer { tx, username });
    }

    {
        let mut state = state.lock().await;
        let message = format!("* {username} has entered the chat");
        state.broadcast(addr, &message).await?;
    }

    loop {
        tokio::select! {
            Some(message) = rx.recv() => {
                framed.send(message).await?;
            }
            result = framed.next() => match result {
                Some(Ok(message)) => {
                    let mut state = state.lock().await;
                    let message = format!("[{}] {}", username, message);
                    state.broadcast(addr, &message).await?;
                }
                Some(Err(e)) => {
                    bail!("an error occurred while processing messages for {}; error = {:?}", username, e);
                }
                None => break,
            },
        }
    }

    {
        let mut state = state.lock().await;
        state.peers.remove(&addr);

        let message = format!("* {username} has left the chat");
        state.broadcast(addr, &message).await?;
    }

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
