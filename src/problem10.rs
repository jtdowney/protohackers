use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;
use tracing::{debug, warn};

use self::codec::{Command, Reply, VcsCodec};
use crate::{
    problem10::codec::{ParseError, PlainCommand},
    server::ConnectionHandler,
};

mod codec;

#[derive(Debug)]
pub struct State {
    blobs: HashMap<[u8; 32], Bytes>,
    files: HashMap<String, Vec<[u8; 32]>>,
    directories: HashMap<String, HashSet<String>>,
}

type SharedState = Arc<RwLock<State>>;

impl Default for State {
    fn default() -> Self {
        let mut directories = HashMap::new();
        directories.insert("/".into(), HashSet::new());

        Self {
            directories,
            blobs: Default::default(),
            files: Default::default(),
        }
    }
}

pub struct Handler;

#[async_trait]
impl ConnectionHandler for Handler {
    type State = SharedState;

    async fn handle_connection(
        stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        _addr: SocketAddr,
        state: Self::State,
    ) -> anyhow::Result<()> {
        let mut framed = Framed::new(stream, VcsCodec);
        loop {
            framed.send(Reply::Ready).await?;

            match framed.next().await {
                Some(Ok(command)) => match command {
                    Command::Get { file, revision } => {
                        debug!(file, revision, "getting file");

                        let data = {
                            let state = state.read();
                            state
                                .files
                                .get(&file)
                                .and_then(|revisions| {
                                    let revision = revision.unwrap_or(revisions.len());
                                    revisions
                                        .get(revision - 1)
                                        .and_then(|blob| state.blobs.get(blob))
                                })
                                .cloned()
                        };

                        match data {
                            Some(bytes) => {
                                framed.send(Reply::OkWithData(bytes)).await?;
                            }
                            _ => {
                                framed.send(Reply::Error("no such file".into())).await?;
                            }
                        }
                    }
                    Command::Put { file, data } => {
                        debug!(file, n = data.len(), "putting file");

                        let hash: [u8; 32] = blake3::hash(&data).into();
                        {
                            let mut state = state.write();
                            state.blobs.entry(hash).or_insert_with(|| data.into());
                        }

                        let mut parts = file.split('/');
                        let _ = parts.next();
                        let mut parts = parts.collect::<Vec<_>>();
                        let filename = parts.pop().unwrap();

                        let mut path = String::from("/");
                        for part in parts {
                            let mut state = state.write();
                            state
                                .directories
                                .entry(path.clone())
                                .or_default()
                                .insert(part.into());

                            path = format!("{path}{part}/");
                            state.directories.entry(path.clone()).or_default();
                        }

                        let revision = {
                            let mut state = state.write();
                            state
                                .directories
                                .entry(path)
                                .or_default()
                                .insert(filename.into());
                            let revisions = state.files.entry(file).or_default();
                            if revisions.is_empty() {
                                revisions.push(hash);
                            } else {
                                let last = revisions.last().unwrap();
                                if last != &hash {
                                    revisions.push(hash);
                                }
                            }

                            revisions.len()
                        };

                        let message = format!("r{revision}");
                        framed.send(Reply::OkWithMessage(message)).await?;
                    }
                    Command::List { mut directory } => {
                        if !directory.ends_with('/') {
                            directory.push('/');
                        }

                        debug!(directory, "listing");

                        let mut entries = {
                            let state = state.read();
                            state
                                .directories
                                .get(&directory)
                                .map(|entries| {
                                    entries
                                        .iter()
                                        .flat_map(|name| {
                                            let mut path = format!("{directory}{name}");
                                            let file = state.files.get(&path).map(|revisions| {
                                                format!("{name} r{}", revisions.len())
                                            });

                                            path.push('/');
                                            let directory = state
                                                .directories
                                                .get(&path)
                                                .map(|_| format!("{name}/ DIR"));
                                            file.or(directory)
                                        })
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default()
                        };

                        entries.sort();

                        framed.send(Reply::OkWithCount(entries.len())).await?;
                        for entry in entries {
                            framed.send(entry).await?;
                        }
                    }
                },
                Some(Err(e)) => {
                    warn!(error = ?e, "error reading frame");
                    let message = match e {
                        ParseError::MalformedCommand(PlainCommand::Help) => {
                            framed
                                .send(Reply::OkWithMessage("usage: HELP|GET|PUT|LIST".into()))
                                .await?;
                            continue;
                        }
                        ParseError::MalformedCommand(PlainCommand::Get) => {
                            "usage: GET file [revision]".into()
                        }
                        ParseError::MalformedCommand(PlainCommand::Put) => {
                            "usage: PUT file length newline data".into()
                        }
                        ParseError::MalformedCommand(PlainCommand::List) => {
                            "usage: LIST dir".into()
                        }
                        ParseError::MalformedString => "strings must be UTF8 valid".into(),
                        ParseError::UnknownCommand(command) => {
                            format!("illegal method: {command}")
                        }
                        ParseError::Network(_) => break,
                    };

                    framed.send(Reply::Error(message)).await?;
                }
                None => break,
            }
        }

        Ok(())
    }
}
