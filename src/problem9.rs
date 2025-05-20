use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use priority_queue::PriorityQueue;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tokio_util::codec::Framed;
use tracing::{debug, trace, warn};

use crate::{codec::JsonLinesCodec, server::ConnectionHandler};

type Job = serde_json::Map<String, serde_json::Value>;

#[derive(Debug)]
struct InProgressJob {
    id: u64,
    priority: u64,
    job: Job,
    queue: String,
}

#[derive(Default)]
pub struct State {
    jobs: HashMap<u64, Job>,
    queues: HashMap<String, PriorityQueue<u64, u64>>,
    waiting_queues: HashMap<String, HashMap<SocketAddr, mpsc::UnboundedSender<InProgressJob>>>,
    in_progress: HashMap<SocketAddr, InProgressJob>,
    last_job_id: u64,
}

type SharedState = Arc<Mutex<State>>;

#[derive(Debug, Deserialize)]
#[serde(tag = "request", rename_all = "lowercase")]
enum Request {
    Put {
        queue: String,
        job: Job,
        #[serde(rename = "pri")]
        priority: u64,
    },
    Get {
        queues: Vec<String>,
        #[serde(default)]
        wait: bool,
    },
    Delete {
        id: u64,
    },
    Abort {
        id: u64,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum Response {
    Ok,
    #[serde(rename(serialize = "ok"))]
    JobEnqueued {
        id: u64,
    },
    #[serde(rename(serialize = "ok"))]
    Job {
        id: u64,
        job: Job,
        queue: String,
        #[serde(rename = "pri")]
        priority: u64,
    },
    #[serde(rename(serialize = "no-job"))]
    NoJob,
    Error {
        error: String,
    },
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
        let mut framed = Framed::new(stream, JsonLinesCodec::<Request, Response>::default());
        while let Some(frame) = framed.next().await {
            match frame {
                Ok(Request::Put {
                    queue,
                    job,
                    priority,
                }) => {
                    debug!(?queue, priority, "received put");

                    let id = {
                        let mut state = state.lock();
                        state.last_job_id += 1;
                        let id = state.last_job_id;
                        state.jobs.insert(id, job);
                        state
                            .queues
                            .entry(queue.clone())
                            .or_default()
                            .push(id, priority);

                        id
                    };

                    trace!(id, "job enqueued");
                    notify_waiting_workers(state.clone(), queue, id, priority)?;

                    framed.send(Response::JobEnqueued { id }).await?;
                }
                Ok(Request::Get { queues, wait }) => {
                    debug!(?queues, wait, "received get");

                    let candidate = {
                        let state = state.lock();
                        state
                            .queues
                            .iter()
                            .filter(|(queue, _)| queues.contains(queue))
                            .filter_map(|(name, queue)| {
                                queue.peek().map(|(&id, &priority)| (name, id, priority))
                            })
                            .max_by_key(|&(_, _, priority)| priority)
                            .map(|(name, id, priority)| (name.clone(), id, priority))
                    };

                    let candidate = candidate.and_then(|(queue, id, priority)| {
                        take_job(state.clone(), addr, id, &queue, priority)
                            .map(move |job| (queue, id, priority, job))
                    });
                    if let Some((queue, id, priority, job)) = candidate {
                        let response = Response::Job {
                            id,
                            job,
                            queue,
                            priority,
                        };
                        trace!(id, ?addr, "handing out job");
                        framed.send(response).await?;
                        continue;
                    }

                    if wait {
                        trace!(?addr, "worker waiting");

                        let mut rx = {
                            let mut state = state.lock();
                            let (tx, rx) = mpsc::unbounded_channel();
                            for queue in queues {
                                state
                                    .waiting_queues
                                    .entry(queue)
                                    .or_default()
                                    .insert(addr, tx.clone());
                            }

                            rx
                        };

                        if let Some(InProgressJob {
                            id,
                            priority,
                            job,
                            queue,
                        }) = rx.recv().await
                        {
                            trace!(id, ?addr, "got job for waiting worker");

                            let response = Response::Job {
                                id,
                                job,
                                queue,
                                priority,
                            };
                            framed.send(response).await?;
                        }
                    } else {
                        trace!(?addr, "sending no job");
                        framed.send(Response::NoJob).await?;
                    }
                }
                Ok(Request::Delete { id }) => {
                    debug!(id, ?addr, "received delete");

                    let removed = {
                        let mut state = state.lock();
                        if state.jobs.remove(&id).is_some() {
                            for queue in state.queues.values_mut() {
                                queue.remove(&id);
                            }

                            state.in_progress.retain(|_, j| j.id != id);

                            true
                        } else {
                            false
                        }
                    };

                    if removed {
                        trace!(?addr, "sending ok to delete");
                        framed.send(Response::Ok).await?;
                    } else {
                        trace!(?addr, "sending no job to delete");
                        framed.send(Response::NoJob).await?;
                    }
                }
                Ok(Request::Abort { id }) => {
                    debug!(id, ?addr, "received abort");

                    let valid = {
                        let state = state.lock();
                        state
                            .in_progress
                            .get(&addr)
                            .map(|j| j.id == id)
                            .unwrap_or_default()
                    };

                    if valid {
                        requeue_in_progress_job(state.clone(), addr)?;
                        trace!(?addr, "sending ok to abort");
                        framed.send(Response::Ok).await?;
                    } else {
                        trace!(?addr, "sending no job to abort");
                        framed.send(Response::NoJob).await?;
                    }
                }
                Err(e) => {
                    warn!(error = ?e, "error reading frame");
                    framed
                        .send(Response::Error {
                            error: "error reading request".into(),
                        })
                        .await?;
                }
            }
        }

        requeue_in_progress_job(state, addr)?;

        Ok(())
    }
}

fn notify_waiting_workers(
    state: SharedState,
    queue: String,
    id: u64,
    priority: u64,
) -> Result<(), anyhow::Error> {
    debug!(queue, id, "notifying any waiting workers");

    let waiter = {
        let mut rng = rand::rng();
        let mut state = state.lock();
        state
            .waiting_queues
            .entry(queue.clone())
            .or_default()
            .iter()
            .choose(&mut rng)
            .map(|(&addr, tx)| (addr, tx.clone()))
    };

    if let Some((addr, tx)) = waiter {
        trace!(?addr, "found worker to notify");

        if let Some(job) = take_job(state, addr, id, &queue, priority) {
            let job = InProgressJob {
                id,
                priority,
                job,
                queue,
            };
            tx.send(job)?;
        }
    }

    Ok(())
}

fn take_job(
    state: SharedState,
    addr: SocketAddr,
    id: u64,
    queue: &str,
    priority: u64,
) -> Option<Job> {
    let mut state = state.lock();
    let job = state.jobs.get(&id)?.clone();
    state.queues.get_mut(queue)?.pop();
    state.in_progress.insert(
        addr,
        InProgressJob {
            id,
            priority,
            job: job.clone(),
            queue: queue.into(),
        },
    );

    Some(job)
}

fn requeue_in_progress_job(state: SharedState, addr: SocketAddr) -> anyhow::Result<()> {
    let requeued_job = {
        let mut state = state.lock();
        match state.in_progress.remove(&addr) {
            Some(InProgressJob {
                id,
                priority,
                job,
                queue,
            }) => {
                debug!(id, queue, ?addr, "requing job");
                state.jobs.insert(id, job);
                state
                    .queues
                    .entry(queue.clone())
                    .or_default()
                    .push(id, priority);

                Some((id, priority, queue))
            }
            _ => None,
        }
    };

    if let Some((id, priority, queue)) = requeued_job {
        notify_waiting_workers(state, queue, id, priority)?;
    }

    Ok(())
}
