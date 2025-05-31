mod codec;

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    net::SocketAddr,
    sync::Arc,
};

use anyhow::anyhow;
use codec::{Action, Message, PestControlCodec, TargetPopulation};
use futures_util::{Sink, SinkExt, StreamExt};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{Mutex, mpsc, oneshot},
};
use tokio_util::codec::Framed;

use crate::server::ConnectionHandler;

const AUTHORITY_ADDR: &str = "pestcontrol.protohackers.com:20547";
const PROTOCOL: &str = "pestcontrol";
const VERSION: u32 = 1;

#[derive(Clone, Default)]
pub struct Handler {
    site_manager_registry: Arc<Mutex<HashMap<u32, mpsc::Sender<SiteVisitCommand>>>>,
}

#[derive(Debug)]
struct SiteVisitCommand {
    populations: Vec<codec::Population>,
    response: oneshot::Sender<anyhow::Result<()>>,
}

#[derive(Debug, Error)]
enum AuthorityError {
    #[error("policy does not exist")]
    PolicyNotFound,
    #[error("policy already exists")]
    PolicyAlreadyExists,
    #[error("authority error: {0}")]
    Other(String),
}

struct SiteManager {
    site: u32,
    authority_stream: Option<Framed<TcpStream, PestControlCodec>>,
    target_populations: Option<Vec<TargetPopulation>>,
    current_policies: HashMap<String, (u32, Action)>,
}

#[derive(Debug)]
enum PolicyOp {
    Delete { species: String, policy_id: u32 },
    Create { species: String, action: Action },
}

async fn send_error<S, M>(sink: &mut S, message: M) -> anyhow::Result<()>
where
    S: Sink<Message> + Unpin,
    S::Error: Display,
    M: Into<String>,
{
    let error = Message::Error {
        message: message.into(),
    };
    sink.send(error)
        .await
        .map_err(|e| anyhow::anyhow!("send error: {}", e))
}

impl SiteManager {
    fn new(site: u32) -> Self {
        Self {
            site,
            authority_stream: None,
            target_populations: None,
            current_policies: HashMap::new(),
        }
    }

    async fn run(mut self, mut rx: mpsc::Receiver<SiteVisitCommand>) {
        while let Some(command) = rx.recv().await {
            let result = self.handle_site_visit(&command.populations).await;
            let _ = command.response.send(result);
        }
    }

    async fn handle_site_visit(&mut self, populations: &[codec::Population]) -> anyhow::Result<()> {
        let mut species_totals: HashMap<String, u32> = HashMap::new();
        for pop in populations {
            if let Some(existing_count) = species_totals.get(&pop.species) {
                if *existing_count != pop.count {
                    return Err(anyhow!(
                        "conflicting population counts for species '{}': {} vs {}",
                        pop.species,
                        existing_count,
                        pop.count
                    ));
                }
            } else {
                species_totals.insert(pop.species.clone(), pop.count);
            }
        }

        if self.authority_stream.is_none() {
            let (stream, target_pops) =
                Self::connect_to_authority_and_get_populations(self.site).await?;
            self.authority_stream = Some(stream);
            self.target_populations = Some(target_pops);
        }

        let target_populations = self.target_populations.as_ref().unwrap();
        let mut species_observed = species_totals;
        let mut policy_operations = Vec::new();

        for target in target_populations {
            let count = species_observed.remove(&target.species).unwrap_or(0);

            let desired_action = if count < target.min {
                Some(Action::Conserve)
            } else if count > target.max {
                Some(Action::Cull)
            } else {
                None
            };

            match (
                self.current_policies.get(&target.species).cloned(),
                desired_action,
            ) {
                (Some((policy_id, _)), None) => {
                    policy_operations.push(PolicyOp::Delete {
                        species: target.species.clone(),
                        policy_id,
                    });
                }
                (Some((policy_id, current_action)), Some(new_action)) => {
                    if current_action != new_action {
                        policy_operations.push(PolicyOp::Delete {
                            species: target.species.clone(),
                            policy_id,
                        });
                        policy_operations.push(PolicyOp::Create {
                            species: target.species.clone(),
                            action: new_action,
                        });
                    }
                }
                (None, Some(action)) => {
                    policy_operations.push(PolicyOp::Create {
                        species: target.species.clone(),
                        action,
                    });
                }
                (None, None) => {}
            }
        }

        let controlled_species: HashSet<String> = target_populations
            .iter()
            .map(|t| t.species.clone())
            .collect();

        let policies_to_remove: Vec<_> = self
            .current_policies
            .iter()
            .filter(|(species, _)| !controlled_species.contains(*species))
            .map(|(species, (policy_id, _))| (species.clone(), *policy_id))
            .collect();

        for (species, policy_id) in policies_to_remove {
            policy_operations.push(PolicyOp::Delete { species, policy_id });
        }

        self.execute_policy_operations(policy_operations).await?;

        Ok(())
    }

    async fn execute_policy_operations(&mut self, operations: Vec<PolicyOp>) -> anyhow::Result<()> {
        if operations.is_empty() {
            return Ok(());
        }

        let authority_stream = self
            .authority_stream
            .as_mut()
            .ok_or_else(|| anyhow!("no authority connection"))?;

        for op in operations {
            match op {
                PolicyOp::Delete { species, policy_id } => {
                    let delete_msg = Message::DeletePolicy { policy: policy_id };
                    authority_stream.send(delete_msg).await?;
                    let response = authority_stream.next().await;

                    match response {
                        Some(Ok(Message::Ok)) => {
                            self.current_policies.remove(&species);
                        }
                        Some(Ok(Message::Error { message })) => {
                            match Self::classify_authority_error(&message) {
                                AuthorityError::PolicyNotFound => {
                                    self.current_policies.remove(&species);
                                }
                                AuthorityError::Other(msg) => {
                                    return Err(anyhow!("error deleting policy: {}", msg));
                                }
                                _ => {
                                    return Err(anyhow!("error deleting policy: {}", message));
                                }
                            }
                        }
                        Some(Err(e)) => {
                            return Err(anyhow!("error receiving delete response: {}", e));
                        }
                        None => {
                            return Err(anyhow!(
                                "connection closed while waiting for delete response"
                            ));
                        }
                        Some(Ok(msg)) => {
                            return Err(anyhow!("unexpected response to DeletePolicy: {:?}", msg));
                        }
                    }
                }
                PolicyOp::Create { species, action } => {
                    let create_msg = Message::CreatePolicy {
                        species: species.clone(),
                        action,
                    };
                    authority_stream.send(create_msg).await?;
                    let response = authority_stream.next().await;

                    match response {
                        Some(Ok(Message::PolicyResult { policy })) => {
                            self.current_policies.insert(species, (policy, action));
                        }
                        Some(Ok(Message::Error { message })) => {
                            match Self::classify_authority_error(&message) {
                                AuthorityError::PolicyAlreadyExists => {
                                    self.current_policies.remove(&species);
                                }
                                _ => {
                                    return Err(anyhow!("error creating policy: {}", message));
                                }
                            }
                        }
                        Some(Err(e)) => {
                            return Err(anyhow!("error receiving create response: {}", e));
                        }
                        None => {
                            return Err(anyhow!(
                                "connection closed while waiting for create response"
                            ));
                        }
                        Some(Ok(msg)) => {
                            return Err(anyhow!("unexpected response to CreatePolicy: {:?}", msg));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn classify_authority_error(message: &str) -> AuthorityError {
        if message.contains("does not exist") || message.contains("no such policy") {
            AuthorityError::PolicyNotFound
        } else if message.contains("already have a policy") {
            AuthorityError::PolicyAlreadyExists
        } else {
            AuthorityError::Other(message.to_string())
        }
    }

    async fn connect_to_authority_and_get_populations(
        site: u32,
    ) -> anyhow::Result<(Framed<TcpStream, PestControlCodec>, Vec<TargetPopulation>)> {
        let stream = TcpStream::connect(AUTHORITY_ADDR).await?;
        let mut framed = Framed::new(stream, PestControlCodec);

        let hello = Message::Hello {
            protocol: PROTOCOL.to_string(),
            version: VERSION,
        };

        framed.send(hello).await?;

        match framed.next().await {
            Some(Ok(Message::Hello { protocol, version })) => {
                if protocol != PROTOCOL || version != VERSION {
                    return Err(anyhow!("invalid hello from authority"));
                }
            }
            _ => return Err(anyhow!("expected hello from authority")),
        }

        let dial = Message::DialAuthority { site };
        framed.send(dial).await?;

        let target_populations = match framed.next().await {
            Some(Ok(Message::TargetPopulations {
                site: recv_site,
                populations,
            })) => {
                if recv_site != site {
                    return Err(anyhow!("received populations for wrong site"));
                }
                populations
            }
            _ => return Err(anyhow!("expected target populations")),
        };

        Ok((framed, target_populations))
    }
}

#[async_trait::async_trait]
impl ConnectionHandler for Handler {
    type State = Handler;

    async fn handle_connection(
        stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        _addr: SocketAddr,
        state: Self::State,
    ) -> anyhow::Result<()> {
        let framed = Framed::new(stream, PestControlCodec);
        let (mut sink, mut stream) = framed.split();

        let hello = Message::Hello {
            protocol: PROTOCOL.to_string(),
            version: VERSION,
        };

        sink.send(hello)
            .await
            .map_err(|e| anyhow!("send error: {}", e))?;

        match stream.next().await {
            Some(Ok(Message::Hello { protocol, version })) => {
                if protocol != PROTOCOL || version != VERSION {
                    send_error(&mut sink, "Invalid protocol or version").await?;
                    return Ok(());
                }
            }
            Some(Ok(_)) | Some(Err(_)) => {
                send_error(&mut sink, "first message must be Hello").await?;
                return Ok(());
            }
            None => {
                return Ok(());
            }
        }

        while let Some(msg) = stream.next().await {
            match msg {
                Ok(Message::SiteVisit { site, populations }) => {
                    if let Err(e) = state.handle_site_visit(site, &populations).await {
                        send_error(&mut sink, format!("Error processing site visit: {}", e))
                            .await?;
                        return Err(e);
                    }
                }
                Ok(_) => {
                    send_error(&mut sink, "Unexpected message type").await?;
                    return Ok(());
                }
                Err(e) => {
                    send_error(&mut sink, format!("Error: {}", e)).await?;
                    return Ok(());
                }
            }
        }

        Ok(())
    }
}

impl Handler {
    async fn get_or_create_site_manager(&self, site: u32) -> mpsc::Sender<SiteVisitCommand> {
        let mut registry = self.site_manager_registry.lock().await;

        if let Some(sender) = registry.get(&site) {
            if !sender.is_closed() {
                return sender.clone();
            }
            registry.remove(&site);
        }

        let (tx, rx) = mpsc::channel::<SiteVisitCommand>(100);
        let manager = SiteManager::new(site);

        tokio::spawn(async move {
            manager.run(rx).await;
        });

        registry.insert(site, tx.clone());
        tx
    }

    async fn handle_site_visit(
        &self,
        site: u32,
        populations: &[codec::Population],
    ) -> anyhow::Result<()> {
        let site_manager = self.get_or_create_site_manager(site).await;
        let (response_tx, response_rx) = oneshot::channel();

        let command = SiteVisitCommand {
            populations: populations.to_vec(),
            response: response_tx,
        };

        if site_manager.send(command).await.is_err() {
            return Err(anyhow!("Site manager for site {} is not responding", site));
        }

        match response_rx.await {
            Ok(result) => result,
            Err(_) => Err(anyhow!(
                "Site manager for site {} dropped response channel",
                site
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio_test::io::Builder;

    use super::*;

    #[tokio::test]
    async fn test_handler_hello_exchange() {
        let client_hello = vec![
            0x50, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x0b, 0x70, 0x65, 0x73, 0x74, 0x63,
            0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x00, 0x00, 0x00, 0x01, 0xce,
        ];

        let server_hello = vec![
            0x50, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x0b, 0x70, 0x65, 0x73, 0x74, 0x63,
            0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x00, 0x00, 0x00, 0x01, 0xce,
        ];

        let mock = Builder::new()
            .read(&client_hello)
            .write(&server_hello)
            .build();

        let handler = Handler::default();
        let result =
            Handler::handle_connection(mock, "127.0.0.1:0".parse().unwrap(), handler).await;

        assert!(result.is_ok());
    }
}
