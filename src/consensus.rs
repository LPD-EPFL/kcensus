use crate::consensus::message::ConsensusMsg::{Commit, ReadRequest, ReadResponse};
use crate::consensus::message::{CommandBatch, ConsensusMessage};
use crate::consensus::read_tracker::ReadTracker;
use crate::eval;
use crate::message::Message::{ConsensusM, Done};
use crate::message::MsgWithSource;
use crate::multi_sink::{MultiSink, ShardMultiSink};
use command::Command;
use log::{debug, info};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

pub(crate) mod command;
pub mod kcensus;
pub(crate) mod message;
pub(crate) mod paxos_family;
mod read_tracker;

pub(crate) struct ConsensusShard<AlgoSettings, AlgoRoundState> {
    // Settings
    nb_nodes: usize,
    my_pid: usize,
    leader_priority: Vec<usize>,

    // Connections
    sinks: ShardMultiSink,

    // Overall state
    next_uid: usize,
    slot: usize,
    queued_commands: HashMap<usize, CommandBatch>,

    read_tracker: ReadTracker,

    settings: AlgoSettings,
    round_state: AlgoRoundState,
}

pub(crate) struct Consensus<AlgoSettings, AlgoRoundState> {
    nb_nodes: usize,
    shards: Vec<ConsensusShard<AlgoSettings, AlgoRoundState>>,
    sinks: Arc<Mutex<MultiSink>>,
}

pub(crate) trait ConsensusShardTrait {
    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<CommandBatch>>;

    fn can_forward_proposals(&self) -> bool;

    async fn propose_start(&mut self, value: CommandBatch, contention: bool) -> io::Result<()>;

    async fn repropose_start(&mut self, v: usize) -> io::Result<()>;

    fn commit_slot(&mut self, v: usize, from_commit_msg: bool) -> CommandBatch;

    fn get_my_v(&self) -> Option<usize>;

    fn ongoing(&self) -> bool;

    fn should_lead(&self) -> bool;
}

impl<AS, ARS> Consensus<AS, ARS>
where
    ConsensusShard<AS, ARS>: ConsensusShardTrait,
{
    pub async fn run(
        &mut self,
        mut msg_rx: Receiver<MsgWithSource>,
        mut new_client_commands_rx: Receiver<Command>,
        committed_commands_tx: Sender<Command>,
    ) -> io::Result<()> {
        let mut count_done = 0usize;
        let mut done = false;

        let mut queued_messages: Vec<VecDeque<ConsensusMessage>> =
            vec![VecDeque::with_capacity(self.nb_nodes); self.shards.len()];
        let likely_queued = (!self.shards[0].can_forward_proposals()) as usize;
        let mut my_queued_commands: Vec<VecDeque<Command>> =
            vec![VecDeque::with_capacity(likely_queued); self.shards.len()];

        'main_loop: while count_done < self.nb_nodes {
            // Read new messages and/or new local command
            let shard = select! {
                command = new_client_commands_rx.recv(), if !done => {
                    match command {
                        Some(command) =>  {
                            let shard = command.shard;
                            if command.read_only {
                                self.shards[shard].start_read(command).await?;
                            } else {
                                let ongoing = self.shards[shard].ongoing()
                                    || !queued_messages[shard].is_empty();
                                let contention = ongoing || self.shards[shard].has_queued_commands();
                                if self.shards[shard].can_forward_proposals() || !contention {
                                    self.shards[shard].propose_start(CommandBatch::Single(command), contention).await?;
                                } else {
                                    my_queued_commands[shard].push_back(command);
                                }
                            }
                            shard
                        }
                        None => {
                            debug_assert!(my_queued_commands.iter().all(|x| x.is_empty()));
                            done = true;
                            self.sinks.lock().await.broadcast(Done).await?;
                            count_done += 1;
                            continue 'main_loop;
                        }
                    }
                }
                opt_msg = msg_rx.recv() => {
                    let msg = opt_msg.unwrap();
                    let shard = match msg.msg {
                        ConsensusM { shard, msg, value } => {
                            let new_value = value.is_some();
                            if let Some(value) = value {
                                debug_assert!(msg.can_include_value());
                                let v = msg.get_v().expect("A value should travel with its uid");
                                self.shards[shard].store_remote_command(v, value);
                            } else {
                                debug_assert!(!msg.should_include_value());
                            };

                            if !self.shards[shard].ready_to_process(&msg) {
                                queued_messages[shard].push_back(msg);
                                continue 'main_loop;
                            }

                            let res_command = self.shards[shard].full_process_message(msg).await?;
                            let commited = self.shards[shard]
                                .commit_commands(&committed_commands_tx, res_command)
                                .await;

                            if !commited && !new_value {
                                continue 'main_loop; // No need to process queue nor repropose.
                            }

                            shard
                        }
                        Done => {
                            count_done += 1;
                            continue 'main_loop;
                        }
                        _ => panic!("Unexpected message type"),
                    };

                    // Process queued messages (if ready)
                    let mut i = 0usize;
                    while i < queued_messages[shard].len() {
                        // TODO: (Optim.) check Commit messages first ?
                        let msg = &queued_messages[shard][i];
                        if self.shards[shard].ready_to_process(msg) {
                            let msg = queued_messages[shard].remove(i).unwrap();
                            let batch = self.shards[shard].full_process_message(msg).await?;
                            if self.shards[shard]
                                .commit_commands(&committed_commands_tx, batch)
                                .await
                            {
                                i = 0; // Restart from the beginning of the queue
                            }
                        } else {
                            i += 1;
                        }
                    }

                    shard
                },
            };

            let ongoing = self.shards[shard].ongoing() || !queued_messages[shard].is_empty();
            let should_repropose = !ongoing && self.shards[shard].has_queued_commands();
            let contention = ongoing || should_repropose;

            if ongoing && !self.shards[shard].ongoing() {
                debug!(
                    "Consensus is not running but messages are still queued. my slot: {:?}, queue: {:?}",
                    self.shards[shard].slot, queued_messages[shard]
                );
            }

            // Process queued commands
            // TODO: (Optim.) peak connection first ?
            if should_repropose && self.shards[shard].should_lead() {
                if let Some(batch) = self.shards[shard].get_new_batch_to_propose() {
                    self.shards[shard].propose_start(batch, false).await?;
                } else {
                    let v = self.shards[shard].get_v_to_repropose();
                    self.shards[shard].repropose_start(v).await?;
                }
            } else if !contention && !my_queued_commands[shard].is_empty() {
                let command = my_queued_commands[shard].pop_front().unwrap();
                assert!(!command.read_only, "read_only command wrongly queued");
                self.shards[shard]
                    .propose_start(CommandBatch::Single(command), false)
                    .await?;
            }
        } // 'main_loop: loop

        let sinks = self.sinks.lock().await;
        eval::log(
            "network-done",
            &format!(
                "Sent {} messages ({} bytes)",
                sinks.stats.msg_count, sinks.stats.byte_count
            ),
            &sinks.stats,
        );

        new_client_commands_rx.close();
        msg_rx.close();
        Ok(())
    } // run
}

impl<AS, ARS> ConsensusShard<AS, ARS>
where
    ConsensusShard<AS, ARS>: ConsensusShardTrait,
{
    #[inline]
    async fn start_read(&mut self, command: Command) -> io::Result<()> {
        let local_ready = self.get_my_v().is_none();
        let uid = self.read_tracker.insert(command, local_ready);
        self.sinks.broadcast(ReadRequest { uid }, None).await
    }

    #[inline]
    fn ready_to_process(&self, msg: &ConsensusMessage) -> bool {
        if msg.get_slot() == self.slot {
            match msg.get_v() {
                None => true,
                Some(v) => match self.queued_commands.get(&v) {
                    None => false,
                    Some(CommandBatch::Single(_)) => true,
                    Some(CommandBatch::Batch(vs)) => {
                        vs.iter().all(|v| self.queued_commands.contains_key(v))
                    }
                },
            }
        } else {
            msg.get_slot() < self.slot // "Process" messages from lower slots, regardless of value
        }
    }

    async fn full_process_message(
        &mut self,
        msg: ConsensusMessage,
    ) -> io::Result<Option<CommandBatch>> {
        debug_assert!(self.ready_to_process(&msg));
        if let Commit { slot, v } = msg.msg {
            if slot < self.slot {
                return Ok(None);
            }
            debug_assert_eq!(slot, self.slot);
            info!("Commit msg: v={v}");
            return Ok(Some(self.commit_slot(v, true)));
        };
        if let ReadRequest { uid } = msg.msg {
            let next_readable_slot = self.slot + self.get_my_v().is_some() as usize;
            self.sinks
                .send(
                    ReadResponse {
                        uid,
                        next_readable_slot,
                    },
                    None,
                    msg.src,
                )
                .await?;
            return Ok(None);
        }
        if let ReadResponse { uid, .. } = msg.msg {
            return Ok(self
                .read_tracker
                .receive_ready(uid)
                .map(CommandBatch::Single));
        }
        debug!("Processing message: {msg:?}");
        self.process_message(msg).await
    }

    #[inline]
    async fn commit_commands(
        &mut self,
        committed_commands_tx: &Sender<Command>,
        batch: Option<CommandBatch>,
    ) -> bool {
        if let Some(batch) = batch {
            let commit = async |command: Command| {
                committed_commands_tx
                    .send(command)
                    .await
                    .expect("Sending commited value");
            };

            match batch {
                CommandBatch::Single(command) => {
                    commit(command).await;
                }
                CommandBatch::Batch(vs) => {
                    for v in vs {
                        let command = self.remove_command(v);
                        if let CommandBatch::Single(command) = command {
                            commit(command).await;
                        } else {
                            panic!("Batches should not include batches.")
                        }
                    }
                }
            }

            let result = self.read_tracker.commit_slot();
            for read_only_command in result.into_iter() {
                commit(read_only_command).await;
            }

            true
        } else {
            false
        }
    }

    #[inline]
    fn store_new_command(&mut self, value: CommandBatch) -> usize {
        let v = self.get_next_uid();
        let old = self.queued_commands.insert(v, value);
        debug_assert!(old.is_none());
        v
    }

    #[inline]
    fn store_remote_command(&mut self, v: usize, value: CommandBatch) {
        // TODO: Allow forwarding values ? (could the value already be there ?)
        let inserted = self.queued_commands.insert(v, value);
        debug_assert!(inserted.is_none());
    }

    #[inline]
    fn has_queued_commands(&self) -> bool {
        !self.queued_commands.is_empty()
    }

    fn get_new_batch_to_propose(&self) -> Option<CommandBatch> {
        if self.queued_commands.len() <= 1 {
            return None;
        }
        let mut vs: Vec<_> = self.queued_commands.keys().copied().collect();
        vs.retain(|v| matches!(self.queued_commands[v], CommandBatch::Single(_)));
        Some(CommandBatch::Batch(vs))
    }

    #[inline]
    fn get_v_to_repropose(&self) -> usize {
        *self.queued_commands.keys().min().unwrap()
    }

    fn remove_command(&mut self, v: usize) -> CommandBatch {
        let out = self.queued_commands.remove(&v);
        self.queued_commands.retain(|_, value| match value {
            CommandBatch::Single(_) => true,
            CommandBatch::Batch(vs) => !vs.contains(&v),
        });
        out.expect("Removing command that does not exist")
    }

    #[inline]
    fn get_requester(&self, v: usize) -> Option<usize> {
        let cmd = self
            .queued_commands
            .get(&v)
            .expect("Queued command not found");
        if let CommandBatch::Single(cmd) = cmd {
            Some(cmd.requester)
        } else {
            None
        }
    }

    fn get_next_uid(&mut self) -> usize {
        let uid = self.next_uid;
        self.next_uid += self.nb_nodes;
        uid
    }
}
