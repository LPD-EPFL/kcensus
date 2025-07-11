use crate::consensus::message::ConsensusMsg::{Commit, ReadRequest, ReadResponse};
use crate::consensus::message::{CommandBatch, ConsensusMessage};
use crate::consensus::read_tracker::ReadTracker;
use crate::eval;
use crate::message::Message::{ConsensusM, Done};
use crate::message::MsgWithSource;
use crate::multi_sink::{MultiSink, ShardMultiSink};
use command::Command;
use log::{debug, info};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::rc::Rc;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};

pub(crate) mod command;
pub mod kcensus;
pub(crate) mod message;
pub(crate) mod paxos_family;
mod read_tracker;

pub(crate) struct ConsensusShard<AlgoSettings, AlgoRound, AlgoRoundState> {
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
    round: AlgoRound,
    round_state: AlgoRoundState,
}

pub(crate) struct Consensus<AlgoSettings, AlgoRound, AlgoRoundState> {
    nb_nodes: usize,
    shards: Vec<ConsensusShard<AlgoSettings, AlgoRound, AlgoRoundState>>,
    sinks: Rc<RefCell<MultiSink>>,
}

pub(crate) trait ConsensusShardTrait {
    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<CommandBatch>>;

    fn can_forward_proposals(&self) -> bool;

    async fn propose_start(&mut self, value: CommandBatch, contention: bool) -> io::Result<()>;

    async fn repropose_start(&mut self, v: usize) -> io::Result<()>;

    fn commit_slot(&mut self, v: usize, from_commit_msg: bool) -> CommandBatch;

    fn get_my_v(&self) -> Option<usize>;

    fn should_lead(&self) -> bool;
}

impl<AS, AR, ARS> Consensus<AS, AR, ARS>
where
    ConsensusShard<AS, AR, ARS>: ConsensusShardTrait,
{
    pub async fn run(
        &mut self,
        mut msg_rx: Receiver<MsgWithSource>,
        mut new_client_commands_rx: Receiver<Command>,
        committed_commands_tx: Sender<Command>,
    ) -> io::Result<()> {
        let mut count_done = 0usize;
        let mut done = false;
        let mut active_shard = 0;

        let mut queued_messages: Vec<VecDeque<ConsensusMessage>> =
            vec![VecDeque::with_capacity(self.nb_nodes); self.shards.len()];
        let likely_queued = (!self.shards[0].can_forward_proposals()) as usize;
        let mut my_queued_commands: Vec<VecDeque<Command>> =
            vec![VecDeque::with_capacity(likely_queued); self.shards.len()];
        let mut should_recheck_queue = false;

        'main_loop: while count_done < self.nb_nodes {
            // Process queued messages (if possible)
            if should_recheck_queue {
                let mut i = 0usize;
                while i < queued_messages[active_shard].len() {
                    // TODO: (Optim.) check Commit messages first ?
                    let msg = &queued_messages[active_shard][i];
                    if self.shards[active_shard].ready_to_process(msg) {
                        let msg = queued_messages[active_shard].remove(i).unwrap();
                        let batch = self.shards[active_shard].full_process_message(msg).await?;
                        if self.shards[active_shard]
                            .commit_commands(&committed_commands_tx, batch)
                            .await
                        {
                            should_recheck_queue = true;
                            continue 'main_loop; // Restart from the beginning of the queue
                        }
                    } else {
                        i += 1;
                    }
                }
                should_recheck_queue = false;
            }

            let ongoing = self.shards[active_shard].get_my_v().is_some()
                || !queued_messages[active_shard].is_empty();
            let should_repropose = !ongoing && self.shards[active_shard].has_queued_commands();
            let mut contention = ongoing || should_repropose;

            // TODO: (Optim.) peak connection first ?
            if should_repropose && self.shards[active_shard].should_lead() {
                if let Some(batch) = self.shards[active_shard].get_new_batch_to_propose() {
                    self.shards[active_shard]
                        .propose_start(batch, false)
                        .await?;
                } else {
                    let v = self.shards[active_shard].get_v_to_repropose();
                    self.shards[active_shard].repropose_start(v).await?;
                }
            } else if !contention && !my_queued_commands[active_shard].is_empty() {
                let command = my_queued_commands[active_shard].pop_front().unwrap();
                assert!(!command.read_only, "read_only command wrongly queued");
                self.shards[active_shard]
                    .propose_start(CommandBatch::Single(command), false)
                    .await?;
                contention = true;
            }

            // Read new messages and/or new local command
            let msg = select! {
                command = new_client_commands_rx.recv(), if !done => {
                    match command {
                        Some(command) =>  {
                            if command.read_only {
                                self.shards[command.shard].start_read(command).await?;
                            } else {
                                if !contention || self.shards[command.shard].can_forward_proposals() {
                                    self.shards[command.shard].propose_start(CommandBatch::Single(command), contention).await?;
                                } else {
                                    my_queued_commands[command.shard].push_back(command);
                                }
                            }
                        }
                        None => {
                            debug_assert!(my_queued_commands.iter().all(|x| x.is_empty()));
                            done = true;
                            self.sinks.borrow_mut().broadcast(Done).await?;
                            count_done += 1;
                        }
                    }
                    continue 'main_loop; // Rechecks exit condition + wait for next msg or command
                }
                opt_msg = msg_rx.recv() => opt_msg.unwrap(),
            };

            match msg.msg {
                ConsensusM { shard, msg, value } => {
                    active_shard = shard;
                    if let Some(value) = value {
                        debug_assert!(msg.can_include_value());
                        let v = msg.get_v().expect("A value should travel with its uid");
                        self.shards[shard].store_remote_command(v, value);
                        should_recheck_queue = true;
                    } else {
                        debug_assert!(!msg.should_include_value());
                    };

                    if !self.shards[shard].ready_to_process(&msg) {
                        queued_messages[shard].push_back(msg);
                        continue 'main_loop;
                    }

                    let res_command = self.shards[shard].full_process_message(msg).await?;
                    if self.shards[shard]
                        .commit_commands(&committed_commands_tx, res_command)
                        .await
                    {
                        should_recheck_queue = true;
                        continue 'main_loop; // Restart from the beginning of the queue
                    }
                }
                Done => count_done += 1,
                _ => panic!("Unexpected message type"),
            }
        } // 'main_loop: loop

        eval::log(
            "network-done",
            &format!(
                "Sent {} messages ({} bytes)",
                self.sinks.borrow().stats.msg_count,
                self.sinks.borrow().stats.byte_count
            ),
            &self.sinks.borrow().stats,
        );

        new_client_commands_rx.close();
        msg_rx.close();
        Ok(())
    } // run
}

impl<AS, AR, ARS> ConsensusShard<AS, AR, ARS>
where
    ConsensusShard<AS, AR, ARS>: ConsensusShardTrait,
{
    #[inline]
    async fn start_read(&mut self, command: Command) -> io::Result<()> {
        let local_ready = self.get_my_v().is_none();
        let uid = self.read_tracker.insert(command, local_ready);
        self.sinks.broadcast(ReadRequest { uid }, None).await
    }

    #[inline]
    fn ready_to_process(&self, msg: &ConsensusMessage) -> bool {
        if msg.get_slot() > self.slot {
            false
        } else {
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
            info!("Commit msg: v={}", v);
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
        debug!("Processing message: {:?}", msg);
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
        if self.queued_commands.is_empty() {
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

    fn get_next_uid(&mut self) -> usize {
        let uid = self.next_uid;
        self.next_uid += self.nb_nodes;
        uid
    }
}
