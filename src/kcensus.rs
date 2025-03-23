use std::collections::HashMap;
use std::io;
use std::io::Error;
use futures::{SinkExt, Stream, StreamExt};
use crate::DeSink;
use crate::kcensus::Flow::{NextMsg, NextSlot};
use crate::message::{KCensusMsg, KCensusMsgWithSource, MsgWithSource, RoundCommand};
use crate::message::RoundCommand::{Commit, Spread};
use crate::message::Message::KCensusMessage;
use crate::node_state::{Knowledge, NodeState, StateDisplay};

// #[derive(Serialize, Deserialize, Debug, Clone)]
pub type Value = String;

// impl Value {
//     pub fn new(proposer: usize, val: String) -> Value {
//         Value { proposer, val }
//     }
// }

pub struct KCensus<St> {
    // Settings
    nb_nodes: usize,
    my_pid: usize,
    majority: usize,

    // Connections
    in_stream: St,
    out_sinks: HashMap<usize, DeSink>,

    // Overall state
    slot: usize,
    max_seen_slot: usize,
    round: usize,
    queued_messages: Vec<KCensusMsgWithSource>,
    queued_values: Vec<Value>,
    values: HashMap<usize, Value>,

    // Round-state
    node_states: Vec<NodeState>,

    // can_commit optimizations / scratchpads
    my_quorum: Vec<usize>,
    next_combination_pos: Vec<usize>,
    frozen_size_checked: usize,
    _bitset_scratchpad: Knowledge,
}

macro_rules! my_state {
    ($self:ident) => { $self.node_states[$self.my_pid] }
}

macro_rules! ready_to_process {
    ($self:ident, $msg:expr) => {
        ($msg.slot == $self.slot && $self.values.contains_key(&$msg.value_uid))
    };
}

pub struct NbNodes(pub usize);
pub struct Pid(pub usize);

enum Flow {
    NextSlot,
    NextMsg,
}


impl<St: Stream<Item=Result<MsgWithSource,Error>> + Unpin> KCensus<St> {
    pub fn new(nb_nodes: NbNodes, my_pid: Pid,
               in_stream: St, out_sinks: HashMap<usize, DeSink>) -> Self {
        let nb_nodes = nb_nodes.0;
        let majority = (nb_nodes / 2) + 1;
        let mut node_states = Vec::with_capacity(nb_nodes);
        for _ in 0..nb_nodes {
            node_states.push(NodeState::new(nb_nodes));
        }
        Self {
            nb_nodes,
            my_pid: my_pid.0,
            majority,

            in_stream,
            out_sinks,

            slot: 0,
            max_seen_slot: 0,
            round: 0,
            queued_messages: Vec::with_capacity(nb_nodes),
            queued_values: Vec::with_capacity(4),

            values: HashMap::with_capacity(nb_nodes),
            node_states,

            my_quorum: Vec::with_capacity(nb_nodes),
            next_combination_pos: Vec::with_capacity(majority-1),
            frozen_size_checked: 0,
            _bitset_scratchpad: Knowledge::with_capacity(nb_nodes),
        }
    }

    pub async fn run(&mut self) -> io::Result<()> {
        self.goto_round(0);
        self.propose_start(format!("v{}!", self.my_pid)).await?;

        'main_loop: loop {
            // Process queued messages (if possible)
            let mut i = 0usize;
            while i < self.queued_messages.len() {
                let msg = &self.queued_messages[i];
                if ready_to_process!(self, msg.msg) {
                    let msg = self.queued_messages.remove(i);
                    match self.process_message(msg.msg, msg.src).await? {
                        NextSlot => continue 'main_loop, // Restart from the beginning of the queue
                        NextMsg => (),
                    }
                    continue; // Don't increment i here !
                }
                i += 1;
            }

            // TODO: Break dynamically based on expected propose count
            if self.slot >= self.nb_nodes {
                break 'main_loop;
            }

            // TODO: (Optim.) peak connection first ?
            if my_state!(self).v_uid == None
                && self.max_seen_slot == self.slot
                && !self.values.is_empty()
            {
                let (v_uid, _) = self.values.iter().next().unwrap();
                self.repropose_start(*v_uid).await?;
            }

            // Read new messages
            let msg = self.in_stream.next().await.unwrap()?;
            let src = msg.src;

            match msg.msg {
                KCensusMessage { msg, value } => {
                    if let Some(v) = value {
                        let inserted = self.values.insert(msg.value_uid, v);
                        // TODO: Allow forwarding values ? (could the value already be there ?)
                        debug_assert!(inserted.is_none());
                    }
                    
                    if !ready_to_process!(self, msg) {
                        self.max_seen_slot = self.max_seen_slot.max(msg.slot);
                        self.queued_messages.push(msg.with_source(src));
                        // TODO: recheck queued messages only if
                        //   "ready_to_process" might have changed
                        continue 'main_loop;
                    }

                    match self.process_message(msg, src).await? {
                        NextSlot => continue 'main_loop,
                        NextMsg => (),
                    }
                }
                _ => panic!("Unexpected message type"),
            }

        }

        Ok(())
    }

    #[inline]
    async fn inner_broadcast(&mut self, command: RoundCommand,
                             value: Option<Value>) -> io::Result<()> {
        let value_uid = my_state!(self).v_uid.unwrap();
        for (_, sink) in self.out_sinks.iter_mut() {
            sink.send(KCensusMessage {
                msg: KCensusMsg {
                    slot: self.slot,
                    round: self.round,
                    value_uid,
                    command: command.clone()
                }, value: value.clone()
            }).await?;
        }
        Ok(())
    }

    #[inline]
    async fn spread_with_value(&mut self, remote_states: Vec<NodeState>,
                               value: &Value) -> io::Result<()> {
        self.inner_broadcast(Spread {
            remote_states: remote_states.clone(),
        }, Some(value.clone())).await
    }

    #[inline]
    async fn broadcast(&mut self, command: RoundCommand) -> io::Result<()> {
        self.inner_broadcast(command, None).await
    }

    #[inline]
    async fn spread(&mut self, remote_states: Vec<NodeState>) -> io::Result<()> {
        self.broadcast(Spread { remote_states }).await
    }

    async fn process_message(&mut self, msg: KCensusMsg, src: usize) -> io::Result<Flow> {
        debug_assert!(ready_to_process!(self, msg)); // Redundant with the following asserts...
        let slot = msg.slot;
        let round = msg.round;
        let msg_v_uid = msg.value_uid;
        let command = msg.command;
        debug_assert!(self.values.contains_key(&msg_v_uid));
        debug_assert_eq!(slot, self.slot);

        // TODO: Ignore some messages if max_seen_slot > slot ?

        match command {
            Spread { remote_states } => {
                if round < self.round {
                    // TODO: Answer with adopted value ? Only if src is not in my knowledge set ?
                    return Ok(NextMsg);
                } else if round > self.round {
                    self.goto_round(round);
                }
                debug_assert!(round == self.round);

                if my_state!(self).v_uid == None {
                    debug_assert!(!my_state!(self).frozen);
                    my_state!(self).v_uid = Some(msg_v_uid);
                }
                let my_v_uid = my_state!(self).v_uid.unwrap();

                let orig_kl = my_state!(self).k.len();
                for pid in 0..self.nb_nodes {
                    let local_node_state = &mut self.node_states[pid];
                    let remote_node_state = &remote_states[pid];
                    local_node_state.k.union_with(&remote_node_state.k);
                    if let Some(node_v_uid) = remote_node_state.v_uid {
                        debug_assert_eq!(local_node_state.v_uid.unwrap_or(node_v_uid), node_v_uid);
                        local_node_state.v_uid = Some(node_v_uid);
                    }
                    local_node_state.frozen |= remote_node_state.frozen;
                }
                if msg_v_uid == my_v_uid && !my_state!(self).frozen {
                    my_state!(self).k.union_with(&remote_states[src].k);
                }

                // Can commit ?
                if self.can_commit() {
                    self.broadcast(Commit).await?;
                    let value = self.commit_slot(my_v_uid);
                    return Ok(NextSlot)
                }

                let msg_frozen = remote_states[src].frozen;

                if msg_frozen {
                    if let Some(adopted_v) = self.try_adopt() {
                        self.goto_round(self.round + 1);
                        my_state!(self).v_uid = Some(adopted_v);
                        self.spread(self.node_states.clone()).await?;
                        return Ok(NextMsg)
                    }
                }

                if ( msg_v_uid != my_v_uid || msg_frozen ) && !my_state!(self).frozen {
                    // Conflict detected
                    my_state!(self).frozen = true;
                    self.spread(self.node_states.clone()).await?;
                    return Ok(NextMsg)
                }

                let kl = my_state!(self).k.len();
                if kl > orig_kl {
                    self.spread(self.node_states.clone()).await?;
                }
            }
            Commit => {
                // TODO: ignore round ?
                // TODO: Handle commit

                println!("Commit msg for value_uid {}", msg_v_uid);
                let value = self.commit_slot(msg_v_uid);
                return Ok(NextSlot)
            }
            // x => panic!("Unexpected kcensus command {:?}", x)
        }
        Ok(NextMsg)
    }


    #[inline]
    async fn propose_start(&mut self, value: Value) -> io::Result<()> {
        debug_assert!(my_state!(self).v_uid == None);

        let value_uid = self.my_pid + (self.slot * self.nb_nodes);
        my_state!(self).v_uid = Some(value_uid);

        self.spread_with_value(self.node_states.clone(), &value).await?;

        self.values.insert(value_uid, value);

        Ok(())
    }

    async fn repropose_start(&mut self, value_uid: usize) -> io::Result<()> {
        debug_assert!(my_state!(self).v_uid == None);

        my_state!(self).v_uid = Some(value_uid);

        self.spread(self.node_states.clone()).await
    }

    #[inline]
    fn commit_slot(&mut self, value_uid: usize) -> Value {
        let value = self.values.remove(&value_uid).unwrap();
        println!("Commited \'{}\' in slot {} after {} rounds from state: {}",
                 value, self.slot, self.round, StateDisplay(&self.node_states));
        self.slot += 1;
        self.max_seen_slot = self.max_seen_slot.max(self.slot);
        self.goto_round(0);
        self.queued_messages.retain(|msg| {msg.msg.slot >= self.slot});
        value
    }

    #[inline]
    fn goto_round(&mut self, round: usize) {
        if round != 0 {
            println!("Failed to commit in slot {} after {} rounds from state: {}",
                     self.slot, self.round, StateDisplay(&self.node_states));
            if round > self.round + 1 {
                println!("!!!!!!! Skipping round !!!!!!!");
            }
        }
        self.round = round;
        for node_state in self.node_states.iter_mut() {
            node_state.clear();
        }
        let inserted = my_state!(self).k.insert(self.my_pid);
        debug_assert!(inserted);
        self.my_quorum.clear();
        self.next_combination_pos.clear();
        self.frozen_size_checked = 0;
    }

    // TODO: Allow can_commit to run for other proposals ?
    // TODO: Speedup for e-paxos / paxos ?
    fn can_commit(&mut self) -> bool {
        let k_size = my_state!(self).k.len();
        debug_assert!(k_size <= self.nb_nodes);
        if k_size < self.majority {
            return false
        }
        let unknown_nodes = self.nb_nodes - k_size;
        let trivial_frozen = self.majority;
        let minority = self.nb_nodes - self.majority;
        let min_frozen = (k_size - minority)
            .max(self.frozen_size_checked + 1); // Skip already checked ones

        /* When new nodes appear, if we already checked combinations of up to k-1 frozen,
           then we know that sets of up to k frozen nodes that include some new nodes are fine
           (thanks to the monotonicity of the score function & min_frozen increasing with new nodes)
           thus we only need to refresh my_quorum when reaching k+1 frozen bellow */
        if self.frozen_size_checked + 1 < min_frozen {
            // Note: this will trigger a refresh of my_quorum
            self.next_combination_pos.clear();
            self.frozen_size_checked = min_frozen - 1;
        }


        for frozen in min_frozen..trivial_frozen {
            let max_others_score = 2 * unknown_nodes - (self.majority - frozen);
            let to_know = self.majority.min(((max_others_score + frozen) / 2) + 1);

            // TODO: If everyone knows a node that knows a majority*, we can:
            //         - Change this condition to to_know <= frozen + 1
            //       OR equivalently:
            //         - Lower trivial_frozen to majority-1
            //         - Immediately return true if k_size >= fast_quorum
            //   *: e.g. we're the sole proposer
            if to_know <= frozen {
                self.frozen_size_checked = frozen;
                self.next_combination_pos.clear();
                continue
            }
            if self.next_combination_pos.is_empty() {
                if self.my_quorum.len() != k_size {
                    self.my_quorum.clear();
                    self.my_quorum.extend(my_state!(self).k.iter());
                    // Optimisation: Put bigger knowledge first to help early skip
                    // Note: !x == (usize::MAX - x)
                    self.my_quorum.sort_by_key(|a| !self.node_states[*a].k.len());

                    // println!("Reordering my_k len: {}", self.my_k.len());
                    // for pid in self.my_k.iter() {
                    //     println!("Knowledge of {}: {:?}", pid, my_knowledge[*pid])
                    // }
                }
                self.next_combination_pos.extend(0..frozen);
            }
            debug_assert_eq!(self.next_combination_pos.len(), frozen);
            loop {
                // TODO: Save intermediate known set to reduce recomputations ? (is it worth it ?)
                let known = &mut self._bitset_scratchpad;
                known.clear();
                let mut unused_knowledge = frozen;
                for pos in self.next_combination_pos.iter() {
                    let pid = self.my_quorum[*pos];
                    known.union_with(&self.node_states[pid].k);
                    unused_knowledge -= 1;
                    // Optimisation: Early skip
                    if known.len() >= to_know {
                        break
                    }
                }

                if known.len() < to_know {
                    return false;
                }

                let changed_suffix_size = self
                    .next_combination(self.my_quorum.len(), unused_knowledge);
                if changed_suffix_size == 0 {
                    break;
                }
            }
            // Save combinations that are checked
            self.frozen_size_checked = frozen;
            self.next_combination_pos.clear();
        }
        true
    }

    #[inline]
    fn next_combination(&mut self, positions: usize, unused: usize) -> usize {
        debug_assert!(positions < self.nb_nodes);
        let pos = &mut self.next_combination_pos;
        let len = pos.len();
        debug_assert!(len < self.majority);

        // Optimisation: Skip combinations that only change the unused nodes
        let min_to_move = 1.max(unused+1);
        for suffix_size in min_to_move..=len {
            let suffix_start = len - suffix_size;
            // Can we move the last "suffix_size" positions ?
            if pos[suffix_start] + suffix_size < positions {
                // Yes: Move them and return
                let new_pos = pos[suffix_start] + 1;
                for j in 0..suffix_size {
                    pos[suffix_start + j] = new_pos + j;
                }
                return suffix_size
            }
        }
        0
    }

    fn try_adopt(&mut self) -> Option<usize> {
        let frozen_count = self.node_states.iter()
            .filter(|x| x.frozen).count();
        if frozen_count < self.majority {
            return None
        }

        let mut max_score = 0usize;
        let mut max_score_v = None;
        for v in self.node_states.iter()
            .filter(|x| x.frozen)
            .map(|x| x.v_uid) // For all v in the frozen set
        {
            if v == max_score_v { continue }

            let mut v_frozen_count = 0;
            self._bitset_scratchpad.clear();
            for node_state in self.node_states.iter()
                .filter(|x| x.frozen)
            {
                if v != node_state.v_uid { continue }
                v_frozen_count += 1;
                self._bitset_scratchpad.union_with(&node_state.k);
            }
            let v_known_count = self._bitset_scratchpad.len();
            debug_assert!(v_frozen_count <= v_known_count);

            if v_known_count > self.majority {
                return v
            }

            let score = v_known_count * 2 - v_frozen_count;

            if score > max_score {
                max_score = score;
                max_score_v = v;
            }
        }
        debug_assert!(max_score_v.is_some());
        max_score_v
    }
}