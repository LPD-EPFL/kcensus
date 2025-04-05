use crate::consensus::paxos::message::PaxosRound;
use crate::consensus::paxos::round_state::PaxosRoundState;
use crate::multi_sink::MultiSink;
use crate::value::Request;
use std::collections::HashMap;

pub mod message;
mod round_state;

pub struct Paxos<Sk> {
    // Settings
    nb_nodes: usize,
    my_pid: usize,

    // Connections
    sinks: MultiSink<Sk>,

    // Overall state
    slot: usize,
    max_seen_slot: usize,
    round: PaxosRound,
    values: HashMap<usize, Request>,

    round_state: PaxosRoundState,
}
