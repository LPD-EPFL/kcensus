use bit_set::BitSet;
use serde::{Deserialize, Serialize};

pub type Knowledge = BitSet;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeState {
    pub v: Option<usize>,
    pub k: Knowledge,
    pub frozen: bool,
    pub leader: bool,
}

impl NodeState {
    #[inline]
    pub fn clear(&mut self) {
        self.v = None;
        self.k.clear();
        self.frozen = false;
        self.leader = false;
    }

    #[inline]
    pub fn new(nb_nodes: usize) -> Self {
        Self {
            v: None,
            k: BitSet::with_capacity(nb_nodes),
            frozen: false,
            leader: false,
        }
    }
}
