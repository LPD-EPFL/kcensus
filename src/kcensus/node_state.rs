use bit_set::BitSet;
use serde::{Deserialize, Serialize};

pub type Knowledge = BitSet;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeState {
    pub v_uid: Option<usize>,
    pub k: Knowledge,
    pub frozen: bool,
}

impl NodeState {
    pub fn clear(&mut self) {
        self.v_uid = None;
        self.k.clear();
        self.frozen = false;
    }

    pub fn new(nb_nodes: usize) -> Self {
        Self {
            v_uid: None,
            k: BitSet::with_capacity(nb_nodes),
            frozen: false,
        }
    }
}
