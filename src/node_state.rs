use serde::{Deserialize, Serialize};
use bit_set::BitSet;
use std::fmt;

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

pub struct StateDisplay<'a> (pub &'a [NodeState]);

impl<'a> fmt::Display for StateDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // write!(f, "[")?;
        for (i, state) in self.0.iter().enumerate() {
            if let Some(v_uid) = state.v_uid {
                write!(f, "\n  {}: uid={}, k={:?}", i, v_uid, state.k)?;
            }
        }
        // write!(f, "\n]")?;
        Ok(())
    }
}