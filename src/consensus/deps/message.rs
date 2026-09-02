use crate::consensus::deps::dep_set::DepSet;
use serde::{Deserialize, Serialize};

/// Messages of the dependency-based ordering layer.
///
/// There are no ballots: the repository assumes stable failures, so a command's
/// coordinator never changes and no other process ever proposes for it. That is the same
/// reason the recovery protocol of EPaxos\*/SwiftPaxos is not implemented.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DepMsg {
    /// A non-voting process handing its command to the replica that will coordinate it.
    ///
    /// EPaxos has no leader to broadcast to, and a non-replica cannot run a coordinator's
    /// quorum, so it forwards instead — one extra delay each way, which is exactly what
    /// `epaxos_latencies` predicts for a non-replica (`propagation.rs:428-432`). SwiftPaxos
    /// never uses this: there a non-voting process proposes for itself, as the paper's
    /// client does.
    Forward,

    /// EPaxos\* `PreAccept(id, c, D)`, and SwiftPaxos' `Propagate(c)`: the coordinator
    /// broadcasts the command together with the dependencies it knows of. Always carries
    /// the payload; in SwiftPaxos the leader's `Accept` carries it as well.
    PreAccept { id: usize, deps: DepSet },

    /// A fast-quorum replica's own proposal: EPaxos\* `PreAcceptOK(id, dep)` and
    /// SwiftPaxos' `FastAck`. The same content, and the same state on arrival; only the
    /// destination differs, because in EPaxos only the coordinator decides while in
    /// SwiftPaxos every replica does.
    ///
    /// EPaxos unicasts it back to the coordinator. SwiftPaxos broadcasts it, as in the
    /// paper and its reference implementation (Fig. 4 lines 19/21, an unconditional
    /// `SendToAll`): each replica evaluates the fast route on its own once it holds
    /// matching proposals from a fast quorum, rather than waiting for the leader's
    /// `Commit`. Either way it carries the dependency set and is sent unconditionally — it
    /// is this replica's own proposal, formed before the leader's is known, so there is
    /// nothing yet to agree with.
    ///
    /// The leader's counterpart — what SwiftPaxos also calls a `FastAck`, sent by the
    /// leader — is the `Accept` below.
    PreAcceptOk { id: usize, deps: DepSet },

    /// EPaxos\* `Accept(b, id, c, D)`: the slow path, carrying the union of the
    /// dependencies reported by a quorum.
    ///
    /// In SwiftPaxos this is the **leader's** accept, broadcast as soon as the leader has
    /// formed its proposal. It carries the command when `with_value`, so that a replica
    /// whose link from the leader is better than its link from the proposer receives the
    /// payload by the shorter of the two paths and can act on this message immediately.
    Accept {
        id: usize,
        deps: DepSet,
        with_value: bool,
    },

    /// EPaxos\* `AcceptOK(b, id)`.
    AcceptOk { id: usize },

    /// EPaxos\* `Commit(b, id, c, D)`: the agreed dependencies.
    Commit { id: usize, deps: DepSet },
}

impl DepMsg {
    /// The instance this message is about. `None` for `Forward`, which carries a command
    /// that has not been given an id yet — the coordinating replica allocates one.
    #[inline]
    pub fn id(&self) -> Option<usize> {
        match self {
            DepMsg::Forward => None,
            DepMsg::PreAccept { id, .. }
            | DepMsg::PreAcceptOk { id, .. }
            | DepMsg::Accept { id, .. }
            | DepMsg::AcceptOk { id }
            | DepMsg::Commit { id, .. } => Some(*id),
        }
    }

    /// Whether a *broadcast* of this message should skip non-replicas.
    ///
    /// Only votes are replica business. A non-replica still executes every command on its
    /// own copy of the store, so it needs the payload (`PreAccept`) and the decision
    /// (`Commit`) like everyone else. Unicasts are never filtered by this — they pick their
    /// destination deliberately, and in SwiftPaxos that destination can be a non-voting
    /// proposer playing the paper's client role.
    #[inline]
    pub fn replicas_only(&self) -> bool {
        match self {
            DepMsg::PreAcceptOk { .. } | DepMsg::Accept { .. } => true,
            DepMsg::Forward
            | DepMsg::PreAccept { .. }
            | DepMsg::AcceptOk { .. }
            | DepMsg::Commit { .. } => false,
        }
    }

    /// The command travels with the proposer's `PreAccept` and, in SwiftPaxos, with the
    /// leader's `Accept` as well: sending it down both paths means each replica gets it
    /// over whichever is shorter, and the redundant copy is the fallback.
    #[inline]
    pub fn includes_value(&self) -> bool {
        match self {
            DepMsg::Forward | DepMsg::PreAccept { .. } => true,
            DepMsg::Accept { with_value, .. } => *with_value,
            _ => false,
        }
    }
}
