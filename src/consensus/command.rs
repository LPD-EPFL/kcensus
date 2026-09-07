use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Command {
    pub requester: usize,
    pub shard: usize,
    pub command: Vec<u8>,
    pub read_only: bool,

    /// The shard's next undecided slot when this command reached consensus. Scratch state for
    /// `report`; unused in dependency mode, which has no slots.
    #[serde(skip)]
    pub arrival_slot: usize,

    /// `None` for reads, which are answered from a quorum rather than ordered, and on every
    /// process but the requester. Never a default: a defaulted report is indistinguishable
    /// from a genuine slow commit that waited for nothing.
    #[serde(skip)]
    pub report: Option<CommitReport>,
}

/// Per-request consensus accounting, logged next to the request's latency so a distribution
/// can be split by what the protocol did rather than only summarised.
///
/// Filled in by the requester alone, and kept off the wire (`#[serde(skip)]` on `Command`) so
/// that message sizes are unchanged. `fast` is therefore *inferred*: the requester is usually
/// not the process that decides -- KCensus picks one leader per proposer -- but the fallback
/// is broadcast to every replica before it can commit, on the same ordered connection the
/// `Commit` arrives on, so seeing a `PaxosAccept` (or an `Accept`, in dependency mode) proves
/// the fast route failed.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct CommitReport {
    /// Whether the command took its protocol's fast route. Leader-based modes have none and
    /// always report `false`.
    pub fast: bool,

    /// How many other commands got in front of this one: slots lost between arrival and
    /// commit, or, in dependency mode, dependencies still un-executed at commit. This is what
    /// separates a command that fast-commits outright from one that loses a round first.
    pub waited_for: usize,
}

impl Command {
    #[inline]
    pub fn new_read_only<ApplicationRequest: Serialize>(
        requester: usize,
        shard: usize,
        app_request: &ApplicationRequest,
    ) -> Self {
        Self::new(requester, shard, app_request, true)
    }

    #[inline]
    pub fn new_write<ApplicationRequest: Serialize>(
        requester: usize,
        shard: usize,
        app_request: &ApplicationRequest,
    ) -> Self {
        Self::new(requester, shard, app_request, false)
    }

    #[inline]
    fn new<ApplicationRequest: Serialize>(
        requester: usize,
        shard: usize,
        app_request: &ApplicationRequest,
        read_only: bool,
    ) -> Self {
        Self {
            requester,
            shard,
            command: bincode::serialize(app_request)
                .expect("Failed to serialize application request"),
            read_only,
            arrival_slot: 0,
            report: None,
        }
    }
}

#[derive(Debug)]
pub struct CommittedCommand<ApplicationRequest> {
    pub requester: usize,
    pub app_request: ApplicationRequest,
    pub report: Option<CommitReport>,
}

impl<ApplicationRequest: DeserializeOwned> From<Command> for CommittedCommand<ApplicationRequest> {
    #[inline]
    fn from(command: Command) -> Self {
        Self {
            requester: command.requester,
            app_request: bincode::deserialize::<ApplicationRequest>(&command.command)
                .expect("Failed to deserialize committed request"),
            report: command.report,
        }
    }
}
