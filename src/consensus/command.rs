use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Command {
    pub proposer: usize,
    pub shard: usize,
    pub command: Vec<u8>,
    pub read_only: bool,
}

impl Command {
    #[inline]
    pub fn new_read_only<ApplicationRequest: Serialize>(
        proposer: usize,
        shard: usize,
        app_request: &ApplicationRequest,
    ) -> Self {
        Self::new(proposer, shard, app_request, true)
    }

    #[inline]
    pub fn new_write<ApplicationRequest: Serialize>(
        proposer: usize,
        shard: usize,
        app_request: &ApplicationRequest,
    ) -> Self {
        Self::new(proposer, shard, app_request, false)
    }

    #[inline]
    fn new<ApplicationRequest: Serialize>(
        proposer: usize,
        shard: usize,
        app_request: &ApplicationRequest,
        read_only: bool,
    ) -> Self {
        Self {
            proposer,
            shard,
            command: bincode::serialize(app_request)
                .expect("Failed to serialize application request"),
            read_only,
        }
    }
}

#[derive(Debug)]
pub struct CommittedCommand<ApplicationRequest> {
    pub proposer: usize,
    pub app_request: ApplicationRequest,
}

impl<ApplicationRequest: DeserializeOwned> From<Command> for CommittedCommand<ApplicationRequest> {
    #[inline]
    fn from(command: Command) -> Self {
        Self {
            proposer: command.proposer,
            app_request: bincode::deserialize::<ApplicationRequest>(&command.command)
                .expect("Failed to deserialize committed request"),
        }
    }
}
