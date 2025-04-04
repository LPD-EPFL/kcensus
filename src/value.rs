use serde::{Deserialize, Serialize};
use tokio::time::Instant;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KVal {
    pub val: String,
    pub proposer: usize,
}

impl KVal {
    #[inline]
    pub fn into_request(self, start_time: Instant) -> Request {
        Request {
            value: self,
            start_time: Some(start_time),
        }
    }

    #[inline]
    pub fn into_remote_req(self) -> Request {
        Request {
            value: self,
            start_time: None,
        }
    }
}

pub struct Request {
    pub value: KVal,
    pub start_time: Option<Instant>,
}
