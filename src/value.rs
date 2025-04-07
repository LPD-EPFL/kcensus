use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KVal {
    pub proposer: usize,
    pub val: Vec<u8>,
}

impl KVal {
    #[inline]
    pub fn new<ApplicationRequest: Serialize>(
        proposer: usize,
        application_request: &ApplicationRequest,
    ) -> Self {
        Self {
            proposer,
            val: bincode::serialize(application_request)
                .expect("Failed to serialize application request"),
        }
    }

    #[inline]
    pub fn into_local_req(self) -> Request {
        Request {
            value: self,
            local: true,
        }
    }

    #[inline]
    pub fn into_remote_req(self) -> Request {
        Request {
            value: self,
            local: false,
        }
    }
}

pub struct Request {
    pub value: KVal,
    pub local: bool,
}

#[derive(Debug)]
pub struct CommittedRequest<ApplicationRequest> {
    pub request: ApplicationRequest,
    pub local: bool,
}

impl<ApplicationRequest: DeserializeOwned> From<Request> for CommittedRequest<ApplicationRequest> {
    fn from(request: Request) -> Self {
        Self {
            request: bincode::deserialize::<ApplicationRequest>(&request.value.val)
                .expect("Failed to deserialize committed request"),
            local: request.local,
        }
    }
}
