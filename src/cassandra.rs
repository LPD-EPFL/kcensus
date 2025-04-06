use std::time::Duration;
use rand_distr::{Exp, Distribution};
use scylla::client::{session::Session, session_builder::SessionBuilder};
use scylla::statement::prepared::PreparedStatement;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::watch::{Receiver as WatchReceiver};
use tokio::time::Instant;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Put {key: String, value: String},
    Get {key: String},
}

#[derive(Debug)]
pub enum Response {
    Put {key: String, value: String},
    Get {key: String, value: Option<String>},
}

pub struct Handler {
    session: Session
}

pub struct PreparedHandler {
    session: Session,
    put_ps: PreparedStatement,
    get_ps: PreparedStatement,
}

impl Handler {
    pub async fn new(uri: &str) -> Self {
        Self { session: SessionBuilder::new().known_node(uri).build().await.expect("Cassandra failed to create session") }
    }

    pub async fn reset_database(&self) {
        self.session.query_unpaged("DROP KEYSPACE IF EXISTS kvstore;", &[]).await.expect("Cassandra failed to drop kvstore");
        self.session.query_unpaged("CREATE KEYSPACE kvstore WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};", &[]).await.expect("Cassandra failed to create keyspace");
        self.session.query_unpaged("CREATE TABLE kvstore.kv_pairs (key text PRIMARY KEY, value text);", &[]).await.expect("Cassandra failed to create kvstore.kv_pairs");
    }

    pub async fn prepare(self) -> PreparedHandler {
        let put_ps = self.session.prepare("INSERT INTO kvstore.kv_pairs (key, value) VALUES (?, ?);").await.expect("Cassandra failed to prepare put statement");
        let get_ps = self.session.prepare("SELECT value FROM kvstore.kv_pairs WHERE key = ?;").await.expect("Cassandra failed to prepare get statement");
        PreparedHandler {
            session: self.session,
            put_ps,
            get_ps
        }
    }
}

impl PreparedHandler {
    pub async fn put(&self, key: &str, value: &str) {
        self.session.execute_unpaged(&self.put_ps, (key, value))
            .await
            .expect("Cassandra failed to set");
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        self.session.execute_unpaged(&self.get_ps, (key,))
            .await
            .expect("Cassandra failed to get")
            .into_rows_result().expect("Cassandra's output was not rows")
            .first_row::<(&str,)>().ok().map(|(str,)| str.to_string())
    }

    pub async fn execute(&self, request: Request) -> Response {
        match request {
            Request::Put { key, value } => { self.put(&key, &value).await; Response::Put {key, value} },
            Request::Get { key } => { Response::Get { value: self.get(&key).await, key}}
        }
    }
}

pub struct Client {
    pub my_pid: usize,
    pub client_request_tx: Sender<Option<Request>>,
    pub client_response_rx: Receiver<Response>,
    pub num_committed_watch_rx: WatchReceiver<usize>,
}

pub enum RequestInterval {
    RoundRobin {nb_nodes: usize}, // 1 request at a time, alternating among nodes
    Exponential {distribution: Exp<f32>},
    Constant {reqs_per_second: f32},
}

impl RequestInterval {
    pub fn new_exponential(throughput: f32) -> Self {
        RequestInterval::Exponential {distribution: Exp::new(throughput).expect("Failed to create exponential distribution")}
    }
}

impl RequestInterval {
    fn next(&mut self, last: &Instant) -> Instant {
        match self {
            RequestInterval::RoundRobin { .. } => { Instant::now() }
            RequestInterval::Exponential { distribution } => { *last + Duration::from_secs_f32(distribution.sample(&mut rand::rng())) }
            RequestInterval::Constant { reqs_per_second } => { *last + Duration::from_secs_f32(1. / *reqs_per_second) }
        }
    }
}

pub struct Workload {
    pub nb_requests: usize,
    pub rw_ratio: f32, // 0 = 100% reads, 1 = 100 %writes
    pub interval: RequestInterval,
}

impl Client {
    pub async fn run(mut self, mut workload: Workload) {
        let mut request_generated = Instant::now();
        for i in 0..workload.nb_requests {
            if let RequestInterval::RoundRobin {nb_nodes} = workload.interval {
                while *self.num_committed_watch_rx.borrow_and_update() % nb_nodes != self.my_pid {
                    self.num_committed_watch_rx.changed().await.expect("Couldn't read back pressure");
                }
            }
            let request = if rand::random_range(0. ..1.) < workload.rw_ratio {
                Request::Put {key: "single-key".into(), value: format!("v{}.{}!", self.my_pid, i)}
            } else {
                Request::Get {key: "single-key".into() }
            };
            request_generated = workload.interval.next(&request_generated);
            let time_before_generation = request_generated.saturating_duration_since(Instant::now());
            if !time_before_generation.is_zero() {
                tokio_timerfd::sleep(time_before_generation).await.expect("Failed to sleep");
            }
            let issued = Instant::now();
            self.client_request_tx
                .send(request.into())
                .await
                .expect("Client failed to queue request");
            let response = self.client_response_rx.recv().await.expect("Client failed to receive response");
            let responded = Instant::now();
            let request_latency = responded.duration_since(request_generated);
            let request_queueing = issued.duration_since(request_generated);
            let request_processing = responded.duration_since(request_generated);
            println!("Executed {:?} in {:?} (queued {:?}, processed in {:?})", response, request_latency, request_queueing, request_processing );
        };
        self.client_request_tx.send(None).await.expect("Client failed to enqueue None request");
    }
}