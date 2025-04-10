use crate::connector::connect_all;
use crate::consensus::command::{Command, CommittedCommand};
use crate::message::{Message, MsgWithSource};
use crate::multi_sink::MultiSink;
use futures::StreamExt;
use log::trace;
use rand_distr::{Distribution, Exp};
use scylla::client::{session::Session, session_builder::SessionBuilder};
use scylla::statement::prepared::PreparedStatement;
use serde::{Deserialize, Serialize};
use std::io;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Instant;
use tokio_stream::Stream;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Put { key: String, value: String },
    Get { key: String },
}

#[derive(Serialize, Debug)]
#[allow(dead_code)]
pub enum Response {
    Put { key: String, value: String },
    Get { key: String, value: Option<String> },
}

pub struct Handler {
    session: Session,
}

pub struct PreparedHandler {
    session: Session,
    put_ps: PreparedStatement,
    get_ps: PreparedStatement,
}

impl Handler {
    pub async fn new(uri: &str) -> Self {
        Self {
            session: SessionBuilder::new()
                .known_node(uri)
                .build()
                .await
                .expect("Cassandra failed to create session"),
        }
    }

    pub async fn reset_database(&self) {
        self.session
            .query_unpaged("DROP KEYSPACE IF EXISTS kvstore;", &[])
            .await
            .expect("Cassandra failed to drop kvstore");
        self.session.query_unpaged("CREATE KEYSPACE kvstore WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};", &[]).await.expect("Cassandra failed to create keyspace");
        self.session
            .query_unpaged(
                "CREATE TABLE kvstore.kv_pairs (key text PRIMARY KEY, value text);",
                &[],
            )
            .await
            .expect("Cassandra failed to create kvstore.kv_pairs");
    }

    pub async fn prepare(self) -> PreparedHandler {
        let put_ps = self
            .session
            .prepare("INSERT INTO kvstore.kv_pairs (key, value) VALUES (?, ?);")
            .await
            .expect("Cassandra failed to prepare put statement");
        let get_ps = self
            .session
            .prepare("SELECT value FROM kvstore.kv_pairs WHERE key = ?;")
            .await
            .expect("Cassandra failed to prepare get statement");
        PreparedHandler {
            session: self.session,
            put_ps,
            get_ps,
        }
    }
}

impl PreparedHandler {
    pub async fn put(&self, key: &str, value: &str) {
        self.session
            .execute_unpaged(&self.put_ps, (key, value))
            .await
            .expect("Cassandra failed to set");
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        self.session
            .execute_unpaged(&self.get_ps, (key,))
            .await
            .expect("Cassandra failed to get")
            .into_rows_result()
            .expect("Cassandra's output was not rows")
            .first_row::<(&str,)>()
            .ok()
            .map(|(str,)| str.to_string())
    }

    pub async fn execute(&self, request: Request) -> Response {
        match request {
            Request::Put { key, value } => {
                self.put(&key, &value).await;
                Response::Put { key, value }
            }
            Request::Get { key } => Response::Get {
                value: self.get(&key).await,
                key,
            },
        }
    }
}

pub enum RequestInterval {
    RoundRobin {
        synchronizer: RoundRobinSynchronizer,
    }, // 1 request at a time, alternating among nodes
    Exponential {
        distribution: Exp<f32>,
    },
    Constant {
        reqs_per_second: f32,
    },
}

pub struct RoundRobinSynchronizer {
    my_pid: usize,
    initiate: bool,
    nb_nodes: usize,
    max_rtt: Duration,
    sinks: MultiSink,
    streams: Pin<Box<dyn Stream<Item = Result<MsgWithSource, io::Error>> + Send>>,
}

impl RoundRobinSynchronizer {
    async fn new(my_pid: usize, nb_nodes: usize, max_rtt: Duration) -> Self {
        let (sinks, streams) = connect_all(my_pid, nb_nodes, 6789).await;
        Self {
            my_pid,
            initiate: my_pid == 0,
            nb_nodes,
            max_rtt,
            sinks,
            streams: Box::pin(streams),
        }
    }

    async fn notify(&mut self) {
        self.sinks
            .inner_send(Message::RoundRobin, (self.my_pid + 1) % self.nb_nodes)
            .await
            .expect("Failed to broadcast round robin");
    }

    async fn wait(&mut self) {
        if self.initiate {
            self.initiate = false;
            return;
        }
        let next_msg = self
            .streams
            .as_mut()
            .next()
            .await
            .expect("We should always be notified");
        match next_msg {
            Ok(MsgWithSource {
                msg: Message::RoundRobin,
                ..
            }) => {}
            Ok(MsgWithSource { .. }) => panic!("Unexpected msg in round-robin synchronizer!"),
            Err(_) => {}
        }
        tokio_timerfd::sleep(self.max_rtt)
            .await
            .expect("Robin failed to sleep.");
    }
}

impl RequestInterval {
    pub async fn new_round_robin(my_pid: usize, nb_nodes: usize, max_rtt: Duration) -> Self {
        let synchronizer = RoundRobinSynchronizer::new(my_pid, nb_nodes, max_rtt).await;
        RequestInterval::RoundRobin { synchronizer }
    }

    pub fn new_exponential(throughput: f32) -> Self {
        RequestInterval::Exponential {
            distribution: Exp::new(throughput).expect("Failed to create exponential distribution"),
        }
    }
}

impl RequestInterval {
    fn next(&mut self, last: &Instant) -> Instant {
        match self {
            RequestInterval::RoundRobin { .. } => Instant::now(),
            RequestInterval::Exponential { distribution } => {
                *last + Duration::from_secs_f32(distribution.sample(&mut rand::rng()))
            }
            RequestInterval::Constant { reqs_per_second } => {
                *last + Duration::from_secs_f32(1. / *reqs_per_second)
            }
        }
    }
}

pub struct Workload {
    pub nb_requests: usize,
    pub rw_ratio: f32, // 0 = 100% reads, 1 = 100 %writes
    pub interval: RequestInterval,
}

pub struct Client {
    my_pid: usize,
    client_request_tx: Sender<Command>,
    client_response_rx: Receiver<Response>,
}

impl Client {
    pub fn new(my_pid: usize) -> (Self, Receiver<Command>, Sender<Response>) {
        let (client_request_tx, client_request_rx) = mpsc::channel(1);
        let (client_response_tx, client_response_rx) = mpsc::channel(1);
        let client = Self {
            my_pid,
            client_request_tx,
            client_response_rx,
        };
        (client, client_request_rx, client_response_tx)
    }

    pub async fn run(mut self, mut workload: Workload) {
        let mut request_generated = Instant::now();
        for i in 0..workload.nb_requests {
            if let RequestInterval::RoundRobin { synchronizer } = &mut workload.interval {
                synchronizer.wait().await;
            }
            let request = if rand::random_range(0. ..1.) < workload.rw_ratio {
                Command::new_write(
                    self.my_pid,
                    &Request::Put {
                        key: "single-key".into(),
                        value: format!("v{}.{}!", self.my_pid, i),
                    },
                )
            } else {
                Command::new_read_only(
                    self.my_pid,
                    &Request::Get {
                        key: "single-key".into(),
                    },
                )
            };
            request_generated = workload.interval.next(&request_generated);
            let time_before_generation =
                request_generated.saturating_duration_since(Instant::now());
            if !time_before_generation.is_zero() {
                tokio_timerfd::sleep(time_before_generation)
                    .await
                    .expect("Failed to sleep");
            }
            let issued = Instant::now();
            self.client_request_tx
                .send(request)
                .await
                .expect("Client failed to queue request");
            let response = self
                .client_response_rx
                .recv()
                .await
                .expect("Client failed to receive response");
            let responded = Instant::now();
            let readable = format!(
                "{} in {:?}",
                if let Response::Put { .. } = response {
                    "PUT"
                } else {
                    "GET"
                },
                responded.duration_since(request_generated)
            );
            let event = ExecutedEvent {
                response,
                latency: responded.duration_since(request_generated),
                queueing: issued.duration_since(request_generated),
                processing: responded.duration_since(request_generated),
            };
            log("executed", &readable, &event);
            if let RequestInterval::RoundRobin { synchronizer } = &mut workload.interval {
                synchronizer.notify().await;
            }
        }
    }
}

#[derive(Serialize)]
struct ExecutedEvent {
    response: Response,
    latency: Duration,
    queueing: Duration,
    processing: Duration,
}

fn log<Event: Serialize>(key: &str, readable: &str, event: &Event) {
    println!(
        "[log={}] {} | {}",
        key,
        readable,
        serde_json::to_string(event).expect("Failed to serialize event")
    );
}

pub struct App {
    my_pid: usize,
    cassandra_handler: Option<PreparedHandler>,
    committed_request_rx: Receiver<Command>,
    client_response_tx: Sender<Response>,
}

impl App {
    pub async fn new(
        db: Option<String>,
        my_pid: usize,
    ) -> ((Client, Receiver<Command>), (Self, Sender<Command>)) {
        let cassandra_handler = if let Some(uri) = db {
            // docker run --name cassandra -p 9042:9042 -d cassandra
            // -db 127.0.0.1:9042
            // docker stop cassandra && docker rm cassandra
            let cassandra = Handler::new(&uri).await;
            cassandra.reset_database().await;
            cassandra.prepare().await.into()
        } else {
            None
        };

        let (client, client_request_rx, client_response_tx) = Client::new(my_pid);
        let (committed_request_tx, committed_request_rx) = mpsc::channel::<Command>(1);
        let app = Self {
            my_pid,
            cassandra_handler,
            committed_request_rx,
            client_response_tx,
        };

        ((client, client_request_rx), (app, committed_request_tx))
    }

    pub async fn run(mut self) {
        while let Some(command) = self.committed_request_rx.recv().await {
            let command: CommittedCommand<Request> = command.into();
            trace!("About to execute committed request: {:?}", command);
            let response = if let Some(cassandra_handler) = self.cassandra_handler.as_ref() {
                cassandra_handler.execute(command.app_request).await
            } else {
                // We mock Cassandra
                match command.app_request {
                    Request::Put { key, value } => Response::Put { key, value },
                    Request::Get { key } => Response::Get { key, value: None },
                }
            };
            if command.proposer == self.my_pid {
                self.client_response_tx
                    .send(response)
                    .await
                    .expect("Server failed to enqueue client Response");
            }
        }
    }
}
