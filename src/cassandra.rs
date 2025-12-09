use crate::consensus::command::{Command, CommittedCommand};
use crate::eval;
use futures::future::join_all;
use log::trace;
use rand_distr::{Distribution, Exp};
use scylla::client::{session::Session, session_builder::SessionBuilder, PoolSize};
use scylla::statement::prepared::PreparedStatement;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Semaphore};
use tokio::{pin, select};
use tokio_timerfd::Delay;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Put {
        key: String,
        value: String,
        shard: u64,
        request_id: u64,
    },
    Get {
        key: String,
        shard: u64,
        request_id: u64,
    },
}

impl Request {
    pub fn shard(&self) -> u64 {
        match self {
            Request::Put { shard, .. } => *shard,
            Request::Get { shard, .. } => *shard,
        }
    }
}

#[derive(Serialize, Debug)]
#[allow(dead_code)]
pub enum Response {
    Put {
        key: String,
        value: String,
        request_id: u64,
    },
    Get {
        key: String,
        value: Option<String>,
        request_id: u64,
    },
}

pub struct Handler {
    session: Session,
}

pub struct PreparedHandler {
    session: Session,
    put_ps: PreparedStatement,
    get_ps: PreparedStatement,
}

// Structure for managing parallel execution with per-shard ordering
pub struct ParallelCassandraExecutor {
    // Per-shard channels to maintain ordering within each shard
    shard_senders: Vec<Sender<(Request, Option<Sender<Response>>)>>,
    shard_join_handles: Vec<tokio::task::JoinHandle<()>>,
    global_semaphore: Arc<Semaphore>,
    total_completed: Arc<AtomicUsize>,
    first_request_time: Arc<std::sync::Mutex<Option<Instant>>>,
}

impl ParallelCassandraExecutor {
    pub fn new(handler: PreparedHandler, shards: usize) -> Self {
        let global_semaphore = Arc::new(Semaphore::new(256));
        let mut shard_senders = Vec::with_capacity(shards);
        let mut shard_join_handles = Vec::with_capacity(shards);

        let handler = Arc::new(handler);

        let total_completed = Arc::new(AtomicUsize::new(0));
        let first_request_time = Arc::new(std::sync::Mutex::new(None));
        // Create per-shard channels and spawn workers
        for _shard_id in 0..shards {
            let (tx, mut rx) = mpsc::channel::<(Request, Option<Sender<Response>>)>(32);
            shard_senders.push(tx);

            // Clone necessary resources for the worker
            let handler = handler.clone();
            let semaphore = global_semaphore.clone();

            let total_completed = total_completed.clone();
            let first_request_time = first_request_time.clone();
            // Spawn per-shard worker to maintain ordering
            let handle = tokio::spawn(async move {
                while let Some((request, response_tx)) = rx.recv().await {
                    {
                        // record first request time
                        let mut first_time = first_request_time.lock().unwrap();
                        if first_time.is_none() {
                            *first_time = Some(Instant::now());
                        }
                    }
                    // Acquire permit from global semaphore
                    let _permit = semaphore.acquire().await.expect("Semaphore was closed");

                    // Execute the request directly
                    let response = handler.execute(request).await;

                    // Send response back
                    if let Some(response_tx) = response_tx {
                        // If a response channel was provided, send the response
                        response_tx
                            .send(response)
                            .await
                            .expect("Server failed to enqueue client Response");
                    };
                    // Permit is automatically released when _permit is dropped
                    total_completed.fetch_add(1, Ordering::Relaxed);
                }
            });
            shard_join_handles.push(handle);
        }

        Self {
            shard_senders,
            global_semaphore,
            shard_join_handles,
            total_completed,
            first_request_time,
        }
    }

    pub async fn execute(&self, request: Request, response_tx: Option<Sender<Response>>) {
        // Send request to the appropriate shard
        self.shard_senders[request.shard() as usize]
            .send((request, response_tx))
            .await
            .expect("Failed to send request to shard worker");
    }
}

impl Handler {
    pub async fn new(uri: &str) -> Self {
        let pool_size = PoolSize::PerShard(NonZeroUsize::new(4).unwrap());
        Self {
            session: SessionBuilder::new()
                .known_node(uri)
                .pool_size(pool_size)
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
            Request::Put {
                key,
                value,
                request_id,
                ..
            } => {
                self.put(&key, &value).await;
                Response::Put {
                    key,
                    value,
                    request_id,
                }
            }
            Request::Get {
                key, request_id, ..
            } => Response::Get {
                value: self.get(&key).await,
                key,
                request_id,
            },
        }
    }
}

pub enum RequestInterval {
    Exponential { distribution: Exp<f32> },
    Constant { reqs_per_second: f32 },
}
impl RequestInterval {
    pub fn new_exponential(throughput: f32) -> Self {
        RequestInterval::Exponential {
            distribution: Exp::new(throughput).expect("Failed to create exponential distribution"),
        }
    }

    fn next(&mut self, last: &Instant) -> Instant {
        let secs = match self {
            RequestInterval::Exponential { distribution } => distribution.sample(&mut rand::rng()),
            RequestInterval::Constant { reqs_per_second } => 1. / *reqs_per_second,
        };
        if secs.is_infinite() || secs.is_nan() {
            return *last + Duration::from_secs(1 << 30);
        }
        *last + Duration::from_secs_f32(secs)
    }
}

pub struct Workload {
    pub duration: Duration,
    pub warmup: Duration,
    pub warmdown: Duration,
    pub rw_ratio: f32, // 0 = 100% reads, 1 = 100 %writes
    pub interval: RequestInterval,
    pub key_distribution: rand_distr::Zipf<f64>,
    pub shards: usize,
}

impl Workload {
    pub fn random_key(&self) -> usize {
        // Zipfian distributions are off by 1
        self.key_distribution.sample(&mut rand::rng()) as usize - 1
    }
}

pub struct Client {
    my_pid: usize,
    speedup: u32,
    client_request_tx: Sender<Command>,
    client_response_rx: Receiver<Response>,
}

impl Client {
    pub fn new(my_pid: usize, speedup: u32) -> (Self, Receiver<Command>, Sender<Response>) {
        let (client_request_tx, client_request_rx) = mpsc::channel(100);
        let (client_response_tx, client_response_rx) = mpsc::channel(100);
        let client = Self {
            my_pid,
            speedup,
            client_request_tx,
            client_response_rx,
        };
        (client, client_request_rx, client_response_tx)
    }

    fn generate_request(&self, workload: &Workload, request_id: u64) -> Command {
        let key = workload.random_key();
        if rand::random_range(0. ..1.) < workload.rw_ratio {
            Command::new_write(
                self.my_pid,
                key % workload.shards,
                &Request::Put {
                    key: format!("key{key}"),
                    value: format!("v{}.{}!", self.my_pid, request_id),
                    shard: (key % workload.shards) as u64,
                    request_id,
                },
            )
        } else {
            Command::new_read_only(
                self.my_pid,
                key % workload.shards,
                &Request::Get {
                    key: format!("key{key}"),
                    shard: (key % workload.shards) as u64,
                    request_id,
                },
            )
        }
    }

    fn log_executed_response(
        &self,
        response: Response,
        scheduled_time: Instant,
        issued_time: Instant,
    ) {
        let responded = Instant::now();
        let readable = format!(
            "{} in {:?}",
            if let Response::Put { .. } = response {
                "PUT"
            } else {
                "GET"
            },
            responded.duration_since(scheduled_time) * self.speedup
        );
        let event = ExecutedEvent {
            response,
            latency: responded.duration_since(scheduled_time) * self.speedup,
            queueing: issued_time.duration_since(scheduled_time) * self.speedup,
            processing: responded.duration_since(issued_time) * self.speedup,
        };
        eval::log("executed", &readable, &event);
    }

    pub async fn run(mut self, mut workload: Workload) {
        let warmup_start = Instant::now();
        let warmup_end = warmup_start + workload.warmup;
        let warmdown_start = warmup_end + workload.duration;
        let warmdown_end = warmdown_start + workload.warmdown;

        // Track scheduled_time and issued_time for each request_id
        let mut request_timings: HashMap<u64, (Instant, Instant)> = HashMap::new();

        // Prepare the first request
        let mut next_request = Some(self.generate_request(&workload, 0u64));

        // Initialize delay object and request state
        let delay = Delay::new(Instant::now()).expect("Failed to init delay");
        pin!(delay);

        let mut scheduled_time = Instant::now();
        let mut current_request_id = 0;
        let mut responses_received = 0;
        let mut total_latency = std::time::Duration::ZERO;

        // Set initial delay for first request
        scheduled_time = workload.interval.next(&scheduled_time);
        delay.as_mut().reset(scheduled_time);
        while next_request.is_some() || responses_received != current_request_id {
            let no_response = self.client_response_rx.is_empty();
            select! {
                // Handle sending the next request when its time arrives
                // We don't want to be stuck trying to push a new request, so we only do it when we
                // have capacity. However, if the capacity is too low, successive requests might end
                // up being queued, waiting for some request to complete... which might double their
                // latency. We therefore make client_request_tx large.
                res = &mut delay, if no_response && next_request.is_some() && self.client_request_tx.capacity() > 0 => {
                    res.expect("Delay failed");

                    let request = next_request.take().unwrap();
                    let issued_time = Instant::now();
                    request_timings.insert(current_request_id as u64, (scheduled_time, issued_time));

                    self.client_request_tx
                        .send(request)
                        .await
                        .expect("Client failed to queue request");
                    // TODO: yield here for lower latency ?

                    current_request_id += 1;

                    // Prepare next request, while we haven't reached the end of the warmdown
                    if Instant::now() < warmdown_end {
                        next_request = Some(
                            self.generate_request(&workload, current_request_id as u64)
                        );
                        scheduled_time = workload.interval.next(&scheduled_time);
                        delay.as_mut().reset(scheduled_time);
                    }
                }

                // Handle receiving responses
                response = self.client_response_rx.recv() => {
                    if let Some(response) = response {
                        // Extract request_id from response and look up timing info
                        let request_id = match &response {
                            Response::Put { request_id, .. } => *request_id,
                            Response::Get { request_id, .. } => *request_id,
                        };

                        let (scheduled_time, issued_time) = request_timings
                            .remove(&request_id)
                            .expect("Response received for an unknown request_id");
                        if (warmup_end..warmdown_start).contains(&scheduled_time) {
                            self.log_executed_response(response, scheduled_time, issued_time);
                        }
                        responses_received += 1;
                        total_latency += Instant::now().duration_since(scheduled_time) * self.speedup;
                    }
                }
            }
        }
        let average_latency = total_latency / responses_received as u32;
        let readable = format!(
            "Issued {} requests in total (avg latency: {}ms) (including warmup+warmdown)",
            responses_received,
            average_latency.as_millis()
        );
        let event = ClientDoneEvent {
            requests: responses_received,
            average_latency: total_latency / responses_received as u32,
        };
        eval::log("client-done", &readable, &event);
    }
}

#[derive(Serialize)]
struct ClientDoneEvent {
    requests: usize,
    average_latency: std::time::Duration,
}

#[derive(Serialize)]
struct ExecutedEvent {
    response: Response,
    latency: Duration,
    queueing: Duration,
    processing: Duration,
}

#[derive(Serialize)]
struct ThroughputEvent {
    requests: usize,
    seconds: Duration,
    throughput: f64,
}

pub struct App {
    my_pid: usize,
    parallel_executor: Option<ParallelCassandraExecutor>,
    committed_request_rx: Receiver<Command>,
    client_response_tx: Sender<Response>,
}

impl App {
    pub async fn new(
        db: Option<String>,
        speedup: u32,
        my_pid: usize,
        shards: usize,
    ) -> ((Client, Receiver<Command>), (Self, Sender<Command>)) {
        let parallel_executor = if let Some(uri) = db {
            // docker run --name cassandra -p 9042:9042 -d cassandra
            // -db 127.0.0.1:9042
            // docker stop cassandra && docker rm cassandra
            let cassandra = Handler::new(&uri).await;
            cassandra.reset_database().await;
            let handler = cassandra.prepare().await;
            let executor = ParallelCassandraExecutor::new(handler, shards);
            Some(executor)
        } else {
            None
        };

        let (client, client_request_rx, client_response_tx) = Client::new(my_pid, speedup);
        let (committed_request_tx, committed_request_rx) = mpsc::channel::<Command>(100);
        let app = Self {
            my_pid,
            parallel_executor,
            committed_request_rx,
            client_response_tx,
        };

        ((client, client_request_rx), (app, committed_request_tx))
    }

    pub async fn run(mut self) {
        while let Some(command) = self.committed_request_rx.recv().await {
            let command: CommittedCommand<Request> = command.into();
            trace!("About to execute committed request: {command:?}");
            if let Some(parallel_executor) = self.parallel_executor.as_ref() {
                // Use parallel executor for Cassandra
                parallel_executor
                    .execute(
                        command.app_request,
                        if command.requester == self.my_pid {
                            Some(self.client_response_tx.clone())
                        } else {
                            None
                        },
                    )
                    .await
            } else {
                let response = match command.app_request {
                    Request::Put {
                        key,
                        value,
                        request_id,
                        ..
                    } => Response::Put {
                        key,
                        value,
                        request_id,
                    },
                    Request::Get {
                        key, request_id, ..
                    } => Response::Get {
                        key,
                        value: None,
                        request_id,
                    },
                };
                if command.requester == self.my_pid {
                    self.client_response_tx
                        .send(response)
                        .await
                        .expect("Server failed to enqueue client Response");
                }
            }
        }
        if let Some(mut parallel_executor) = self.parallel_executor {
            // Ensure all pending requests are processed before exiting
            parallel_executor.shard_senders.clear();
            join_all(parallel_executor.shard_join_handles).await;
            parallel_executor.global_semaphore.close();

            // Log throughput at the end
            let completed = parallel_executor.total_completed.load(Ordering::Relaxed);
            let first_time = parallel_executor.first_request_time.lock().unwrap();
            if let Some(start_time) = *first_time {
                let elapsed = start_time.elapsed();
                let elapsed_secs = elapsed.as_secs_f64();
                let throughput = completed as f64 / elapsed_secs;
                let readable = format!(
                    "{completed} requests in {elapsed_secs:.3} seconds ({throughput:.2} req/s)"
                );
                let event = ThroughputEvent {
                    requests: completed,
                    seconds: elapsed,
                    throughput,
                };
                eval::log("throughput", &readable, &event);
            }
        }
    }
}
