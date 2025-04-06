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
    pub nb_nodes: usize,
    pub my_pid: usize,
    pub client_request_tx: Sender<Option<Request>>,
    pub client_response_rx: Receiver<Response>,
    pub num_committed_watch_rx: WatchReceiver<usize>,
}

impl Client {
    pub async fn run(mut self) {
        for i in 0..10 {
            while *self.num_committed_watch_rx.borrow_and_update() % self.nb_nodes != self.my_pid {
                self.num_committed_watch_rx.changed().await.expect("Couldn't read backpressure");
            }
            let generated = Instant::now();
            self.client_request_tx
                .send(Request::Put {key: "single-key".into(), value: format!("v{}.{}!", self.my_pid, i)}.into())
                .await
                .expect("Client failed to queue request");
            let response = self.client_response_rx.recv().await.expect("Client failed to receive response");
            let responded = Instant::now();
            println!("Executed {:?} in {:?}", response, responded.duration_since(generated));
        };
        self.client_request_tx.send(None).await.expect("Client failed to enqueue None request");
    }
}