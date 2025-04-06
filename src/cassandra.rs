use std::time::Duration;
use scylla::client::{session::Session, session_builder::SessionBuilder};
use scylla::statement::prepared::PreparedStatement;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Sender, Receiver};
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

pub struct Client {}

impl Client {
    pub async fn run(self, nb_nodes: usize, my_pid: usize, client_request_tx: Sender<Option<Request>>, mut client_response_rx: Receiver<Response>) {
        let start = Instant::now() + Duration::from_millis((50 + my_pid * 200) as u64);
        let mut interval =
            tokio::time::interval_at(start, Duration::from_millis((200 * nb_nodes) as u64));
        for i in 0..10 {
            interval.tick().await;
            let generated = Instant::now();
            client_request_tx
                .send(Request::Put {key: "single-key".into(), value: format!("v{}.{}!", my_pid, i)}.into())
                .await
                .expect("Client failed to queue request");
            let response = client_response_rx.recv().await.expect("Client failed to receive response");
            let responded = Instant::now();
            println!("Executed {:?} in {:?}", response, responded.duration_since(generated));
        };
        client_request_tx.send(None).await.expect("Client failed to enqueue None request");
    }
}