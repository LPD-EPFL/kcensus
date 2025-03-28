use crate::kcensus::{KVal, Request};
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;

pub async fn simple_requester(nb_nodes: usize, my_pid: usize, start_tx: Sender<Option<Request>>) {
    let start = Instant::now() + Duration::from_millis((50 + my_pid * 200) as u64);
    let mut interval =
        tokio::time::interval_at(start, Duration::from_millis((200 * nb_nodes) as u64));
    for i in 0..10 {
        interval.tick().await;
        start_tx
            .send(Some(
                KVal {
                    val: format!("v{}.{}!", my_pid, i),
                    proposer: my_pid,
                }
                .into_request(Instant::now()),
            ))
            .await
            .expect("Sending v");
    }

    start_tx.send(None).await.expect("Sending none");
}
