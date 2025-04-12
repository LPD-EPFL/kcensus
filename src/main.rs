use std::io;
use kcensus::run;

#[tokio::main(flavor = "current_thread")]
pub(crate) async fn main() -> io::Result<()> {
    run().await
}