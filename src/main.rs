use kcensus::run;
use std::io;

#[tokio::main(flavor = "current_thread")]
pub(crate) async fn main() -> io::Result<()> {
    run().await
}
