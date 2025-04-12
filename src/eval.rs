use serde::Serialize;

pub fn log<Event: Serialize>(key: &str, readable: &str, event: &Event) {
    println!(
        "[log={}] {} | {}",
        key,
        readable,
        serde_json::to_string(event).expect("Failed to serialize event")
    );
}
