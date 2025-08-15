use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use std::env;

use crate::storage::AppState;

// Declare the modules to make them available
mod commands;
mod protocol;
mod server;
mod storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Logs from your program will appear here!");

    // Initialize the shared database
    let (stream_notifier_tx, _) = broadcast::channel::<()>(16);
    let replica_of = env::args().nth(4);

    let state = Arc::new(AppState {
        db: Arc::new(Mutex::new(HashMap::new())),
        blocked_clients: Arc::new(Mutex::new(HashMap::new())),
        stream_notifier: stream_notifier_tx,
        replica_of,
        replication_id: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
        replication_offset: 0,
        replicas: Arc::new(Mutex::new(Vec::new())),
    });

    // Start the server
    if let Err(e) = server::run(state).await {
        eprintln!("Server error: {}", e);
    }

    Ok(())
}
