use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::storage::{BlockedClients, Db};

// Declare the modules to make them available
mod commands;
mod protocol;
mod server;
mod storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Logs from your program will appear here!");

    // Initialize the shared database
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let blocked_clients: BlockedClients = Arc::new(Mutex::new(HashMap::new()));
    
    // Start the server
    if let Err(e) = server::run(db, blocked_clients).await {
        eprintln!("Server error: {}", e);
    }
    
    Ok(())
}