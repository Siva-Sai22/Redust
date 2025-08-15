use crate::commands;
use crate::protocol;
use crate::storage::AppState;
use crate::storage::TransactionState;
use std::env;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub async fn run(state: Arc<AppState>) -> std::io::Result<()> {
    let port = match env::args().nth(2) {
        Some(port) => port,
        None => String::from("6379"),
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    println!("🚀 Server listening on 127.0.0.1:{}", port);

    if let Some(replica) = state.replica_of.clone() {
        let arr = replica.split(" ").collect::<Vec<&str>>();
        let master_addr = format!("{}:{}", arr[0], arr[1]);

        let mut master_stream = TcpStream::connect(master_addr).await?;
        master_stream.write_all(b"*1\r\n$4\r\nPING\r\n").await?;

        let mut buf = [0; 1024];
        master_stream.read(&mut buf).await?;

        master_stream
            .write_all(
                format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n",
                    port
                )
                .as_bytes(),
            )
            .await?;
        master_stream.read(&mut buf).await?;

        master_stream
            .write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
            .await?;
        master_stream.read(&mut buf).await?;

        master_stream
            .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
            .await?;
        master_stream.read(&mut buf).await?;
    }

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("Accepted new connection from: {}", addr);
        let state_clone = state.clone();
        let transation_state = TransactionState {
            in_transaction: false,
            queued_commands: Vec::new(),
        };
        tokio::spawn(async move {
            handle_stream(socket, state_clone, transation_state).await;
        });
    }
}

async fn handle_stream(
    mut stream: TcpStream,
    state: Arc<AppState>,
    mut transation_state: TransactionState,
) {
    let mut buf = [0; 1024];

    loop {
        let n = match stream.read(&mut buf).await {
            Ok(0) => return, // Connection closed
            Ok(n) => n,
            Err(e) => {
                eprintln!("failed to read from socket; err = {:?}", e);
                return;
            }
        };

        if let Ok(received) = std::str::from_utf8(&buf[..n]) {
            if let Ok(parsed) = protocol::parse_resp(received) {
                match commands::handle_command(
                    parsed.clone(),
                    &mut stream,
                    &state,
                    &mut transation_state,
                )
                .await
                {
                    Ok(_) => {
                        if parsed[0].to_uppercase() == "PSYNC" {
                            let mut replicas = state.replicas.lock().await;
                            replicas.push(stream);
                            return;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error handling command: {}", e);
                        // Optionally, write an error back to the client
                        let _ = stream.write_all(b"-ERR server error\r\n").await;
                        return;
                    }
                }
            }
        }
    }
}
