use crate::commands;
use crate::protocol;
use crate::storage;
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

        // Read the FULLRESYNC response line
        let mut response_line = Vec::new();
        let mut byte = [0u8; 1];
        while response_line.len() < 2 || response_line[response_line.len() - 2..] != [b'\r', b'\n']
        {
            master_stream.read_exact(&mut byte).await?;
            response_line.push(byte[0]);
        }
        let _ = String::from_utf8_lossy(&response_line);

        // Read the RDB size line ($<length>\r\n)
        let mut rdb_size_line = Vec::new();
        let mut byte = [0u8; 1];
        while rdb_size_line.len() < 2 || rdb_size_line[rdb_size_line.len() - 2..] != [b'\r', b'\n']
        {
            master_stream.read_exact(&mut byte).await?;
            rdb_size_line.push(byte[0]);
        }
        let rdb_size_str = String::from_utf8_lossy(&rdb_size_line);

        // Parse the RDB size
        let rdb_size: usize = rdb_size_str
            .trim_start_matches('$')
            .trim_end_matches("\r\n")
            .parse()
            .expect("Invalid RDB size format");

        // Read the exact RDB content
        let mut rdb_data = vec![0u8; rdb_size];
        master_stream.read_exact(&mut rdb_data).await?;

        let state_clone = state.clone();
        tokio::spawn(async move {
            handle_master_stream(master_stream, state_clone, Vec::new()).await;
        });
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
    let mut buffer = Vec::with_capacity(1024);
    let mut temp_buf = [0; 1024];

    loop {
        // Attempt to parse commands from the buffer before reading more data
        loop {
            let received_str = match std::str::from_utf8(&buffer) {
                Ok(s) => s,
                Err(_) => break, // Incomplete UTF-8, need more data
            };

            match protocol::parse_resp(received_str) {
                Ok((parsed, consumed_bytes)) => {
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
                                let replica_info = storage::ReplicaInfo {
                                    stream: stream,
                                    offset: 0,
                                };
                                replicas.push(replica_info);
                                return;
                            }
                        }
                        Err(e) => {
                            eprintln!("Error handling command: {}", e);
                            let _ = stream.write_all(b"-ERR server error\r\n").await;
                            return;
                        }
                    }
                    // Remove the processed command from the buffer
                    buffer.drain(..consumed_bytes);
                }
                Err(_) => {
                    // Not enough data to parse a full command, break to read more
                    break;
                }
            }
        }

        // Read more data from the client
        let n = match stream.read(&mut temp_buf).await {
            Ok(0) => return, // Connection closed
            Ok(n) => n,
            Err(e) => {
                eprintln!("failed to read from socket; err = {:?}", e);
                return;
            }
        };
        buffer.extend_from_slice(&temp_buf[..n]);
    }
}

async fn handle_master_stream(mut stream: TcpStream, state: Arc<AppState>, initial_data: Vec<u8>) {
    let mut buffer = initial_data;
    let mut temp_buf = [0; 1024];

    loop {
        loop {
            let received_str = match std::str::from_utf8(&buffer) {
                Ok(s) => s,
                Err(_) => break, // Incomplete UTF-8 sequence, need more data
            };

            match protocol::parse_resp(received_str) {
                Ok((parsed_command, consumed_bytes)) => {
                    println!("parsed command: {:?}", parsed_command);
                    let mut dummy_transaction_state = TransactionState {
                        in_transaction: false,
                        queued_commands: Vec::new(),
                    };

                    let command_result = if parsed_command[0].to_uppercase() == "REPLCONF" {
                        // For REPLCONF, use the real stream to send the ACK back.
                        commands::handle_command(
                            parsed_command,
                            &mut stream,
                            &state,
                            &mut dummy_transaction_state,
                        )
                        .await
                    } else {
                        // For other commands (SET, etc.), use a dummy sink.
                        let mut sink = tokio::io::sink();
                        commands::handle_command(
                            parsed_command,
                            &mut sink,
                            &state,
                            &mut dummy_transaction_state,
                        )
                        .await
                    };

                    match command_result {
                        Ok(_) => {
                            println!("Processed propagated command.");
                        }
                        Err(e) => {
                            eprintln!("Error processing propagated command: {}", e);
                        }
                    }

                    // Remove the processed command from the buffer
                    let mut offset = state.slave_replication_offset.lock().await;
                    *offset += consumed_bytes as u64;
                    buffer.drain(..consumed_bytes);
                }
                Err(_) => {
                    // Not enough data to parse a full command, break to read more
                    break;
                }
            }
        }

        // Read more data from the master
        match stream.read(&mut temp_buf).await {
            Ok(0) => {
                println!("Master connection closed.");
                return;
            }
            Ok(n) => {
                buffer.extend_from_slice(&temp_buf[..n]);
            }
            Err(e) => {
                eprintln!("Failed to read from master socket; err = {:?}", e);
                return;
            }
        }
    }
}
