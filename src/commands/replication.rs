use crate::{protocol, storage::AppState};
use base64::{engine::general_purpose, Engine as _};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub async fn handle_replconf<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    if args.len() >= 2 {
        match args[0].to_uppercase().as_str() {
            "GETACK" => {
                let offset = state.slave_replication_offset.lock().await;
                // Use the RESP array serializer for a correctly formatted response
                let response = protocol::serialize_resp_array(&[
                    "REPLCONF".to_string(),
                    "ACK".to_string(),
                    offset.to_string(),
                ]);
                stream.write_all(response.as_bytes()).await?;

                return Ok(());
            }
            "LISTENING-PORT" | "CAPA" => {
                stream.write_all(b"+OK\r\n").await?;
                return Ok(());
            }
            _ => {}
        }
    }
    stream.write_all(b"+OK\r\n").await
}

pub async fn handle_psync<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    if args.len() < 2 {
        let err_msg = "-ERR wrong number of arguments for 'psync' command\r\n";
        stream.write_all(err_msg.as_bytes()).await?;
        return Ok(());
    }

    stream
        .write_all(
            format!(
                "+FULLRESYNC {} {}\r\n",
                state.master_replication_id,
                state.master_replication_offset.lock().await
            )
            .as_bytes(),
        )
        .await?;
    // Sending empty rdb file as a placeholder
    let empty_rdb_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
    let empty_rdb = general_purpose::STANDARD.decode(empty_rdb_base64).unwrap();

    // Write RESP bulk string: $<len>\r\n<bytes>\r\n
    stream
        .write_all(format!("${}\r\n", empty_rdb.len()).as_bytes())
        .await?;
    stream.write_all(&empty_rdb).await
}

pub async fn handle_wait<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    if args.len() != 2 {
        let err_msg = "-ERR wrong number of arguments for 'wait' command\r\n";
        stream.write_all(err_msg.as_bytes()).await?;
        return Ok(());
    }

    // Parse arguments
    let num_replicas: usize = match args[0].parse() {
        Ok(n) => n,
        Err(_) => {
            let err_msg = "-ERR value is not an integer or out of range\r\n";
            stream.write_all(err_msg.as_bytes()).await?;
            return Ok(());
        }
    };

    let timeout_ms: u64 = match args[1].parse() {
        Ok(t) => t,
        Err(_) => {
            let err_msg = "-ERR value is not an integer or out of range\r\n";
            stream.write_all(err_msg.as_bytes()).await?;
            return Ok(());
        }
    };

    if num_replicas == 0 {
        stream.write_all(b":0\r\n").await?;
        return Ok(());
    }

    // Get current master replication offset
    let master_offset = *state.master_replication_offset.lock().await;

    if master_offset == 0 {
        let replicas = state.replicas.lock().await;
        let response = format!(":{}\r\n", replicas.len());
        drop(replicas);
        stream.write_all(response.as_bytes()).await?;
        return Ok(());
    }

    // Start timing for the timeout
    let start_time = std::time::Instant::now();
    let timeout_duration = std::time::Duration::from_millis(timeout_ms);

    println!("Master replication offset: {}", master_offset);

    // Keep checking until timeout expires or indefinitely if timeout is 0
    loop {
        let mut replicas = state.replicas.lock().await;
        // Send GETACK to all replicas to refresh their offsets
        for replica in replicas.iter_mut() {
            // Send REPLCONF GETACK
            let cmd = protocol::serialize_resp_array(&[
                "REPLCONF".to_string(),
                "GETACK".to_string(),
                "*".to_string(),
            ]);

            let mut stream = TcpStream::from_std(replica.stream.try_clone().unwrap()).unwrap();
            stream.write_all(cmd.as_bytes()).await?;
        }
        drop(replicas);

        // Short delay to allow replicas to respond and for the ACK handler to update the state
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Check which replicas have caught up
        let replicas = state.replicas.lock().await;
        let acknowledged_count = replicas
            .iter()
            .filter(|replica| replica.offset >= master_offset)
            .count();

        // println!("Ack count: {}", acknowledged_count);

        // for replica in replicas.iter() {
        //     println!(
        //         "Replica {} offset: {}",
        //         replica.stream.peer_addr().unwrap(),
        //         replica.offset
        //     );
        // }

        // If we have enough replicas, return immediately
        if acknowledged_count >= num_replicas {
            let response = format!(":{}\r\n", acknowledged_count);
            stream.write_all(response.as_bytes()).await?;
            return Ok(());
        }

        // Check for timeout
        if timeout_ms > 0 && start_time.elapsed() >= timeout_duration {
            break;
        }

        // Release the lock and wait a bit before checking again
        drop(replicas);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Timeout expired, return the current count of acknowledged replicas
    let replicas = state.replicas.lock().await;
    let acknowledged_count = replicas
        .iter()
        .filter(|replica| replica.offset >= master_offset)
        .count();

    let response = format!(":{}\r\n", acknowledged_count);
    stream.write_all(response.as_bytes()).await
}
