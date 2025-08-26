use crate::{protocol, storage::AppState};
use base64::{engine::general_purpose, Engine as _};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

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

                let command_with_args = vec![
                    "REPLCONF".to_string(),
                    "ACK".to_string(),
                    offset.to_string(),
                ];
                let serialized_cmd = protocol::serialize_resp_array(&command_with_args);
                let cmd_bytes = serialized_cmd.as_bytes();
                let cmd_len = cmd_bytes.len() as u64;
                let mut master_offset = state.master_replication_offset.lock().await;
                *master_offset += cmd_len;

                return Ok(());
            }
            "ACK" => {
                if args.len() < 2 {
                    return Ok(());
                }

                if let Ok(offset) = args[1].parse::<u64>() {
                    let mut replicas = state.replicas.lock().await;
                    for replica in replicas.iter_mut() {
                        if replica.offset < offset {
                            replica.offset = offset;
                        }
                    }
                }
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
    let required_replicas = args[0].parse::<usize>().unwrap_or(0);
    let timeout_ms = args[1].parse::<u64>().unwrap_or(0);

    // Get the current master offset - this is what we want replicas to acknowledge
    let current_master_offset = *state.master_replication_offset.lock().await;

    // Create timeout deadline
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout_ms);

    // Loop until we have enough replicas or timeout
    let mut acknowledged_replicas = 0;

    loop {
        // Check how many replicas have sufficient offset
        {
            let replicas = state.replicas.lock().await;
            acknowledged_replicas = replicas
                .iter()
                .filter(|replica| replica.offset >= current_master_offset)
                .count();

            // Exit if we have enough replicas
            if acknowledged_replicas >= required_replicas {
                break;
            }
        }

        // Check if timeout occurred
        if tokio::time::Instant::now() >= deadline {
            break;
        }

        // Send GETACK to all replicas to try to get updated offsets
        let getack_cmd = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
        {
            let mut replicas = state.replicas.lock().await;
            if !replicas.is_empty() {
                for replica in replicas.iter_mut() {
                    let _ = replica.stream.write_all(getack_cmd.as_bytes()).await;
                }

                // Update master offset for the GETACK command
                let cmd_len = getack_cmd.len() as u64;
                let mut master_offset = state.master_replication_offset.lock().await;
                *master_offset += cmd_len;
            }
        }

        // Sleep a bit before checking again (50ms seems reasonable)
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // Return the count of replicas that acknowledged the commands
    let response = format!(":{}\r\n", acknowledged_replicas);
    stream.write_all(response.as_bytes()).await
}
