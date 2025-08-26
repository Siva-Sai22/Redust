use crate::storage::AppState;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

pub async fn handle_ping<W: AsyncWriteExt + Unpin>(stream: &mut W) -> std::io::Result<()> {
    stream.write_all(b"+PONG\r\n").await
}

pub async fn handle_echo<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    args: &[String],
) -> std::io::Result<()> {
    if let Some(arg) = args.get(0) {
        let data = format!("${}\r\n{}\r\n", arg.len(), arg);
        stream.write_all(data.as_bytes()).await
    } else {
        stream
            .write_all(b"-ERR wrong number of arguments for 'echo' command\r\n")
            .await
    }
}

pub async fn handle_info<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
) -> std::io::Result<()> {
    // --- Start building the response parts ---

    // Part 1: Role
    let role_str = if state.replica_of.is_some() {
        "role:slave"
    } else {
        "role:master"
    };

    // Part 2: Replication ID
    let replid_str = format!("master_replid:{}", state.master_replication_id);
    // Part 3: Replication Offset
    let reploff_str = format!("master_repl_offset:{}", state.master_replication_offset.lock().await);

    // --- Construct the final RESP response ---

    let response = format!("{}\r\n{}\r\n{}", role_str, replid_str, reploff_str);

    stream
        .write_all(format!("${}\r\n{}\r\n", response.len(), response).as_bytes())
        .await
}
