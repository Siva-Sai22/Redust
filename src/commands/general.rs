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
    let mut response = String::new();
    if let Some(_) = &state.replica_of {
        response.push_str("$10\r\nrole:slave\r\n");
    } else {
        response.push_str("$11\r\nrole:master\r\n");
    }
    stream.write_all(response.as_bytes()).await
}
