use crate::commands::handle_command;
use crate::storage::{AppState, TransactionState};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn handle_multi<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    transation_state: &mut TransactionState,
) -> std::io::Result<()> {
    let ok = "+OK\r\n";
    transation_state.queued_commands.clear();
    transation_state.in_transaction = true;
    stream.write_all(ok.as_bytes()).await
}

pub async fn handle_exec<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    transation_state: &mut TransactionState,
) -> std::io::Result<()> {
    let empty_arr = "*0\r\n";
    if !transation_state.in_transaction {
        return stream.write_all(b"-ERR EXEC without MULTI\r\n").await;
    }
    if transation_state.queued_commands.is_empty() {
        transation_state.in_transaction = false;
        return stream.write_all(empty_arr.as_bytes()).await;
    }

    let queued_commands = transation_state.queued_commands.clone();
    transation_state.queued_commands.clear();
    transation_state.in_transaction = false;

    let mut response = String::new();
    response.push_str(&format!("*{}\r\n", queued_commands.len()));

    for commands in queued_commands {
        let (mut reader, mut writer) = tokio::io::duplex(4096);
        let stream_id = String::from("");
        let _ = Box::pin(handle_command(
            commands.to_vec(),
            &mut writer,
            state,
            transation_state,
            stream_id
        ))
        .await;

        drop(writer);

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        response.push_str(String::from_utf8_lossy(&buf).as_ref());
    }

    stream.write_all(response.as_bytes()).await
}

pub async fn handle_discard<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    transation_state: &mut TransactionState,
) -> std::io::Result<()> {
    let ok = "+OK\r\n";
    if !transation_state.in_transaction {
        return stream.write_all(b"-ERR DISCARD without MULTI\r\n").await;
    }

    transation_state.in_transaction = false;
    transation_state.queued_commands.clear();
    stream.write_all(ok.as_bytes()).await
}
