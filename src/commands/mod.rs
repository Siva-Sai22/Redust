pub mod general;
pub mod list;
pub mod stream;
pub mod string;
pub mod transaction;
pub mod replication;

use crate::storage::{AppState, TransactionState};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

// Central function to process commands.
pub async fn handle_command<W: AsyncWriteExt + Unpin>(
    parsed: Vec<String>,
    stream: &mut W,
    state: &Arc<AppState>,
    transation_state: &mut TransactionState,
) -> std::io::Result<()> {
    let command = parsed.get(0).unwrap().to_uppercase();
    let args = &parsed[1..];

    if transation_state.in_transaction
        && command != "MULTI"
        && command != "EXEC"
        && command != "DISCARD"
    {
        transation_state.queued_commands.push(parsed.to_vec());
        stream.write_all(b"+QUEUED\r\n").await?;
        return Ok(());
    }

    match command.as_str() {
        "PING" => general::handle_ping(stream).await,
        "ECHO" => general::handle_echo(stream, args).await,
        "INFO" => general::handle_info(stream, state).await,
        "SET" => string::handle_set(stream, state, args).await,
        "GET" => string::handle_get(stream, state, args).await,
        "INCR" => string::handle_incr(stream, state, args).await,
        "LPUSH" | "RPUSH" => list::handle_lpush_rpush(&command, stream, state, args).await,
        "LRANGE" => list::handle_lrange(stream, state, args).await,
        "LLEN" => list::handle_llen(stream, state, args).await,
        "LPOP" => list::handle_lpop(stream, state, args).await,
        "BLPOP" => list::handle_blpop(stream, state, args).await,
        "TYPE" => stream::handle_type(stream, state, args).await,
        "XADD" => stream::handle_xadd(stream, state, args).await,
        "XRANGE" => stream::handle_xrange(stream, state, args).await,
        "XREAD" => stream::handle_xread(stream, state, args).await,
        "MULTI" => transaction::handle_multi(stream, transation_state).await,
        "EXEC" => transaction::handle_exec(stream, state, transation_state).await,
        "DISCARD" => transaction::handle_discard(stream, transation_state).await,
        "REPLCONF" => replication::handle_replconf(stream, state, args).await,
        "PSYNC" => replication::handle_psync(stream, state, args).await,
        _ => {
            let err_msg = format!(
                "-ERR unknown command `{}`, with args beginning with: {:?}\r\n",
                command,
                args.get(0)
            );
            stream.write_all(err_msg.as_bytes()).await
        }
    }
}
