use crate::storage::AppState;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use base64::{engine::general_purpose, Engine as _};

pub async fn handle_replconf<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    _state: &Arc<AppState>,
    _args: &[String],
) -> std::io::Result<()> {
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
                state.replication_id, state.replication_offset
            )
            .as_bytes(),
        )
        .await?;
    // Sending empty rdb file as a placeholder
    let empty_rdb_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
    let empty_rdb = general_purpose::STANDARD.decode(empty_rdb_base64).unwrap();

    // Write RESP bulk string: $<len>\r\n<bytes>\r\n
    stream.write_all(format!("${}\r\n", empty_rdb.len()).as_bytes()).await?;
    stream.write_all(&empty_rdb).await
}