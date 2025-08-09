use crate::storage::AppState;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

pub async fn handle_replconf<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    stream.write_all(b"+OK\r\n").await
}
