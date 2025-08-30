use std::sync::Arc;

use tokio::{io::AsyncWriteExt, sync::oneshot};

use crate::storage::AppState;

pub async fn handle_subscribe<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
    stream_id: String,
) -> std::io::Result<()> {
    if args.is_empty() {
        stream
            .write_all(b"-ERR wrong number of arguments for 'SUBSCRIBE' command\r\n")
            .await?;
        return Ok(());
    }

    let mut subscribers = state.subscribers.lock().await;
    let mut total_subscriptions = state.client_subscriptions.lock().await;

    let channel = args[0].clone();
    if !total_subscriptions.contains_key(&stream_id) {
        total_subscriptions.insert(stream_id.clone(), Vec::new());
    }
    let client_channels = total_subscriptions.get_mut(&stream_id).unwrap();

    if !client_channels.contains(&channel) {
        let entry = subscribers
            .entry(channel.clone())
            .or_insert_with(Vec::new);
        let (tx, _rx) = oneshot::channel(); 
        entry.push(tx);
        client_channels.push(channel.clone());

        let response = format!(
            "*3\r\n$9\r\nsubscribe\r\n${}\r\n{}\r\n:{}\r\n",
            channel.len(),
            channel,
            client_channels.len().to_string(),
        );

        stream.write_all(response.as_bytes()).await?;
        return Ok(());
    }

    Ok(())
}
