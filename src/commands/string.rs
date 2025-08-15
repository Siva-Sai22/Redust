use crate::protocol;
use crate::storage::{AppState, DataStoreValue, ValueEntry};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

pub async fn handle_set<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    let ok = "+OK\r\n";
    if let (Some(key), Some(value)) = (args.get(0), args.get(1)) {
        let mut expires_at = None;
        if args.len() > 2 && args[2].to_uppercase() == "PX" {
            if let Some(ms_str) = args.get(3) {
                if let Ok(ms) = ms_str.parse::<u64>() {
                    expires_at = Some(Instant::now() + Duration::from_millis(ms));
                }
            }
        }
        let mut map = state.db.lock().await;
        let entry = ValueEntry {
            value: DataStoreValue::String(value.to_string()),
            expires_at,
        };
        map.insert(key.to_string(), entry);
        let _ = stream.write_all(ok.as_bytes()).await;
        let mut replicas = state.replicas.lock().await;
        for replica in replicas.iter_mut() {
            let mut command_with_args = vec!["SET".to_string()];
            command_with_args.extend_from_slice(args);
            let response = protocol::serialize_resp_array(&command_with_args);
            replica.write_all(response.as_bytes()).await?;
        }
        return Ok(());
    } else {
        stream
            .write_all(b"-ERR wrong number of arguments for 'set' command\r\n")
            .await
    }
}

pub async fn handle_get<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    let null = "$-1\r\n";
    let type_err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    if let Some(key) = args.get(0) {
        let mut map = state.db.lock().await;
        if let Some(entry) = map.get(key) {
            // Check expiry
            if entry.expires_at.map_or(false, |e| Instant::now() > e) {
                map.remove(key);
                stream.write_all(null.as_bytes()).await?;
                return Ok(());
            }

            match &entry.value {
                DataStoreValue::String(val) => {
                    let response = format!("${}\r\n{}\r\n", val.len(), val);
                    stream.write_all(response.as_bytes()).await
                }

                _ => stream.write_all(type_err.as_bytes()).await,
            }
        } else {
            stream.write_all(null.as_bytes()).await
        }
    } else {
        stream
            .write_all(b"-ERR wrong number of arguments for 'get' command\r\n")
            .await
    }
}

pub async fn handle_incr<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    if let Some(key) = args.get(0) {
        let mut map = state.db.lock().await;
        if let Some(entry) = map.get_mut(key) {
            match &mut entry.value {
                DataStoreValue::String(val) => {
                    let prev = match val.parse::<i64>() {
                        Ok(t) => t,
                        _ => {
                            return stream
                                .write_all(b"-ERR value is not an integer or out of range\r\n")
                                .await;
                        }
                    };
                    *val = (prev + 1).to_string();
                    stream.write_all(format!(":{}\r\n", val).as_bytes()).await
                }
                _ => {
                    stream
                        .write_all(b"-ERR value is not an integer or out of range\r\n")
                        .await
                }
            }
        } else {
            map.insert(
                key.to_string(),
                ValueEntry {
                    value: DataStoreValue::String("1".to_string()),
                    expires_at: None,
                },
            );
            let _ = stream.write_all(":1\r\n".as_bytes()).await;
            let mut replicas = state.replicas.lock().await;
            for replica in replicas.iter_mut() {
                let mut command_with_args = vec!["INCR".to_string()];
                command_with_args.extend_from_slice(args);
                let response = protocol::serialize_resp_array(&command_with_args);
                replica.write_all(response.as_bytes()).await?;
            }
            return Ok(());
        }
    } else {
        stream
            .write_all(b"-ERR wrong number of arguments for 'incr' command\r\n")
            .await
    }
}
