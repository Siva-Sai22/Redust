use crate::protocol;
use crate::storage::{AppState, BlockedSender, DataStoreValue, ValueEntry};
use nanoid::nanoid;
use std::cmp::{max, min};
use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tokio::time::timeout;

pub async fn handle_lpush_rpush<W: AsyncWriteExt + Unpin>(
    command: &str,
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    let type_err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    if let (Some(key), Some(_value)) = (args.get(0), args.get(1)) {
        let mut db_map = state.db.lock().await;
        let entry = db_map.entry(key.to_string()).or_insert_with(|| ValueEntry {
            value: DataStoreValue::List(Vec::new()),
            expires_at: None,
        });

        if let DataStoreValue::List(list) = &mut entry.value {
            // Check if there is a blocked client BEFORE pushing the data.
            // If so, we can wake them up.
            let mut client_to_wake = None;
            {
                // Scoped lock for blocked_clients
                let mut blocked_map = state.blocked_clients.lock().await;
                if let Some(queue) = blocked_map.get_mut(key) {
                    if !queue.is_empty() {
                        // Get the sender, but don't remove it yet.
                        client_to_wake = queue.pop_front();
                    }
                    if queue.is_empty() {
                        blocked_map.remove(key);
                    }
                }
            }

            // Now, perform the push operations
            for element in &args[1..] {
                if command == "LPUSH" {
                    list.insert(0, element.to_string());
                } else {
                    list.push(element.to_string());
                }
            }

            // Wake the client *after* data is pushed.
            if let Some(waiter) = client_to_wake {
                // The receiving BLPOP will handle popping the data.
                let _ = waiter.sender.send(());
            }

            let response = format!(":{}\r\n", list.len());
            let _ = stream.write_all(response.as_bytes()).await;
            let mut replicas = state.replicas.lock().await;
            for replica in replicas.iter_mut() {
                let mut command_with_args = if command == "LPUSH" {
                    vec!["LPUSH".to_string()]
                } else {
                    vec!["RPUSH".to_string()]
                };
                command_with_args.extend_from_slice(args);
                let response = protocol::serialize_resp_array(&command_with_args);
                replica.write_all(response.as_bytes()).await?;
            }
            return Ok(());
        } else {
            stream.write_all(type_err.as_bytes()).await
        }
    } else {
        stream
            .write_all(b"-ERR wrong number of arguments for command\r\n")
            .await
    }
}

pub async fn handle_lrange<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    let null = "$-1\r\n";
    let empty_arr = "*0\r\n";
    if let (Some(key), Some(start_ind), Some(end_ind)) = (args.get(0), args.get(1), args.get(2)) {
        let map = state.db.lock().await;
        if let Some(entry) = map.get(key) {
            match &entry.value {
                DataStoreValue::List(val) => {
                    let mut start = match start_ind.parse::<i32>() {
                        Ok(s) => s,
                        Err(_) => {
                            return stream
                                .write_all(b"-ERR value is not an integer or out of range\r\n")
                                .await;
                        }
                    };

                    let mut end = match end_ind.parse::<i32>() {
                        Ok(e) => e,
                        Err(_) => {
                            return stream
                                .write_all(b"-ERR value is not an integer or out of range\r\n")
                                .await;
                        }
                    };

                    let n = val.len() as i32;

                    if start < 0 {
                        start = n + start;
                        start = max(start, 0);
                    }

                    if end < 0 {
                        end = n + end;
                        start = max(start, 0);
                    }

                    end = min(end, n - 1);
                    if start > end || start >= n {
                        return stream.write_all(empty_arr.as_bytes()).await;
                    }

                    let mut response = format!("*{}\r\n", end - start + 1);
                    for i in start..=end {
                        write!(&mut response, "${}\r\n", val[i as usize].len()).unwrap();
                        write!(&mut response, "{}\r\n", val[i as usize]).unwrap();
                    }

                    stream.write_all(response.as_bytes()).await
                }

                _ => stream.write_all(null.as_bytes()).await,
            }
        } else {
            stream.write_all(empty_arr.as_bytes()).await
        }
    } else {
        stream
            .write_all(b"-ERR wrong number of arguments for 'lrange' command\r\n")
            .await
    }
}

pub async fn handle_llen<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    if let Some(key) = args.get(0) {
        let map = state.db.lock().await;
        if let Some(entry) = map.get(key) {
            match &entry.value {
                DataStoreValue::List(val) => {
                    let response = format!(":{}\r\n", val.len());
                    stream.write_all(response.as_bytes()).await
                }

                _ => stream.write_all(b":0\r\n").await,
            }
        } else {
            stream.write_all(b":0\r\n").await
        }
    } else {
        stream
            .write_all(b"-ERR wrong number of arguments for 'llen' command\r\n")
            .await
    }
}

pub async fn handle_lpop<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    let null = "$-1\r\n";
    let type_err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    if let Some(key) = args.get(0) {
        let mut map = state.db.lock().await;
        if let Some(entry) = map.get_mut(key) {
            let _ = match &mut entry.value {
                DataStoreValue::List(val) => {
                    if let Some(num_of_ele) = args.get(1) {
                        let num_of_ele = num_of_ele.parse::<u32>().unwrap();
                        let mut response = format!("*{}\r\n", num_of_ele);
                        for _ in 0..num_of_ele {
                            let ele = val.remove(0);
                            write!(&mut response, "${}\r\n{}\r\n", ele.len(), ele).unwrap();
                        }
                        stream.write_all(response.as_bytes()).await
                    } else {
                        let ele = val.remove(0);
                        let response = format!("${}\r\n{}\r\n", ele.len(), ele);
                        stream.write_all(response.as_bytes()).await
                    }
                }
                _ => stream.write_all(type_err.as_bytes()).await,
            };
            let mut replicas = state.replicas.lock().await;
            for replica in replicas.iter_mut() {
                let mut command_with_args = vec!["LPOP".to_string()];
                command_with_args.extend_from_slice(args);
                let response = protocol::serialize_resp_array(&command_with_args);
                replica.write_all(response.as_bytes()).await?;
            }
            return Ok(());
        } else {
            stream.write_all(null.as_bytes()).await
        }
    } else {
        stream
            .write_all(b"-ERR wrong number of arguments for 'lpop' command\r\n")
            .await
    }
}

pub async fn handle_blpop<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    let null = "$-1\r\n";
    if args.len() < 2 {
        return stream
            .write_all(b"-ERR wrong number of arguments for 'blpop' command\r\n")
            .await;
    }

    let timeout_secs = match args.last().unwrap().parse::<f32>() {
        Ok(t) => t,
        Err(_) => {
            return stream
                .write_all(b"-ERR value is not an integer or out of range\r\n")
                .await;
        }
    };

    for key in &args[0..(args.len() - 1)] {
        let mut db_map = state.db.lock().await;
        if let Some(entry) = db_map.get_mut(key) {
            if let DataStoreValue::List(val) = &mut entry.value {
                if !val.is_empty() {
                    let ele = val.remove(0);
                    let response = format!(
                        "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        key.len(),
                        key,
                        ele.len(),
                        ele
                    );
                    return stream.write_all(response.as_bytes()).await; // Early return, no blocking needed
                }
            }
        }
        // --- IMPORTANT: Drop the lock before waiting ---
        drop(db_map);

        // --- Step 2: If no data, prepare to block ---
        let (tx, rx) = oneshot::channel::<()>(); // We only need a signal, not data
        let blocked_id = nanoid!();
        {
            // Lock, modify, and quickly unlock the blocked clients map
            let mut blocked_map = state.blocked_clients.lock().await;
            blocked_map
                .entry(key.to_string())
                .or_default()
                .push_back(BlockedSender {
                    id: blocked_id.clone(),
                    sender: tx,
                });
        }

        // --- Step 3: Wait for the signal (or timeout) ---
        let wait_result = if timeout_secs == 0.0 {
            rx.await.map_err(|_| "channel closed")
        } else {
            match timeout(Duration::from_secs_f32(timeout_secs), rx).await {
                Ok(Ok(_)) => Ok(()),                 // Signal received
                Ok(Err(_)) => Err("channel closed"), // Sender was dropped
                Err(_) => Err("timeout"),            // Timeout elapsed
            }
        };

        // --- Step 4: Handle the result after waking up ---
        if wait_result.is_ok() {
            // We were woken up by a push command.
            // The data is now guaranteed to be in the list.
            let mut db_map = state.db.lock().await; // Re-acquire the lock
            if let Some(entry) = db_map.get_mut(key) {
                if let DataStoreValue::List(val) = &mut entry.value {
                    if !val.is_empty() {
                        let ele = val.remove(0);
                        let response = format!(
                            "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                            key.len(),
                            key,
                            ele.len(),
                            ele
                        );
                        stream.write_all(response.as_bytes()).await?;
                    } else {
                        // This case is unlikely if woken up correctly, but handle it defensively.
                        stream.write_all(null.as_bytes()).await?;
                    }
                }
            }
        } else {
            // We timed out or the channel was closed.
            // Clean up the waiting client entry.
            let mut blocked_map = state.blocked_clients.lock().await;
            if let Some(queue) = blocked_map.get_mut(key) {
                if let Some(pos) = queue.iter().position(|bs| bs.id == blocked_id) {
                    queue.remove(pos);
                }
                if queue.is_empty() {
                    blocked_map.remove(key);
                }
            }
            stream.write_all(null.as_bytes()).await?;
        }
    }
    let mut replicas = state.replicas.lock().await;
    for replica in replicas.iter_mut() {
        let mut command_with_args = vec!["BLPOP".to_string()];
        command_with_args.extend_from_slice(args);
        let response = protocol::serialize_resp_array(&command_with_args);
        replica.write_all(response.as_bytes()).await?;
    }
    Ok(())
}
