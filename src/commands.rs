use crate::storage::{BlockedClients, BlockedSender, DataStoreValue, Db, ValueEntry};
use nanoid::nanoid;
use std::cmp::{max, min};
use std::fmt::Write;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::timeout;

// Central function to process commands.
pub async fn handle_command(
    parsed: Vec<String>,
    db: &Db,
    stream: &mut TcpStream,
    blocked_clients: &BlockedClients,
) -> std::io::Result<()> {
    let command = parsed.get(0).unwrap().to_uppercase();
    let args = &parsed[1..];

    let ok = "+OK\r\n";
    let null = "$-1\r\n";
    let empty_arr = "*0\r\n";
    let type_err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

    match command.as_str() {
        "PING" => {
            stream.write_all(b"+PONG\r\n").await?;
        }
        "ECHO" => {
            if let Some(arg) = args.get(0) {
                let data = format!("${}\r\n{}\r\n", arg.len(), arg);
                stream.write_all(data.as_bytes()).await?;
            } else {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'echo' command\r\n")
                    .await?;
            }
        }
        "SET" => {
            if let (Some(key), Some(value)) = (args.get(0), args.get(1)) {
                let mut expires_at = None;
                if args.len() > 2 && args[2].to_uppercase() == "PX" {
                    if let Some(ms_str) = args.get(3) {
                        if let Ok(ms) = ms_str.parse::<u64>() {
                            expires_at = Some(Instant::now() + Duration::from_millis(ms));
                        }
                    }
                }
                let mut map = db.lock().await;
                let entry = ValueEntry {
                    value: DataStoreValue::String(value.to_string()),
                    expires_at,
                };
                map.insert(key.to_string(), entry);
                stream.write_all(ok.as_bytes()).await?;
            } else {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'set' command\r\n")
                    .await?;
            }
        }
        "GET" => {
            if let Some(key) = args.get(0) {
                let mut map = db.lock().await;
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
                            stream.write_all(response.as_bytes()).await?;
                        }

                        _ => {
                            stream.write_all(type_err.as_bytes()).await?;
                        }
                    }
                } else {
                    stream.write_all(null.as_bytes()).await?;
                }
            } else {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'get' command\r\n")
                    .await?;
            }
        }
        "LPUSH" | "RPUSH" => {
            if let (Some(key), Some(_value)) = (args.get(0), args.get(1)) {
                let mut db_map = db.lock().await;
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
                        let mut blocked_map = blocked_clients.lock().await;
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
                    stream.write_all(response.as_bytes()).await?;
                } else {
                    stream.write_all(type_err.as_bytes()).await?;
                }
            } else {
                stream
                    .write_all(b"-ERR wrong number of arguments for command\r\n")
                    .await?;
            }
        }
        "LRANGE" => {
            if let (Some(key), Some(start_ind), Some(end_ind)) =
                (args.get(0), args.get(1), args.get(2))
            {
                let map = db.lock().await;
                if let Some(entry) = map.get(key) {
                    match &entry.value {
                        DataStoreValue::List(val) => {
                            let mut start = match start_ind.parse::<i32>() {
                                Ok(s) => s,
                                Err(_) => {
                                    stream
                                        .write_all(
                                            b"-ERR value is not an integer or out of range\r\n",
                                        )
                                        .await?;
                                    return Ok(());
                                }
                            };

                            let mut end = match end_ind.parse::<i32>() {
                                Ok(e) => e,
                                Err(_) => {
                                    stream
                                        .write_all(
                                            b"-ERR value is not an integer or out of range\r\n",
                                        )
                                        .await?;
                                    return Ok(());
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
                                stream.write_all(empty_arr.as_bytes()).await?;
                                return Ok(());
                            }

                            let mut response = format!("*{}\r\n", end - start + 1);
                            for i in start..=end {
                                write!(&mut response, "${}\r\n", val[i as usize].len()).unwrap();
                                write!(&mut response, "{}\r\n", val[i as usize]).unwrap();
                            }

                            stream.write_all(response.as_bytes()).await?;
                        }

                        _ => {
                            stream.write_all(null.as_bytes()).await?;
                        }
                    }
                } else {
                    stream.write_all(empty_arr.as_bytes()).await?;
                }
            } else {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'lrange' command\r\n")
                    .await?;
            }
        }
        "LLEN" => {
            if let Some(key) = args.get(0) {
                let map = db.lock().await;
                if let Some(entry) = map.get(key) {
                    match &entry.value {
                        DataStoreValue::List(val) => {
                            let response = format!(":{}\r\n", val.len());
                            stream.write_all(response.as_bytes()).await?;
                        }

                        _ => {
                            stream.write_all(b":0\r\n").await?;
                        }
                    }
                } else {
                    stream.write_all(b":0\r\n").await?;
                }
            } else {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'llen' command\r\n")
                    .await?;
            }
        }
        "LPOP" => {
            if let Some(key) = args.get(0) {
                let mut map = db.lock().await;
                if let Some(entry) = map.get_mut(key) {
                    match &mut entry.value {
                        DataStoreValue::List(val) => {
                            if let Some(num_of_ele) = args.get(1) {
                                let num_of_ele = num_of_ele.parse::<u32>().unwrap();
                                let mut response = format!("*{}\r\n", num_of_ele);
                                for _ in 0..num_of_ele {
                                    let ele = val.remove(0);
                                    write!(&mut response, "${}\r\n{}\r\n", ele.len(), ele).unwrap();
                                }
                                stream.write_all(response.as_bytes()).await?;
                            } else {
                                let ele = val.remove(0);
                                let response = format!("${}\r\n{}\r\n", ele.len(), ele);
                                stream.write_all(response.as_bytes()).await?;
                            }
                        }
                        _ => {
                            stream.write_all(type_err.as_bytes()).await?;
                        }
                    }
                } else {
                    stream.write_all(null.as_bytes()).await?;
                }
            } else {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'lpop' command\r\n")
                    .await?;
            }
        }
        "BLPOP" => {
            if args.len() < 2 {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'blpop' command\r\n")
                    .await?;
                return Ok(());
            }

            let timeout_secs = match args.last().unwrap().parse::<f32>() {
                Ok(t) => t,
                Err(_) => {
                    stream
                        .write_all(b"-ERR value is not an integer or out of range\r\n")
                        .await?;
                    return Ok(());
                }
            };

            for key in &args[0..(args.len() - 1)] {
                let mut db_map = db.lock().await;
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
                            return Ok(()); // Early return, no blocking needed
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
                    let mut blocked_map = blocked_clients.lock().await;
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
                    let mut db_map = db.lock().await; // Re-acquire the lock
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
                    let mut blocked_map = blocked_clients.lock().await;
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
        }
        "TYPE" => {
            if args.len() != 1 {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'type' command\r\n")
                    .await?;
            }

            let map = db.lock().await;
            if let Some(entry) = map.get(&args[0]) {
                match &entry.value {
                    DataStoreValue::List(_) => {
                        stream.write_all(b"+list\r\n").await?;
                    }

                    DataStoreValue::String(_) => {
                        stream.write_all(b"+string\r\n").await?;
                    }
                }
            } else {
                stream.write_all(b"+none\r\n").await?;
            }
        }
        _ => {
            let err_msg = format!(
                "-ERR unknown command `{}`, with args beginning with: {:?}\r\n",
                command,
                args.get(0)
            );
            stream.write_all(err_msg.as_bytes()).await?;
        }
    }
    Ok(())
}
