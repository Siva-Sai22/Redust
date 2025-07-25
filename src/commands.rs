use crate::storage::{DataStoreValue, Db, ValueEntry};
use std::cmp::{max, min};
use std::fmt::Write;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

// Central function to process commands.
pub async fn handle_command(
    parsed: Vec<String>,
    db: &Db,
    stream: &mut TcpStream,
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
        "RPUSH" => {
            if let Some(key) = args.get(0) {
                let mut map = db.lock().await;
                let entry = map.entry(key.to_string()).or_insert(ValueEntry {
                    value: DataStoreValue::List(Vec::new()),
                    expires_at: None,
                });
                match &mut entry.value {
                    DataStoreValue::List(val) => {
                        for i in 0..(args.len() - 1) {
                            val.push(args.get(i + 1).unwrap().to_string());
                        }
                        let response = format!(":{}\r\n", val.len());
                        stream.write_all(response.as_bytes()).await?;
                    }

                    _ => {
                        stream.write_all(type_err.as_bytes()).await?;
                    }
                }
            } else {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'rpush' command\r\n")
                    .await?;
            }
        }
        "LPUSH" => {
            if let Some(key) = args.get(0) {
                let mut map = db.lock().await;
                let entry = map.entry(key.to_string()).or_insert(ValueEntry {
                    value: DataStoreValue::List(Vec::new()),
                    expires_at: None,
                });
                match &mut entry.value {
                    DataStoreValue::List(val) => {
                        for i in 0..(args.len() - 1) {
                            val.insert(0, args.get(i + 1).unwrap().to_string());
                        }
                        let response = format!(":{}\r\n", val.len());
                        stream.write_all(response.as_bytes()).await?;
                    }

                    _ => {
                        stream.write_all(type_err.as_bytes()).await?;
                    }
                }
            } else {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'lpush' command\r\n")
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
