use crate::protocol;
use crate::storage::{AppState, DataStoreValue, Db, Stream, ValueEntry};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

pub async fn handle_type<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    if args.len() != 1 {
        return stream
            .write_all(b"-ERR wrong number of arguments for 'type' command\r\n")
            .await;
    }

    let map = state.db.lock().await;
    if let Some(entry) = map.get(&args[0]) {
        match &entry.value {
            DataStoreValue::List(_) => stream.write_all(b"+list\r\n").await,

            DataStoreValue::String(_) => stream.write_all(b"+string\r\n").await,

            DataStoreValue::Stream(_) => stream.write_all(b"+stream\r\n").await,
        }
    } else {
        stream.write_all(b"+none\r\n").await
    }
}

pub async fn handle_xadd<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    if args.len() < 2 {
        return stream
            .write_all(b"-ERR wrong number of arguments for 'stream' command\r\n")
            .await;
    }
    let key = args[0].to_string();
    let id = args[1].to_string();
    let mut map = state.db.lock().await;
    let entry = map.entry(key.to_string()).or_insert(ValueEntry {
        value: DataStoreValue::Stream(Stream {
            entries: BTreeMap::new(),
            last_id: "0-0".to_string(),
        }),
        expires_at: None,
    });

    if let DataStoreValue::Stream(btreemap) = &mut entry.value {
        let mut fields = HashMap::new();
        for i in (2..args.len()).step_by(2) {
            if i + 1 < args.len() {
                fields.insert(args[i].to_string(), args[i + 1].to_string());
            } else {
                return stream
                    .write_all(b"-ERR wrong number of arguments for 'xadd' command\r\n")
                    .await;
            }
        }

        let last_id = btreemap
            .last_id
            .split('-')
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        let last_timestamp = last_id[0].parse::<u128>().unwrap_or(0);
        let last_seq = last_id[1].parse::<u64>().unwrap_or(0);

        let cur_timestamp: u128;
        let cur_seq: u64;

        if id == "*" {
            cur_seq = 0;
            cur_timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
        } else {
            let cur_id = id
                .split('-')
                .map(|s| s.to_string())
                .collect::<Vec<String>>();

            if cur_id.len() != 2 {
                return stream
                    .write_all(b"-ERR Invalid stream ID specified as stream command argument\r\n")
                    .await;
            }

            cur_seq = if cur_id[1] == "*" {
                if cur_id[0] == last_id[0] {
                    last_seq + 1
                } else {
                    0
                }
            } else {
                cur_id[1].parse::<u64>().unwrap_or(0)
            };
            cur_timestamp = cur_id[0].parse::<u128>().unwrap_or(0);
        }

        if cur_seq == 0 && cur_timestamp == 0 {
            return stream
                .write_all(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                .await;
        }

        if cur_timestamp < last_timestamp
            || (cur_timestamp == last_timestamp && cur_seq <= last_seq)
        {
            return stream.write_all(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n").await;
        }

        let calc_id = format!("{}-{}", cur_timestamp, cur_seq);

        btreemap.entries.insert(calc_id.clone(), fields);
        btreemap.last_id = calc_id.clone();

        let response = format!("${}\r\n{}\r\n", calc_id.len(), calc_id);
        stream.write_all(response.as_bytes()).await?;

        let _ = state.stream_notifier.send(());
    }

    let mut command_with_args = vec!["XADD".to_string()];
    command_with_args.extend_from_slice(args);
    protocol::replicate_command(state, command_with_args).await?;
    Ok(())
}

pub async fn handle_xrange<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    let null = "$-1\r\n";
    let type_err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    if args.len() < 3 {
        return stream
            .write_all(b"-ERR wrong number of arguments for 'xrange' command\r\n")
            .await;
    }

    let key = args[0].to_string();
    let mut start = args[1].to_string();
    let mut end = args[2].to_string();

    if !start.contains("-") {
        start = format!("{}-0", start);
    }
    if !end.contains("-") && end != "+" {
        end = format!("{}-0", end);
    }

    let map = state.db.lock().await;
    if let Some(entry) = map.get(&key) {
        if let DataStoreValue::Stream(btreemap) = &entry.value {
            let mut response = String::new();
            let range = if end == "+" {
                btreemap.entries.range(start..)
            } else {
                btreemap.entries.range(start..=end)
            };
            response.push_str(&format!("*{}\r\n", range.clone().count()));
            for (id, fields) in range {
                response.push_str("*2\r\n");
                response.push_str(&format!("${}\r\n{}\r\n", id.len(), id));
                response.push_str(&format!("*{}\r\n", fields.len() * 2));
                for (field, value) in fields {
                    response.push_str(&format!("${}\r\n{}\r\n", field.len(), field));
                    response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                }
            }
            stream.write_all(response.as_bytes()).await
        } else {
            stream.write_all(type_err.as_bytes()).await
        }
    } else {
        stream.write_all(null.as_bytes()).await
    }
}

pub async fn handle_xread<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    state: &Arc<AppState>,
    args: &[String],
) -> std::io::Result<()> {
    let null = "$-1\r\n";
    let (no_of_keys, start_idx, timeout_ms) = if args[0].to_lowercase() == "block" {
        let timeout_ms = args[1].parse::<u64>().unwrap_or(0);
        (((args.len() - 3) / 2), 3, timeout_ms)
    } else {
        (((args.len() - 1) / 2), 1, 0)
    };

    let is_blocking = args[0].to_lowercase() == "block";

    async fn check_for_data(
        db: &Db,
        args: &[String],
        no_of_keys: usize,
        start_idx: usize,
    ) -> Option<Vec<(String, Vec<(String, HashMap<String, String>)>)>> {
        let mut results = Vec::new();
        let db_map = db.lock().await;

        for i in 0..no_of_keys {
            let key = &args[i + start_idx];
            let id = args[args.len() - no_of_keys + i].to_string();

            if let Some(entry) = db_map.get(key) {
                if let DataStoreValue::Stream(stream_data) = &entry.value {
                    let entries: Vec<_> = stream_data
                        .entries
                        .range((Excluded(id.to_string()), Unbounded))
                        .map(|(id, fields)| (id.clone(), fields.clone()))
                        .collect();

                    if !entries.is_empty() {
                        results.push((key.to_string(), entries));
                    }
                }
            }
        }

        if results.is_empty() {
            None
        } else {
            Some(results)
        }
    }

    let mut mod_args = Vec::<String>::new();
    for i in 0..args.len() {
        mod_args.push(args[i].to_string());
    }
    for i in 0..no_of_keys {
        let key = args[i + start_idx].to_string();
        let mut id = args[args.len() - no_of_keys + i].to_string();
        let db_map = state.db.lock().await;
        if let Some(entry) = db_map.get(&key) {
            if let DataStoreValue::Stream(stream_data) = &entry.value {
                if id == "$" {
                    id = stream_data.last_id.to_string();
                }
            }
        }
        mod_args[args.len() - no_of_keys + i] = id;
    }

    // 1. Fast Path: Check for data immediately.
    let mut final_results = check_for_data(&state.db, &mod_args, no_of_keys, start_idx).await;

    // 2. Blocking Path: If no data and BLOCK was specified.
    if final_results.is_none() && is_blocking {
        let mut rx = state.stream_notifier.subscribe();

        if timeout_ms > 0 {
            // Timed block
            if let Ok(Ok(_)) = timeout(Duration::from_millis(timeout_ms), rx.recv()).await {
                // Woken by a notification, check again.
                final_results = check_for_data(&state.db, &mod_args, no_of_keys, start_idx).await;
            }
        } else {
            // Indefinite block (timeout is 0)
            loop {
                if rx.recv().await.is_ok() {
                    // Woken by a notification, check for data.
                    final_results =
                        check_for_data(&state.db, &mod_args, no_of_keys, start_idx).await;
                    if final_results.is_some() {
                        // Data found for our keys, break the wait loop.
                        break;
                    }
                // Spurious wakeup (data was for other keys), loop and wait again.
                } else {
                    // Channel closed, server is likely shutting down.
                    break;
                }
            }
        }
    }

    // 3. Format and send the response.
    if let Some(results) = final_results {
        let mut response = String::new();
        response.push_str(&format!("*{}\r\n", results.len()));
        for (key, entries) in results {
            response.push_str("*2\r\n");
            response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
            response.push_str(&format!("*{}\r\n", entries.len()));
            for (entry_id, fields) in entries {
                response.push_str("*2\r\n");
                response.push_str(&format!("${}\r\n{}\r\n", entry_id.len(), entry_id));
                response.push_str(&format!("*{}\r\n", fields.len() * 2));
                for (field_key, field_value) in fields {
                    write!(&mut response, "${}\r\n{}\r\n", field_key.len(), field_key).unwrap();
                    write!(
                        &mut response,
                        "${}\r\n{}\r\n",
                        field_value.len(),
                        field_value
                    )
                    .unwrap();
                }
            }
        }
        stream.write_all(response.as_bytes()).await
    } else {
        // No results found (either non-blocking or timed out).
        stream.write_all(null.as_bytes()).await
    }
}
