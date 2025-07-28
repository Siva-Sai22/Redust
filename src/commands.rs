use crate::storage::{
    AppState, BlockedSender, DataStoreValue, Db, Stream, TransactionState, ValueEntry,
};
use nanoid::nanoid;
use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::timeout;

// Central function to process commands.
pub async fn handle_command(
    parsed: Vec<String>,
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    transation_state: &mut TransactionState,
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
                let mut map = state.db.lock().await;
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
                let map = state.db.lock().await;
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
                let map = state.db.lock().await;
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
                let mut map = state.db.lock().await;
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
        }
        "TYPE" => {
            if args.len() != 1 {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'type' command\r\n")
                    .await?;
            }

            let map = state.db.lock().await;
            if let Some(entry) = map.get(&args[0]) {
                match &entry.value {
                    DataStoreValue::List(_) => {
                        stream.write_all(b"+list\r\n").await?;
                    }

                    DataStoreValue::String(_) => {
                        stream.write_all(b"+string\r\n").await?;
                    }

                    DataStoreValue::Stream(_) => {
                        stream.write_all(b"+stream\r\n").await?;
                    }
                }
            } else {
                stream.write_all(b"+none\r\n").await?;
            }
        }
        "XADD" => {
            if args.len() < 2 {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'stream' command\r\n")
                    .await?;
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
                        stream
                            .write_all(b"-ERR wrong number of arguments for 'xadd' command\r\n")
                            .await?;
                        return Ok(());
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
                        stream
                            .write_all(
                                b"-ERR Invalid stream ID specified as stream command argument\r\n",
                            )
                            .await?;
                        return Ok(());
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
                    stream
                        .write_all(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                        .await?;
                    return Ok(());
                }

                if cur_timestamp < last_timestamp
                    || (cur_timestamp == last_timestamp && cur_seq <= last_seq)
                {
                    stream.write_all(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n").await?;
                    return Ok(());
                }

                let calc_id = format!("{}-{}", cur_timestamp, cur_seq);

                btreemap.entries.insert(calc_id.clone(), fields);
                btreemap.last_id = calc_id.clone();

                let response = format!("${}\r\n{}\r\n", calc_id.len(), calc_id);
                stream.write_all(response.as_bytes()).await?;

                let _ = state.stream_notifier.send(());
            }
        }
        "XRANGE" => {
            if args.len() < 3 {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'xrange' command\r\n")
                    .await?;
                return Ok(());
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
                    stream.write_all(response.as_bytes()).await?;
                } else {
                    stream.write_all(type_err.as_bytes()).await?;
                }
            } else {
                stream.write_all(null.as_bytes()).await?;
            }
        }
        "XREAD" => {
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
            let mut final_results =
                check_for_data(&state.db, &mod_args, no_of_keys, start_idx).await;

            // 2. Blocking Path: If no data and BLOCK was specified.
            if final_results.is_none() && is_blocking {
                let mut rx = state.stream_notifier.subscribe();

                if timeout_ms > 0 {
                    // Timed block
                    if let Ok(Ok(_)) = timeout(Duration::from_millis(timeout_ms), rx.recv()).await {
                        // Woken by a notification, check again.
                        final_results =
                            check_for_data(&state.db, &mod_args, no_of_keys, start_idx).await;
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
                            write!(&mut response, "${}\r\n{}\r\n", field_key.len(), field_key)
                                .unwrap();
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
                stream.write_all(response.as_bytes()).await?;
            } else {
                // No results found (either non-blocking or timed out).
                stream.write_all(null.as_bytes()).await?;
            }
        }
        "INCR" => {
            if let Some(key) = args.get(0) {
                let mut map = state.db.lock().await;
                if let Some(entry) = map.get_mut(key) {
                    match &mut entry.value {
                        DataStoreValue::String(val) => {
                            let prev = match val.parse::<i64>() {
                                Ok(t) => t,
                                _ => {
                                    stream
                                        .write_all(
                                            b"-ERR value is not an integer or out of range\r\n",
                                        )
                                        .await?;
                                    return Ok(());
                                }
                            };
                            *val = (prev + 1).to_string();
                            stream.write_all(format!(":{}\r\n", val).as_bytes()).await?;
                        }
                        _ => {
                            stream
                                .write_all(b"-ERR value is not an integer or out of range\r\n")
                                .await?;
                            return Ok(());
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
                    stream.write_all(":1\r\n".as_bytes()).await?;
                }
            } else {
                stream
                    .write_all(b"-ERR wrong number of arguments for 'incr' command\r\n")
                    .await?;
            }
        }
        "MULTI" => {
            transation_state.queued_commands.clear();
            transation_state.in_transaction = true;
            stream.write_all(ok.as_bytes()).await?;
        }
        "EXEC" => {
            if !transation_state.in_transaction {
                stream.write_all(b"-ERR EXEC without MULTI\r\n").await?;
                return Ok(());
            }
            if transation_state.queued_commands.is_empty() {
                stream.write_all(empty_arr.as_bytes()).await?;
                return Ok(());
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
