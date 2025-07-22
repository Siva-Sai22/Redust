use core::str;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

struct ValueEntry {
    value: String,
    expires_at: Option<Instant>,
}

type Db = Arc<Mutex<HashMap<String, ValueEntry>>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db: Db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let db_clone = db.clone();
        tokio::spawn(async move {
            handle_stream(socket, db_clone).await;
        });
    }
}

async fn handle_stream(mut stream: TcpStream, db: Db) {
    println!("accepted new connection");

    let mut buf = [0; 512];
    let pong = "+PONG\r\n";
    let ok = "+OK\r\n";
    let null = "$-1\r\n";

    loop {
        let n = match stream.read(&mut buf).await {
            Ok(n) => n,
            Err(e) => {
                eprintln!("failed to read from socket; err = {:?}", e);
                return;
            }
        };

        if n == 0 {
            continue;
        }

        let received = str::from_utf8(&buf[..n]).unwrap();
        let parsed = parse_resp(received).unwrap();

        match parsed[0].as_str() {
            "ECHO" => {
                if let Some(arg) = parsed.get(1) {
                    let data = format!("${}\r\n{}\r\n", arg.len(), arg);
                    stream.write_all(data.as_bytes()).await.unwrap();
                } else {
                    let err_msg = "-ERR wrong number of arguments for 'echo' command\r\n";
                    stream.write_all(err_msg.as_bytes()).await.unwrap();
                }
            }
            "PING" => {
                stream.write_all(pong.as_bytes()).await.unwrap();
            }
            "SET" => {
                if let (Some(key), Some(value)) = (parsed.get(1), parsed.get(2)) {
                    let mut expires_at = None;

                    if parsed.len() > 3 {
                        if let (Some(option), Some(ms_str)) = (parsed.get(3), parsed.get(4)) {
                            if option.to_uppercase() == "PX" {
                                if let Ok(ms) = ms_str.parse::<u64>() {
                                    expires_at = Some(Instant::now() + Duration::from_millis(ms));
                                }
                            }
                        }
                    }

                    let entry = ValueEntry {
                        value: value.to_string(),
                        expires_at,
                    };

                    let mut map = db.lock().await;
                    map.insert(key.to_string(), entry);
                    stream.write_all(ok.as_bytes()).await.unwrap();
                } else {
                    let err_msg = "-ERR wrong number of arguments for 'set' command\r\n";
                    stream.write_all(err_msg.as_bytes()).await.unwrap();
                }
            }
            "GET" => {
                if let Some(key) = parsed.get(1) {
                    let mut map = db.lock().await;
                    if let Some(entry) = map.get(key) {
                        if let Some(expiry) = entry.expires_at {
                            if Instant::now() > expiry {
                                map.remove(key);
                                stream.write_all(null.as_bytes()).await.unwrap();
                                continue;
                            }
                        }

                        let response = format!("${}\r\n{}\r\n", entry.value.len(), entry.value);
                        stream.write_all(response.as_bytes()).await.unwrap();
                    } else {
                        stream.write_all(null.as_bytes()).await.unwrap();
                    }
                } else {
                    let err_msg = "-ERR wrong number of arguments for 'get' command\r\n";
                    stream.write_all(err_msg.as_bytes()).await.unwrap();
                }
            }
            _ => {
                let err_msg = "-ERR unknown command\r\n";
                stream.write_all(err_msg.as_bytes()).await.unwrap();
            }
        }
    }
}

// *2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n
fn parse_resp(input: &str) -> Result<Vec<String>, &str> {
    let mut lines = input.split("\r\n").filter(|s| !s.is_empty());

    let array_header = lines.next().ok_or("Input is empty!")?;
    if !array_header.starts_with('*') {
        return Err("Expected an array ('*')");
    }

    let num_elements: usize = array_header[1..]
        .parse()
        .map_err(|_| "Invalid array length")?;

    let mut result = Vec::with_capacity(num_elements);
    for _ in 0..num_elements {
        let bulk_header = lines
            .next()
            .ok_or("Unexpected end of input while expecting bulk string header")?;
        if !bulk_header.starts_with('$') {
            return Err("Expected a bulk string ('$')");
        }
        // We could parse the length here for validation, but for simplicity, we'll skip it.
        // let _string_len: usize = bulk_header[1..].parse().map_err(|_| "Invalid bulk string length")?;
        let string_content = lines
            .next()
            .ok_or("Unexpected end of input while expecting string content")?;
        result.push(string_content.to_string());
    }

    Ok(result)
}
