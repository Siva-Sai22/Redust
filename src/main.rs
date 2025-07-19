use core::str;
use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async {
            handle_stream(socket).await;
        });
    }
}

async fn handle_stream(mut stream: TcpStream) {
    println!("accepted new connection");

    let mut buf = [0; 512];

    loop {
        let n = match stream.read(&mut buf).await {
            Ok(n) => n,
            Err(e) => {
                eprintln!("failed to read from socket; err = {:?}", e);
                return;
            }
        };

        if n != 0 {
            let received = str::from_utf8(&buf[..n]).unwrap();
            let parsed = parse_resp(received).unwrap();
            if parsed.len() >= 2 && parsed[0] == "ECHO" {
                let data = format!("${}\r\n{}\r\n", parsed[1].len(), parsed[1]);
                stream.write(data.as_bytes()).await.unwrap();
            } else {
                let data = "+PONG\r\n";
                stream.write(data.as_bytes()).await.unwrap();
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
