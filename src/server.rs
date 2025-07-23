use crate::commands;
use crate::protocol;
use crate::storage::{Db};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub async fn run(db: Db) -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("🚀 Server listening on 127.0.0.1:6379");
    
    loop {
        let (socket, addr) = listener.accept().await?;
        println!("Accepted new connection from: {}", addr);
        let db_clone = db.clone();
        tokio::spawn(async move {
            handle_stream(socket, db_clone).await;
        });
    }
}

async fn handle_stream(mut stream: TcpStream, db: Db) {
    let mut buf = [0; 1024];

    loop {
        let n = match stream.read(&mut buf).await {
            Ok(0) => return, // Connection closed
            Ok(n) => n,
            Err(e) => {
                eprintln!("failed to read from socket; err = {:?}", e);
                return;
            }
        };

        if let Ok(received) = std::str::from_utf8(&buf[..n]) {
            if let Ok(parsed) = protocol::parse_resp(received) {
                if let Err(e) = commands::handle_command(parsed, &db, &mut stream).await {
                    eprintln!("Error handling command: {}", e);
                    // Optionally, write an error back to the client
                    let _ = stream.write_all(b"-ERR server error\r\n").await;
                    return;
                }
            }
        }
    }
}