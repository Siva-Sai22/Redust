# Redis Clone in Rust

This project is a toy Redis server implemented in Rust. It is an asynchronous, multi-threaded server built using Tokio, capable of handling multiple concurrent clients and a subset of Redis commands.

## Features

The server supports a variety of Redis commands across different data types.

### General Commands
- `PING`: Checks the server's availability.
- `ECHO`: Returns the provided string.
- `INFO`: Provides information about the server (e.g., role).

### String Commands
- `SET <key> <value> [PX <milliseconds>]`: Sets a string value for a key, with optional expiration.
- `GET <key>`: Retrieves the value of a key.
- `INCR <key>`: Increments the integer value of a key by one.

### List Commands
- `LPUSH <key> <element...>`: Prepends one or more elements to a list.
- `RPUSH <key> <element...>`: Appends one or more elements to a list.
- `LPOP <key> [count]`: Removes and returns the first element(s) of a list.
- `BLPOP <key...> <timeout>`: A blocking version of `LPOP`.
- `LRANGE <key> <start> <stop>`: Gets a range of elements from a list.
- `LLEN <key>`: Gets the length of a list.

### Stream Commands
- `TYPE <key>`: Returns the type of value stored at a key.
- `XADD <key> <ID> <field> <value>...`: Adds a new entry to a stream.
- `XRANGE <key> <start> <end>`: Returns a range of entries from a stream.
- `XREAD [BLOCK <milliseconds>] STREAMS <key...> <ID...>`: Reads from one or more streams, with an option to block.

### Transactions
- `MULTI`: Marks the start of a transaction block.
- `EXEC`: Executes all commands queued in a transaction.
- `DISCARD`: Flushes all commands queued in a transaction.

## Architecture
- **Asynchronous I/O**: Built on `tokio` for high-performance, non-blocking network I/O.
- **Concurrent**: Handles multiple client connections simultaneously, each in its own green thread (task).
- **In-Memory Storage**: Uses a thread-safe, in-memory `HashMap` to store data.
- **RESP Protocol**: Parses and responds using the Redis Serialization Protocol (RESP).

## How to Run

1.  **Prerequisites**: Ensure you have Rust and Cargo installed (e.g., `cargo 1.78.0`).
2.  **Build & Run**: Execute the provided shell script to compile and start the server.
    ```sh
    ./your_program.sh
    ```
    The server will start and listen on `127.0.0.1:6379` by default.
3.  **Interact with the server**: Use a Redis client like `redis-cli` to connect and send commands.
    ```sh
    redis-cli
    > PING
    PONG
    > SET name redis
    OK
    > GET name
    "redis"
    ```
