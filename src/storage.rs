use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::TcpStream;
use std::time::Instant;
use tokio::sync::{Mutex, oneshot, broadcast};

pub enum DataStoreValue {
    String(String),
    List(Vec<String>),
    Stream(Stream)
}

pub struct BlockedSender {
    pub id: String,
    pub sender: oneshot::Sender<()>
}

pub struct ValueEntry {
    pub value: DataStoreValue,
    pub expires_at: Option<Instant>,
}

pub struct Stream {
    pub entries: BTreeMap<String, HashMap<String, String>>,
    pub last_id: String,
}

pub struct ReplicaInfo {
    pub stream: TcpStream,
    pub offset: u64,
}

pub struct AppState {
    pub db: Db,
    pub blocked_clients: BlockedClients,
    pub stream_notifier: broadcast::Sender<()>,
    pub replica_of: Option<String>,
    pub master_replication_id: String,
    pub master_replication_offset: Mutex<u64>,
    pub replicas: Mutex<Vec<ReplicaInfo>>,
    pub slave_replication_offset: Mutex<u64>,
}

pub struct TransactionState {
    pub in_transaction: bool,
    pub queued_commands: Vec<Vec<String>>,
}


pub type Db = Mutex<HashMap<String, ValueEntry>>;
pub type BlockedClients = Mutex<HashMap<String, VecDeque<BlockedSender>>>;
