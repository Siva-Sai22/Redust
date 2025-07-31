use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
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

pub struct AppState {
    pub db: Db,
    pub blocked_clients: BlockedClients,
    pub stream_notifier: broadcast::Sender<()>,
    pub replica_of: Option<String>,
}

pub struct TransactionState {
    pub in_transaction: bool,
    pub queued_commands: Vec<Vec<String>>,
}


pub type Db = Arc<Mutex<HashMap<String, ValueEntry>>>;
pub type BlockedClients = Arc<Mutex<HashMap<String, VecDeque<BlockedSender>>>>;
