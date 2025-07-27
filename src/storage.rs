use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, oneshot};

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

pub type Db = Arc<Mutex<HashMap<String, ValueEntry>>>;
pub type BlockedClients = Arc<Mutex<HashMap<String, VecDeque<BlockedSender>>>>;
