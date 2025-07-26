use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, oneshot};

pub enum DataStoreValue {
    String(String),
    List(Vec<String>),
}

pub struct BlockedSender {
    pub id: String,
    pub sender: oneshot::Sender<()>
}

pub struct ValueEntry {
    pub value: DataStoreValue,
    pub expires_at: Option<Instant>,
}

pub type Db = Arc<Mutex<HashMap<String, ValueEntry>>>;
pub type BlockedClients = Arc<Mutex<HashMap<String, VecDeque<BlockedSender>>>>;
