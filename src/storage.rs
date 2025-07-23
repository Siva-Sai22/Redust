use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

pub enum DataStoreValue {
    String(String),
    List(Vec<String>)
}

pub struct ValueEntry {
    pub value: DataStoreValue,
    pub expires_at: Option<Instant>,
}

pub type Db = Arc<Mutex<HashMap<String, ValueEntry>>>;