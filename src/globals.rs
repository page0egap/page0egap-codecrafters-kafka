use std::sync::{Arc, OnceLock, RwLock};
use crate::records::RecordBatch;

// 定义全局变量，使用标准库的OnceLock
pub static RECORD_BATCHES: OnceLock<Arc<RwLock<Vec<RecordBatch>>>> = OnceLock::new();