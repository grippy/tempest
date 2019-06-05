use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_millis() -> usize {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_millis() as usize,
        Err(_) => 0,
    }
}

pub mod logger {
    pub use log::{debug, error, info, trace, warn};

}
