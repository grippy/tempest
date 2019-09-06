use std::time::{SystemTime, UNIX_EPOCH};

/// A common function for returning the current milliseconds
/// since epoch
pub fn now_millis() -> usize {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_millis() as usize,
        Err(_) => 0,
    }
}

/// Re-export logging functions
pub mod logger {
    pub use log::{debug, error, info, log, trace, warn};
}
