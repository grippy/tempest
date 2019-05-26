use config;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_millis() -> usize {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_millis() as usize,
        Err(_) => 0,
    }
}

pub fn read_config(path: &str) -> Result<config::Config, config::ConfigError>  {
    let mut file = config::Config::default();
    match file.merge(config::File::with_name(path)) {
        Ok(file) => Ok(file.to_owned()),
        Err(err) => Err(err)
    }
}