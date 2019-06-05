use config;
use serde_derive::Deserialize;
use std::path::PathBuf;

fn read_config(path: &str) -> Result<config::Config, config::ConfigError> {
    let mut file = config::Config::default();
    match file.merge(config::File::with_name(path)) {
        Ok(file) => Ok(file.to_owned()),
        Err(err) => Err(err),
    }
}

pub fn get_topology_config(path: &str) -> Result<TopologyConfig, config::ConfigError> {
    match read_config(path) {
        Ok(config) => {
            let topology: TopologyConfig = config.try_into()?;
            Ok(topology)
        }
        Err(err) => Err(err),
    }
}

#[derive(Debug, Deserialize)]
pub struct TopologyConfig {
    pub name: String,
    pub path: String,
    pub host: Option<String>,
    pub port: Option<String>,
    pub db_uri: Option<String>,
    pub task: Vec<TaskConfig>,
    pub source: Option<SourceConfig>,
}

#[derive(Debug, Deserialize)]
pub struct TaskConfig {
    pub name: String,
    pub path: String,
    pub workers: Option<u64>,
    pub poll_interval: Option<u64>,
    pub poll_count: Option<u16>,
    pub max_backoff: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    pub config: config::Value,
}
