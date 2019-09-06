use crate::common::logger::*;
use config;
use serde_derive::Deserialize;

/// Config is used to parse `Topology.toml` files

static TARGET_SERVICE_CONFIG: &'static str = "tempest::service::config";

/// Read a config from a file path
fn read_config(path: &str) -> Result<config::Config, config::ConfigError> {
    let mut file = config::Config::default();
    match file.merge(config::File::with_name(path)) {
        Ok(file) => Ok(file.to_owned()),
        Err(err) => Err(err),
    }
}

/// Try and parse a config file path into a `TopologyConfig`
pub fn get_topology_config(path: &str) -> Result<TopologyConfig, config::ConfigError> {
    match read_config(path) {
        Ok(config) => {
            let topology: TopologyConfig = config.try_into()?;
            debug!(
                target: TARGET_SERVICE_CONFIG,
                "Topology config: {:?}", &topology
            );
            Ok(topology)
        }
        Err(err) => Err(err),
    }
}

/// The TopologyConfig is used to deserialize the main Topology
/// section from a `Topology.toml` file.
#[derive(Debug, Deserialize)]
pub struct TopologyConfig {
    pub name: String,
    pub host: Option<String>,
    pub port: Option<String>,
    pub graceful_shutdown: Option<u64>,
    pub task: Vec<TaskConfig>,
    pub source: Option<SourceConfig>,
    pub metric: Option<MetricConfig>,
}

/// The TaskConfig is used to deserialize a `Task`
/// section (`[[task]]`) from a `Topology.toml` file.
#[derive(Debug, Deserialize)]
pub struct TaskConfig {
    pub name: String,
    pub workers: Option<u64>,
    pub poll_interval: Option<u64>,
    pub poll_count: Option<u16>,
    pub max_backoff: Option<u64>,
}

/// The SourceConfig is used to partially deserialize a `[source.config]`
/// section from a `Topology.toml` file.
///
/// The config is stored as a `config::Value` to generate Source configuration
/// options.
#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    pub config: config::Value,
}

/// The MetricConfig is used to deserialize a `[metric]`
/// section from a `Topology.toml` file.
#[derive(Debug, Deserialize)]
pub struct MetricConfig {
    pub flush_interval: Option<u64>,
    pub target: Vec<config::Value>,
}
