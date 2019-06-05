use crate::service::config::{get_topology_config, TopologyConfig};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "Package", about = "Topology package cli")]
pub struct PackageOpt {
    #[structopt(short = "h", long = "host", default_value = "0.0.0.0")]
    /// Topology host
    pub host: String,

    #[structopt(short = "p", long = "port", default_value = "8765")]
    /// Topology port
    pub port: String,

    #[structopt(
        short = "d",
        long = "db_uri",
        default_value = "redis://127.0.0.1:6379/0"
    )]
    /// Tempest redis db uri for coordinating services
    pub db_uri: String,

    #[structopt(subcommand)]
    pub cmd: PackageCmd,
}

impl PackageOpt {
    pub fn topology_id(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    // all cmds could possibly have a config option
    pub fn get_config(&self) -> Result<Option<TopologyConfig>, config::ConfigError> {
        let cfg = match &self.cmd {
            PackageCmd::Standalone(ref opt) => &opt.config,
            PackageCmd::Topology(ref opt) => &opt.config,
            PackageCmd::Task(ref opt) => &opt.config,
        };
        match &cfg {
            Some(ConfigOpt::Config { path }) => {
                if let Some(path) = path {
                    match get_topology_config(path) {
                        Ok(cfg) => return Ok(Some(cfg)),
                        Err(err) => return Err(err),
                    }
                }
            }
            _ => {}
        }
        Ok(None)
    }
}

#[derive(Debug, Clone, StructOpt)]
pub enum PackageCmd {
    #[structopt(name = "standalone")]
    Standalone(StandaloneOpt),

    #[structopt(name = "topology")]
    Topology(TopologyOpt),

    #[structopt(name = "task")]
    Task(TaskOpt),
}

#[derive(Debug, Clone, StructOpt)]
pub struct StandaloneOpt {
    #[structopt(subcommand)]
    pub config: Option<ConfigOpt>,
}

#[derive(Debug, Clone, StructOpt)]
pub struct TopologyOpt {
    #[structopt(subcommand)]
    pub config: Option<ConfigOpt>,
}

#[derive(Default, Debug, Clone, StructOpt)]
pub struct TaskOpt {
    #[structopt(short = "n", long = "name")]
    /// Name of the task we want to run
    pub name: String,

    #[structopt(short = "w", long = "workers")]
    /// Number of workers to spin up for this task per node
    pub workers: Option<u64>,

    #[structopt(short = "i", long = "poll_interval")]
    /// Poll interval milliseconds
    pub poll_interval: Option<u64>,

    #[structopt(short = "c", long = "poll_count")]
    /// Number of messages to read per poll
    pub poll_count: Option<u16>,

    #[structopt(short = "b", long = "max_backoff")]
    /// Max milliseconds to back of if polling is empty
    pub max_backoff: Option<u64>,

    #[structopt(subcommand)]
    pub config: Option<ConfigOpt>,
}

#[derive(Debug, Clone, StructOpt)]
pub enum ConfigOpt {
    #[structopt(name = "config")]
    /// Topology.toml config to apply overrides to the
    /// subcommand for the standalone, topology or task cmd
    Config {
        #[structopt(short = "p", long = "path")]
        path: Option<String>,
        // TODO: implement toml string loading
        // #[structopt(short = "t", long = "toml")]
        // /// Parse this String as a toml table
        // toml: Option<String>,
    },
}
