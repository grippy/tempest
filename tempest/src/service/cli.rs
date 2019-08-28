use crate::service::config::{get_topology_config, TopologyConfig};
use structopt::StructOpt;

/// PackageOpt provides and interface for
/// parsing command line arguments for topology package
#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "Tempest Package", about = "Topology cli options")]
pub(crate) struct PackageOpt {
    #[structopt(short = "h", long = "host", default_value = "0.0.0.0")]
    /// Topology host
    pub host: String,

    #[structopt(short = "p", long = "port", default_value = "8765")]
    /// Topology port
    pub port: String,

    #[structopt(long = "agent_host", default_value = "0.0.0.0")]
    /// Agent host
    pub agent_host: String,

    #[structopt(long = "agent_port", default_value = "7654")]
    /// Agent port
    pub agent_port: String,

    #[structopt(short = "g", long = "graceful_shutdown", default_value = "30000")]
    /// Graceful shutdown milliseconds
    pub graceful_shutdown: u64,

    #[structopt(subcommand)]
    pub cmd: PackageCmd,
}

impl PackageOpt {
    pub fn host_port(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    // all cmds could possibly have a config option
    pub fn get_config(&self) -> Result<Option<TopologyConfig>, config::ConfigError> {
        let cfg = match &self.cmd {
            PackageCmd::Task(ref opt) => &opt.config,
            PackageCmd::Topology(ref opt) => &opt.config,
            PackageCmd::Standalone(ref opt) => &opt.config,
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

/// Package sub-command option
///
#[derive(Debug, Clone, StructOpt)]
pub(crate) enum PackageCmd {
    /// Standalone option for running a topology
    /// and all tasks as a single, multi-threaded process
    #[structopt(name = "standalone")]
    Standalone(StandaloneOpt),

    /// Run a topology (source & pipeline) process
    #[structopt(name = "topology")]
    Topology(TopologyOpt),

    /// Run a task by name
    #[structopt(name = "task")]
    Task(TaskOpt),
}

/// Sub-command for running a Standalone process
#[derive(Debug, Clone, StructOpt)]
pub(crate) struct StandaloneOpt {
    #[structopt(subcommand)]
    /// Topology.toml config option sub-command
    pub config: Option<ConfigOpt>,
}

#[derive(Debug, Clone, StructOpt)]
pub(crate) struct TopologyOpt {
    #[structopt(subcommand)]
    /// Topology.toml config
    pub config: Option<ConfigOpt>,
}

#[derive(Default, Debug, Clone, StructOpt)]
pub(crate) struct TaskOpt {
    #[structopt(short = "n", long = "name")]
    /// Name of the topology task to run
    pub name: String,

    // Not currently implemented
    // #[structopt(short = "w", long = "workers")]
    // /// Number of workers to spin up for this task per node
    // pub workers: Option<u64>,
    #[structopt(short = "i", long = "poll_interval")]
    /// Poll interval milliseconds
    pub poll_interval: Option<u64>,

    #[structopt(short = "c", long = "poll_count")]
    /// Number of messages to read per poll
    pub poll_count: Option<u16>,

    #[structopt(short = "b", long = "max_backoff")]
    /// Max backoff wait in milliseconds
    pub max_backoff: Option<u64>,

    #[structopt(subcommand)]
    /// Topology.toml config
    pub config: Option<ConfigOpt>,
}

#[derive(Debug, Clone, StructOpt)]
pub(crate) enum ConfigOpt {
    #[structopt(name = "config")]
    /// Topology.toml config
    Config {
        #[structopt(short = "p", long = "path")]
        path: Option<String>,
        // TODO: implement toml string loading
        // #[structopt(short = "t", long = "toml")]
        // /// Parse this String as a toml table
        // toml: Option<String>,
    },
}

#[derive(Default, Debug, Clone, StructOpt)]
pub(crate) struct AgentOpt {
    #[structopt(short = "h", long = "host", default_value = "0.0.0.0")]
    /// Agent host
    pub host: String,

    #[structopt(short = "p", long = "port", default_value = "7654")]
    /// Agent port
    pub port: String,
}

impl AgentOpt {
    pub fn new(host: String, port: String) -> Self {
        AgentOpt { host, port }
    }

    pub fn host_port(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
