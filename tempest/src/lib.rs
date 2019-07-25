#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(patterns_in_fns_without_body)]

pub use actix;

pub mod common;
pub mod metric;
pub mod pipeline;
pub mod service;
pub mod source;
pub mod task;
pub mod topology;

pub mod prelude {
    pub use crate::common::now_millis;
    pub use crate::pipeline::{Pipeline, Task};
    pub use crate::service::cli::{PackageCmd, PackageOpt};
    pub use crate::service::config::{get_topology_config, TaskConfig, TopologyConfig};
    pub use crate::service::task::TaskService;
    pub use crate::service::topology::TopologyService;
    pub use crate::source::{Msg, SourceMsg};
    pub use crate::task;
    pub use crate::topology::{Topology, TopologyBuilder, TopologyOptions};
    pub use actix;
}

pub use config;
pub use structopt::StructOpt;
