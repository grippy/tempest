#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

pub mod common;
pub mod pipeline;
pub mod service;
pub mod source;
pub mod task;
pub mod topology;

pub mod prelude {
    pub use crate::common::{now_millis, read_config};
    pub use crate::pipeline::{Pipeline, Task, TaskOptions};
    pub use crate::service::task::TaskService;
    pub use crate::service::topology::TopologyService;
    pub use crate::source::{Msg, SourceMsg};
    pub use crate::task;
    pub use crate::topology::{Topology, TopologyBuilder, TopologyOptions};
}