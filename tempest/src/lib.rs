pub mod common;
pub mod metric;
pub mod pipeline;
pub mod rt;
pub mod source;
pub mod task;
pub mod topology;

pub(crate) mod service;

// Base imports for implementing Topology & Task's
pub mod prelude {
    pub use crate::metric::{self, MetricLogLevel, MetricTarget};
    pub use crate::pipeline::Pipeline;
    pub use crate::rt::{self, run};
    pub use crate::source::{Msg, SourceMsg};
    pub use crate::task;
    pub use crate::topology::{Topology, TopologyBuilder, TopologyFailurePolicy, TopologyOptions};
}

pub use actix;
pub use config;
