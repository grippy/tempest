pub mod common;
pub mod metric;
pub mod pipeline;
pub mod rt;
pub mod task;
pub mod topology;

pub(crate) mod service;

// Base imports for implementing Topology & Task's
pub mod prelude {
    pub use crate::common::{now_millis};
    pub use crate::metric::{self, MetricLogLevel, MetricTarget};
    pub use crate::pipeline::Pipeline;
    pub use crate::rt::{self, run};
    pub use crate::task;
    pub use crate::topology::{Topology, TopologyBuilder, TopologyFailurePolicy, TopologyOptions};

    // convenience so topology packages don't need to
    // define tempest-source as a dependency
    pub use tempest_source::prelude::*;
}
