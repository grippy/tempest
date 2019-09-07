//! # Tempest is a message processing framework
//! Inspired by Apache Storm and written in Rust.
//!
//! # Book
//! - Take a look at the [Tempest Book](https://github.com/grippy/tempest/tree/master/tempest-book/src).
//!   Still trying to determine best place to host the generated book output.
//!
//! # Documentation
//! - Docs are hosted on `http://docs.rs`
//!
//! # Examples
//! - Take a look at the [topology_example](https://github.com/grippy/tempest/tree/master/tempest/examples/topology-example)

/// Shared code
pub mod common;
/// Module for wiring up Counters, Gauges, and Timers to Statsd, Prometheus, Log, Files, etc.
pub mod metric;
/// Topology pipeline utilities
pub mod pipeline;
/// Topology runtime module
pub mod rt;
/// Module for working with Tasks
pub mod task;
/// Topology module and actors
pub mod topology;

pub(crate) mod service;

/// Base imports for implementing
/// and running topologies
pub mod prelude {

    pub use crate::common::now_millis;
    pub use crate::metric::{self, MetricLogLevel, MetricTarget};
    pub use crate::pipeline::Pipeline;
    pub use crate::rt::{self, run};
    pub use crate::task;
    pub use crate::topology::{Topology, TopologyBuilder, TopologyFailurePolicy, TopologyOptions};

    // Export tempest_source dependency
    // for `Source` implementations with `tempest`
    // dependencies that use metrics
    pub use tempest_source::prelude::*;
}
