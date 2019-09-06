use crate::metric::Metrics;
use std::sync::mpsc;

/// Main actor for aggregating flushed metrics
pub(crate) struct AggregateActor {
    /// Aggregate metrics
    metrics: Metrics,
}