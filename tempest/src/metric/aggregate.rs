use crate::metric::Metrics;
use std::sync::mpsc;

pub(crate) struct AggregateActor {
    // metrics
    metrics: Metrics,
}