use crate::metric::Metrics;
use std::sync::mpsc;

pub struct AggregateActor {
    // metrics
    metrics: Metrics,
}