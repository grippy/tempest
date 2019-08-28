mod console;
mod file;
mod log;
mod prelude;
mod prometheus;
mod statsd;

use actix::prelude::*;
use std::collections::HashMap;
use std::io::Error;
use std::net::AddrParseError;
use std::time::Duration;

use self::log::{Log, LogActor};
use crate::common::logger::*;
use crate::metric::{AggregateMetrics, Labels, MetricTarget, MetricValue, Metrics, Root};
use crate::service::agent_client::AgentClient;
use crate::service::cli::AgentOpt;
use crate::service::codec::AgentRequest;

use console::{Console, ConsoleActor};
use file::{File, FileActor};
use prometheus::{Prometheus, PrometheusActor};
use statsd::{Statsd, StatsdActor};

pub(crate) type BackendResult<T> = Result<T, BackendError>;

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum BackendErrorKind {
    // General std::io::Error
    Io(std::io::Error),
    // Error parsing client address
    AddrParse(&'static str),
    /// Error kind when client connection encounters an error
    Client(String),
    /// Error kind when we just need one
    Other(String),
}

#[derive(Debug)]
pub(crate) struct BackendError {
    kind: BackendErrorKind,
}

impl BackendError {
    pub(crate) fn new(kind: BackendErrorKind) -> Self {
        BackendError { kind: kind }
    }

    pub(crate) fn from_other(err: String) -> Self {
        BackendError::new(BackendErrorKind::Other(err))
    }

    pub(crate) fn from_io(err: std::io::Error) -> Self {
        BackendError::new(BackendErrorKind::Io(err))
    }

    pub(crate) fn from_addr() -> Self {
        BackendError::new(BackendErrorKind::AddrParse("Address parsing error"))
    }
}

impl From<AddrParseError> for BackendError {
    fn from(_: AddrParseError) -> BackendError {
        BackendError::from_addr()
    }
}

impl From<Error> for BackendError {
    fn from(err: Error) -> BackendError {
        BackendError::from_io(err)
    }
}

#[derive(Message, Clone)]
pub struct Msg {
    //  Root metrics prefix
    pub root_prefix: String,
    //  Root metrics labels
    pub root_labels: Labels,
    //  Interval metrics bucket
    pub metrics: Metrics,
}

/// Subscribe(actor_name, recipient<Flush>)
#[derive(Message)]
pub struct Subscribe(pub &'static str, pub Recipient<Flush>);

#[derive(Message)]
pub struct Flush();

/// Backend Actor
pub struct MetricsBackendActor {
    // Console backend actor
    consoles: Vec<Addr<ConsoleActor>>,
    // Log backend actor
    logs: Vec<Addr<LogActor>>,
    // Prometheus backend actor
    proms: Vec<Addr<PrometheusActor>>,
    // Statsd backend actor
    statsd: Vec<Addr<StatsdActor>>,
    // File backend actor
    files: Vec<Addr<FileActor>>,
    // subscribers: Vec<(&'static str, Recipient<Flush>)>,
    subscribers: Vec<Subscribe>,
    // Topology.toml metric.flush_interval
    probe_interval: Duration,
    // MetricsAggregateActor address
    pub aggregate: Option<Addr<MetricsAggregateActor>>,
}

impl Default for MetricsBackendActor {
    fn default() -> Self {
        Self {
            consoles: Vec::new(),
            logs: Vec::new(),
            proms: Vec::new(),
            statsd: Vec::new(),
            files: Vec::new(),
            subscribers: Vec::new(),
            probe_interval: Duration::from_millis(5000),
            aggregate: None,
        }
    }
}

impl MetricsBackendActor {
    pub fn subscribe(actor_name: &'static str, recipient: Recipient<Flush>) {
        let metric_backend = MetricsBackendActor::from_registry();
        if metric_backend.connected() {
            metric_backend.do_send(Subscribe(actor_name, recipient));
        }
    }

    // Probes
    fn probe(&mut self, ctx: &mut Context<Self>) {
        // send a message to each Actor subscribed to FlushMsg
        for subscriber in &self.subscribers {
            let _ = subscriber.1.do_send(Flush());
        }
        ctx.run_later(self.probe_interval, Self::probe);
    }

    fn start_console(&mut self, target: &MetricTarget) {
        info!("Starting metric console backend: {:?}", target);
        self.consoles.push(
            ConsoleActor {
                console: Console::new(target.clone()),
            }
            .start(),
        )
    }

    fn start_log(&mut self, target: &MetricTarget) {
        info!("Starting metric log backend: {:?}", target);
        self.logs.push(
            LogActor {
                log: Log::new(target.clone()),
            }
            .start(),
        )
    }

    fn start_prometheus(&mut self, target: &MetricTarget) {
        match Prometheus::new(target.clone()) {
            Ok(prom) => {
                info!("Starting metric prometheus backend: {:?}", target);
                self.proms
                    .push(PrometheusActor { prometheus: prom }.start())
            }
            Err(err) => {
                error!("Failed to start init prometheus backend: {:?}", err);
            }
        }
    }

    fn start_statsd(&mut self, target: &MetricTarget) {
        // Try parsing the configuration
        // host to make sure it's a valid address
        match Statsd::new(target.clone()) {
            Ok(statsd) => {
                info!("Starting metric statsd backend: {:?}", target);
                self.statsd.push(StatsdActor { statsd: statsd }.start())
            }
            Err(err) => {
                error!("Failed to start statsd client: {:?}", err);
            }
        }
    }

    fn start_file(&mut self, target: &MetricTarget, target_name: String) {
        match File::new(target.clone(), target_name) {
            Ok(file) => {
                info!("Starting metric file backend: {:?}", target);
                self.files.push(FileActor { file: file }.start())
            }
            Err(err) => {
                error!("Failed to start file backend: {:?}", err);
            }
        }
    }
}

impl Supervised for MetricsBackendActor {}
impl SystemService for MetricsBackendActor {}

impl Actor for MetricsBackendActor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Context<Self>) {
        // This actor should read the root configuration values
        for target in Root::get_targets().iter() {
            match target {
                MetricTarget::Console { .. } => {
                    self.start_console(target);
                }
                MetricTarget::Log { .. } => {
                    self.start_log(target);
                }
                MetricTarget::Prometheus { .. } => {
                    self.start_prometheus(target);
                }
                MetricTarget::Statsd { .. } => {
                    self.start_statsd(target);
                }
                MetricTarget::File { .. } => {
                    let target_name = Root::get_target_name();
                    self.start_file(target, target_name);
                }
            }
        }

        // Set probe_interval using root.flush_interval
        self.probe_interval = Duration::from_millis(Root::get_flush_interval());
        debug!("Metrics probe_interval is {:?}", self.probe_interval);

        // Fire the probe
        self.probe(ctx);
    }
}

impl Handler<Msg> for MetricsBackendActor {
    type Result = ();

    fn handle(&mut self, msg: Msg, _ctx: &mut Context<Self>) {
        // println!("MetricsBackendActor#Handle");
        for addr in &self.consoles {
            addr.do_send(msg.clone());
        }
        for addr in &self.logs {
            addr.do_send(msg.clone());
        }
        for addr in &self.proms {
            addr.do_send(msg.clone());
        }
        for addr in &self.statsd {
            addr.do_send(msg.clone());
        }
        for addr in &self.files {
            addr.do_send(msg.clone());
        }

        // Check if MetricsAggregateActor is connected
        // and send this msg to it
        // This isn't a `backend` which is
        // configurable as a MetricTarget
        if let Some(aggregate) = &self.aggregate {
            aggregate.do_send(msg.clone());
        };
    }
}

impl Handler<Subscribe> for MetricsBackendActor {
    type Result = ();

    fn handle(&mut self, msg: Subscribe, _: &mut Self::Context) {
        // This should try and replace the exiting subscription
        let mut replaced = false;
        for subscriber in &mut self.subscribers {
            if subscriber.0 == msg.0 {
                subscriber.0 = msg.0;
                replaced = true;
                debug!("Re-subscribed {} to metrics backend", &msg.0);
            }
        }

        if !replaced {
            debug!("Subscribing {} to metrics backend", &msg.0);
            self.subscribers.push(msg);
        }
    }
}

// Trait Write
#[allow(patterns_in_fns_without_body)]
pub trait Backend {
    // remove {} which stops mut warning here
    // and triggers patterns_in_fns_without_body warning
    // which is globally disallowed
    fn write(&mut self, mut msg: Msg);
}

/// Merge Labels into Map
pub fn merge_labels(map: &mut HashMap<String, String>, labels: Labels) {
    if labels.is_some() {
        for el in &labels.unwrap() {
            map.insert(el.0.clone(), el.1.clone());
        }
    }
}

/// MetricsAggregateActor is used to collect metrics
/// for all metrics sent to the MetricBackendActor
/// This is primarily used for testing topology io
/// and isn't technically a `backend` but more of
/// and plugin at runtime
///
/// This only aggregates Counters for now.
/// Gauges and Timers aren't currently supported!
///
pub struct MetricsAggregateActor {
    metrics: Metrics,
    agent_client: Addr<AgentClient>,
}

impl Default for MetricsAggregateActor {
    fn default() -> Self {
        Self {
            metrics: Metrics::default(),
            agent_client: AgentClient::connect(AgentOpt::default()),
        }
    }
}

impl MetricsAggregateActor {
    pub(crate) fn new(opts: AgentOpt) -> Self {
        Self {
            metrics: Metrics::default(),
            agent_client: AgentClient::connect(opts),
        }
    }
}

impl Supervised for MetricsAggregateActor {}
impl SystemService for MetricsAggregateActor {}

impl Actor for MetricsAggregateActor {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Context<Self>) {
        trace!("MetricsAggregateActor started");
    }
}

impl Handler<Msg> for MetricsAggregateActor {
    type Result = ();

    fn handle(&mut self, mut msg: Msg, _ctx: &mut Context<Self>) {
        let mut labels_map = HashMap::new();

        // apply root first
        merge_labels(&mut labels_map, msg.root_labels);

        // apply metrics labels (overriding root)
        merge_labels(&mut labels_map, msg.metrics.labels);

        let mut name = vec![];

        if let Some(topology_name) = labels_map.get("topology_name") {
            name.push(topology_name.clone());
        }

        if let Some(task_name) = labels_map.get("task_name") {
            name.push(task_name.clone());
        }

        if msg.metrics.names.len() > 0 {
            name.append(&mut msg.metrics.names.clone());
        }

        let sep = ".";
        let name = name.join(&sep);
        let mut has_counters = false;

        for (_, metric) in msg.metrics.bucket.map.iter_mut() {
            match &metric.value {
                MetricValue::Counter(counter) => {
                    let mut parts = vec![name.clone(), metric.names.join(&sep)];
                    if metric.labels.is_some() {
                        let mut metric_labels = HashMap::new();
                        merge_labels(&mut metric_labels, metric.labels.clone());
                        if let Some(status) = &metric_labels.get("status") {
                            parts.push(status.to_string());
                        };
                    }
                    let key = parts.join(&sep);
                    warn!("Aggregate counter: {:?} {:?}", &key, &counter);
                    self.metrics.counter(vec![&key[..]], counter.value);
                    has_counters = true;
                }
                _ => {}
            }
        }

        // Convert metrics to Aggregate metrics
        // and send them to the AgentServer
        if has_counters {
            let mut aggregate = AggregateMetrics::default();
            for (_, metric) in self.metrics.bucket.map.iter() {
                match &metric.value {
                    MetricValue::Counter(counter) => {
                        aggregate.insert(metric.names[0].clone(), counter.value.clone());
                    }
                    _ => {}
                }
            }
            // println!("{:?}", &aggregate);
            self.agent_client
                .do_send(AgentRequest::AggregateMetricsPut(aggregate));
        }
    }
}
