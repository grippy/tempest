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
use crate::metric::{Labels, MetricTarget, Metrics, Root, ROOT};
use console::{Console, ConsoleActor};
use file::{File, FileActor};
use prometheus::{Prometheus, PrometheusActor};
use statsd::{Statsd, StatsdActor};

pub type BackendResult<T> = Result<T, BackendError>;

#[derive(Debug)]
pub enum BackendErrorKind {
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
pub struct BackendError {
    kind: BackendErrorKind,
}

impl BackendError {
    pub fn new(kind: BackendErrorKind) -> Self {
        BackendError { kind: kind }
    }

    pub fn from_other(err: String) -> Self {
        BackendError::new(BackendErrorKind::Other(err))
    }

    pub fn from_io(err: std::io::Error) -> Self {
        BackendError::new(BackendErrorKind::Io(err))
    }

    pub fn from_addr() -> Self {
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
    // The root prefix
    pub root_prefix: String,

    // The root lablels
    pub root_labels: Labels,

    // The bucket to push
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
            let result = subscriber.1.do_send(Flush());
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

    fn start_file(&mut self, target: &MetricTarget, root: &Root) {
        match File::new(target.clone(), root) {
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
        let root = ROOT.lock().unwrap();
        for target in &root.targets {
            match target {
                MetricTarget::Console { prefix } => {
                    self.start_console(target);
                }
                MetricTarget::Log { level, prefix } => {
                    self.start_log(target);
                }
                MetricTarget::Prometheus { uri, prefix } => {
                    self.start_prometheus(target);
                }
                MetricTarget::Statsd { host, prefix } => {
                    self.start_statsd(target);
                }
                MetricTarget::File {
                    path,
                    clobber,
                    prefix,
                } => {
                    self.start_file(target, &root);
                }
                _ => {
                    warn!("Target not configured yet: {:?}", &target);
                }
            }
        }

        // Set probe_interval using root.flush_interval
        self.probe_interval = Duration::from_millis(root.flush_interval);
        debug!("Metrics probe_interval is {:?}", self.probe_interval);

        // Fire the probe
        self.probe(ctx);
    }
}

impl Handler<Msg> for MetricsBackendActor {
    type Result = ();

    fn handle(&mut self, msg: Msg, ctx: &mut Context<Self>) {
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
