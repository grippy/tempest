use actix::dev::{MessageResponse, ResponseChannel};
use actix::prelude::*;
use config;
use histogram::Histogram;
use lazy_static::*;
use serde_derive::{Deserialize, Serialize};
use serde_json as json;

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::Duration;
use std::time::Instant;

use crate::common::logger::*;
use crate::service::config::MetricConfig;

pub mod backend;

static TARGET_NAME: &'static str = "";
static TARGET_METRIC: &'static str = "metric";
static TEMPEST_NAME: &'static str = "tempest";
static HISTOGRAM_BUCKET: &'static str = "bucket";
static HISTOGRAM_LE: &'static str = "le";

thread_local! {
    pub static ROOT: RefCell<Root> = RefCell::new(Root::default());
}

fn string_vec(v: Vec<&str>) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

// converts Vec<(&str, &str)> to Vec<(String, String)>
fn string_label_vec(v: Vec<(&str, &str)>) -> Vec<(String, String)> {
    v.iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

/// Configure the metric targets here
/// This will apply this named to prefix to all metric
/// routed through this target
pub fn parse_metrics_config(cfgs: MetricConfig) {
    // Flush Interval
    if let Some(value) = cfgs.flush_interval {
        Root::flush_interval(value);
    }

    let mut targets = vec![];
    for target in cfgs.target.iter() {
        match parse_config_value(target.to_owned()) {
            Some(t) => {
                trace!(target: TARGET_METRIC, "Configure metric {:?} target", &t);
                targets.push(t);
            }
            None => {}
        }
    }
    Root::targets(targets);
}

/// Parse the `Topology.toml` metric config value
fn parse_config_value(cfg: config::Value) -> Option<MetricTarget> {
    match cfg.try_into::<MetricTarget>() {
        Ok(target) => Some(target),
        Err(err) => {
            error!(target: TARGET_METRIC, "Error {:?}", err);
            None
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum MetricTarget {
    /// Stdout
    Console { prefix: Option<String> },
    /// Statsd configuration
    Statsd {
        host: String,
        prefix: Option<String>,
    },
    /// Prometheus? More like Brometheus!
    Prometheus { uri: String, prefix: Option<String> },
    /// Write to a file. Default is append which is clobber=false
    File {
        path: String,
        clobber: Option<bool>,
        prefix: Option<String>,
    },
    /// Write log metrics using this level (default=info)
    Log {
        level: Option<MetricLogLevel>,
        prefix: Option<String>,
    },
}

/// Metric Log Level
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricLogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl MetricLogLevel {
    fn to_level(&self) -> log::Level {
        match self {
            MetricLogLevel::Error => log::Level::Error,
            MetricLogLevel::Warn => log::Level::Warn,
            MetricLogLevel::Info => log::Level::Info,
            MetricLogLevel::Debug => log::Level::Debug,
            MetricLogLevel::Trace => log::Level::Trace,
        }
    }
}

#[derive(Clone)]
pub struct Root {
    // Root name (used for naming metric File and log targets)
    pub target_name: String,

    // Root prefix
    pub prefix: String,

    /// Root labels added to all metric labels
    /// before sending to the backend
    pub labels: Labels,

    // The list of configured backends for an instance
    pub targets: Vec<MetricTarget>,

    /// Flush interval in milliseonds
    /// Configureable with `Topology.toml` metric.flush_interval value
    pub flush_interval: u64,
}

impl Default for Root {
    fn default() -> Self {
        Root {
            target_name: TARGET_NAME.into(),
            prefix: TEMPEST_NAME.into(),
            labels: None,
            targets: vec![],
            flush_interval: 5000,
        }
    }
}

impl Root {
    // Static methods that wrap ROOT
    //

    pub fn labels(labels: Vec<Label>) {
        ROOT.with(|root| {
            for label in labels {
                Root::add_label(&mut *root.borrow_mut(), label.0, label.1);
            }
        });
    }

    pub fn label(key: String, value: String) {
        ROOT.with(|root| {
            Root::add_label(&mut *root.borrow_mut(), key, value);
        });
    }

    pub fn target_name(name: String) {
        ROOT.with(|root| {
            let mut r = root.borrow_mut();
            r.target_name = name;
        });
    }

    pub fn targets(targets: Vec<MetricTarget>) {
        ROOT.with(|root| {
            for target in targets {
                Root::add_target(&mut *root.borrow_mut(), target);
            }
        });
    }

    pub fn flush_interval(value: u64) {
        ROOT.with(|root| {
            let mut r = root.borrow_mut();
            r.flush_interval = value;
        });
    }

    fn get_prefix() -> String {
        ROOT.with(|root| root.borrow().prefix.clone())
    }

    fn get_labels() -> Labels {
        ROOT.with(|root| root.borrow().labels.clone())
    }

    fn get_target_name() -> String {
        ROOT.with(|root| root.borrow().target_name.clone())
    }

    fn get_targets() -> Vec<MetricTarget> {
        ROOT.with(|root| root.borrow().targets.clone())
    }

    pub fn get_targets_len() -> usize {
        ROOT.with(|root| root.borrow().targets.clone().len())
    }

    fn get_flush_interval() -> u64 {
        ROOT.with(|root| root.borrow().flush_interval)
    }

    // Instance methods

    fn add_label(&mut self, key: String, value: String) {
        if self.labels.is_none() {
            self.labels = Some(vec![]);
        }
        let mut labels = self.labels.clone().unwrap();
        labels.push((key, value));
        self.labels = Some(labels);
    }

    fn add_target(&mut self, target: MetricTarget) {
        self.targets.push(target);
    }
}

#[derive(Debug)]
pub struct TestMetrics {
    aggregate: AggregateMetrics,
}

impl TestMetrics {
    pub fn new(aggregate: AggregateMetrics) -> Self {
        Self { aggregate }
    }

    pub fn get(&self, key: &str) -> Option<&isize> {
        self.aggregate.get(key)
    }
}

#[derive(Serialize, Deserialize, Debug, Message, Default, Clone)]
pub struct AggregateMetrics {
    // TODO impl iterator so we can remove pub
    pub counters: BTreeMap<String, isize>,
}

static TMP_AGGREGATE_METRICS: &'static str = "/tmp/aggregate-metrics";

impl AggregateMetrics {
    pub fn read_tmp() -> Self {
        let aggregate_metrics = match fs::read(TMP_AGGREGATE_METRICS) {
            Ok(buf) => match json::from_slice::<AggregateMetrics>(&buf) {
                Ok(aggregate) => aggregate,
                Err(err) => AggregateMetrics::default(),
            },
            Err(err) => {
                error!(
                    "Error reading aggregate metrics file: {:?}",
                    TMP_AGGREGATE_METRICS
                );
                AggregateMetrics::default()
            }
        };

        let _ = fs::remove_file(TMP_AGGREGATE_METRICS);
        aggregate_metrics
    }

    pub fn write_tmp(&self) {
        let body = json::to_string(&self).unwrap();
        match fs::write(TMP_AGGREGATE_METRICS, body) {
            Err(err) => {
                error!("Error writing aggregate metrics: {:?}", &err);
            }
            _ => {}
        }
    }

    pub fn insert(&mut self, key: String, value: isize) {
        self.counters.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&isize> {
        self.counters.get(key)
    }
}

#[derive(Clone)]
pub struct Metrics {
    /// Names parts joined together with
    /// backend delimiters
    pub names: Vec<String>,

    /// Labels
    pub labels: Labels,

    /// Storage for our metric instances
    pub bucket: Bucket,
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics::new(vec![])
    }
}

impl Metrics {
    pub fn new(names: Vec<&str>) -> Self {
        Metrics {
            names: string_vec(names),
            labels: None,
            bucket: Bucket::default(),
        }
    }

    pub fn named(&mut self, names: Vec<&str>) -> Self {
        self.names = string_vec(names);
        self.clone()
    }

    // builder for ::new("").labels(vec![]);
    pub fn labels(&mut self, labels: Vec<(&str, &str)>) -> Self {
        for label in labels {
            self.add_label(label.0, label.1);
        }
        self.clone()
    }

    pub fn add_label(&mut self, key: &str, value: &str) {
        if self.labels.is_none() {
            self.labels = Some(vec![]);
        }
        let mut labels = self.labels.clone().unwrap();
        labels.push((key.to_string(), value.to_string()));
        self.labels = Some(labels);
    }

    pub fn flush(&mut self) {
        if self.bucket.map.len() == 0 {
            trace!("Empty metrics bucket. Skip flush: {:?}", &self.names);
            return;
        }

        // clone this bucket now
        let metrics = self.clone();

        // replace bucket with a fresh instance, returns bucket map
        self.bucket.clear();

        // create msg for backend actor
        let backend_metrics = backend::MetricsBackendActor::from_registry();
        if backend_metrics.connected() {
            backend_metrics.do_send(backend::Msg {
                root_prefix: Root::get_prefix(),
                root_labels: Root::get_labels(),
                metrics: metrics,
            });
        }
    }

    pub fn counter(&mut self, names: Vec<&str>, value: isize) {
        self.bucket.metric(names, MetricKind::Counter).add(value);
    }

    pub fn incr(&mut self, names: Vec<&str>) {
        self.bucket.metric(names, MetricKind::Counter).add(1);
    }

    pub fn decr(&mut self, names: Vec<&str>, value: isize) {
        self.bucket.metric(names, MetricKind::Counter).add(-1);
    }

    pub fn gauge(&mut self, names: Vec<&str>, value: isize) {
        self.bucket.metric(names, MetricKind::Gauge).set(value);
    }

    // Label variants

    pub fn counter_labels(&mut self, names: Vec<&str>, value: isize, labels: Vec<(&str, &str)>) {
        self.bucket
            .metric_labels(names, labels, MetricKind::Counter)
            .add(value);
    }

    pub fn incr_labels(&mut self, names: Vec<&str>, labels: Vec<(&str, &str)>) {
        self.bucket
            .metric_labels(names, labels, MetricKind::Counter)
            .add(1);
    }

    pub fn decr_labels(&mut self, names: Vec<&str>, value: isize, labels: Vec<(&str, &str)>) {
        self.bucket
            .metric_labels(names, labels, MetricKind::Counter)
            .add(-1);
    }

    pub fn gauge_labels(&mut self, names: Vec<&str>, value: isize, labels: Vec<(&str, &str)>) {
        self.bucket
            .metric_labels(names, labels, MetricKind::Gauge)
            .set(value);
    }

    pub fn timer(&mut self) -> SimpleTimer {
        SimpleTimer::default()
    }

    pub fn time(&mut self, names: Vec<&str>, mut timer: SimpleTimer) {
        timer.stop();
        self.bucket.metric(names, MetricKind::Timer).timer(timer);
    }

    pub fn time_labels(
        &mut self,
        names: Vec<&str>,
        mut timer: SimpleTimer,
        labels: Vec<(&str, &str)>,
    ) {
        timer.stop();
        self.bucket
            .metric_labels(names, labels, MetricKind::Timer)
            .timer(timer);
    }
}

#[derive(Default, Clone)]
pub struct Bucket {
    map: BTreeMap<MetricId, Metric>,
}

impl Bucket {
    fn metric(&mut self, names: Vec<&str>, kind: MetricKind) -> &mut Metric {
        let id = Metric::to_hash(&names, &None);
        if !self.map.contains_key(&id) {
            let metric = Metric::new(id, names, kind);
            self.map.insert(id, metric);
        }
        self.map.get_mut(&id).unwrap()
    }

    fn metric_labels(
        &mut self,
        names: Vec<&str>,
        labels: Vec<(&str, &str)>,
        kind: MetricKind,
    ) -> &mut Metric {
        let id = Metric::to_hash(&names, &Some(&labels));
        if !self.map.contains_key(&id) {
            let metric = Metric::new_labels(id, names, labels, kind);
            self.map.insert(id, metric);
        }
        self.map.get_mut(&id).unwrap()
    }

    fn clear(&mut self) {
        self.map.clear();
    }
}

#[derive(Clone)]
struct Metric {
    id: u64,
    names: Vec<String>,
    labels: Labels,
    kind: MetricKind,
    value: MetricValue,
}

impl Metric {
    fn default_value(kind: &MetricKind) -> MetricValue {
        match kind {
            MetricKind::Counter => MetricValue::Counter(Counter::default()),
            MetricKind::Gauge => MetricValue::Gauge(0),
            MetricKind::Timer => MetricValue::Timer(Timer::default()),
        }
    }

    fn new(id: u64, names: Vec<&str>, kind: MetricKind) -> Self {
        Metric {
            id: id,
            names: string_vec(names),
            labels: None,
            value: Metric::default_value(&kind),
            kind: kind,
        }
    }

    fn new_labels(id: u64, names: Vec<&str>, labels: Vec<(&str, &str)>, kind: MetricKind) -> Self {
        Metric {
            id: id,
            names: string_vec(names),
            labels: Some(string_label_vec(labels)),
            value: Metric::default_value(&kind),
            kind: kind,
        }
    }

    // This packs the values as multiple values
    // so we can properly format backend metrics...
    fn to_value(&mut self, format: MetricFormat) -> FormatedMetric {
        match &self.value {
            MetricValue::Counter(counter) => {
                let _v = counter.value.to_string();
                match format {
                    MetricFormat::Standard => FormatedMetric::Standard(_v),
                    MetricFormat::Statsd => FormatedMetric::Statsd(_v),
                    MetricFormat::Prometheus => FormatedMetric::Prometheus(vec![(None, None, _v)]),
                }
            }
            MetricValue::Gauge(v) => {
                let _v = v.to_string();
                match format {
                    MetricFormat::Standard => FormatedMetric::Standard(_v),
                    MetricFormat::Statsd => FormatedMetric::Statsd(_v),
                    MetricFormat::Prometheus => FormatedMetric::Prometheus(vec![(None, None, _v)]),
                }
            }
            MetricValue::Timer(timer) => {
                match format {
                    MetricFormat::Standard => {
                        // Standard timer just return the simple timer value
                        FormatedMetric::Standard(timer.simple.value.to_string())
                    }
                    MetricFormat::Statsd => {
                        FormatedMetric::Statsd(timer.simple.value_as_ms().to_string())
                    }
                    MetricFormat::Prometheus => {
                        let mut values = vec![];
                        let percentiles = vec![
                            0.05f64, 0.1f64, 0.25f64, 0.5f64, 0.75f64, 0.90f64, 0.95f64, 0.99f64,
                        ];
                        let labels = vec![
                            "0.05", "0.1", "0.2", "0.25", "0.5", "0.75", "0.9", "0.95", "0.99",
                        ];
                        // Add the sum across the entire bucket
                        // use the counter/value instead of iterating the histogram bucket
                        // https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md
                        // https://docs.rs/histogram/0.6.9/histogram/

                        for (i, p) in percentiles.iter().enumerate() {
                            match timer.histogram.percentile(*p) {
                                Ok(v) => {
                                    values.push((
                                        Some(HISTOGRAM_BUCKET),
                                        Some((HISTOGRAM_LE, labels[i])),
                                        v.to_string(),
                                    ));
                                }
                                Err(err) => {
                                    error!(
                                        "Error packing prometheus histogram values for key: {:?}",
                                        &err
                                    );
                                }
                            }
                        }
                        let count = timer.simple.counter.value.to_string();
                        // must contain an +Inf which is the same value as count
                        values.push((
                            Some(HISTOGRAM_BUCKET),
                            Some((HISTOGRAM_LE, "+Inf")),
                            count.clone(),
                        ));
                        values.push((Some("sum"), None, timer.simple.value.to_string()));
                        values.push((Some("count"), None, count));
                        FormatedMetric::Prometheus(values)
                    }
                }
            }
        }
    }

    fn to_hash(names: &Vec<&str>, labels: &Option<&Vec<(&str, &str)>>) -> u64 {
        let mut s = DefaultHasher::new();
        for name in names.iter() {
            name.hash(&mut s);
        }
        if let Some(values) = labels {
            for label in values.iter() {
                label.0.hash(&mut s);
                label.1.hash(&mut s);
            }
        }
        s.finish()
    }

    // only works with Counter
    fn add(&mut self, value: isize) {
        match &self.value {
            MetricValue::Counter(mut counter) => {
                // counter implements copy
                // so this is doable
                counter.add(value);
                self.value = MetricValue::Counter(counter);
            }
            _ => {}
        }
    }

    // only works with Gauge
    fn set(&mut self, value: isize) {
        match self.value {
            MetricValue::Gauge(_) => {
                self.value = MetricValue::Gauge(value);
            }
            _ => {}
        }
    }

    // Timer
    fn timer(&mut self, simple: SimpleTimer) {
        match &self.value {
            MetricValue::Timer(timer) => {
                let mut new_timer = timer.clone();
                new_timer.incr(simple);
                self.value = MetricValue::Timer(new_timer);
            }
            _ => {}
        }
    }
}

// Hash of the name + labels
type MetricId = u64;

pub type Label = (String, String);
pub type Labels = Option<Vec<Label>>;

#[derive(Clone)]
pub enum MetricKind {
    Counter,
    Gauge,
    Timer,
}

impl MetricKind {
    fn as_str(&self) -> &'static str {
        match self {
            MetricKind::Counter => "Counter",
            MetricKind::Gauge => "Gauge",
            MetricKind::Timer => "Timer",
        }
    }

    // Differentiate between as_str for prometheus
    // this way we have to address unsupported
    // `MetricKind` enums in the future
    fn as_prom_str(&self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Timer => "histogram",
        }
    }
    // Differentiate between as_str for prometheus
    // this way we have to address unsupported
    // `MetricKind` enums in the future
    fn as_statsd_str(&self) -> &'static str {
        match self {
            MetricKind::Counter => "c",
            MetricKind::Gauge => "g",
            MetricKind::Timer => "ms",
        }
    }
}

pub enum MetricFormat {
    Standard,
    Prometheus,
    Statsd,
}

pub enum FormatedMetric {
    // Value
    Standard(String),
    // Needs to account for a vec of histogram messages
    // (name, label(k, v), value)
    Prometheus(
        Vec<(
            Option<&'static str>,
            Option<(&'static str, &'static str)>,
            String,
        )>,
    ),
    // Value
    Statsd(String),
}

#[derive(Clone)]
pub enum MetricValue {
    Counter(Counter),
    Gauge(isize),
    Timer(Timer),
}

#[derive(Debug, Clone, Copy)]
pub struct Counter {
    pub value: isize,
}

impl Default for Counter {
    fn default() -> Self {
        Self { value: 0 }
    }
}

impl Counter {
    fn add(&mut self, value: isize) {
        self.value += value;
    }
}

#[derive(Clone, Default)]
pub struct Timer {
    pub simple: SimpleTimer,
    pub histogram: Histogram,
}

// TODO: implement default so we
// can override default Historgram config

impl Timer {
    fn incr(&mut self, simple: SimpleTimer) {
        self.simple.incr(simple.elapsed_ns());
        let _ = self.histogram.increment(simple.elapsed_ns());
    }
}

#[derive(Debug, Clone)]
pub struct SimpleTimer {
    start: Instant,
    end: Instant,
    counter: Counter,
    value: u64,
}

impl SimpleTimer {
    fn now() -> Self {
        let now = Instant::now();
        SimpleTimer {
            start: now.clone(),
            end: now,
            value: 0u64,
            counter: Counter::default(),
        }
    }

    pub fn incr(&mut self, value: u64) {
        self.value += value;
        self.counter.add(1isize);
    }

    pub fn stop(&mut self) {
        self.end = Instant::now();
    }
    // nano
    pub fn elapsed_ns(&self) -> u64 {
        let duration = self.end - self.start;
        (duration.as_secs() * 1_000_000_000) + u64::from(duration.subsec_nanos())
    }
    // micro
    pub fn elapsed_us(&self) -> u64 {
        let duration = self.end - self.start;
        (duration.as_secs() * 1_000_000) + u64::from(duration.subsec_micros())
    }
    // milli
    pub fn elapsed_ms(&self) -> u64 {
        (self.elapsed_us() / 1000) as u64
    }

    pub fn value_as_ms(&self) -> u64 {
        self.value / 1_000_000_000
    }
}

impl Default for SimpleTimer {
    fn default() -> Self {
        SimpleTimer::now()
    }
}
