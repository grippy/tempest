use actix::prelude::*;
use config;
use histogram::Histogram;
use serde_derive::{Deserialize, Serialize};
use serde_json as json;

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use crate::common::logger::*;
use crate::service::config::MetricConfig;

/// A module of backend metric actors
pub mod backend;

static TARGET_NAME: &'static str = "";
static TARGET_METRIC: &'static str = "metric";
static TEMPEST_NAME: &'static str = "tempest";
static HISTOGRAM_BUCKET: &'static str = "bucket";
static HISTOGRAM_LE: &'static str = "le";

thread_local! {
    /// Thread-local, Root instance variable
    static ROOT: RefCell<Root> = RefCell::new(Root::default());
}

/// Helper method which converts a Vec<&str>
/// to Vec<String>
fn string_vec(v: Vec<&str>) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// Helper method which converts Vec<(&str, &str)>
/// to Vec<(String, String)>
fn string_label_vec(v: Vec<(&str, &str)>) -> Vec<(String, String)> {
    v.iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

/// Configure `Root` values
pub(crate) fn configure(flush_interval: Option<u64>, targets: Option<Vec<MetricTarget>>) {
    if let Some(ms) = flush_interval {
        Root::flush_interval(ms);
    }
    if let Some(_targets) = targets {
        Root::targets(_targets);
    }
}

/// Configure metric targets from
/// a `Topology.toml` `MetricsConfig` (`[metric]`) section.
pub(crate) fn parse_metrics_config(cfgs: MetricConfig) {
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
    configure(cfgs.flush_interval, Some(targets));
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

/// Defines configuration details for interacting
/// with various backend targets.
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
    /// Write to a file. Default is append (which is clobber=false)
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

impl MetricTarget {
    /// Shortcut for generating a `Console` target
    pub fn console(prefix: Option<String>) -> Self {
        Self::Console { prefix: prefix }
    }

    /// Shortcut for generating a `Statsd` target
    pub fn statsd(host: String, prefix: Option<String>) -> Self {
        Self::Statsd {
            host: host,
            prefix: prefix,
        }
    }

    /// Shortcut for generating a `Prometheus` target
    pub fn prometheus(uri: String, prefix: Option<String>) -> Self {
        Self::Prometheus {
            uri: uri,
            prefix: prefix,
        }
    }

    /// Shortcut for generating a `File` target
    pub fn file(path: String, clobber: Option<bool>, prefix: Option<String>) -> Self {
        Self::File {
            path: path,
            clobber: clobber,
            prefix: prefix,
        }
    }

    /// Shortcut for generating a `Log` target
    pub fn log(level: Option<MetricLogLevel>, prefix: Option<String>) -> Self {
        Self::Log {
            level: level,
            prefix: prefix,
        }
    }
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

/// Root defines a `global` thread-local instance type
/// Think of this as state describing all metrics
/// of an application
#[derive(Clone)]
pub(crate) struct Root {
    /// Root name (used for naming metric File and log targets)
    pub target_name: String,

    /// Root prefix to pre-pend to all metric names
    pub prefix: String,

    /// Root labels added to all metric labels
    /// before sending to the backend
    pub labels: Labels,

    /// A vector of backend targets
    pub targets: Vec<MetricTarget>,

    /// Flush interval in milliseconds
    /// Configurable via `Topology.toml` (`[metric.flush_interval]`)
    /// Default value is 5 seconds.
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
    // Static ROOT methods

    /// Add a vector of labels to the `Root` instance
    pub fn labels(labels: Vec<Label>) {
        ROOT.with(|root| {
            for label in labels {
                Root::add_label(&mut *root.borrow_mut(), label.0, label.1);
            }
        });
    }

    /// Add a label (key, value) to the `Root` instance
    #[allow(dead_code)]
    pub fn label(key: String, value: String) {
        ROOT.with(|root| {
            Root::add_label(&mut *root.borrow_mut(), key, value);
        });
    }

    /// Set the `Root` instance target name
    pub fn target_name(name: String) {
        ROOT.with(|root| {
            let mut r = root.borrow_mut();
            r.target_name = name;
        });
    }

    /// Add target to `Root` instance
    pub fn targets(targets: Vec<MetricTarget>) {
        ROOT.with(|root| {
            for target in targets {
                Root::add_target(&mut *root.borrow_mut(), target);
            }
        });
    }

    /// Set the flush `Root` instance flush_interval
    pub fn flush_interval(value: u64) {
        ROOT.with(|root| {
            let mut r = root.borrow_mut();
            r.flush_interval = value;
        });
    }

    /// Clone the `Root` instance prefix
    fn get_prefix() -> String {
        ROOT.with(|root| root.borrow().prefix.clone())
    }

    /// Clone the `Root` instance labels
    fn get_labels() -> Labels {
        ROOT.with(|root| root.borrow().labels.clone())
    }

    /// Clone the `Root` instance target_name
    fn get_target_name() -> String {
        ROOT.with(|root| root.borrow().target_name.clone())
    }

    /// Clone the `Root` instance target
    fn get_targets() -> Vec<MetricTarget> {
        ROOT.with(|root| root.borrow().targets.clone())
    }

    /// Return the `Root` instance targets length
    pub fn get_targets_len() -> usize {
        ROOT.with(|root| root.borrow().targets.len())
    }

    /// Return the `Root` instance flush_interval
    fn get_flush_interval() -> u64 {
        ROOT.with(|root| root.borrow().flush_interval)
    }

    // Instance methods

    /// Add label (key/value) pair
    fn add_label(&mut self, key: String, value: String) {
        if self.labels.is_none() {
            self.labels = Some(vec![]);
        }
        let mut labels = self.labels.clone().unwrap();
        labels.push((key, value));
        self.labels = Some(labels);
    }

    /// Add target
    fn add_target(&mut self, target: MetricTarget) {
        self.targets.push(target);
    }
}

/// A wrapper around `AggregateMetrics`
#[derive(Debug)]
pub struct TestMetrics {
    aggregate: AggregateMetrics,
}

impl TestMetrics {
    pub fn new(aggregate: AggregateMetrics) -> Self {
        Self { aggregate }
    }

    /// Return a count for a given metric name
    pub fn get(&self, key: &str) -> Option<&isize> {
        self.aggregate.get(key)
    }
}

/// AggregateMetrics aggregate counter key values.
#[derive(Serialize, Deserialize, Debug, Message, Default, Clone)]
pub struct AggregateMetrics {
    // TODO impl iterator so we can remove pub
    pub counters: BTreeMap<String, isize>,
}

/// Base file name for writing aggregate metrics to disk.
/// This is a hack for now (and probable doesn't work on Windows).
/// More work should be done to explore the `AgentClient` making
/// some kind of `GET` request to return `AggregateMetrics`.
static TMP_AGGREGATE_METRICS: &'static str = "/tmp/aggregate-metrics-agent";

impl AggregateMetrics {
    /// Append the suffix and generate the aggregate metrics file name
    fn get_file_name(suffix: &String) -> String {
        format!("{}-{}", TMP_AGGREGATE_METRICS, suffix)
    }

    /// Read the aggregate metric temp file
    pub fn read_tmp(suffix: &String) -> Self {
        let filename = AggregateMetrics::get_file_name(suffix);
        let aggregate_metrics = match fs::read(&filename.as_str()) {
            Ok(buf) => match json::from_slice::<AggregateMetrics>(&buf) {
                Ok(aggregate) => aggregate,
                Err(_err) => AggregateMetrics::default(),
            },
            Err(err) => {
                error!(
                    "Error reading aggregate metrics file: {:?} {:?}",
                    TMP_AGGREGATE_METRICS, &err,
                );
                AggregateMetrics::default()
            }
        };

        let _ = fs::remove_file(&filename.as_str());
        aggregate_metrics
    }

    /// Write the aggregate metrics to disk
    pub fn write_tmp(&self, suffix: &String) {
        let filename = AggregateMetrics::get_file_name(suffix);
        let body = json::to_string(&self).unwrap();
        match fs::write(&filename.as_str(), body) {
            Err(err) => {
                error!("Error writing aggregate metrics: {:?}", &err);
            }
            _ => {}
        }
    }

    /// Insert a new key and counter value
    pub fn insert(&mut self, key: String, value: isize) {
        self.counters.insert(key, value);
    }

    /// Lookup a counter value by key name
    pub fn get(&self, key: &str) -> Option<&isize> {
        self.counters.get(key)
    }
}

/// The main structure for storing metrics (counter, gauges, timers).
/// Most internal structures implement Metrics instances.
///
/// Names and Labels are created with a hierarchy that goes like this:
///
/// - Root names are pre-pended to all Metrics names.
/// - Metric names are pre-pended to all metric bucket names.
/// - Root labels are merged with all Metrics labels.
/// - Metrics labels replace any existing values contained in Root labels.
/// - Metrics labels are merged with all Bucket Metric labels.
/// - Bucket Metric labels replace any existing values contained in Metrics labels.
///
#[derive(Clone)]
pub struct Metrics {
    /// A vector names. Each name part is joined
    /// using the defined backend delimiter (".", "_", etc).
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

    /// Builder for setting `names`
    pub fn named(&mut self, names: Vec<&str>) -> Self {
        self.names = string_vec(names);
        self.clone()
    }

    /// Builder for `Metrics::new("").labels(vec![])`;
    pub fn labels(&mut self, labels: Vec<(&str, &str)>) -> Self {
        for label in labels {
            self.add_label(label.0, label.1);
        }
        self.clone()
    }

    /// Add a label (key/value) pair
    pub fn add_label(&mut self, key: &str, value: &str) {
        if self.labels.is_none() {
            self.labels = Some(vec![]);
        }
        let mut labels = self.labels.clone().unwrap();
        labels.push((key.to_string(), value.to_string()));
        self.labels = Some(labels);
    }

    /// Flush this instance to the `MetricsBackendActor`
    /// Bucket is cleared of all value and reused
    /// each time this is called
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

    /// Set counter value for these name parts
    pub fn counter(&mut self, names: Vec<&str>, value: isize) {
        self.bucket.metric(names, MetricKind::Counter).add(value);
    }

    /// Increment counter by 1 for these name parts
    pub fn incr(&mut self, names: Vec<&str>) {
        self.bucket.metric(names, MetricKind::Counter).add(1);
    }

    /// Decrement counter by 1 for these name parts
    pub fn decr(&mut self, names: Vec<&str>) {
        self.bucket.metric(names, MetricKind::Counter).add(-1);
    }

    /// Set gauge value for these name parts
    pub fn gauge(&mut self, names: Vec<&str>, value: isize) {
        self.bucket.metric(names, MetricKind::Gauge).set(value);
    }

    // Label variants

    /// Set counter value for these name parts and labels
    pub fn counter_labels(&mut self, names: Vec<&str>, value: isize, labels: Vec<(&str, &str)>) {
        self.bucket
            .metric_labels(names, labels, MetricKind::Counter)
            .add(value);
    }

    /// Increment counter by 1 for these name parts and labels
    pub fn incr_labels(&mut self, names: Vec<&str>, labels: Vec<(&str, &str)>) {
        self.bucket
            .metric_labels(names, labels, MetricKind::Counter)
            .add(1);
    }

    /// Decrement counter by 1 for these name parts and labels
    pub fn decr_labels(&mut self, names: Vec<&str>, labels: Vec<(&str, &str)>) {
        self.bucket
            .metric_labels(names, labels, MetricKind::Counter)
            .add(-1);
    }

    /// Set gauge value for these name parts and labels
    pub fn gauge_labels(&mut self, names: Vec<&str>, value: isize, labels: Vec<(&str, &str)>) {
        self.bucket
            .metric_labels(names, labels, MetricKind::Gauge)
            .set(value);
    }

    /// Start SimpleTimer and return it
    pub fn timer(&mut self) -> SimpleTimer {
        SimpleTimer::default()
    }

    /// Set timer for these name parts
    pub fn time(&mut self, names: Vec<&str>, mut timer: SimpleTimer) {
        timer.stop();
        self.bucket.metric(names, MetricKind::Timer).timer(timer);
    }

    /// Set timer for these name parts and labels
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

/// A map of `MetricId` to `Metric`.
///
#[derive(Default, Clone)]
pub struct Bucket {
    map: BTreeMap<MetricId, Metric>,
}

impl Bucket {
    /// The main interface for returning a Metric
    /// from the bucket by name parts and kind.
    /// If the MetricId doesn't exist then a new Metric
    /// instance is added to the Bucket.
    fn metric(&mut self, names: Vec<&str>, kind: MetricKind) -> &mut Metric {
        let id = Metric::to_hash(&names, &None);
        if !self.map.contains_key(&id) {
            let metric = Metric::new(id, names, kind);
            self.map.insert(id, metric);
        }
        self.map.get_mut(&id).unwrap()
    }

    /// The main interface for returning a Metric
    /// from the bucket by name parts, labels, and kind.
    /// If the MetricId doesn't exist then a new Metric
    /// instance is added to the Bucket.
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

/// This is a Metric that maps the name parts, labels, and kind
/// to a MetricValue.
#[derive(Clone)]
struct Metric {
    id: u64,
    names: Vec<String>,
    labels: Labels,
    kind: MetricKind,
    value: MetricValue,
}

impl Metric {
    /// Generate a default MetricValue for a given MetricKind.
    fn default_value(kind: &MetricKind) -> MetricValue {
        match kind {
            MetricKind::Counter => MetricValue::Counter(Counter::default()),
            MetricKind::Gauge => MetricValue::Gauge(0),
            MetricKind::Timer => MetricValue::Timer(Timer::default()),
        }
    }

    /// New constructor for name parts and kind.
    fn new(id: u64, names: Vec<&str>, kind: MetricKind) -> Self {
        Metric {
            id: id,
            names: string_vec(names),
            labels: None,
            value: Metric::default_value(&kind),
            kind: kind,
        }
    }

    /// New constructor for name parts, labels, and kind.
    fn new_labels(id: u64, names: Vec<&str>, labels: Vec<(&str, &str)>, kind: MetricKind) -> Self {
        Metric {
            id: id,
            names: string_vec(names),
            labels: Some(string_label_vec(labels)),
            value: Metric::default_value(&kind),
            kind: kind,
        }
    }

    /// Format metrics based on the MetricFormat
    /// This is how we transform metrics into types used by
    /// statsd, prometheus, etc.
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

                        // This is just a guess at what someone might want see
                        // Should revisit this to make sure these are adequate
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

    /// Hash name parts and labels. This is used to generate a MetricId's.
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

    /// Add a Counter value
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

    /// Set a Guage value
    fn set(&mut self, value: isize) {
        match self.value {
            MetricValue::Gauge(_) => {
                self.value = MetricValue::Gauge(value);
            }
            _ => {}
        }
    }

    /// Set a SimpleTimer
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

/// Type alias for storing a hash of the name parts & labels
type MetricId = u64;

/// Type alias for storing a key/value as a tuple.
pub(crate) type Label = (String, String);

/// Type alias for storing a optional Label vector.
pub(crate) type Labels = Option<Vec<Label>>;

/// Defines the kind of metric we want to work with
#[derive(Clone)]
pub(crate) enum MetricKind {
    /// Increment/decrement an isize value
    Counter,
    /// Replace an isize value
    Gauge,
    /// Track time
    Timer,
}

impl MetricKind {
    /// Helper for converting the enum value to &str
    /// This is the general use case for log, file, and console
    /// backends.
    fn as_str(&self) -> &'static str {
        match self {
            MetricKind::Counter => "Counter",
            MetricKind::Gauge => "Gauge",
            MetricKind::Timer => "Timer",
        }
    }

    /// Helper for converting the enum value to &str.
    /// This is used for generating Prometheus metrics.
    fn as_prom_str(&self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Timer => "histogram",
        }
    }

    /// Helper for converting the enum value to &str.
    /// This is used for generating Statsd metrics.
    fn as_statsd_str(&self) -> &'static str {
        match self {
            MetricKind::Counter => "c",
            MetricKind::Gauge => "g",
            MetricKind::Timer => "ms",
        }
    }
}

/// Defines a format style for how we expect
/// to work with metrics in each backend
pub(crate) enum MetricFormat {
    /// Standard is used we don't need to account for any
    /// special considerations.
    Standard,
    /// This format is used for Prometheus
    /// Special formating is applied to generate
    /// histograms.
    Prometheus,
    /// This format is used for Statsd
    /// Special formating is applied to generate
    /// timers.
    Statsd,
}

/// A formated metric struct for a `line` is constructed
/// for backend target types.
pub(crate) enum FormatedMetric {
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

/// The value of a given MetricKind
#[derive(Clone)]
pub(crate) enum MetricValue {
    Counter(Counter),
    Gauge(isize),
    Timer(Timer),
}

/// A counter structure
#[derive(Debug, Clone, Copy)]
pub(crate) struct Counter {
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

/// A timer structure
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

/// A timer that stores a counter.
/// Used for determining counts/time (rates) and histograms.
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

    /// Increase the timer value bump the counter value
    pub fn incr(&mut self, value: u64) {
        self.value += value;
        self.counter.add(1isize);
    }

    /// Set the end time for this timer
    pub fn stop(&mut self) {
        self.end = Instant::now();
    }
    /// Elapsed time in nanoseconds
    pub fn elapsed_ns(&self) -> u64 {
        let duration = self.end - self.start;
        (duration.as_secs() * 1_000_000_000) + u64::from(duration.subsec_nanos())
    }
    /// Elapsed time in microseconds
    pub fn elapsed_us(&self) -> u64 {
        let duration = self.end - self.start;
        (duration.as_secs() * 1_000_000) + u64::from(duration.subsec_micros())
    }

    /// Elapsed time in milliseconds
    pub fn elapsed_ms(&self) -> u64 {
        (self.elapsed_us() / 1000) as u64
    }

    /// Convert the value to milliseconds
    pub fn value_as_ms(&self) -> u64 {
        self.value / 1_000_000_000
    }
}

impl Default for SimpleTimer {
    fn default() -> Self {
        SimpleTimer::now()
    }
}
