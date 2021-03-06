use actix::prelude::*;
use serde_derive::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use crate::common::logger::*;
use crate::common::now_millis;
use crate::metric::{self, Metrics};
use crate::pipeline::*;
use crate::service::server::TopologyServer;
use tempest_source::prelude::*;

/// Log targets
static TARGET_SOURCE_ACTOR: &'static str = "tempest::topology::SourceActor";
static TARGET_TOPOLOGY_ACTOR: &'static str = "tempest::topology::TopologyActor";
static TARGET_PIPELINE_ACTOR: &'static str = "tempest::topology::PipelineActor";

/// Topology trait for building topologies.
pub trait Topology<SB: SourceBuilder> {
    /// Method for returning a topology instance
    fn builder() -> TopologyBuilder<SB>;

    /// Method for returning a topology instance configured for testing
    fn test_builder() -> TopologyBuilder<SB> {
        Self::builder()
    }
}

/// Enum for configuring how to process message failures.
#[derive(Clone, Debug, PartialEq)]
pub enum TopologyFailurePolicy {
    /// Messages with Errors/Timeouts are left unacked.
    /// This is used when the message Source has it's own
    /// mechanism for dealing with Failures.
    /// For example, SQS automatically re-delivers unacked messages
    /// after a period of time. This should also be used
    /// when the Tempest source client implements it's own way
    /// of dealing with failure.
    None,

    /// Messages are automatically acked, regardless of msg state
    /// (success, error, timeout)
    BestEffort,

    /// Messages are held within the Topology and retried up to this limit
    /// The retry interval is automatically every 60s.
    Retry(usize),
}

impl Default for TopologyFailurePolicy {
    fn default() -> Self {
        TopologyFailurePolicy::BestEffort
    }
}

/// Enum for configuring the maximum number of pending messages.
#[derive(Clone, Debug, PartialEq)]
pub enum TopologyMaxPending {
    Count(u64),
    NoLimit,
}

impl Default for TopologyMaxPending {
    fn default() -> Self {
        TopologyMaxPending::NoLimit
    }
}

// Some of these properties are stored as String
// to make it easier to share with Actix actors
/// Topology configuration options
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopologyOptions {
    /// Name of the topology
    pub name: String,

    /// host:port
    host_port: Option<String>,

    /// The host:port of the agent this topology
    /// should communicates with
    agent_host_port: Option<String>,

    /// How should we handle failures?
    failure_policy: Option<TopologyFailurePolicy>,

    /// Max amount of time, in milliseconds, to wait before
    /// moving a pending message into a failure state.
    /// What happens after a timeout depends on
    /// the failure policy configuration.
    msg_timeout: Option<usize>,

    // /// Max number of pending messages allowed.
    // /// A message is considered pending until its
    // /// acked at the source (or dropped on error).
    // /// This value limits the number of messages stored in-memory
    // /// waiting to be worked on by tasks
    // max_pending: Option<TopologyMaxPending>,
    /// Metric flush interval in milliseconds
    /// This value is overridden by `Topology.toml`
    /// configuration.
    pub metric_flush_interval: Option<u64>,

    /// Define a list of metric targets
    /// for sending metrics too.
    /// This value is overridden by `Topology.toml`
    /// configuration.
    pub metric_targets: Vec<metric::MetricTarget>,

    /// Graceful shutdown period
    pub graceful_shutdown: Option<u64>,
}

impl TopologyOptions {
    /// The topology name
    pub fn name(&mut self, name: &'static str) {
        self.name = name.to_string();
    }

    /// Topology failure policy
    pub fn failure_policy(&mut self, policy: TopologyFailurePolicy) {
        self.failure_policy = Some(policy);
    }

    /// Topology message time out value. This is the maximum amount of time to wait
    /// before a source message goes into an error state.
    pub fn msg_timeout(&mut self, ms: usize) {
        self.msg_timeout = Some(ms);
    }

    /// This is the host and port value `host:port` (example: 127.0.0.1:5678)
    pub fn host_port(&mut self, host_port: String) {
        self.host_port = Some(host_port);
    }

    /// This is the agent host and port value `host:port` (example: 127.0.0.1:6789)
    pub fn agent_host_port(&mut self, host_port: String) {
        self.agent_host_port = Some(host_port);
    }

    /// The maximum amount of time to wait before a hard shutdown occurs
    pub fn graceful_shutdown(&mut self, ms: u64) {
        self.graceful_shutdown = Some(ms);
    }

    /// The maximum amount of time to wait before a flushing accumulated metrics
    pub fn metric_flush_interval(&mut self, ms: u64) {
        self.metric_flush_interval = Some(ms);
    }

    /// Push a metric target onto the vector metric targets
    pub fn metric_target(&mut self, target: metric::MetricTarget) {
        self.metric_targets.push(target);
    }
}

/// Builder for constructing and configuring topology instances.
#[derive(Default)]
pub struct TopologyBuilder<SB: SourceBuilder> {
    /// Configuration options for the topology
    pub options: TopologyOptions,
    /// Topology pipeline
    pub pipeline: Pipeline,
    /// Topology source builder
    pub source_builder: SB,
}

impl<SB> TopologyBuilder<SB>
where
    SB: SourceBuilder + Default,
    <SB as SourceBuilder>::Source: Source + 'static + Default,
{
    /// Set the topology name
    pub fn name(mut self, name: &'static str) -> Self {
        self.options.name(name);
        self
    }

    /// Set the failure policy for handling message failures
    pub fn failure_policy(mut self, policy: TopologyFailurePolicy) -> Self {
        self.options.failure_policy(policy);
        self
    }

    /// Set the message timeout in milliseconds
    pub fn msg_timeout(mut self, ms: usize) -> Self {
        self.options.msg_timeout(ms);
        self
    }

    /// Set the graceful shutdown time in milliseconds
    pub fn graceful_shutdown(mut self, ms: u64) -> Self {
        self.options.graceful_shutdown(ms);
        self
    }

    /// Set the interval in milliseconds for how often metrics should be flushed
    pub fn metric_flush_interval(mut self, ms: u64) -> Self {
        self.options.metric_flush_interval(ms);
        self
    }

    /// Add a `MetricTarget`
    pub fn metric_target(mut self, target: metric::MetricTarget) -> Self {
        self.options.metric_target(target);
        self
    }

    /// Set the topology pipeline
    pub fn pipeline(mut self, pipe: Pipeline) -> Self {
        self.pipeline = pipe.build();
        self
    }

    /// Set the topology source builder
    pub fn source(mut self, sb: SB) -> Self {
        self.source_builder = sb;
        self
    }

    /// Return the `SourceActor` instance for this topology
    pub(crate) fn source_actor(&self) -> SourceActor {
        SourceActor {
            source: Box::new(self.source_builder.build()),
            ack_queue: VecDeque::new(),
            backoff: 1u64,
            metrics: Metrics::default().named(vec!["source"]),
            shutdown: false,
        }
    }

    /// Return the `TopologyActor` instance for this topology
    pub(crate) fn topology_actor(&self) -> TopologyActor {
        TopologyActor {
            options: self.options.clone(),
            metrics: Metrics::default().named(vec!["topology"]),
            retry: None,
        }
    }

    /// Return the `PipelineActor` instance for this topology
    pub(crate) fn pipeline_actor(&self) -> PipelineActor {
        PipelineActor {
            pipeline: self.pipeline.runtime(),
            inflight: PipelineInflight::new(self.options.msg_timeout.clone()),
            available: PipelineAvailable::new(self.pipeline.names()),
            aggregate: PipelineAggregate::new(self.pipeline.names()),
            metrics: Metrics::default().named(vec!["pipeline"]),
        }
    }
}

/// Shutdown message for initiating a shutdown
#[derive(Message, Debug)]
pub(crate) struct ShutdownMsg {}

/// A TaskMsg defines the data structure for responses
/// task message requests.
///
// A Task response return multiple messages
// which are then passed into the descendant tasks.
// For this reason, we need to keep track
// of the index of the sub-task so we know
// when to mark the edge as visited.
#[derive(Serialize, Deserialize, Message, Debug)]
pub(crate) struct TaskMsg {
    pub msg_id: MsgId,
    pub edge: Edge,
    pub index: usize,
    pub msg: Msg,
}

/// An enum for tasks requesting work
///
#[derive(Message, Debug)]
pub(crate) enum TaskRequest {
    /// Used for requesting messages for a given task name
    /// The session_id is used for routing responses
    /// GetAvailable(session_id, task_name, count)
    GetAvailable(usize, String, Option<usize>),

    // The return wrapper for task clients asking for messages.
    // GetAvailableResponse(session_id, task_name, tasks)
    GetAvailableResponse(usize, String, Option<Vec<TaskMsg>>),
}

/// An enum for tasks notifying the topology they've finished
/// processing a TaskMsg
#[derive(Message, Serialize, Deserialize, Debug)]
pub(crate) enum TaskResponse {
    /// This message should be marked as successful for this edge and sub-task index. Contains an optional
    /// vector of new messages to send into descendant tasks.
    /// Ack(msg_id, edge, index, response)
    Ack(MsgId, Edge, usize, Option<Vec<Msg>>),
    /// This message should be marked as an error for this edge and sub-task index.
    /// Error(msg_id, edge, index)
    Error(MsgId, Edge, usize),
}

/// `TopologySourceMsg` is a data structure which derives Message
/// so we can pass SourceMsg around (which doesn't derive Message)
#[derive(Message, Debug, Clone)]
struct TopologySourceMsg {
    msg: SourceMsg,
}

// Default Source stub to get around having to implement SourceActor::default
// and not knowing what the Source type is
pub(crate) struct DefaultSource {}
impl Source for DefaultSource {
    fn name(&self) -> &'static str {
        "Default"
    }
}

/// Main actor for interacting with `Source` instances.
pub struct SourceActor {
    /// Some struct that implements Source trait
    source: Box<dyn Source>,
    /// The queue of messages to ack
    ack_queue: VecDeque<MsgId>,
    /// Backoff delay
    backoff: u64,
    /// Metrics
    metrics: Metrics,
    /// Shutdown command controls when to stop polling
    /// for new messages. this is configured for graceful shutdowns
    /// and allows the topology to continue draining inflight messages
    shutdown: bool,
}

// Default is required to make use of the actix System Registry
// This method should never be called though...
impl Default for SourceActor {
    fn default() -> Self {
        SourceActor {
            source: Box::new(DefaultSource {}),
            ack_queue: VecDeque::new(),
            backoff: 1u64,
            metrics: Metrics::default().named(vec!["source"]),
            shutdown: false,
        }
    }
}

impl SourceActor {
    /// Resets backoff and poll_interval to the source config
    fn reset_backoff(&mut self) {
        let poll_interval = match self.source.poll_interval() {
            Ok(SourceInterval::Millisecond(ms)) => ms,
            Err(_err) => &1000u64,
        };
        self.backoff = *poll_interval;
    }

    /// Bump the backoff value
    fn backoff(&mut self, bump: u64) {
        // read max_backoff from source
        let max_backoff = self.source.max_backoff().unwrap();
        if self.backoff < *max_backoff {
            self.backoff += bump;
        }
    }

    /// Call `source.monitor`
    fn monitor(&mut self, _ctx: &mut Context<Self>) {
        // Call the source.monitor method
        // up to the source to determine what this does
        let _ = self.source.monitor();
    }

    /// Call `source.poll` which returns a vector of messages
    /// for processing inside the topology.
    fn poll(&mut self, ctx: &mut Context<Self>) {
        trace!(
            target: TARGET_SOURCE_ACTOR,
            "SourceActor#poll before (backoff={})",
            self.backoff
        );
        let results = match self.source.poll() {
            Ok(option) => match option {
                Some(results) => {
                    self.metrics
                        .incr_labels(vec!["poll"], vec![("status", "success")]);
                    results
                }
                None => vec![],
            },
            Err(_err) => {
                self.metrics
                    .incr_labels(vec!["poll"], vec![("status", "error")]);
                vec![]
            }
        };

        // if results are empty
        // we need to initiate the backoff

        let msg_count = results.len();
        if msg_count == 0usize {
            self.backoff(100u64);
        } else {
            self.reset_backoff();
        }

        if msg_count > 0usize {
            self.metrics
                .counter(vec!["msg", "read"], msg_count as isize);
        }
        // What's our current backoff
        self.metrics.gauge(vec!["backoff"], self.backoff as isize);

        // reschedule poll again if we aren't in shutdown mode
        if !self.shutdown {
            ctx.run_later(Duration::from_millis(self.backoff), Self::poll);
        } else {
            warn!(
                target: TARGET_SOURCE_ACTOR,
                "SourceActor shutdown enabled: stop polling"
            );
        }

        let topology = TopologyActor::from_registry();
        if !topology.connected() {
            error!(
                target: TARGET_SOURCE_ACTOR,
                "TopologyActor#poll topology actor isn't connected"
            );
            self.metrics.counter_labels(
                vec!["msg", "dropped"],
                results.len() as isize,
                vec![("from", "source"), ("reason", "topology_disconnected")],
            );
            return;
        }

        // println!("Source polled {:?} msgs", results.len());
        // send these msg to the topology actor
        for src_msg in results {
            match topology.try_send(TopologySourceMsg { msg: src_msg }) {
                // count errors here...
                // we need to know when to backoff
                // if we can reach the addr
                Err(SendError::Full(msg)) => {
                    // sleep & update backoff here?
                    // we need future `poll` calls
                    // from getting into the method
                    // at the same time we need to slow
                    // down how fast we push messages into
                    // the addr
                    error!(
                        target: TARGET_SOURCE_ACTOR,
                        "TopologyActor mailbox is full, dropping msg: {:?}", &msg
                    );
                    self.metrics.incr_labels(
                        vec!["msg", "dropped"],
                        vec![("from", "source"), ("reason", "topology_full")],
                    );
                }
                Err(SendError::Closed(msg)) => {
                    // we need to kill this actor here
                    // TODO: move this to deadletter q?
                    error!(
                        target: TARGET_SOURCE_ACTOR,
                        "TopologyActor is closed, dropping msg: {:?}", &msg
                    );
                    self.metrics.incr_labels(
                        vec!["msg", "dropped"],
                        vec![("from", "source"), ("reason", "topology_closed")],
                    );
                }
                Ok(_) => {
                    self.metrics
                        .incr_labels(vec!["msg", "moved"], vec![("to", "topology")]);
                }
            }
        }
    }

    /// Drain the ack_queue and send all messages into the source.batch_ack method
    fn batch_ack(&mut self, _ctx: &mut Context<Self>) {
        let msgs = self.ack_queue.drain(..).collect::<Vec<_>>();
        let len = msgs.len();
        if len > 0 {
            trace!(target: TARGET_SOURCE_ACTOR, "Acking: {} msgs", &len);
            let result = self.source.batch_ack(msgs);
            self.ack_result(len, result);
        }
    }

    /// Drain the ack_queue and ack individual messages (one at a time)
    fn individual_ack(&mut self, _ctx: &mut Context<Self>) {
        let msgs = self.ack_queue.drain(..).collect::<Vec<_>>();
        trace!(
            target: TARGET_SOURCE_ACTOR,
            "Ack individual msgs: {} msgs",
            msgs.len()
        );
        for msg in msgs {
            let result = self.source.ack(msg);
            self.ack_result(1usize, result);
        }
    }

    /// Handle a result from acking messages (individual and batch)
    fn ack_result(&mut self, sent: usize, results: SourceResult<(i32, i32)>) {
        match results {
            Ok((tried, acked)) => {
                let mut labels = vec![];
                let error_count = (tried - acked).to_string();
                if tried == acked {
                    labels.push(("status", "success"));
                } else {
                    labels.push(("status", "partial_success"));
                    labels.push(("error_count", &error_count));
                }
                self.metrics
                    .counter_labels(vec!["msg", "acked"], acked as isize, labels);
            }
            Err(err) => {
                error!(target: TARGET_SOURCE_ACTOR, "Ack msg err: {:?}", &err);
                self.metrics.counter_labels(
                    vec!["msg", "acked"],
                    0isize,
                    vec![
                        ("status", "error"),
                        ("error_count", &(*&sent as isize).to_string()),
                    ],
                );
            }
        }
    }
}

impl Actor for SourceActor {
    type Context = Context<Self>;

    /// The SourceActor was started, run initialization hooks
    fn started(&mut self, ctx: &mut Context<Self>) {
        // setup the source here...
        // TODO: verify this isn't an error
        match self.source.setup() {
            Err(err) => {
                warn!(
                    target: TARGET_SOURCE_ACTOR,
                    "Failed to setup source... trigger shutdown: {:?}", &err
                );
                System::current().stop();
                self.metrics.incr(vec!["setup", "error"]);
                return;
            }
            _ => {}
        }

        // Add metrics labels
        self.metrics.add_label("source_name", self.source.name());

        // From here on out we use the backoff as the polling interval
        // and poll_interval is only used to reset the backoff
        self.reset_backoff();

        // start polling the source
        ctx.run_later(Duration::from_millis(self.backoff), Self::poll);

        // initialize defined ack policy...
        let ack_interval = match self.source.ack_interval() {
            Ok(v) => v,
            Err(_err) => &SourceInterval::Millisecond(1000),
        };

        // schedule next poll for batch or individual acking
        // no-ack doesn't run the interval
        let duration = ack_interval.as_duration();
        match self.source.ack_policy() {
            Ok(policy) => match policy {
                SourceAckPolicy::Batch(_batch_size) => {
                    ctx.run_interval(duration, Self::batch_ack);
                }
                SourceAckPolicy::Individual => {
                    ctx.run_interval(duration, Self::individual_ack);
                }
                SourceAckPolicy::None => {
                    warn!(
                        target: TARGET_SOURCE_ACTOR,
                        "SourceAckPolicy is None. Disabling ack interval"
                    );
                }
            },
            _ => {}
        }

        // schedule monitoring
        let monitor_interval = self.source.monitor_interval();
        if let Ok(SourceInterval::Millisecond(duration)) = &monitor_interval {
            if duration > &0u64 {
                let dur = monitor_interval.unwrap().as_duration();
                warn!(
                    target: TARGET_SOURCE_ACTOR,
                    "Configuring source monitor with interval: {:?}", &dur
                );
                ctx.run_interval(dur, Self::monitor);
            }
        }

        metric::backend::MetricsBackendActor::subscribe(
            "SourceActor",
            ctx.address().clone().recipient(),
        );
    }

    /// The `SourceActor` was stopped, run shutdown hooks.
    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        let _ = self.source.shutdown();
    }
}

impl Supervised for SourceActor {}

impl SystemService for SourceActor {}

/// Wrapper for acking messages by MsgId
#[derive(Message, Debug)]
pub struct SourceAckMsg(MsgId);

impl Handler<SourceAckMsg> for SourceActor {
    type Result = ();

    /// Handle SourceAckMsg messages and send the MsgId to the `ack_queue`
    fn handle(&mut self, msg: SourceAckMsg, _ctx: &mut Context<Self>) {
        // println!("push ack_queue msg_id: {:?}", &msg);
        match &self.source.ack_policy() {
            Ok(policy) => match policy {
                SourceAckPolicy::Batch(_size) => {
                    self.ack_queue.push_back(msg.0);
                }
                SourceAckPolicy::Individual => {
                    self.ack_queue.push_back(msg.0);
                }
                SourceAckPolicy::None => {}
            },
            _ => {}
        }
    }
}

impl Handler<ShutdownMsg> for SourceActor {
    type Result = ();

    /// Handle `ShutdownMsg` and trigger the shutdown flag
    fn handle(&mut self, _msg: ShutdownMsg, _ctx: &mut Context<Self>) {
        warn!(
            target: TARGET_SOURCE_ACTOR,
            "SourceActor received shutdown msg"
        );
        self.shutdown = true;
    }
}

impl Handler<metric::backend::Flush> for SourceActor {
    type Result = ();

    /// Handle `metric::backend::Flush` messages. Flushes `SourceActor` and `Source` (if configured) metrics
    fn handle(&mut self, _msg: metric::backend::Flush, _ctx: &mut Context<Self>) {
        // Flush source metrics
        self.source.flush_metrics();
        // Flush source_actor metrics
        self.metrics.flush();
    }
}

/// `TopologyRetry` is used when `TopologyFailurePolicy::Retry(count)`
/// is configured.
///
/// Be advised: This has a clone cost to it:
/// - All inflight source messages are stored in this data structure.
/// - For every retry, we must clone the source message.
/// - Every ack and error must clone the msg id
///
/// This isn't optimal but will work for retrying messages
/// if the source service doesn't implement one for you.
///
#[derive(Default)]
struct TopologyRetry {
    /// The number of times to retry a message
    count: usize,
    /// Track polled messages and keep count of how many times
    /// they've been retried
    inflight: HashMap<MsgId, TopologySourceMsg>,
    /// A queue of which msgs to retry
    queue: VecDeque<MsgId>,
}

impl TopologyRetry {
    /// Instantiate `TopologyRetry` with a max retry count
    fn new(count: usize) -> Self {
        Self {
            count: count,
            ..Default::default()
        }
    }

    /// When new source messages are received
    /// by the `TopologyActor`, they're stored
    /// here for the duration of the Pipeline
    fn store(&mut self, msg: TopologySourceMsg) {
        let msg_id = msg.msg.id.clone();
        self.inflight.insert(msg_id, msg);
    }

    // Dumb implementation for now...
    /// Stores a MsgId into the internal queue, checking if it
    /// exceeded its retry count
    fn put(&mut self, msg_id: MsgId) -> bool {
        let mut success = false;
        match self.inflight.get_mut(&msg_id) {
            Some(msg) => {
                if msg.msg.delivered < self.count {
                    self.queue.push_back(msg_id);
                    success = true;
                }
            }
            None => {}
        }
        success
    }

    /// Get a list of source messages and bump the delivered count
    /// on the way out
    fn get(&mut self) -> Vec<TopologySourceMsg> {
        // A msg id must exist in the inflight message
        // otherwise we drop it...
        let mut retry = vec![];
        for msg_id in self.queue.drain(..) {
            match self.inflight.get_mut(&msg_id) {
                Some(msg) => {
                    msg.msg.delivered += 1;
                    retry.push(msg.clone());
                }
                None => {}
            }
        }
        retry
    }

    /// Delete inflight message by MsgId
    fn delete(&mut self, msg_id: MsgId) {
        self.inflight.remove(&msg_id);
    }
}

/// Main actor for running and coordinating topologies.
///
/// This actor is central to all message processing:
/// - It handles polled SourceActor messages and sends them to the PipelineActor.
/// - It responds to Task requests and responses.
/// - It handles message failures and retry failures
///
#[derive(Default)]
pub struct TopologyActor {
    /// Topology options
    options: TopologyOptions,
    /// Metrics
    metrics: Metrics,
    /// Retry data structure
    retry: Option<TopologyRetry>,
}

impl TopologyActor {
    /// Failure handling code for a given `MsgId`
    fn handle_failure(&mut self, msg_id: MsgId) {
        match &self.options.failure_policy {
            Some(policy) => match policy {
                TopologyFailurePolicy::BestEffort => {
                    let source = SourceActor::from_registry();
                    if !source.connected() {
                        error!(
                            target: TARGET_TOPOLOGY_ACTOR,
                            "SourceActor isn't connected, fail to generate BestEffort ack"
                        );
                        self.metrics.incr_labels(
                            vec!["source", "msg", "ack", "dropped"],
                            vec![("reason", "source_disconnected")],
                        );
                        return;
                    }
                    // Source ack msg
                    let _ = source.try_send(SourceAckMsg(msg_id));
                }
                TopologyFailurePolicy::Retry(count) => match self.retry.as_mut() {
                    Some(retry) => {
                        let msg_id2 = msg_id.clone();
                        if !retry.put(msg_id) {
                            // delete retry inflight entry (we prob hit max retries)
                            retry.delete(msg_id2);
                            warn!(
                                target: TARGET_TOPOLOGY_ACTOR,
                                "TopologyFailurePolicy::Retry({}), dropped", &count
                            );
                            self.metrics.incr(vec!["source", "msg", "retry", "dropped"]);
                        } else {
                            warn!(
                                target: TARGET_TOPOLOGY_ACTOR,
                                "TopologyFailurePolicy::Retry({}), queue msg", &count
                            );
                            self.metrics.incr(vec!["source", "msg", "retry", "queued"]);
                        }
                    }
                    None => {}
                },
                TopologyFailurePolicy::None => {
                    warn!(
                        target: TARGET_TOPOLOGY_ACTOR,
                        "TopologyFailurePolicy::None, drop msg"
                    );
                    self.metrics.incr_labels(
                        vec!["source", "msg", "ack", "dropped"],
                        vec![("reason", "failure_policy_none")],
                    );
                }
            },
            None => {
                warn!(
                    target: TARGET_TOPOLOGY_ACTOR,
                    "TopologyFailurePolicy undefined, drop msg"
                );
                self.metrics.incr_labels(
                    vec!["source", "msg", "ack", "dropped"],
                    vec![("reason", "failure_policy_none")],
                );
            }
        }
    }

    /// Get messages in the retry queue and push them back into the `TopologyActor`
    fn retry_failure(&mut self, ctx: &mut Context<Self>) {
        if self.retry.is_none() {
            return;
        }
        // Flush retry queue of TopologySourceMsg back into self...
        // This should have controls on the number of messages here...
        // Otherwise, the retry could just lead to more failures
        // (downstream services, etc.)
        let addr = ctx.address();
        match self.retry.as_mut() {
            Some(retry) => {
                for mut msg in retry.get() {
                    // We must reset the msg.ts
                    // to avoid a quick timeout
                    msg.msg.ts = now_millis();
                    let _ = addr.do_send(msg);
                }
            }
            None => {}
        }
    }
}

impl Actor for TopologyActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        // Set the mailbox capacity to infinite
        ctx.set_mailbox_capacity(0);

        // initialize topology retry mechanics?
        if let Some(TopologyFailurePolicy::Retry(count)) = self.options.failure_policy {
            self.retry = Some(TopologyRetry::new(count));
            ctx.run_interval(Duration::from_secs(60), Self::retry_failure);
        }

        metric::backend::MetricsBackendActor::subscribe(
            "TopologyActor",
            ctx.address().clone().recipient(),
        );
    }
}

impl Supervised for TopologyActor {}
impl SystemService for TopologyActor {}

impl Handler<TopologySourceMsg> for TopologyActor {
    type Result = ();

    /// Handle `TopologySourceMsg` messages sent from the `SourceActor`
    fn handle(&mut self, msg: TopologySourceMsg, _ctx: &mut Context<Self>) {
        let pipeline = PipelineActor::from_registry();
        if !pipeline.connected() {
            // TODO: wire up a retry here?
            error!(
                target: TARGET_TOPOLOGY_ACTOR,
                "PipelineActor isn't connected, dropping msg: {:?}", &msg
            );
            self.metrics.incr_labels(
                vec!["msg", "dropped"],
                vec![("from", "source"), ("reason", "pipeline_disconnected")],
            );
            return;
        }

        // Store this message if the failure policy is Retry and
        // this is the first time delivering this message.
        // The msg must have a delivered == 0 on it... otherwise nothing
        // will be retried
        if self.retry.is_some() {
            if msg.msg.delivered == 0 {
                match self.retry.as_mut() {
                    Some(retry) => {
                        retry.store(msg.clone());
                    }
                    None => {}
                }
            }
        }

        match pipeline.try_send(PipelineMsg::TaskRoot(msg)) {
            Err(SendError::Full(msg)) => {
                // TODO: wire up "holding queue" and trigger backoff here
                error!(
                    target: TARGET_TOPOLOGY_ACTOR,
                    "PipelineActor mailbox is full, dropping msg: {:?}", &msg
                );
                self.metrics.incr_labels(
                    vec!["msg", "dropped"],
                    vec![("from", "source"), ("reason", "pipeline_mailbox_full")],
                );
            }
            Err(SendError::Closed(msg)) => {
                // TODO: trigger shutdown here
                error!(
                    target: TARGET_TOPOLOGY_ACTOR,
                    "PipelineActor is closed, dropping msg: {:?}", &msg
                );
                self.metrics.incr_labels(
                    vec!["msg", "dropped"],
                    vec![("from", "source"), ("reason", "pipeline_closed")],
                );
            }
            Ok(_) => {
                self.metrics
                    .incr_labels(vec!["msg", "moved"], vec![("to", "pipeline")]);
            }
        }
    }
}

// Client -> Server request for available tasks and
// and response back out through the TopologyServer

impl Handler<TaskRequest> for TopologyActor {
    type Result = ();

    /// Handle `TaskRequest` sent from a connected `TopologySession`
    fn handle(&mut self, msg: TaskRequest, _ctx: &mut Context<Self>) {
        match &msg {
            TaskRequest::GetAvailable(_, _, _) => {
                // forward this to pipeline actor
                let pipeline = PipelineActor::from_registry();
                if !pipeline.connected() {
                    // TODO: handle implications here
                    error!(
                        target: TARGET_TOPOLOGY_ACTOR,
                        "PipelineActor isn't connected, dropping GetAvailable request"
                    );
                    self.metrics.incr_labels(
                        vec!["task", "request", "dropped"],
                        vec![("reason", "pipeline_disconnected")],
                    );
                    return;
                }
                pipeline.do_send(msg);
            }
            TaskRequest::GetAvailableResponse(_, _, _) => {
                let topology_server = TopologyServer::from_registry();
                if !topology_server.connected() {
                    // TODO: handle implications here. Does this mean the TopologyServer is no longer running?
                    error!(
                        target: TARGET_TOPOLOGY_ACTOR,
                        "TopologyServer isn't connected, dropping GetAvailableResponse"
                    );
                    self.metrics.incr_labels(
                        vec!["task", "response", "dropped"],
                        vec![("reason", "topology_server_connected_error")],
                    );
                    return;
                }
                topology_server.do_send(msg);
            }
        }
    }
}

// Client -> Server response for processed tasks

impl Handler<TaskResponse> for TopologyActor {
    type Result = ();

    /// Handle `TaskResponse` sent from a connected `TopologySession`
    /// Converts the response into a PipelineMsg and then forwards it to
    /// the `PipelineActor`
    fn handle(&mut self, msg: TaskResponse, _ctx: &mut Context<Self>) {
        // println!("Handler<TaskResponse> for TopologyActor: {:?}", &msg);
        // forward this to the pipeline actor
        let pipeline = PipelineActor::from_registry();
        if !pipeline.connected() {
            // TODO: handle implications here.
            error!(
                target: TARGET_TOPOLOGY_ACTOR,
                "PipelineActor isn't connected, dropping TaskResponse"
            );
            self.metrics.incr_labels(
                vec!["task", "response", "dropped"],
                vec![("reason", "pipeline_disconnected")],
            );
            return;
        }
        let pipe_msg = match &msg {
            TaskResponse::Ack(_, _, _, _) => PipelineMsg::TaskAck(msg),
            TaskResponse::Error(_, _, _) => PipelineMsg::TaskError(msg),
        };
        pipeline.do_send(pipe_msg);
    }
}

impl Handler<PipelineMsg> for TopologyActor {
    type Result = ();

    /// Handle `PipelineMsg` sent from the `PipelineActor`.
    /// Successfully processed messages are directed to the `SourceActor` for acking.
    /// Messages with errors are are passed to `self.handle_failure`
    fn handle(&mut self, msg: PipelineMsg, _ctx: &mut Context<Self>) {
        // println!("Handler<PipelineMsg> for TopologyActor {:?}", &msg);
        match msg {
            PipelineMsg::SourceMsgAck(msg_id) => {
                let source = SourceActor::from_registry();
                if !source.connected() {
                    error!(
                        target: TARGET_TOPOLOGY_ACTOR,
                        "SourceActor isn't connected, dropping PipelineMsg::SourceMsgAck"
                    );
                    self.metrics.incr_labels(
                        vec!["source", "msg", "ack", "dropped"],
                        vec![("reason", "source_disconnected")],
                    );
                    return;
                }

                // We have to cleanup the retry inflight data
                // if retry is initialized
                if self.retry.is_some() {
                    let retry = self.retry.as_mut().unwrap();
                    retry.delete(msg_id.clone());
                }

                // send msg to source ack_queue
                let _ = source.try_send(SourceAckMsg(msg_id));

                self.metrics
                    .incr_labels(vec!["source", "msg", "ack"], vec![("from", "pipeline")]);
            }
            PipelineMsg::SourceMsgTimeout(msg_id) => {
                debug!(
                    target: TARGET_TOPOLOGY_ACTOR,
                    "Pipeline timeout msg: {:?}", &msg_id
                );
                self.handle_failure(msg_id);
                self.metrics
                    .incr_labels(vec!["source", "msg", "timeout"], vec![("from", "pipeline")]);
            }
            PipelineMsg::SourceMsgError(msg_id) => {
                debug!(
                    target: TARGET_TOPOLOGY_ACTOR,
                    "Pipeline error msg: {:?}", &msg_id
                );
                self.handle_failure(msg_id);
                self.metrics
                    .incr_labels(vec!["source", "msg", "error"], vec![("from", "pipeline")]);
            }
            _ => {}
        }
    }
}

impl Handler<metric::backend::Flush> for TopologyActor {
    type Result = ();
    /// Handle `metric::backend::Flush` messages.
    fn handle(&mut self, _msg: metric::backend::Flush, _ctx: &mut Context<Self>) {
        self.metrics.flush();
    }
}

#[derive(Message, Debug)]
enum PipelineMsg {
    TaskRoot(TopologySourceMsg),
    TaskAck(TaskResponse),
    TaskError(TaskResponse),
    SourceMsgAck(MsgId),
    SourceMsgTimeout(MsgId),
    SourceMsgError(MsgId),
}

/// Main actor for tracking messages and where they
/// are in the pipeline at any given time.
///
/// The PipelineActor uses the pipeline definition as a
/// template. When a message enters the Pipeline,
/// an entry is added to the `inflight` data structure.
/// This inflight member keeps track of a message and which edges
/// have been visited. Once all edges are visited a message is
/// ready for acking with the source.
///
/// Pipeline message aggregation works like this:
/// - A message is read from a source and added to the Pipeline and made available for the root task.
/// - The root task will take the original source message and handle it.
/// - When the task root is finished, it can send back the original message,
///   X number of new messages, or no messages at all.
///
/// In the case where the task root sends back X number of new messages, we have a new problem:
/// Let's say one of those X messages is sent to another Task which generates Y number of messages.
/// How do we know when we're done processing the original message so we can ack it?
///
/// To address this possibility: Task results (messages generated by task handlers) are aggregated before
/// being made available to the next set of descendent tasks.  Internally, these are called sub-messages
/// (or sub-tasks in some comments). This gives us a mechanism to track progress for any given source message.
///
/// This should warrant more explanation with examples (TBD later).
#[derive(Default)]
pub struct PipelineActor {
    /// Pipeline task definitions and graph
    pub pipeline: Pipeline,
    /// Inflight messages and their state
    /// HashMap<msg_id, (timestamp, state)>
    pub inflight: PipelineInflight,
    /// Queues of available TaskMsg by task name
    pub available: PipelineAvailable,
    /// Aggregate messages by task_name and msg_id
    /// before making them available to descendant tasks
    pub aggregate: PipelineAggregate,
    /// Metrics
    pub metrics: Metrics,
}

impl Actor for PipelineActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        metric::backend::MetricsBackendActor::subscribe(
            "PipelineActor",
            ctx.address().clone().recipient(),
        );
    }
}

impl Supervised for PipelineActor {}

impl SystemService for PipelineActor {}

impl PipelineActor {
    /// Take the source message and add it to the Pipeline
    /// Make the message available to the task root
    /// and initialize the inflight message state.
    pub fn task_root(&mut self, src_msg: SourceMsg) {
        let matrix = self.pipeline.matrix.clone();
        let root = matrix[0].0.to_string();
        let task_name = matrix[0].1.to_string();
        let edge = (root, task_name.clone());

        // 1. msg state (we always start with 1 msg)
        let mut msg_state = PipelineMsgState::new(matrix);
        &msg_state.edge_start(edge.clone(), 1);

        // 2. inflight
        self.inflight
            .root(src_msg.id.clone(), src_msg.ts.clone(), msg_state);

        // 3. available
        // create TaskMsg and add it to PipelineAvailable
        // for the root task name
        let task_msg = TaskMsg {
            msg_id: src_msg.id.clone(),
            edge: edge,
            index: 0,
            msg: src_msg.msg.clone(),
        };

        self.available.push(&task_name, task_msg);

        // bump metrics
        self.metrics
            .incr_labels(vec!["task", "available"], vec![("task_name", &task_name)]);
    }

    /// Take the `TaskResponse` and ack the edge that was just
    /// processed by the task.
    pub(crate) fn task_ack(&mut self, task_resp: TaskResponse) {
        // println!("PipelineActor.task_resp: {:?}", task_resp);
        // update pending msg states
        // mark edge visited
        // move to the next task (if necessary, release aggregate holds)
        // determine if we have a timeout error

        match task_resp {
            TaskResponse::Ack(msg_id, edge, index, task_result) => {
                let ack_name = &edge.1[..];

                // grab the list of descendants for this edge
                let mut descendants = &vec![];
                match self.pipeline.descendants.get(ack_name) {
                    Some(names) => descendants = names,
                    None => {}
                };
                // println!("Ack name: {:?}, descendants: {:?}", &ack_name, &descendants);
                // store messages in aggregate hold
                if let Some(msgs) = task_result {
                    // println!("Aggregate {} msgs for descendants", msgs.len());
                    // We need to clone these message for all downstream descendants
                    for name in descendants.iter() {
                        self.aggregate
                            .hold(&name.to_string(), msg_id.clone(), msgs.clone());
                    }
                }

                // What's the PipelineInflightStatus for this msg id?
                let status = self.inflight.ack(&msg_id, &edge, index);
                // println!("Inflight status: {:?} {:?}", &msg_id, &status);

                match status {
                    PipelineInflightStatus::AckEdge(task_visited) => {
                        if task_visited {
                            // all ancestor edges are complete...
                            // time to release all aggregate messages siting in the
                            // holding pen to the descendant tasks

                            let mut ack_source = false;

                            for name in descendants.iter() {
                                // take the aggregate holding msgs
                                // wrap them in TaskMsg and move them to the holding
                                // queues
                                let next_edge = (ack_name.to_string(), name.to_string());
                                match self.aggregate.remove(&name.to_string(), &msg_id) {
                                    Some(msgs) => {
                                        // println!("Release msgs: {:?}=>{}", &next_edge, msgs.len());
                                        // we need to update the inflight msg state here
                                        if let Some((_ts, msg_state)) =
                                            self.inflight.get_mut(&msg_id)
                                        {
                                            msg_state.edge_start(next_edge.clone(), msgs.len());
                                        } else {
                                            // TODO:
                                            // We might not have a msg_state if the source msg timed out
                                            // and we cleaned up... in this case we should skip moving to
                                            // to the available queues
                                        }

                                        for (index, msg) in msgs.iter().enumerate() {
                                            let task_msg = TaskMsg {
                                                msg_id: msg_id.clone(),
                                                edge: next_edge.clone(),
                                                index: index,
                                                msg: msg.to_vec(),
                                            };
                                            self.available.push(&name.to_string(), task_msg);
                                        }
                                    }
                                    None => {
                                        // what happens if we reach this?
                                        // mark this next_edge as visited
                                        self.inflight.ack_dead_end(
                                            &msg_id,
                                            &next_edge,
                                            &self.pipeline,
                                        );

                                        // if this put the msg_)id into a finished state...
                                        // we need to set the ack source flag
                                        if self.inflight.finished(&msg_id) {
                                            ack_source = true;
                                            // if this msg_id is finished...
                                            // do we really need to keep going here?
                                            break;
                                        }
                                    }
                                }
                            }

                            if ack_source {
                                debug!(
                                    target: TARGET_PIPELINE_ACTOR,
                                    "ack_deadend triggered force ack source for this msg_id: {:?}",
                                    &msg_id
                                );
                                let topology = TopologyActor::from_registry();
                                if !topology.connected() {
                                    // TODO: determine the implications here
                                    error!(target: TARGET_PIPELINE_ACTOR, "TopologyActor isn't connected, skipping PipelineMsg::SourceMsgAck");
                                    return;
                                }
                                topology.do_send(PipelineMsg::SourceMsgAck(msg_id));
                            }
                        } else {
                            // nothing to do but wait...
                        }
                    }
                    PipelineInflightStatus::AckSource => {
                        // println!("AckSource: {:?}", &msg_id);
                        let topology = TopologyActor::from_registry();
                        if !topology.connected() {
                            error!(
                                target: TARGET_PIPELINE_ACTOR,
                                "TopologyActor isn't connected, skipping PipelineMsg::SourceMsgAck"
                            );
                            return;
                        }
                        // Cleanup after msg_id
                        self.cleanup(&msg_id);
                        topology.do_send(PipelineMsg::SourceMsgAck(msg_id));
                    }
                    PipelineInflightStatus::PendingEdge => {
                        // Nothing to do but wait for this edge to finish processing
                        // message for the edge
                        debug!(
                            target: TARGET_PIPELINE_ACTOR,
                            "PendingEdge: waiting to finish msg state: {:?}", &msg_id
                        );
                    }
                    PipelineInflightStatus::Removed => {
                        // This msg_id no longer exists in the inflight messages
                        // assume the cleanup process already took care of this...
                        // Call cleanup again to remove anything related
                        // to this message id
                        debug!(
                            target: TARGET_PIPELINE_ACTOR,
                            "PipelineInflightStatus::Removed {:?}", &msg_id
                        );
                        self.cleanup(&msg_id);
                    }
                    PipelineInflightStatus::Timeout => {
                        let topology = TopologyActor::from_registry();
                        if !topology.connected() {
                            error!(
                                target: TARGET_PIPELINE_ACTOR,
                                "TopologyActor isn't connected, skipping PipelineMsg::SourceMsgTimeout"
                            );
                            return;
                        }
                        self.cleanup(&msg_id);
                        topology.do_send(PipelineMsg::SourceMsgTimeout(msg_id));
                    }
                }
            }
            TaskResponse::Error(msg_id, ..) => {
                let topology = TopologyActor::from_registry();
                if !topology.connected() {
                    error!(
                        target: TARGET_PIPELINE_ACTOR,
                        "TopologyActor isn't connected, skipping PipelineMsg::SourceMsgError"
                    );
                    return;
                }
                self.cleanup(&msg_id);
                topology.do_send(PipelineMsg::SourceMsgError(msg_id));
            }
        }
    }

    pub fn cleanup(&mut self, msg_id: &MsgId) {
        self.inflight.clean_msg_id(msg_id);
        self.aggregate.clean_msg_id(msg_id);
    }
}

/// Handle PipelineMsg for PipelineActor

impl Handler<PipelineMsg> for PipelineActor {
    type Result = ();

    /// Handle `PipelineMsg` sent from the `TopologyActor`
    fn handle(&mut self, msg: PipelineMsg, _ctx: &mut Context<Self>) {
        match msg {
            PipelineMsg::TaskRoot(topology_src_msg) => {
                debug!(
                    target: TARGET_PIPELINE_ACTOR,
                    "PipelineMsg::TaskRoot(src_msg) = {:?}", &topology_src_msg
                );
                self.task_root(topology_src_msg.msg);
            }
            PipelineMsg::TaskAck(task_resp) => {
                // this should return a list of PipelineMsg's
                // for sending back up through TopologyActor
                // if we're done processing the original message
                // An Option(None) means we're still not ready
                // for Source Ack yet...
                debug!(
                    target: TARGET_PIPELINE_ACTOR,
                    "PipelineMsg::TaskAck(task_resp) = {:?}", &task_resp
                );
                self.task_ack(task_resp);
            }
            PipelineMsg::TaskError(task_resp) => {
                // update available & aggregate (if necessary)
                // and send PipelineMsg::Ack to topology_addr
                debug!(
                    target: TARGET_PIPELINE_ACTOR,
                    "PipelineMsg::TaskError(task_resp) unimplemented"
                );
                self.task_ack(task_resp);
            }
            _ => {
                // not implemented here
                warn!(
                    target: TARGET_PIPELINE_ACTOR,
                    "Handler<PipelineMsg> for PipelineMsg reach match unimplemented match arm for msg: {:?}",
                    &msg
                );
            }
        }
    }
}

impl Handler<TaskRequest> for PipelineActor {
    type Result = ();

    /// Handle `TaskRequest` messages and send responses back
    /// to the connected Task service.
    fn handle(&mut self, msg: TaskRequest, _ctx: &mut Context<Self>) {
        match msg {
            TaskRequest::GetAvailable(session_id, name, count) => {
                let topology = TopologyActor::from_registry();
                if !topology.connected() {
                    error!(
                        target: TARGET_PIPELINE_ACTOR,
                        "TopologyActor isn't connected, skipping  TaskRequest::GetAvailable"
                    );
                    return;
                }
                trace!(
                    target: TARGET_PIPELINE_ACTOR,
                    "TaskRequest::GetAvailable(session_id, name, count): {}, {}, {:?}",
                    &session_id,
                    &name,
                    &count
                );
                let tasks = self.available.pop(&name, count);
                topology.do_send(TaskRequest::GetAvailableResponse(session_id, name, tasks));
            }
            _ => {
                warn!(
                    target: TARGET_PIPELINE_ACTOR,
                    "Handler<TaskRequest> for TaskRequest only implements TaskRequest::GetAvailable"
                );
            }
        }
    }
}

impl Handler<metric::backend::Flush> for PipelineActor {
    type Result = ();

    /// Handle `metric::backend::Flush` messages
    fn handle(&mut self, _msg: metric::backend::Flush, _ctx: &mut Context<Self>) {
        // Mark available,
        self.metrics
            .gauge(vec!["inflight"], self.inflight.size() as isize);
        let stats1 = self.available.stats();
        trace!("Available len {}", &stats1.len());
        for (task, size) in stats1 {
            self.metrics
                .gauge_labels(vec!["available"], size, vec![("task", &task)]);
        }
        let stats2 = self.aggregate.stats();
        trace!("aggregate stats len {}", &stats2.len());
        for (task, size) in stats2 {
            self.metrics
                .gauge_labels(vec!["aggregate"], size, vec![("task", &task)]);
        }
        self.metrics.flush();
    }
}
