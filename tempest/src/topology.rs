use actix::prelude::*;
use serde_derive::{Deserialize, Serialize};
use serde_json;
use std::collections::VecDeque;
use std::error;
use std::fmt;
use std::mem;
use std::time::Duration;

use crate::common::logger::*;
use crate::common::now_millis;
use crate::metric::{self, Metrics};
use crate::pipeline::*;
use crate::service::server::TopologyServer;
use crate::source::*;

/// Log targets
static TARGET_SOURCE_ACTOR: &'static str = "tempest::topology::SourceActor";
static TARGET_TOPOLOGY_ACTOR: &'static str = "tempest::topology::TopologyActor";
static TARGET_PIPELINE_ACTOR: &'static str = "tempest::topology::PipelineActor";

/**
 * This is what a user implements in order to run a topology
 *
 */
pub trait Topology<SB: SourceBuilder> {
    // should return a topology service
    // constructed from the builder
    fn service() {}

    fn builder() -> TopologyBuilder<SB>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum TopologyFailurePolicy {
    BestEffort,
    Retry(usize),
}

impl Default for TopologyFailurePolicy {
    fn default() -> Self {
        TopologyFailurePolicy::BestEffort
    }
}

// Some of these properties are stored as String
// to make it easier to share with Actix actors
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopologyOptions {
    /// Name of the topology
    pub name: String,

    /// The tempest db uri
    db_uri: Option<String>,

    /// Topology id (host:port)
    topology_id: Option<String>,

    /// The host:port of the agent this topology
    /// should communicates with
    agent_id: Option<String>,

    /// How should we handle failures?
    failure_policy: Option<TopologyFailurePolicy>,

    /// Max amount of time, in milliseconds, to wait before
    /// moving a pending message into a failure state.
    /// What happens after a timeout depends on
    /// the failure policy configuration.
    msg_timeout: Option<usize>,
}

impl TopologyOptions {
    pub fn name(&mut self, name: &'static str) {
        self.name = name.to_string();
    }

    pub fn failure_policy(&mut self, policy: TopologyFailurePolicy) {
        self.failure_policy = Some(policy);
    }

    pub fn msg_timeout(&mut self, ms: usize) {
        self.msg_timeout = Some(ms);
    }

    // set at runtime: host:port
    pub fn topology_id(&mut self, id: String) {
        self.topology_id = Some(id);
    }

    // set at runtime: redis://host:port/db
    pub fn db_uri(&mut self, uri: String) {
        self.db_uri = Some(uri);
    }

    // set at runtime: host:port
    pub fn agent_id(&mut self, id: String) {
        self.agent_id = Some(id);
    }
}

#[derive(Default)]
pub struct TopologyBuilder<SB: SourceBuilder> {
    pub options: TopologyOptions,
    pub pipeline: Pipeline,
    pub source_builder: SB,
}

impl<SB> TopologyBuilder<SB>
where
    SB: SourceBuilder + Default,
    <SB as SourceBuilder>::Source: Source + 'static + Default,
{
    pub fn name(mut self, name: &'static str) -> Self {
        self.options.name(name);
        self
    }

    pub fn failure_policy(mut self, policy: TopologyFailurePolicy) -> Self {
        self.options.failure_policy = Some(policy);
        self
    }

    pub fn msg_timeout(mut self, ms: usize) -> Self {
        self.options.msg_timeout = Some(ms);
        self
    }

    pub fn pipeline(mut self, pipe: Pipeline) -> Self {
        self.pipeline = pipe.build();
        self
    }

    pub fn source(mut self, sb: SB) -> Self {
        self.source_builder = sb;
        self
    }

    pub fn source_actor(&self) -> SourceActor {
        SourceActor {
            source: Box::new(self.source_builder.build()),
            ack_queue: VecDeque::new(),
            backoff: 1u64,
            metrics: Metrics::default().named(vec!["source"]),
        }
    }

    pub fn topology_actor(&self) -> TopologyActor {
        TopologyActor {
            options: self.options.clone(),
            metrics: Metrics::default().named(vec!["topology"]),
        }
    }

    pub fn pipeline_actor(&self) -> PipelineActor {
        PipelineActor {
            pipeline: self.pipeline.clone(),
            inflight: PipelineInflight::new(self.options.msg_timeout.clone()),
            available: PipelineAvailable::new(&self.pipeline.tasks),
            aggregate: PipelineAggregate::new(&self.pipeline.tasks),
            metrics: Metrics::default().named(vec!["pipeline"]),
        }
    }
}

/// Actors

// A Task can return multiple messages
// in a response which are then passed into
// the next TaskService. For this reason,
// we need to keep track
// of the index of the sub-task so we know
// when to mark the edge as visted.

#[derive(Serialize, Deserialize, Message, Debug)]
pub struct TaskMsg {
    pub source_id: MsgId,
    pub edge: Edge,
    pub index: usize,
    pub msg: Msg,
}

#[derive(Message, Debug)]
pub enum TaskRequest {
    // session_id, task_name, count
    GetAvailable(usize, String, Option<usize>),
    // session_id, task_name, tasks
    GetAvailableResponse(usize, String, Option<Vec<TaskMsg>>),
}

#[derive(Message, Serialize, Deserialize, Debug)]
pub enum TaskResponse {
    // source_id, edge, index, response
    Ack(MsgId, Edge, usize, Option<Vec<Msg>>),
    // source_id, edge, index
    Error(MsgId, Edge, usize),
}

// Default Source stub to get around having to implement SourceActor::default
// and not knowing what the Source type is
pub struct DefaultSource {}
impl Source for DefaultSource {
    fn name(&self) -> &'static str {
        "Default"
    }

    fn healthy(&mut self) -> SourceResult<()> {
        unimplemented!("Failed to run healthy check")
    }
}

pub struct SourceActor {
    // Some struct that implements Source trait
    source: Box<Source>,
    // This queue of messages to ack
    ack_queue: VecDeque<MsgId>,
    // backoff delay
    backoff: u64,
    // metrics
    metrics: Metrics,
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
        }
    }
}

impl SourceActor {
    /// Resets backoff and poll_interval to the source config
    fn reset_backoff(&mut self) {
        let poll_interval = match self.source.poll_interval() {
            Ok(SourceInterval::Millisecond(ms)) => ms,
            Err(err) => &1000u64,
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
            Err(err) => {
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
                .counter(vec!["messages", "read"], msg_count as isize);
        }
        // What's our current backoff
        self.metrics.gauge(vec!["backoff"], self.backoff as isize);

        // reschedule poll again
        ctx.run_later(Duration::from_millis(self.backoff), Self::poll);

        let topology = TopologyActor::from_registry();
        if !topology.connected() {
            debug!(
                target: TARGET_SOURCE_ACTOR,
                "TopologyActor#poll topology actor isn't connected"
            );
            return;
        }
        // println!("Source polled {:?} msgs", results.len());
        // send these msg to the topology actor
        for msg in results {
            match topology.try_send(msg) {
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
                }
                Err(SendError::Closed(msg)) => {
                    // we need to kill this actor here
                    // TODO: move this to deadletter q?
                }
                _ => {}
            }
        }
    }

    /// Drain the batch_ack_queue and send all messages into the source.batch_ack method
    fn batch_ack(&mut self, _ctx: &mut Context<Self>) {
        let msgs = self.ack_queue.drain(..).collect::<Vec<_>>();
        let len = msgs.len();
        if len > 0 {
            trace!(target: TARGET_SOURCE_ACTOR, "Acking: {} msgs", &len);
            let result = self.source.batch_ack(msgs);
            self.ack_result(len, result);
        }
    }

    /// Drain the queue and ack individual msgs one at a time
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
                    .counter_labels(vec!["messages", "acked"], acked as isize, labels);
            }
            Err(err) => {
                self.metrics.counter_labels(
                    vec!["messages", "acked"],
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

    fn started(&mut self, ctx: &mut Context<Self>) {
        // setup the source here...
        // TODO: verify this isn't an error
        match self.source.setup() {
            Err(err) => {
                warn!(
                    target: TARGET_SOURCE_ACTOR,
                    "Failed to setup source... trigger shutdown here"
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
            Err(err) => &SourceInterval::Millisecond(1000),
        };

        // schedule next poll for batch or individual polling
        let duration = ack_interval.as_duration();
        if let Ok(SourceAckPolicy::Batch(batch_size)) = self.source.ack_policy() {
            ctx.run_interval(duration, Self::batch_ack);
        } else {
            ctx.run_interval(duration, Self::individual_ack);
        }

        metric::backend::MetricsBackendActor::subscribe(
            "SourceActor",
            ctx.address().clone().recipient(),
        );
    }
}

impl Supervised for SourceActor {}

impl SystemService for SourceActor {}

#[derive(Message, Debug)]
pub struct SourceAckMsg(MsgId);

impl Handler<SourceAckMsg> for SourceActor {
    type Result = ();

    fn handle(&mut self, msg: SourceAckMsg, ctx: &mut Context<Self>) {
        // println!("push ack_queue msg_id: {:?}", &msg);
        self.ack_queue.push_back(msg.0);
    }
}

impl Handler<metric::backend::Flush> for SourceActor {
    type Result = ();

    fn handle(&mut self, msg: metric::backend::Flush, ctx: &mut Context<Self>) {
        self.metrics.flush();
    }
}

#[derive(Default)]
pub struct TopologyActor {
    // topology options
    options: TopologyOptions,
    // metrics
    metrics: Metrics,
}

impl Actor for TopologyActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        metric::backend::MetricsBackendActor::subscribe(
            "TopologyActor",
            ctx.address().clone().recipient(),
        );
    }
}

impl Supervised for TopologyActor {}
impl SystemService for TopologyActor {}

// Source -> Pipeline

impl Handler<SourceMsg> for TopologyActor {
    type Result = ();

    fn handle(&mut self, msg: SourceMsg, ctx: &mut Context<Self>) {
        let pipeline = PipelineActor::from_registry();
        if !pipeline.connected() {
            // TODO: wire up a retry here?
            error!(
                target: TARGET_TOPOLOGY_ACTOR,
                "PipelineActor isn't connected, dropping msg: {:?}", &msg
            );
            self.metrics.incr_labels(
                vec!["messages", "dropped"],
                vec![("from", "source"), ("reason", "pipline_disconnected")],
            );
            return;
        }
        match pipeline.try_send(PipelineMsg::TaskRoot(msg)) {
            Err(SendError::Full(msg)) => {
                // TODO: wireup a "holding queue" and trigger backoff here
                error!(
                    target: TARGET_TOPOLOGY_ACTOR,
                    "PipelineActor mailbox is full, dropping msg: {:?}", &msg
                );
                self.metrics.incr_labels(
                    vec!["messages", "dropped"],
                    vec![("from", "source"), ("reason", "pipline_mailbox_full")],
                );
            }
            Err(SendError::Closed(msg)) => {
                // TODO: trigger shutdown here
                error!(
                    target: TARGET_TOPOLOGY_ACTOR,
                    "PipelineActor is closed, dropping msg: {:?}", &msg
                );
                self.metrics.incr_labels(
                    vec!["messages", "dropped"],
                    vec![("from", "source"), ("reason", "pipline_closed")],
                );
            }
            _ => {}
        }
    }
}

// Client -> Server request for available tasks and
// and response back out through the TopologyServer

impl Handler<TaskRequest> for TopologyActor {
    type Result = ();

    fn handle(&mut self, msg: TaskRequest, ctx: &mut Context<Self>) {
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
                        vec![("reason", "pipline_disconnected")],
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

    // This is for a message moving back up the chain...
    // mostly for stats purposes
    fn handle(&mut self, msg: TaskResponse, ctx: &mut Context<Self>) {
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
                vec![("reason", "pipline_disconnected")],
            );
            return;
        }
        let pipe_msg = match &msg {
            TaskResponse::Ack(_, _, _, _) => PipelineMsg::TaskAck(msg),
            TaskResponse::Error(_, _, _) => PipelineMsg::TaskAck(msg),
        };
        pipeline.do_send(pipe_msg);
    }
}

impl Handler<PipelineMsg> for TopologyActor {
    type Result = ();

    // This is for a message moving back up the chain...
    // mostly for stats purposes
    fn handle(&mut self, msg: PipelineMsg, ctx: &mut Context<Self>) {
        // println!("Handler<PipelineMsg> for TopologyActor {:?}", &msg);
        match msg {
            PipelineMsg::SourceMsgAck(msg_id) => {
                let source = SourceActor::from_registry();
                if !source.connected() {
                    // TODO: handle implications here.
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
                // send msg to source ack_queue
                let _ = source.try_send(SourceAckMsg(msg_id));

                // TODO: handle results for try_send
                self.metrics
                    .incr_labels(vec!["source", "msg", "ack"], vec![("from", "pipeline")]);
            }
            PipelineMsg::SourceMsgTimeout(msg_id) => {
                // TODO: What is the FailurePolicy here?
                info!(
                    target: TARGET_TOPOLOGY_ACTOR,
                    "PipelineMsg::SourceMsgTimeout(msg_id): unimplemented"
                );
                self.metrics
                    .incr_labels(vec!["source", "msg", "timeout"], vec![("from", "pipeline")]);
            }
            PipelineMsg::SourceMsgError(msg_id) => {
                // TODO: What is the FailurePolicy here?
                info!(
                    target: TARGET_TOPOLOGY_ACTOR,
                    "PipelineMsg::SourceMsgError(msg_id): unimplemented"
                );
                self.metrics
                    .incr_labels(vec!["source", "msg", "error"], vec![("from", "pipeline")]);
            }
            _ => {}
        }
    }
}

impl Handler<metric::backend::Flush> for TopologyActor {
    type Result = ();
    fn handle(&mut self, msg: metric::backend::Flush, ctx: &mut Context<Self>) {
        self.metrics.flush();
    }
}

#[derive(Message, Debug)]
pub enum PipelineMsg {
    TaskRoot(SourceMsg),
    TaskAck(TaskResponse),
    TaskError(TaskResponse),
    SourceMsgAck(MsgId),
    SourceMsgTimeout(MsgId),
    SourceMsgError(MsgId),
}

#[derive(Default)]
pub struct PipelineActor {
    /// Pipeline task definitions and graph
    pub pipeline: Pipeline,
    /// Messages in-flight and their state
    /// HashMap<source_msg_id, (timestamp, state)>
    pub inflight: PipelineInflight,
    /// Queues of available tasks for processing
    /// by connected TaskServices
    pub available: PipelineAvailable,
    /// Aggregate tasks by msg_id and task_name
    /// before making them available for downstream tasks
    pub aggregate: PipelineAggregate,
    /// metrics
    metrics: Metrics,
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
        // create TaskMsg and it to PipelineAvailable
        // for the starting task name
        let task_msg = TaskMsg {
            source_id: src_msg.id.clone(),
            edge: edge,
            index: 0,
            msg: src_msg.msg.clone(),
        };
        self.available.push(&task_name, task_msg);
    }

    pub fn task_ack(&mut self, task_resp: TaskResponse) {
        // println!("PipelineActor.task_resp: {:?}", task_resp);
        // update pending msg states
        // mark edge visted
        // move to the next task (if necessary, release aggregate holds)
        // determine if we have a timeout error

        match task_resp {
            TaskResponse::Ack(msg_id, edge, index, task_result) => {
                let ack_name = &edge.1[..];

                // metrics
                // let metric_base_name = format!("ack.{}_{}", &edge.0[..], &edge.1[..]);
                // let metric_ingress_marker = format!("{}.ingress", &metric_base_name);
                // ingress msg count is always one msg
                // self.metrics.marker(&metric_ingress_marker[..]);

                // grab the list of decendants for this edge
                let mut decendants = &vec![];
                match self.pipeline.decendants.get(ack_name) {
                    Some(names) => decendants = names,
                    None => {}
                };
                // println!("Ack name: {:?}, decendants: {:?}", &ack_name, &decendants);
                // store messages in aggregate hold
                if let Some(msgs) = task_result {
                    // track metrics for how many decendant messages we have now
                    // let metric_egress_count = format!("{}.egress.count", &metric_base_name);
                    // let metric_egress_gauge = format!("{}.egress.gauge", &metric_base_name);
                    // self.metrics.count(&metric_egress_count[..], msgs.len());
                    // self.metrics
                    // .gauge(&metric_egress_count[..], msgs.len() as isize);

                    // println!("Aggregate {} msgs for decendants", msgs.len());
                    // We need to clone these message for all downstream decendants
                    for name in decendants.iter() {
                        self.aggregate
                            .hold(&name.to_string(), msg_id.clone(), msgs.clone());
                    }
                }

                // What's the PipelineInflightStatus for this msg id?
                let status = self.inflight.ack(&msg_id, &edge, index);
                // println!("Inflight status: {:?} {:?}", &msg_id, &status);

                match status {
                    PipelineInflightStatus::AckEdge(task_visited) => {
                        // if this edge has multiple ancestors
                        // see if they've all be visited yet
                        if task_visited {
                            // TODO: is this still the case?
                            // this is where it gets a litte tricky...
                            // if we only have a single decendant we can proceed
                            // otherwise, if a decendant has multiple ancestors
                            // and we need to release all msgs at the same time

                            // all ancestor edges are complete...
                            // time to release all aggregate messages siting in the
                            // holding pen to the decendant tasks

                            let mut ack_source = false;

                            for name in decendants.iter() {
                                // take the aggregate holding msgs
                                // wrap them in TaskMsg and move them to the holding
                                // queues
                                let next_edge = (ack_name.to_string(), name.to_string());
                                match self.aggregate.remove(&name.to_string(), &msg_id) {
                                    Some(msgs) => {
                                        // println!("Release msgs: {:?}=>{}", &next_edge, msgs.len());
                                        // we need to update the inflight msg state here
                                        if let Some((ts, msg_state)) =
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
                                                source_id: msg_id.clone(),
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
                                        self.inflight.ack_deadend(
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
                        warn!(
                            target: TARGET_PIPELINE_ACTOR,
                            "PipelineInflightStatus::Removed unimplemented"
                        );
                        self.cleanup(&msg_id);
                    }
                    PipelineInflightStatus::Timeout => {
                        warn!(
                            target: TARGET_PIPELINE_ACTOR,
                            "PipelineInflightStatus::Timeout unimplemented"
                        );
                        self.cleanup(&msg_id);
                        // TODO: Figure out what to do here
                    }
                }
            }
            TaskResponse::Error(msg_id, edge, index) => {
                let metric_base_name = format!("pipeline.error.{}_{}", &edge.0[..], &edge.1[..]);
                // self.metrics.marker(&metric_base_name[..]);
                // all or nothing! this should trigger an AckError
                // which then bubbles up to the topology
                warn!(
                    target: TARGET_PIPELINE_ACTOR,
                    "TaskResponse::Error(msg_id, edge, index) unimplemented"
                );
                self.cleanup(&msg_id);
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

    fn handle(&mut self, msg: PipelineMsg, ctx: &mut Context<Self>) {
        match msg {
            PipelineMsg::TaskRoot(src_msg) => {
                debug!(
                    target: TARGET_PIPELINE_ACTOR,
                    "PipelineMsg::TaskRoot(src_msg) = {:?}", &src_msg
                );
                self.task_root(src_msg);
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
                warn!(
                    target: TARGET_PIPELINE_ACTOR,
                    "PipelineMsg::TaskError(task_resp) unimplemented"
                );
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

    fn handle(&mut self, msg: TaskRequest, ctx: &mut Context<Self>) {
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

    fn handle(&mut self, msg: metric::backend::Flush, ctx: &mut Context<Self>) {
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
