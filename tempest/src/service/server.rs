use actix::prelude::*;
use rand::{self, Rng};
use std::collections::HashMap;

use crate::common::logger::*;
use crate::metric::{self, AggregateMetrics, Metrics};
use crate::service::codec::{AgentRequest, TopologyResponse};
use crate::service::session;
use crate::topology::TaskRequest;

static TARGET_TOPOLOGY_SERVER: &'static str = "tempest::service::TopologyServer";
static TARGET_AGENT_SERVER: &'static str = "tempest::service::AgentServer";

pub(crate) struct TopologyConnect {
    pub addr: Addr<session::TopologySession>,
}

/// Response type for Connect message
///
impl actix::Message for TopologyConnect {
    type Result = usize;
}

/// Session is disconnected
///
#[derive(Message)]
pub(crate) struct Disconnect {
    pub id: usize,
}

/// `TopologyServer`
///
pub(crate) struct TopologyServer {
    sessions: HashMap<usize, Addr<session::TopologySession>>,
    metrics: Metrics,
}

impl Default for TopologyServer {
    fn default() -> TopologyServer {
        TopologyServer {
            sessions: HashMap::new(),
            metrics: Metrics::default().named(vec!["topology", "server"]),
        }
    }
}

impl Supervised for TopologyServer {}

impl SystemService for TopologyServer {}

impl Actor for TopologyServer {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        metric::backend::MetricsBackendActor::subscribe(
            "TopologyServer",
            ctx.address().clone().recipient(),
        );
    }
}

/// Handler for Connect message.
///
/// Register new session and assign unique id to this session
impl Handler<TopologyConnect> for TopologyServer {
    type Result = usize;

    fn handle(&mut self, msg: TopologyConnect, _: &mut Context<Self>) -> Self::Result {
        // register session with random id
        let id = rand::thread_rng().gen::<usize>();
        info!(
            target: TARGET_TOPOLOGY_SERVER,
            "TaskService client {} joined", &id
        );
        self.sessions.insert(id, msg.addr);
        // send id back
        id
    }
}

/// Handler for Disconnect message.
impl Handler<Disconnect> for TopologyServer {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        info!(
            target: TARGET_TOPOLOGY_SERVER,
            "TaskService client {} disconnected", &msg.id
        );
        // remove address
        self.sessions.remove(&msg.id);
    }
}

impl Handler<TaskRequest> for TopologyServer {
    type Result = ();

    fn handle(&mut self, msg: TaskRequest, _ctx: &mut Context<Self>) {
        // println!("Handler<TaskRequest> for TopologyServer: {:?}", &msg);
        match msg {
            TaskRequest::GetAvailableResponse(session_id, _name, tasks) => {
                // TODO: this client might not exist anymore...
                // we need a way to put that back?
                // println!("returning tasks for session id: {}", &session_id);
                match self.sessions.get_mut(&session_id) {
                    Some(session) => {
                        // println!("TaskRequest::ReturnAvailable tasks: {:?}", &tasks);
                        session.do_send(TopologyResponse::TaskGet(tasks));
                    }
                    None => {
                        error!(
                            "Can't find session with id {} (out of {} sessions)",
                            &session_id,
                            self.sessions.len(),
                        );
                    }
                }
            }
            _ => {
                error!(
                    "Handler<TaskRequest> for TopologyServer not implemented for this msg: {:?}",
                    &msg
                );
            }
        }
    }
}

impl Handler<metric::backend::Flush> for TopologyServer {
    type Result = ();

    fn handle(&mut self, _msg: metric::backend::Flush, _ctx: &mut Context<Self>) {
        self.metrics.flush();
    }
}

/// `AgentServer`
///

pub(crate) struct AgentConnect {
    pub addr: Addr<session::AgentSession>,
}

/// Response type for Connect message
///
impl actix::Message for AgentConnect {
    type Result = usize;
}

pub(crate) struct AgentServer {
    sessions: HashMap<usize, Addr<session::AgentSession>>,
    aggregate_metrics: AggregateMetrics,
    tmp_file_suffix: String,
}

impl Default for AgentServer {
    fn default() -> Self {
        AgentServer {
            sessions: HashMap::new(),
            aggregate_metrics: AggregateMetrics::default(),
            tmp_file_suffix: "default".to_string(),
        }
    }
}

impl AgentServer {
    pub fn new(tmp_file_suffix: String) -> Self {
        AgentServer {
            tmp_file_suffix: tmp_file_suffix,
            ..AgentServer::default()
        }
    }
}

impl Handler<AgentRequest> for AgentServer {
    type Result = ();

    fn handle(&mut self, msg: AgentRequest, _ctx: &mut Context<Self>) {
        // println!("Handler<AgentRequest> for AgentServer: {:?}", &msg);
        match msg {
            AgentRequest::AggregateMetricsPut(aggregate) => {
                for (key, value) in aggregate.counters.iter() {
                    // always replace existing value
                    self.aggregate_metrics.insert(key.to_string(), *value);
                }
                // write tmp file...
                self.aggregate_metrics.write_tmp(&self.tmp_file_suffix);
            }
            _ => {
                println!(
                    "impl Handler<AgentRequest> for AgentServer missing match arm for msg {:?}",
                    &msg
                );
            }
        }
    }
}

impl Supervised for AgentServer {}

impl SystemService for AgentServer {}

impl Actor for AgentServer {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {}
}

/// Handler for Connect message.
///
/// Register new session and assign unique id to this session
impl Handler<AgentConnect> for AgentServer {
    type Result = usize;

    fn handle(&mut self, msg: AgentConnect, _: &mut Context<Self>) -> Self::Result {
        // register session with random id
        let id = rand::thread_rng().gen::<usize>();
        info!(target: TARGET_AGENT_SERVER, "client {} joined", &id);
        self.sessions.insert(id, msg.addr);
        // send id back
        id
    }
}

impl Handler<Disconnect> for AgentServer {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        info!(
            target: TARGET_AGENT_SERVER,
            "client {} disconnected", &msg.id
        );
        self.sessions.remove(&msg.id);
    }
}
