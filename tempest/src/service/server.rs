use actix::prelude::*;
use actix::Response;
use rand::{self, Rng};
use std::collections::{HashMap, HashSet};

use crate::common::logger::*;
use crate::metric::{self, Metrics};
use crate::service::codec::TopologyResponse;
use crate::service::session;
use crate::topology::{TaskMsg, TaskRequest};

static TARGET_TOPOLOGY_SERVER: &'static str = "tempest::service::TopologyServer";

pub struct Connect {
    pub addr: Addr<session::TopologySession>,
}

/// Response type for Connect message
///
/// Chat server returns unique session id
impl actix::Message for Connect {
    type Result = usize;
}

/// Session is disconnected
#[derive(Message)]
pub struct Disconnect {
    pub id: usize,
}

/// `TopologyServer`

pub struct TopologyServer {
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
impl Handler<Connect> for TopologyServer {
    type Result = usize;

    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) -> Self::Result {
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

    fn handle(&mut self, msg: TaskRequest, ctx: &mut Context<Self>) {
        // println!("Handler<TaskRequest> for TopologyServer: {:?}", &msg);
        match msg {
            TaskRequest::GetAvailableResponse(session_id, name, tasks) => {
                // TODO: this client might not exist anymore...
                // we need a way to put that back?
                // println!("returning tasks for session id: {}", &session_id);
                match self.sessions.get_mut(&session_id) {
                    Some(session) => {
                        // println!("TaskRequest::ReturnAvailable tasks: {:?}", &tasks);
                        session.do_send(TopologyResponse::TaskGet(tasks));
                    }
                    None => {
                        println!(
                            "Can't find session matching this id: total sessions: {}",
                            self.sessions.len()
                        );
                    }
                }
            }
            _ => {
                println!("here borked");
            }
        }
    }
}

impl Handler<metric::backend::Flush> for TopologyServer {
    type Result = ();

    fn handle(&mut self, msg: metric::backend::Flush, ctx: &mut Context<Self>) {
        self.metrics.flush();
    }
}
