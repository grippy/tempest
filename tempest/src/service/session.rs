use actix::prelude::*;
use std::io;
use std::time::{Duration, Instant};
use tokio_io::io::WriteHalf;
use tokio_tcp::TcpStream;

use crate::common::logger::*;
use crate::service::codec::{
    AgentCodec, AgentRequest, AgentResponse, TopologyCodec, TopologyRequest, TopologyResponse,
};
use crate::service::server::{self, AgentServer, TopologyServer};
use crate::topology::{TaskRequest, TopologyActor};

static TARGET_TOPOLOGY_SESSION: &'static str = "tempest::service::TopologySession";
static TARGET_AGENT_SESSION: &'static str = "tempest::service::AgentSession";

pub(crate) struct TopologySession {
    id: usize,
    addr: Addr<TopologyServer>,
    hb: Instant,
    framed: actix::io::FramedWrite<WriteHalf<TcpStream>, TopologyCodec>,
}

impl Actor for TopologySession {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // we'll start heartbeat process on session start.
        self.hb(ctx);
        self.addr
            .send(server::TopologyConnect {
                addr: ctx.address(),
            })
            .into_actor(self)
            .then(|res, act, ctx| {
                match res {
                    Ok(res) => act.id = res,
                    _ => ctx.stop(),
                }
                actix::fut::ok(())
            })
            .wait(ctx);
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        self.addr.do_send(server::Disconnect { id: self.id });
        Running::Stop
    }
}

impl actix::io::WriteHandler<io::Error> for TopologySession {}

impl StreamHandler<TopologyRequest, io::Error> for TopologySession {
    fn handle(&mut self, msg: TopologyRequest, _ctx: &mut Self::Context) {
        match msg {
            // we update heartbeat time on ping from peer
            TopologyRequest::Ping => {
                self.hb = Instant::now();
            }
            // respond to a TaskGet request
            TopologyRequest::TaskGet(name, count) => {
                let topology = TopologyActor::from_registry();
                if !topology.connected() {
                    // TODO: handle the implication here
                    warn!(
                        target: TARGET_TOPOLOGY_SESSION,
                        "TopologyActor isn't connected, dropping TopologyRequest::TaskGet"
                    );
                    return;
                }
                let task_req = TaskRequest::GetAvailable(self.id, name, Some(count as usize));
                let _ = topology.try_send(task_req);
            }

            // respond to a TaskPut request
            TopologyRequest::TaskPut(task_resp) => {
                // From here we need to send this into the TopologyActor
                // as a TaskResponse
                let topology = TopologyActor::from_registry();
                if !topology.connected() {
                    warn!(target: TARGET_TOPOLOGY_SESSION, "TopologyActor isn't connected, dropping TopologyRequest::TaskPut(task_resp)");
                    return;
                }
                let _ = topology.try_send(task_resp);
            }
        }
    }
}

impl Handler<TopologyResponse> for TopologySession {
    type Result = ();

    fn handle(&mut self, msg: TopologyResponse, _ctx: &mut Self::Context) {
        // println!(" Handler<TopologyResponse> for TopologySession: {:?}", &msg);
        self.framed.write(msg);
    }
}

impl TopologySession {
    pub fn new(
        addr: Addr<TopologyServer>,
        framed: actix::io::FramedWrite<WriteHalf<TcpStream>, TopologyCodec>,
    ) -> TopologySession {
        TopologySession {
            addr,
            framed,
            id: 0,
            hb: Instant::now(),
        }
    }

    fn hb(&self, ctx: &mut actix::Context<Self>) {
        ctx.run_later(Duration::new(1, 0), |act, ctx| {
            // check client heartbeats
            if Instant::now().duration_since(act.hb) > Duration::new(10, 0) {
                // heartbeat timed out
                warn!(
                    target: TARGET_TOPOLOGY_SESSION,
                    "Client heartbeat failed, disconnecting!"
                );
                // notify server
                act.addr.do_send(server::Disconnect { id: act.id });
                // stop actor
                ctx.stop();
            }
            act.framed.write(TopologyResponse::Ping);
            act.hb(ctx);
        });
    }
}

/// AgentSession
///

pub(crate) struct AgentSession {
    /// unique session id
    id: usize,
    addr: Addr<AgentServer>,
    framed: actix::io::FramedWrite<WriteHalf<TcpStream>, AgentCodec>,
    hb: Instant,
}

impl AgentSession {
    pub fn new(
        addr: Addr<AgentServer>,
        framed: actix::io::FramedWrite<WriteHalf<TcpStream>, AgentCodec>,
    ) -> AgentSession {
        AgentSession {
            id: 0,
            addr,
            framed,
            hb: Instant::now(),
        }
    }

    fn hb(&self, ctx: &mut actix::Context<Self>) {
        ctx.run_later(Duration::new(5, 0), |act, ctx| {
            // check client heartbeats
            if Instant::now().duration_since(act.hb) > Duration::new(10, 0) {
                // heartbeat timed out
                warn!(
                    target: TARGET_AGENT_SESSION,
                    "Client heartbeat failed, disconnecting!",
                );
                // notify server
                act.addr.do_send(server::Disconnect { id: act.id });
                // stop actor
                ctx.stop();
            }
            act.framed.write(AgentResponse::Ping);
            act.hb(ctx);
        });
    }
}

impl Actor for AgentSession {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);
        self.addr
            .send(server::AgentConnect {
                addr: ctx.address(),
            })
            .into_actor(self)
            .then(|res, act, ctx| {
                match res {
                    Ok(res) => act.id = res,
                    _ => ctx.stop(),
                }
                actix::fut::ok(())
            })
            .wait(ctx);
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        self.addr.do_send(server::Disconnect { id: self.id });
        Running::Stop
    }
}

impl actix::io::WriteHandler<io::Error> for AgentSession {}

impl StreamHandler<AgentRequest, io::Error> for AgentSession {
    fn handle(&mut self, msg: AgentRequest, _ctx: &mut Self::Context) {
        // println!("StreamHandler<AgentRequest,> for AgentSession {:?}", &msg);
        match &msg {
            AgentRequest::Ping => {
                self.hb = Instant::now();
            }
            AgentRequest::AggregateMetricsPut(_aggregate) => {
                // send the aggregate to the server addr
                self.addr.do_send(msg);
            }
        }
    }
}

impl Handler<AgentResponse> for AgentSession {
    type Result = ();

    fn handle(&mut self, msg: AgentResponse, _ctx: &mut Self::Context) {
        // println!("Handler<AgentResponse> for AgentSession {:?} {:?}", &self.id, &msg);
        self.framed.write(msg);
    }
}
