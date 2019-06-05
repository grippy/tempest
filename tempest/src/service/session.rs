use actix::prelude::*;
use std::io;
use std::time::{Duration, Instant};
use tokio_io::io::WriteHalf;
use tokio_tcp::TcpStream;

use crate::common::logger::*;
use crate::service::codec::{TopologyCodec, TopologyRequest, TopologyResponse};
use crate::service::server::{self, TopologyServer};
use crate::topology::{TaskRequest, TopologyActor};

static TARGET_TOPOLOGY_SESSION: &'static str = "tempest::service::TopologySession";

pub struct TopologySession {
    /// unique session id
    id: usize,
    /// this is address of chat server
    addr: Addr<TopologyServer>,
    /// Client must send ping at least once per 10 seconds, otherwise we drop
    /// connection.
    hb: Instant,
    /// Framed wrapper
    framed: actix::io::FramedWrite<WriteHalf<TcpStream>, TopologyCodec>,
}

impl Actor for TopologySession {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // we'll start heartbeat process on session start.
        self.hb(ctx);

        // register self in chat server. `AsyncContext::wait` register
        // future within context, but context waits until this future resolves
        // before processing any other events.
        self.addr
            .send(server::Connect {
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
        // notify chat server
        self.addr.do_send(server::Disconnect { id: self.id });
        Running::Stop
    }
}

impl actix::io::WriteHandler<io::Error> for TopologySession {}

impl StreamHandler<TopologyRequest, io::Error> for TopologySession {
    fn handle(&mut self, msg: TopologyRequest, ctx: &mut Self::Context) {
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

    fn handle(&mut self, msg: TopologyResponse, ctx: &mut Self::Context) {
        // println!(" Handler<TopologyResponse> for TopologySession: {:?}", &msg);
        self.framed.write(msg);
    }
}

/// Helper methods
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

    /// helper method that sends ping to client every second.
    ///
    /// also this method check heartbeats from client
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
