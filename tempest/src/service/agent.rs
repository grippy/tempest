use std::net;
use std::str::FromStr;
use std::time::{Duration, Instant};

use actix::prelude::*;
use futures::Stream;
use structopt::StructOpt;
use tokio_codec::FramedRead;
use tokio_io::AsyncRead;
use tokio_signal::unix::{Signal, SIGINT, SIGTERM};
use tokio_tcp::{TcpListener, TcpStream};
use tokio_timer::Delay;

use super::cli::AgentOpt;
use super::codec::AgentCodec;
use super::server::AgentServer;
use super::session::AgentSession;
use crate::common::logger::*;

static TARGET_AGENT_SERVICE: &'static str = "tempest::service::AgentService";

pub struct AgentService {
    server: Addr<AgentServer>,
}

impl Actor for AgentService {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {}
}

#[derive(Message)]
struct TcpConnect(pub TcpStream, pub net::SocketAddr);

impl Handler<TcpConnect> for AgentService {
    type Result = ();

    fn handle(&mut self, msg: TcpConnect, ctx: &mut Context<Self>) {
        info!(target: TARGET_AGENT_SERVICE, "TcpConnect: {}", &msg.1);

        let server = self.server.clone();
        AgentSession::create(move |ctx| {
            let (r, w) = msg.0.split();
            AgentSession::add_stream(FramedRead::new(r, AgentCodec), ctx);
            AgentSession::new(server, actix::io::FramedWrite::new(w, AgentCodec, ctx))
        });
    }
}

impl AgentService {
    pub fn run(opts: AgentOpt) {
        let sys = System::new("Agent");
        let host = opts.host_port();
        let addr = net::SocketAddr::from_str(&host[..]).unwrap();
        let listener = TcpListener::bind(&addr).unwrap();

        AgentService::create(|ctx| {
            ctx.add_message_stream(listener.incoming().map_err(|_| ()).map(|st| {
                let addr = st.peer_addr().unwrap();
                TcpConnect(st, addr)
            }));
            let server = AgentServer::from_registry();
            AgentService { server: server }
        });

        // sigint implements shutdown
        let shutdown = Signal::new(SIGINT)
            .flatten_stream()
            .for_each(|_| {
                warn!(
                    target: TARGET_AGENT_SERVICE,
                    "SIGINT received, shutting down"
                );
                System::current().stop();
                Ok(())
            })
            .map_err(|_| ());
        actix::spawn(shutdown);
        info!(
            target: TARGET_AGENT_SERVICE,
            "Launching agent service with opts: {:?}", &opts
        );
        let _ = sys.run();
    }
}
