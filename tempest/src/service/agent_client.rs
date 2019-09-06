use std::str::FromStr;
use std::time::Duration;
use std::{io, net};

use actix::prelude::*;
use futures::Future;
use tokio_codec::FramedRead;
use tokio_io::io::WriteHalf;
use tokio_io::AsyncRead;
use tokio_tcp::TcpStream;

use crate::common::logger::*;
use crate::service::cli::AgentOpt;
use crate::service::codec;

static TARGET_AGENT_CLIENT: &'static str = "tempest::service::AgentClient";
static TARGET_AGENT_CLIENT_SERVICE: &'static str = "tempest::service::AgentClientService";

/// Message type for connecting an `AgentClient`
/// to an `AgentService`
#[derive(Message)]
pub(crate) struct AgentClientConnect {
    pub addr: Addr<AgentClientService>,
}

/// `AgentClient` is used to send aggregation metrics
/// from running processes (`TopologyService` & `TaskService`) to an `AgentService`.
/// This is currently only used for testing purposes.
#[derive(Default)]
pub(crate) struct AgentClient {
    service: Option<Addr<AgentClientService>>,
}

impl Supervised for AgentClient {}

impl SystemService for AgentClient {}

impl Actor for AgentClient {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {}

    fn stopping(&mut self, _: &mut Context<Self>) -> Running {
        warn!(target: TARGET_AGENT_CLIENT, "AgentClient disconnected",);
        Running::Stop
    }
}

impl AgentClient {
    /// Take the configured `AgentOpt` and connect to
    /// an `AgentService`.
    pub(crate) fn connect(opts: AgentOpt) -> Addr<AgentClient> {
        let client_addr = AgentClient::default().start();
        actix::SystemRegistry::set(client_addr.clone());
        let host = opts.host_port();
        info!(
            target: TARGET_AGENT_CLIENT,
            "Starting agent client: {}", &host
        );
        let addr = net::SocketAddr::from_str(&host[..]).unwrap();
        Arbiter::spawn(
            TcpStream::connect(&addr)
                .and_then(|stream| {
                    let service = AgentClientService::create(|ctx| {
                        let (r, w) = stream.split();
                        ctx.add_stream(FramedRead::new(r, codec::AgentClientCodec));
                        AgentClientService {
                            name: "agent-client".into(),
                            framed: actix::io::FramedWrite::new(w, codec::AgentClientCodec, ctx),
                        }
                    });
                    let client = AgentClient::from_registry();
                    if client.connected() {
                        let _ = client.do_send(AgentClientConnect { addr: service });
                    }
                    futures::future::ok(())
                })
                .map_err(|e| {
                    error!(
                        target: TARGET_AGENT_CLIENT,
                        "Can't connect to agent server: {:?}", e
                    );
                }),
        );

        client_addr
    }
}

impl Handler<codec::AgentRequest> for AgentClient {
    type Result = ();

    /// Handle `AgentRequest` message and send them to the `AgentService`
    fn handle(&mut self, msg: codec::AgentRequest, _ctx: &mut Context<Self>) {
        match &msg {
            codec::AgentRequest::AggregateMetricsPut(_aggregate) => {
                if let Some(service) = &self.service {
                    let _ = service.do_send(msg);
                }
            }
            _ => {
                println!(
                    "Handler<codec::AgentRequest> for AgentClient missing match arm {:?}",
                    &msg
                );
            }
        }
    }
}

impl Handler<AgentClientConnect> for AgentClient {
    type Result = ();

    fn handle(&mut self, msg: AgentClientConnect, _ctx: &mut Context<Self>) {
        self.service = Some(msg.addr)
    }
}

#[allow(dead_code)]
pub struct AgentClientService {
    name: String,
    framed: actix::io::FramedWrite<WriteHalf<TcpStream>, codec::AgentClientCodec>,
}

impl Actor for AgentClientService {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        // send ping every 5s to avoid disconnects
        ctx.run_interval(Duration::from_secs(5), Self::hb);
    }

    fn stopping(&mut self, _: &mut Context<Self>) -> Running {
        warn!(
            target: TARGET_AGENT_CLIENT_SERVICE,
            "AgentClientService disconnected",
        );
        Running::Stop
    }
}

impl AgentClientService {
    fn hb(&mut self, _ctx: &mut Context<Self>) {
        self.framed.write(codec::AgentRequest::Ping);
    }
}

impl actix::io::WriteHandler<io::Error> for AgentClientService {}

impl StreamHandler<codec::AgentResponse, io::Error> for AgentClientService {
    fn handle(&mut self, msg: codec::AgentResponse, _ctx: &mut Context<Self>) {
        match &msg {
            codec::AgentResponse::Ping => {}
        }
    }
}

impl Handler<codec::AgentRequest> for AgentClientService {
    type Result = ();

    fn handle(&mut self, msg: codec::AgentRequest, _ctx: &mut Context<Self>) {
        self.framed.write(msg);
    }
}
