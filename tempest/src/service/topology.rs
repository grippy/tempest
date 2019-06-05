use std::net;
use std::str::FromStr;

use actix::prelude::*;
use futures::Stream;
use structopt::StructOpt;
use tokio_codec::FramedRead;
use tokio_io::AsyncRead;
use tokio_tcp::{TcpListener, TcpStream};

use super::cli::PackageOpt;
use super::codec::TopologyCodec;
use super::server::TopologyServer;
use super::session::TopologySession;
use crate::common::logger::*;
use crate::source::{Source, SourceBuilder};
use crate::topology::{PipelineActor, SourceActor, Topology, TopologyActor, TopologyBuilder};

static TARGET_TOPOLOGY_SERVICE: &'static str = "tempest::service::TopologyService";

// A TopologyService is an actor that creates a TCP Server
// and forward requests to the TopologyActor
// This actor makes TaskRequest through the TopologyActor
// to the PipelineActor for work

pub struct TopologyService {
    server: Addr<TopologyServer>,
}

impl Actor for TopologyService {
    type Context = Context<Self>;
}

#[derive(Message)]
struct TcpConnect(pub TcpStream, pub net::SocketAddr);

impl Handler<TcpConnect> for TopologyService {
    type Result = ();

    fn handle(&mut self, msg: TcpConnect, ctx: &mut Context<Self>) {
        info!(target: TARGET_TOPOLOGY_SERVICE, "TcpConnect: {}", &msg.1);

        let server = self.server.clone();
        TopologySession::create(move |ctx| {
            let (r, w) = msg.0.split();
            TopologySession::add_stream(FramedRead::new(r, TopologyCodec), ctx);
            TopologySession::new(server, actix::io::FramedWrite::new(w, TopologyCodec, ctx))
        });
    }
}

impl TopologyService {
    pub fn run<'a, SB>(mut opts: PackageOpt, build: fn() -> TopologyBuilder<SB>)
    where
        SB: SourceBuilder + Default,
        <SB as SourceBuilder>::Source: Source + Default + 'a,
        <SB as SourceBuilder>::Source: 'static,
    {
        let mut builder = build();
        match opts.get_config() {
            Ok(Some(cfg)) => {
                // replace top-level topology opts
                // with TopologyConfig values?
                if let Some(db_uri) = &cfg.db_uri {
                    opts.db_uri = db_uri.to_string();
                }
                if let Some(host) = &cfg.host {
                    opts.host = host.to_string();
                }
                if let Some(port) = &cfg.port {
                    opts.port = port.to_string();
                }

                // this is where we need to override the source cfg
                if let Some(source) = cfg.source {
                    builder.source_builder.parse_config_value(source.config)
                }
            }
            Err(err) => panic!("Error with config option: {:?}", &err),
            _ => {}
        }

        // apply cli args for the package
        builder.options.topology_id(opts.topology_id());
        builder.options.db_uri(opts.db_uri.clone());

        let sys = System::new("Topology");
        actix::SystemRegistry::set(builder.topology_actor().start());
        actix::SystemRegistry::set(builder.source_actor().start());
        actix::SystemRegistry::set(builder.pipeline_actor().start());
        actix::SystemRegistry::set(TopologyServer::default().start());

        // Create server listener
        let host = opts.topology_id();
        info!(
            target: TARGET_TOPOLOGY_SERVICE,
            "Starting topology: {} w/ opts: {:?}", &host, &opts
        );

        let addr = net::SocketAddr::from_str(&host[..]).unwrap();
        let listener = TcpListener::bind(&addr).unwrap();

        // Our chat server `Server` is an actor, first we need to start it
        // and then add stream on incoming tcp connections to it.
        // TcpListener::incoming() returns stream of the (TcpStream, net::SocketAddr)
        // items So to be able to handle this events `Server` actor has to implement
        // stream handler `StreamHandler<(TcpStream, net::SocketAddr), io::Error>`
        TopologyService::create(|ctx| {
            ctx.add_message_stream(listener.incoming().map_err(|_| ()).map(|st| {
                let addr = st.peer_addr().unwrap();
                TcpConnect(st, addr)
            }));
            // grab the topology server from the registry
            let server = TopologyServer::from_registry();
            TopologyService { server: server }
        });

        let ctrl_c = tokio_signal::ctrl_c().flatten_stream();
        let handle_shutdown = ctrl_c
            .for_each(|()| {
                warn!(
                    target: TARGET_TOPOLOGY_SERVICE,
                    "Ctrl-C received, shutting down"
                );
                System::current().stop();
                Ok(())
            })
            .map_err(|_| ());
        actix::spawn(handle_shutdown);

        info!(
            target: TARGET_TOPOLOGY_SERVICE,
            "Launching topology service with builder opts: {:?}", builder.options
        );
        let _ = sys.run();
    }
}
