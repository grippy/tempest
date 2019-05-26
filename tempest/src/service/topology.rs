use std::net;
use std::str::FromStr;

use actix::prelude::*;
use futures::Stream;
use structopt::StructOpt;
use tokio_codec::FramedRead;
use tokio_io::AsyncRead;
use tokio_tcp::{TcpListener, TcpStream};

use super::codec::TopologyCodec;
use super::server::TopologyServer;
use super::session::TopologySession;
use crate::source::{Source, SourceBuilder};
use crate::topology::{PipelineActor, SourceActor, Topology, TopologyActor, TopologyBuilder};


// cli args
#[derive(Debug, StructOpt)]
#[structopt(name = "{topology}", about = "Topology")]
pub struct TopologyServiceCliArgs {
    #[structopt(short = "h", long = "host", default_value = "0.0.0.0")]
    host: String,
    #[structopt(short = "p", long = "port", default_value = "8765")]
    port: String,
    #[structopt(short = "db", long = "tempest_db_uri")]
    tempest_db_uri: String,
}

// A TopologyService is an actor that creates a TCP Server
// and forward requests to the TopologyActor
// This actor makes TaskRequest to the PipelineActor for work

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
        println!("tcp connect");

        let server = self.server.clone();
        TopologySession::create(move |ctx| {
            let (r, w) = msg.0.split();
            TopologySession::add_stream(FramedRead::new(r, TopologyCodec), ctx);
            TopologySession::new(server, actix::io::FramedWrite::new(w, TopologyCodec, ctx))
        });
    }
}

impl TopologyService {
    pub fn run<'a, SB>(f: fn() -> TopologyBuilder<SB>)
    where
        SB: SourceBuilder + Default,
        <SB as SourceBuilder>::Source: Source + Default + 'a,
        <SB as SourceBuilder>::Source: 'static,
    {
        let builder = f();
        let sys = System::new("Topology");
        actix::SystemRegistry::set(builder.topology_actor().start());
        actix::SystemRegistry::set(builder.source_actor().start());
        actix::SystemRegistry::set(builder.pipeline_actor().start());
        actix::SystemRegistry::set(TopologyServer::default().start());

        // Create server listener
        let addr = net::SocketAddr::from_str("0.0.0.0:12345").unwrap();
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
                println!("Ctrl-C received, shutting down");
                System::current().stop();
                Ok(())
            })
            .map_err(|_| ());
        actix::spawn(handle_shutdown);

        println!("Launching topology service... {:?}", builder.options);
        let _ = sys.run();
    }
}
