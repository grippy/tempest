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
use crate::metric;
use crate::metric::backend::MetricsBackendActor;
use crate::metric::{parse_metrics_config, Metrics};

use crate::source::{Source, SourceBuilder};
use crate::topology::{PipelineActor, SourceActor, Topology, TopologyActor, TopologyBuilder};

static TARGET_TOPOLOGY_SERVICE: &'static str = "tempest::service::TopologyService";

pub struct TopologyService {
    server: Addr<TopologyServer>,
    metrics: Metrics,
}

impl Actor for TopologyService {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        metric::backend::MetricsBackendActor::subscribe(
            "TopologyService",
            ctx.address().clone().recipient(),
        );
    }
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

impl Handler<metric::backend::Flush> for TopologyService {
    type Result = ();

    fn handle(&mut self, msg: metric::backend::Flush, ctx: &mut Context<Self>) {
        self.metrics.flush();
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
        let mut topology_name = builder.options.name.to_string();

        match opts.get_config() {
            Ok(Some(cfg)) => {
                // replace top-level topology opts
                // with TopologyConfig values?
                topology_name = cfg.name.clone();

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
                if let Some(metrics_cfg) = cfg.metric {
                    parse_metrics_config(metrics_cfg)
                }
            }
            Err(err) => panic!("Error with config option: {:?}", &err),
            _ => {}
        }

        // Add topology name to the Root metrics
        metric::Root::labels(vec![("topology_name".to_string(), topology_name)]);

        // Apply cli args for the package
        builder.options.topology_id(opts.topology_id());
        builder.options.db_uri(opts.db_uri.clone());

        let sys = System::new("Topology");
        // register this first so it's ready for the rest of the services
        actix::SystemRegistry::set(MetricsBackendActor::default().start());
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

        TopologyService::create(|ctx| {
            ctx.add_message_stream(listener.incoming().map_err(|_| ()).map(|st| {
                let addr = st.peer_addr().unwrap();
                TcpConnect(st, addr)
            }));
            // grab the topology server from the registry
            let server = TopologyServer::from_registry();
            TopologyService {
                server: server,
                metrics: Metrics::default().named(vec!["topology", "service"]),
            }
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
