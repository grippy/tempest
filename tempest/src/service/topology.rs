use std::net;
use std::str::FromStr;
use std::time::{Duration, Instant};

use actix::prelude::*;
use futures::Stream;
use tokio_codec::FramedRead;
use tokio_io::AsyncRead;
use tokio_signal::unix::{Signal, SIGINT, SIGTERM};
use tokio_tcp::{TcpListener, TcpStream};
use tokio_timer::Delay;

use super::cli::{AgentOpt, PackageOpt};
use super::codec::TopologyCodec;
use super::server::TopologyServer;
use super::session::TopologySession;
use crate::common::logger::*;
use crate::metric;
use crate::metric::backend::{MetricsAggregateActor, MetricsBackendActor};
use crate::metric::{parse_metrics_config, Metrics};
use crate::topology::{ShutdownMsg, SourceActor, TopologyBuilder};

use tempest_source::prelude::{Source, SourceBuilder};

static TARGET_TOPOLOGY_SERVICE: &'static str = "tempest::service::TopologyService";

/// Graceful shutdown wait period if not defined on TopologyBuilder
static DEFAULT_GRACEFUL_SHUTDOWN: u64 = 30000;

/// Main actor for constructing and running a `TopologyServer`
pub(crate) struct TopologyService {
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

/// Message for wiring up new tcp connections
#[derive(Message)]
struct TcpConnect(pub TcpStream, pub net::SocketAddr);

impl Handler<TcpConnect> for TopologyService {
    type Result = ();

    /// Handle `TcpConnect` message. This creates a `TopologySession`
    /// which provides the communication channel for connected `TaskService`s.
    fn handle(&mut self, msg: TcpConnect, _ctx: &mut Context<Self>) {
        info!(target: TARGET_TOPOLOGY_SERVICE, "TcpConnect: {}", &msg.1);

        let server = self.server.clone();
        TopologySession::create(move |ctx| {
            let (r, w) = msg.0.split();
            TopologySession::add_stream(FramedRead::new(r, TopologyCodec), ctx);
            TopologySession::new(server, actix::io::FramedWrite::new(w, TopologyCodec, ctx))
        });
    }
}

/// Handle `metric::backend::Flush` messages
impl Handler<metric::backend::Flush> for TopologyService {
    type Result = ();

    fn handle(&mut self, _msg: metric::backend::Flush, _ctx: &mut Context<Self>) {
        self.metrics.flush();
    }
}

impl TopologyService {
    /// The main run command for launching a `TopologyService`.
    pub(crate) fn run<'a, SB>(
        mut opts: PackageOpt,
        build: fn() -> TopologyBuilder<SB>,
        test: Option<Duration>,
    ) where
        SB: SourceBuilder + Default,
        <SB as SourceBuilder>::Source: Source + Default + 'a,
        <SB as SourceBuilder>::Source: 'static,
    {
        let mut builder = build();
        let mut topology_name = builder.options.name.to_string();
        let metric_flush_interval = builder.options.metric_flush_interval.clone();
        let metric_targets = builder.options.metric_targets.clone();

        info!("Running {:?} topology process", &topology_name);

        match opts.get_config() {
            Ok(Some(cfg)) => {
                // replace top-level topology opts
                // with TopologyConfig values?
                topology_name = cfg.name.clone();

                if let Some(host) = &cfg.host {
                    opts.host = host.to_string();
                }
                if let Some(port) = &cfg.port {
                    opts.port = port.to_string();
                }

                if let Some(ms) = cfg.graceful_shutdown {
                    opts.graceful_shutdown = ms;
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

        // Only use TopologyOptions to configure metrics if nothing
        // was parsed from the Topology.toml configuration
        let targets_len = metric::Root::get_targets_len();
        if targets_len == 0usize {
            info!("Configure metrics with TopologyOption values");
            metric::configure(metric_flush_interval, Some(metric_targets));
        }

        // Add topology name to the Root metrics
        metric::Root::target_name(format!("topology.{}", topology_name.clone()));
        metric::Root::labels(vec![("topology_name".to_string(), topology_name)]);

        // Apply cli args for the package
        builder.options.host_port(opts.host_port());
        builder.options.graceful_shutdown(opts.graceful_shutdown);

        let graceful_shutdown_ms = match &builder.options.graceful_shutdown {
            Some(ms) => *ms,
            None => DEFAULT_GRACEFUL_SHUTDOWN,
        };

        let sys = System::new("Topology");

        let agent_host = opts.agent_host.clone();
        let agent_port = opts.agent_port.clone();

        // Register this metric backend  so it's ready for the rest of the services
        let mut metrics_backend = MetricsBackendActor::default();

        // Wire-up hooks to shutdown because we're testing?
        if let Some(duration) = test {
            let agent_opts = AgentOpt::new(agent_host, agent_port);
            let metrics_aggregate = MetricsAggregateActor::new(agent_opts).start();
            metrics_backend.aggregate = Some(metrics_aggregate.clone());
            actix::SystemRegistry::set(metrics_aggregate);

            // Since we're testing... wire up a short-circuit
            // to automatically trigger a graceful shutdown

            let graceful_duration = graceful_shutdown_ms.clone();
            let shutdown = Delay::new(Instant::now() + duration)
                .map(move |_| {
                    let source = SourceActor::from_registry();
                    if source.connected() {
                        source.do_send(ShutdownMsg {});
                    }
                    let exit = Delay::new(Instant::now() + Duration::from_millis(1000))
                        .map(|_| {
                            actix::System::current().stop();
                        })
                        .map_err(|_| ());
                    let flush =
                        Delay::new(Instant::now() + Duration::from_millis(graceful_duration))
                            .map(move |_| {
                                // let result = flush_tx.send(());
                                // warn!("flush aggregate metrics result: {:?}", &result);
                                actix::spawn(exit);
                            })
                            .map_err(|_| ());
                    actix::spawn(flush);
                })
                .map_err(|_| ());
            actix::spawn(shutdown);
        };
        // Register all topology actors
        actix::SystemRegistry::set(metrics_backend.start());
        actix::SystemRegistry::set(builder.topology_actor().start());
        actix::SystemRegistry::set(builder.source_actor().start());
        actix::SystemRegistry::set(builder.pipeline_actor().start());
        actix::SystemRegistry::set(TopologyServer::default().start());

        // Create server listener
        let host = opts.host_port();
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

        // sigint implements shutdown
        // use tokio_signal::ctrl_c;
        // let cc = ctrl_c()
        //     .flatten_stream()
        //     .for_each(|_| {
        //         warn!(
        //             target: TARGET_TOPOLOGY_SERVICE,
        //             "ctrl+c received, shutting down"
        //         );
        //         System::current().stop();
        //         Ok(())
        //     })
        //     .map_err(|_| ());

        let shutdown = Signal::new(SIGINT)
            .flatten_stream()
            .for_each(|_| {
                warn!(
                    target: TARGET_TOPOLOGY_SERVICE,
                    "SIGINT received, shutting down"
                );
                System::current().stop();
                Ok(())
            })
            .map_err(|_| ());

        // sigterm implements graceful shutdown
        let graceful_shutdown = Signal::new(SIGTERM)
            .flatten_stream()
            .for_each(move |_| {
                warn!(
                    target: TARGET_TOPOLOGY_SERVICE,
                    "SIGTERM received, initiate graceful shutdown delay {:?}",
                    &graceful_shutdown_ms
                );
                // map future so we don't block
                let stop = Delay::new(Instant::now() + Duration::from_millis(graceful_shutdown_ms))
                    .map(|_| actix::System::current().stop())
                    .map_err(|_| ());
                actix::spawn(stop);

                // Send ShutdownMsg to SourceActor...
                let source = SourceActor::from_registry();
                if source.connected() {
                    source.do_send(ShutdownMsg {});
                }

                Ok(())
            })
            .map_err(|_| ());

        // actix::spawn(cc);
        actix::spawn(shutdown);
        actix::spawn(graceful_shutdown);
        info!(
            target: TARGET_TOPOLOGY_SERVICE,
            "Launching topology service with builder opts: {:?}", builder.options
        );
        let _ = sys.run();
        info!(
            target: TARGET_TOPOLOGY_SERVICE,
            "Topology shutdown complete"
        );
    }
}
