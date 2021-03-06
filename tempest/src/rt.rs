use crate::common::logger::*;
use crate::metric::{self, AggregateMetrics, TestMetrics};
use crate::service::agent::AgentService;
use crate::service::cli::{AgentOpt, PackageCmd, PackageOpt};
use crate::service::task::TaskService;
use crate::service::topology::TopologyService;
use crate::topology::TopologyBuilder;

use tempest_source::prelude::{Source, SourceBuilder};

use structopt::StructOpt;

pub mod test {

    //! Multi-threaded test runtime for running Topologies and
    //! aggregating metrics.
    //!
    //! This runtime works like this:
    //!
    //! - You'll probably use the `TopologyBuilder.test_builder`
    //!    method for initializing the runtime.
    //! - Topology and Tasks runtimes are launched as threads.
    //! - An Agent is spawned on a thread which collects aggregated metrics
    //!   sent from the topology and tasks.
    //! - The aggregated metrics are prefixed as Topology.name or Task.name.
    //! - A `TestRun` is configurable with timeouts for initiating topology
    //!   shutdowns (in order to kill the threads).
    //! - The Agent flushes the aggregated metrics to disk so we can read
    //!   and output them as `TestMetrics` from the completion of this runtime.
    //! - Test assertions can be written using the aggregated metrics.
    //!
    use super::*;
    use lazy_static::lazy_static;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    lazy_static! {

        // In order to take advantage of multi-threaded testing...
        // we need isolate how we use topology and agent ports
        // otherwise, threads running topology tests will use the same
        // same ports which causes issues trying to dump
        // aggregated metrics to disk.
        static ref PORT: Arc<Mutex<u16>> = {
            Arc::new(Mutex::new(3000))
        };
    }

    /// Check if a port is already in use
    fn port_is_available(port: u16) -> bool {
        match TcpListener::bind(("0.0.0.0", port)) {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    /// Multi-threaded cargo tests will throw errors
    /// if we try and listen on ports already in use.
    /// This method will auto-increment a number until
    /// it finds an available port.
    fn get_port() -> u16 {
        let mut port = PORT.lock().unwrap();
        *port += 1;
        while !port_is_available(*port) {
            *port += 1;
        }
        *port
    }

    /// Data structure for testing topology code
    ///
    pub struct TestRun<SB: SourceBuilder> {
        /// Topology builder
        builder: fn() -> TopologyBuilder<SB>,
        /// Topology agent port
        topology_port: Option<u16>,
        /// Metrics agent port
        agent_port: Option<u16>,
        /// The duration of these test before runtime initiate shutdown
        duration: Duration,
        /// The duration to wait before forcing a shutdown
        graceful_shutdown: Duration,
        /// Backend metric targets for sending metrics...
        metric_target: Option<Vec<metric::MetricTarget>>,
    }

    impl<SB> TestRun<SB>
    where
        SB: SourceBuilder + Default + 'static,
        <SB as SourceBuilder>::Source: Source + Default,
        <SB as SourceBuilder>::Source: 'static,
    {
        pub fn new(builder: fn() -> TopologyBuilder<SB>) -> Self {
            TestRun {
                builder: builder,
                topology_port: None,
                agent_port: None,
                duration: Duration::from_millis(10000),
                graceful_shutdown: Duration::from_millis(10000),
                metric_target: None,
            }
        }

        /// The topology port to use for this test run.
        /// Will auto-assign if None (default)
        pub fn topology_port(mut self, port: u16) -> Self {
            self.topology_port = Some(port);
            self
        }

        /// The agent port to use for this test run.
        /// Will auto-assign if None (default)
        pub fn agent_port(mut self, port: u16) -> Self {
            self.agent_port = Some(port);
            self
        }

        /// Time to wait in milliseconds
        /// before initiating a topology shutdown
        pub fn duration_millis(mut self, ms: u64) -> Self {
            self.duration = Duration::from_millis(ms);
            self
        }

        /// Time to wait in seconds
        /// before initiating a topology shutdown
        pub fn duration_secs(mut self, secs: u64) -> Self {
            self.duration = Duration::from_secs(secs);
            self
        }

        /// Time to wait in milliseconds after initiating
        /// a topology shutdown and issuing a hard stop
        pub fn graceful_shutdown_millis(mut self, ms: u64) -> Self {
            self.graceful_shutdown = Duration::from_millis(ms);
            self
        }

        /// Time to wait in seconds after initiating
        /// a topology shutdown and issuing a hard stop
        pub fn graceful_shutdown_secs(mut self, secs: u64) -> Self {
            self.graceful_shutdown = Duration::from_secs(secs);
            self
        }

        /// Test custom metric targets
        pub fn metric_target(mut self, target: Vec<metric::MetricTarget>) -> Self {
            self.metric_target = Some(target);
            self
        }

        /// Main command for initiating a test
        pub fn run(self) -> TestMetrics {
            run(
                self.builder,
                self.topology_port,
                self.agent_port,
                self.duration,
                self.graceful_shutdown,
                self.metric_target,
            )
        }
    }

    fn run<'a, SB>(
        builder: fn() -> TopologyBuilder<SB>,
        mut topology_port: Option<u16>,
        mut agent_port: Option<u16>,
        duration: Duration,
        graceful_shutdown: Duration,
        metric_target: Option<Vec<metric::MetricTarget>>,
    ) -> TestMetrics
    where
        SB: SourceBuilder + Default + 'static,
        <SB as SourceBuilder>::Source: Source + Default + 'a,
        <SB as SourceBuilder>::Source: 'static,
    {
        // auto-assign ports
        if topology_port.is_none() {
            topology_port = Some(get_port());
        }

        if agent_port.is_none() {
            agent_port = Some(get_port());
        }

        let init_metrics = || {
            metric::Root::flush_interval(1000);
            if let Some(metric_target) = metric_target {
                metric::Root::targets(metric_target);
            };
        };
        let init_metrics2 = init_metrics.clone();

        // Construct a package cli command:
        // /. --graceful_shutdown [X] --port [Y] topology
        let mut args = vec![
            ".".to_string(),
            "--graceful_shutdown".to_string(),
            graceful_shutdown.as_millis().to_string(),
        ];

        if let Some(port) = topology_port {
            args.push("--port".to_string());
            args.push(port.to_string());
        }
        if let Some(port) = agent_port {
            args.push("--agent_port".to_string());
            args.push(port.to_string());
        }
        args.push("topology".to_string());
        let topology_opt = PackageOpt::from_iter(args.as_slice());

        // Construct AgentOpt
        let mut args = vec![".".to_string()];
        if let Some(port) = agent_port {
            args.push("--port".to_string());
            args.push(port.to_string());
        }
        let agent_opt = AgentOpt::from_iter(args.as_slice());

        let _agent_service = thread::spawn(move || {
            AgentService::run(agent_opt);
        });
        let _ = thread::sleep(Duration::from_millis(300));

        let topology_service = thread::spawn(move || {
            init_metrics2();
            TopologyService::run(topology_opt, builder, Some(duration));
        });
        let mut workers = vec![topology_service];

        // pause to allow topology time to start
        let _ = thread::sleep(Duration::from_millis(500));
        let topology = builder();
        for (name, _task) in topology.pipeline.tasks {
            let mut args = vec![".".to_string()];
            if let Some(port) = topology_port {
                args.push("--port".to_string());
                args.push(port.to_string());
            }
            if let Some(port) = agent_port {
                args.push("--agent_port".to_string());
                args.push(port.to_string());
            }

            args.push("task".to_string());
            args.push("--name".to_string());
            args.push(name.to_string());
            args.push("--poll_interval".to_string());
            args.push(1.to_string());
            args.push("--poll_count".to_string());
            args.push(100.to_string());
            let opt = PackageOpt::from_iter(args.as_slice());
            let init_metrics2 = init_metrics.clone();
            let handle = thread::spawn(move || {
                init_metrics2();
                super::task::run(builder, name.to_string(), Some(opt), Some(()));
            });
            workers.push(handle);
        }
        // The topology is configured to automatically
        // trigger a shutdown...
        for handle in workers {
            let _ = handle.join();
        }

        let suffix = format!("{}", agent_port.unwrap());
        let aggregate = AggregateMetrics::read_tmp(&suffix);
        info!("Aggregate metrics: {:?}", &aggregate);
        TestMetrics::new(aggregate)
    }
}

mod topology {

    /// This runtime wraps calling `TopologyService::run` and
    /// should only be used internally within this crate.
    use super::*;

    pub(crate) fn run<'a, SB>(builder: fn() -> TopologyBuilder<SB>)
    where
        SB: SourceBuilder + Default,
        <SB as SourceBuilder>::Source: Source + Default + 'a,
        <SB as SourceBuilder>::Source: 'static,
    {
        let opt = PackageOpt::from_args();
        TopologyService::run(opt, builder, None);
    }
}

mod task {

    /// This runtime wraps calling `TaskService::run` and
    /// should only be used internally within this crate.
    use super::*;

    pub(crate) fn run<'a, SB>(
        builder: fn() -> TopologyBuilder<SB>,
        name: String,
        opt: Option<PackageOpt>,
        test: Option<()>,
    ) where
        SB: SourceBuilder + Default,
        <SB as SourceBuilder>::Source: Source + Default + 'a,
        <SB as SourceBuilder>::Source: 'static,
    {
        let opt2 = if opt.is_none() {
            PackageOpt::from_args()
        } else {
            opt.unwrap()
        };

        let mut topology = builder();
        let metric_flush_interval = topology.options.metric_flush_interval;
        let metric_targets = topology.options.metric_targets;
        let topology_name = &topology.options.name;
        let task = match topology.pipeline.remove(&name) {
            Some(t) => t,
            None => {
                panic!("No topology task exists with the name: {}", &name);
            }
        };
        TaskService::run(
            topology_name.to_string(),
            name,
            opt2,
            task,
            metric_flush_interval,
            metric_targets,
            test,
        );
    }
}

mod standalone {

    /// This runtime spawns threads for `TopologyService::run`
    /// and `TaskService::run` (for each task).
    ///
    /// It should only be used internally within this crate.
    ///
    use super::*;
    use std::thread;
    use std::time::Duration;

    pub(crate) fn run<'a, SB>(builder: fn() -> TopologyBuilder<SB>)
    where
        SB: SourceBuilder + Default + 'static,
        <SB as SourceBuilder>::Source: Source + Default + 'a,
        <SB as SourceBuilder>::Source: 'static,
    {
        info!("Running standalone topology w/ threads");
        let topology_service = thread::spawn(move || {
            super::topology::run(builder);
        });

        // pause to allow topology time to start
        let _ = thread::sleep(Duration::from_millis(500));
        let topology = builder();
        let mut workers = vec![topology_service];
        for (name, _task) in topology.pipeline.tasks {
            let handle = thread::spawn(move || {
                super::task::run(builder, name.to_string(), None, None);
            });
            workers.push(handle);
        }
        for handle in workers {
            let _ = handle.join();
        }
    }
}

/// The main runtime and public interface for running Topology packages.
/// Command line arguments are converted to `PackageOpt`
///
pub fn run<'a, SB>(builder: fn() -> TopologyBuilder<SB>)
where
    SB: SourceBuilder + Default + 'static,
    <SB as SourceBuilder>::Source: Source + Default + 'a,
    <SB as SourceBuilder>::Source: 'static,
{
    // Use package opt to determine the cmd
    // and parse these opts again inside each service

    let opt = PackageOpt::from_args();
    info!("Package opts: {:?}", &opt);

    match opt.cmd {
        PackageCmd::Standalone(_standalone_opt) => {
            standalone::run(builder);
        }
        PackageCmd::Task(task_opt) => {
            task::run(builder, task_opt.name, None, None);
        }
        PackageCmd::Topology(_topology_opt) => {
            topology::run(builder);
        }
    }
}
