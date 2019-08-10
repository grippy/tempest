use crate::common::logger::*;
use crate::metric::{self, AggregateMetrics, TestMetrics};
use crate::service::agent::AgentService;
use crate::service::cli::{AgentOpt, PackageCmd, PackageOpt};
use crate::service::task::TaskService;
use crate::service::topology::TopologyService;
use crate::source::{Source, SourceBuilder};
use crate::topology::TopologyBuilder;

use actix::prelude::*;
use std::time::{Duration, Instant};
use structopt::StructOpt;

pub mod test {

    use super::*;
    use std::thread;
    use std::time::Duration;

    pub struct TestRun<SB: SourceBuilder> {
        /// Topology
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

        /// Defaults to 8765
        pub fn topology_port(mut self, port: u16) -> Self {
            self.topology_port = Some(port);
            self
        }

        /// Defaults to 7654
        pub fn agent_port(mut self, port: u16) -> Self {
            self.agent_port = Some(port);
            self
        }

        pub fn duration_millis(mut self, ms: u64) -> Self {
            self.duration = Duration::from_millis(ms);
            self
        }

        pub fn duration_secs(mut self, secs: u64) -> Self {
            self.duration = Duration::from_secs(secs);
            self
        }

        pub fn graceful_shutdown_millis(mut self, ms: u64) -> Self {
            self.graceful_shutdown = Duration::from_millis(ms);
            self
        }

        pub fn graceful_shutdown_secs(mut self, secs: u64) -> Self {
            self.graceful_shutdown = Duration::from_secs(secs);
            self
        }

        pub fn metric_target(mut self, target: Vec<metric::MetricTarget>) -> Self {
            self.metric_target = Some(target);
            self
        }

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
        topology_port: Option<u16>,
        agent_port: Option<u16>,
        duration: Duration,
        graceful_shutdown: Duration,
        metric_target: Option<Vec<metric::MetricTarget>>,
    ) -> TestMetrics
    where
        SB: SourceBuilder + Default + 'static,
        <SB as SourceBuilder>::Source: Source + Default + 'a,
        <SB as SourceBuilder>::Source: 'static,
    {
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

        let agent_service = thread::spawn(move || {
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
        for (name, task) in topology.pipeline.tasks {
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
        let aggregate = AggregateMetrics::read_tmp();
        info!("Aggregate metrics: {:?}", &aggregate);
        TestMetrics::new(aggregate)
    }
}

mod topology {

    use super::*;

    pub fn run<'a, SB>(builder: fn() -> TopologyBuilder<SB>)
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

    use super::*;
    use crate::task::Task;

    pub fn run<'a, SB>(
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
        let topology_name = &topology.options.name;
        let task = match topology.pipeline.remove(&name) {
            Some(t) => t,
            None => {
                panic!("No topology task exists with the name: {}", &name);
            }
        };
        TaskService::run(topology_name.to_string(), name, opt2, task, test);
    }
}

mod standalone {

    use super::*;
    use std::thread;
    use std::time::Duration;

    pub fn run<'a, SB>(builder: fn() -> TopologyBuilder<SB>)
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
        for (name, task) in topology.pipeline.tasks {
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
        PackageCmd::Standalone(standalone_opt) => {
            standalone::run(builder);
        }
        PackageCmd::Task(task_opt) => {
            task::run(builder, task_opt.name, None, None);
        }
        PackageCmd::Topology(topology_opt) => {
            topology::run(builder);
        }
    }
}