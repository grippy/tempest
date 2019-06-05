use std::any::TypeId;
use std::str::FromStr;
use std::time::Duration;
use std::{io, net, process, thread};

use actix::prelude::*;
use futures::Future;
use serde_derive::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio_codec::FramedRead;
use tokio_io::io::WriteHalf;
use tokio_io::AsyncRead;
use tokio_tcp::TcpStream;

use crate::common::logger::*;
use crate::service::cli::{PackageCmd, PackageOpt, TaskOpt};
use crate::service::codec;
use crate::task::{Task, TaskActor, TaskMsgWrapper};

static TARGET_TASK_SERVICE: &'static str = "tempest::service::TaskService";

pub struct TaskService<T: Task + 'static + Default> {
    name: String,
    addr: Addr<TaskActor<T>>,
    poll_interval: u64,
    next_interval: u64,
    backoff: u64,
    max_backoff: u64,
    poll_count: u16,
    framed: actix::io::FramedWrite<WriteHalf<TcpStream>, codec::TopologyClientCodec>,
}

impl<T> Actor for TaskService<T>
where
    T: Task + Default + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        // send ping every 5s to avoid disconnects
        ctx.run_interval(Duration::from_secs(5), Self::hb);
        ctx.run_later(Duration::from_millis(self.poll_interval), Self::poll);
    }

    fn stopping(&mut self, _: &mut Context<Self>) -> Running {
        warn!(
            target: TARGET_TASK_SERVICE,
            "Task {} disconnected", &self.name
        );
        // Stop application on disconnect
        System::current().stop();
        Running::Stop
    }
}

impl<T> TaskService<T>
where
    T: Task + Default + 'static,
{
    pub fn run(name: String, mut opts: PackageOpt, build: fn() -> T)
    where
        T: Task + Default + std::fmt::Debug,
    {
        let sys = System::new("Task");
        let task = build();

        // we must have cmd with a TaskOpt at this point...
        // if not how did we initiate this run command?
        // Use a default and merge it with our Config values if we have them
        let mut task_opt = TaskOpt::default();
        match &opts.cmd {
            PackageCmd::Task(_task_opt) => {
                if _task_opt.workers.is_some() {
                    task_opt.workers = _task_opt.workers;
                }
                if _task_opt.poll_count.is_some() {
                    task_opt.poll_count = _task_opt.poll_count;
                }
                if _task_opt.poll_interval.is_some() {
                    task_opt.poll_interval = _task_opt.poll_interval;
                }
                if _task_opt.max_backoff.is_some() {
                    task_opt.max_backoff = _task_opt.max_backoff;
                }
            }
            _ => {}
        }

        // This is multi-step config affair
        // We can define per task config settings in the Topology.toml config
        // Parse this file if one exists as a cli arg and then
        // replace the PackageOpts with whatever values we find for this task name
        match &opts.get_config() {
            Ok(Some(cfg)) => {
                // replace top-level topology opts
                if let Some(db_uri) = &cfg.db_uri {
                    opts.db_uri = db_uri.to_string();
                }
                if let Some(host) = &cfg.host {
                    opts.host = host.to_string();
                }
                if let Some(port) = &cfg.port {
                    opts.port = port.to_string();
                }

                // find this task by name...
                if let Some(task_cfg) = &cfg.task.iter().find(|t| &t.name == &name) {
                    // print!("found task w/ config: {:?}", &task_cfg);
                    if task_cfg.workers.is_some() {
                        task_opt.workers = task_cfg.workers;
                    }
                    if task_cfg.poll_count.is_some() {
                        task_opt.poll_count = task_cfg.poll_count;
                    }
                    if task_cfg.poll_interval.is_some() {
                        task_opt.poll_interval = task_cfg.poll_interval;
                    }
                    if task_cfg.max_backoff.is_some() {
                        task_opt.max_backoff = task_cfg.max_backoff;
                    }
                }
            }
            Err(err) => panic!("Error with config option: {:?}", &err),
            _ => {}
        }

        // TODO: figure out how to spawn workers
        // let workers = task_opt.workers;

        // replace the cmd w/ the merged config+opts
        opts.cmd = PackageCmd::Task(task_opt);

        let host = opts.topology_id();
        info!(
            target: TARGET_TASK_SERVICE,
            "Starting task: {} {:?} connected to {} w/ opts: {:?}", &name, &task, &host, &opts
        );
        let addr = net::SocketAddr::from_str(&host[..]).unwrap();

        Arbiter::spawn(
            TcpStream::connect(&addr)
                .and_then(|stream| {
                    let addr = TaskService::create(|ctx| {
                        let (r, w) = stream.split();
                        ctx.add_stream(FramedRead::new(r, codec::TopologyClientCodec));

                        let mut poll_interval = 1;
                        let mut poll_count = 10;
                        let mut max_backoff = 5000;
                        match opts.cmd {
                            PackageCmd::Task(task_opt) => {
                                if let Some(v) = task_opt.poll_interval {
                                    poll_interval = v;
                                }
                                if let Some(v) = task_opt.poll_count {
                                    poll_count = v;
                                }
                                if let Some(v) = task_opt.max_backoff {
                                    max_backoff = v;
                                }
                            }
                            _ => {}
                        }
                        TaskService {
                            name: name,
                            addr: TaskActor { task: task }.start(),
                            poll_interval: poll_interval,
                            next_interval: poll_interval.clone(),
                            backoff: 0,
                            max_backoff: max_backoff,
                            poll_count: poll_count,
                            framed: actix::io::FramedWrite::new(w, codec::TopologyClientCodec, ctx),
                        }
                    });
                    futures::future::ok(())
                })
                .map_err(|e| {
                    // we need to have a backoff and retry here
                    // no reason we need to bail if the topology server isn't reachable
                    // right away
                    error!(
                        target: TARGET_TASK_SERVICE,
                        "Can't connect to server: {:?}", e
                    );
                    process::exit(1)
                }),
        );

        let _ = sys.run();
    }

    fn hb(&mut self, ctx: &mut Context<Self>) {
        self.framed.write(codec::TopologyRequest::Ping);
    }

    fn poll(&mut self, _ctx: &mut Context<Self>) {
        // TODO: make the taskget count configurable
        self.framed.write(codec::TopologyRequest::TaskGet(
            self.name.clone(),
            self.poll_count,
        ));
    }
}

impl<T> actix::io::WriteHandler<io::Error> for TaskService<T> where T: Task + Default + 'static {}

/// Server communication
impl<T> StreamHandler<codec::TopologyResponse, io::Error> for TaskService<T>
where
    T: Task + Default + 'static,
{
    fn handle(&mut self, msg: codec::TopologyResponse, ctx: &mut Context<Self>) {
        match msg {
            codec::TopologyResponse::TaskGet(opts) => match opts {
                Some(tasks) => {
                    // pass the self.address() into TopologyActor here
                    // because we can't register TaskService globally
                    // with no Default implementation
                    if tasks.len() > 0 {
                        // map tasks into TaskActor
                        for task_msg in tasks {
                            self.addr.do_send(TaskMsgWrapper {
                                task_msg: task_msg,
                                service: ctx.address(),
                            });
                        }
                        self.backoff = 0;
                        self.next_interval = self.poll_interval;
                        ctx.run_later(Duration::from_millis(self.next_interval), Self::poll);
                    } else {
                        if self.backoff < self.max_backoff {
                            self.backoff += 100;
                            self.next_interval += self.backoff;
                        }
                        ctx.run_later(Duration::from_millis(self.next_interval), Self::poll);
                    }
                }
                None => {
                    if self.backoff < self.max_backoff {
                        self.backoff += 1000;
                        self.next_interval += self.backoff;
                    }
                    debug!(
                        target: TARGET_TASK_SERVICE,
                        "Task {} poll setting backoff to next interval: {}",
                        &self.name,
                        &self.next_interval
                    );
                    ctx.run_later(Duration::from_millis(self.next_interval), Self::poll);
                }
            },
            _ => (),
        }
    }
}

/// Server communication
impl<T> Handler<codec::TopologyRequest> for TaskService<T>
where
    T: Task + Default + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: codec::TopologyRequest, ctx: &mut Context<Self>) {
        self.framed.write(msg);
    }
}
