use std::any::TypeId;
use std::str::FromStr;
use std::time::Duration;
use std::{io, net, process, thread};

use actix::prelude::*;
use futures::Future;
use serde_derive::{Deserialize, Serialize};
use tokio_codec::FramedRead;
use tokio_io::io::WriteHalf;
use tokio_io::AsyncRead;
use tokio_tcp::TcpStream;

use crate::service::codec;
use crate::task::{Task, TaskActor, TaskMsgWrapper};

// Cli inputs
// - name
// - struct path
// - read_msg_count
// - max_backoff

pub struct TaskService<T: Task + 'static + Default> {
    name: String,
    addr: Addr<TaskActor<T>>,
    poll_interval: u64,
    backoff: u64,
    max_backoff: u64,
    read_msg_count: u16,
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
        println!("Disconnected");
        // Stop application on disconnect
        System::current().stop();
        Running::Stop
    }
}

impl<T> TaskService<T>
where
    T: Task + Default + 'static,
{
    pub fn run(name: String, default: fn() -> T)
    // pub fn run(task: T)
    where
        T: Task + Default + std::fmt::Debug,
    {
        // this needs to come from env vars or cmd args
        let task = default();
        println!("Starting task: {} {:?}", &name, &task);

        let sys = System::new("Task");
        let addr = net::SocketAddr::from_str("127.0.0.1:12345").unwrap();
        Arbiter::spawn(
            TcpStream::connect(&addr)
                .and_then(|stream| {
                    let addr = TaskService::create(|ctx| {
                        let (r, w) = stream.split();
                        ctx.add_stream(FramedRead::new(r, codec::TopologyClientCodec));
                        TaskService {
                            name: name,
                            addr: TaskActor { task: task }.start(),
                            poll_interval: 1,
                            backoff: 0,
                            max_backoff: 5000,
                            read_msg_count: 10,
                            framed: actix::io::FramedWrite::new(w, codec::TopologyClientCodec, ctx),
                        }
                    });
                    futures::future::ok(())
                })
                .map_err(|e| {
                    // we need to have a backoff and retry here
                    // no reason we need to bail if the topology server isn't reachable
                    // right away
                    println!("Can't connect to server: {}", e);
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
            self.read_msg_count,
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
                        self.backoff = 1;
                        self.poll_interval = 1;
                        ctx.run_later(Duration::from_millis(self.poll_interval), Self::poll);
                    } else {
                        if self.backoff < self.max_backoff {
                            self.backoff += 100;
                            self.poll_interval += self.backoff;
                        }
                        ctx.run_later(Duration::from_millis(self.poll_interval), Self::poll);
                    }
                }
                None => {
                    if self.backoff < self.max_backoff {
                        self.backoff += 1000;
                        self.poll_interval += self.backoff;
                    }
                    ctx.run_later(Duration::from_millis(self.poll_interval), Self::poll);
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
