use tempest_source::prelude::Msg;

use crate::common::logger::*;
use crate::metric::{self, Metrics};
use crate::service::codec::TopologyRequest;
use crate::service::task::TaskService;
use crate::topology::{TaskMsg, TaskResponse};

use actix::prelude::*;
use std::fmt;
use std::io;

static TARGET_TASK_ACTOR: &'static str = "tempest::task::TaskActor";

/// Defines an output type for handling Task messages.
pub type TaskResult = Result<Option<Vec<Msg>>, TaskError>;

/// Common error kinds for generating task errors
#[derive(Debug)]
pub enum TaskErrorKind {
    // General std::io::Error
    Io(std::io::Error),
    /// Task custom error
    Custom(&'static str),
}

/// Data structure for generating task errors
#[derive(Debug)]
pub struct TaskError {
    kind: TaskErrorKind,
}

impl TaskError {
    pub fn new(kind: TaskErrorKind) -> Self {
        Self { kind: kind }
    }

    pub fn custom(msg: &'static str) -> Self {
        Self::new(TaskErrorKind::Custom(msg))
    }

    pub fn from_error_kind(kind: io::ErrorKind) -> Self {
        Self::from_io_err(io::Error::from(kind))
    }

    pub fn from_io_err(err: io::Error) -> Self {
        Self::new(TaskErrorKind::Io(err))
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Task error: {:?}", self)
    }
}

/// Task trait for implementing task handlers
pub trait Task {
    /// The name of a task.
    fn name(&self) -> &'static str;

    /// Hook for defining custom initialization logic
    /// for configuring internal fields.
    fn started(&mut self) {}

    /// Hook for defining custom shutdown logic
    /// for deconstructing internal fields
    fn shutdown(&mut self) {}

    /// Skip handling messages and auto-generate
    /// a TaskResult of Ok(None) if this returns a `false` value.
    /// Otherwise, call `self.handle` if this returns a `true` value.
    fn filter(&self, _msg: &Msg) -> bool {
        true
    }

    /// The main method for handling Task messages
    fn handle(&mut self, _msg: Msg) -> TaskResult {
        Ok(None)
    }

    /// Hook for flushing instance metrics. Whatever implements this
    /// trait will need to define its own metrics instance (for example, `self.metrics`).
    ///
    /// If implemented, this will look like:
    ///
    /// ```text
    /// fn flush_metrics(&mut self) {
    ///     self.metrics.flush();
    /// }
    /// ```
    ///
    fn flush_metrics(&mut self) {}
}

/// Task message wrapper which keeps track of the TaskService
/// actor address in order communicate TaskResults
/// back to the topology.
#[derive(Message)]
pub(crate) struct TaskMsgWrapper {
    pub service: Addr<TaskService>,
    pub task_msg: TaskMsg,
}

/// Main actor for handling task messages.
pub(crate) struct TaskActor {
    pub name: String,
    pub task: Box<dyn Task + 'static>,
    pub metrics: Metrics,
}

impl Actor for TaskActor {
    type Context = Context<Self>;

    /// Task actor started hook
    fn started(&mut self, ctx: &mut Context<Self>) {
        // call started hook
        self.task.started();
        metric::backend::MetricsBackendActor::subscribe(
            "TaskActor",
            ctx.address().clone().recipient(),
        );
    }
}

impl Handler<TaskMsgWrapper> for TaskActor {
    type Result = ();

    /// Handle `TaskMsgWrapper` messages and run task filter & handling code.
    fn handle(&mut self, w: TaskMsgWrapper, _ctx: &mut Context<Self>) {
        // println!("TaskActor handle TaskMsgWrapper {:?}", &w.task_msg);
        // convert to this to the Task::handle msg
        let edge = &w.task_msg.edge;
        let task_edge = format!("({},{})", &edge.0[..], &edge.1[..]);

        // TODO: figure out where task message deser fits
        // let timer = self.metrics.timer();
        // let msg = self.task.deser(w.task_msg.msg);
        // self.metrics
        //     .time_labels(vec!["handle", "deser"], timer, vec![("edge", &task_edge)]);

        // The count of all messages prior to filter/handling
        self.metrics
            .incr_labels(vec!["msg", "inflow"], vec![("edge", &task_edge)]);

        // Filter and handle msg...
        let msg = w.task_msg.msg;
        let timer = self.metrics.timer();
        if self.task.filter(&msg) {
            self.metrics
                .incr_labels(vec!["msg", "handle"], vec![("edge", &task_edge)]);
            match self.task.handle(msg) {
                Ok(opts) => {
                    match &opts {
                        Some(msgs) => {
                            self.metrics.counter_labels(
                                vec!["msg", "outflow"],
                                *&msgs.len() as isize,
                                vec![("edge", &task_edge)],
                            );
                        }
                        None => {
                            // No metrics for this case because
                            // we aren't returning any messages
                        }
                    }
                    let req = TopologyRequest::TaskPut(TaskResponse::Ack(
                        w.task_msg.msg_id,
                        w.task_msg.edge,
                        w.task_msg.index,
                        opts,
                    ));

                    w.service.do_send(req);
                    // TODO: handle do_send error here?
                }
                Err(err) => {
                    // Task handler returned an error
                    // log and mark metrics
                    error!(target: TARGET_TASK_ACTOR, "Task.handle: {:?}", &err);
                    self.metrics
                        .incr_labels(vec!["msg", "error"], vec![("edge", &task_edge)]);
                    let req = TopologyRequest::TaskPut(TaskResponse::Error(
                        w.task_msg.msg_id,
                        w.task_msg.edge,
                        w.task_msg.index,
                    ));

                    w.service.do_send(req);
                    // TODO: handle do_send error here?
                }
            }
        } else {
            // task.filter rejected this msg
            // ack it as None and skip metrics
            self.metrics
                .incr_labels(vec!["msg", "skipped"], vec![("edge", &task_edge)]);
            let req = TopologyRequest::TaskPut(TaskResponse::Ack(
                w.task_msg.msg_id,
                w.task_msg.edge,
                w.task_msg.index,
                None,
            ));
            w.service.do_send(req);
        }

        self.metrics
            .time_labels(vec!["handle"], timer, vec![("edge", &task_edge)]);
    }
}

impl Handler<metric::backend::Flush> for TaskActor {
    type Result = ();

    /// Handle `metric::backend::Flush` messages and flush both `TaskActor` and `Task` (if configured) metrics.
    fn handle(&mut self, _msg: metric::backend::Flush, _ctx: &mut Context<Self>) {
        // Flush task metrics
        self.task.flush_metrics();

        // Flush self metrics
        self.metrics.flush();
    }
}
