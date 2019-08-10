use crate::source::Msg;

use crate::common::logger::*;
use crate::metric::{self, Metrics};
use crate::service::codec::TopologyRequest;
use crate::service::task::TaskService;
use crate::topology::{TaskMsg, TaskResponse};

use actix::prelude::*;
use serde_derive::{Deserialize, Serialize};
use std::fmt;
use std::io;

static TARGET_TASK_ACTOR: &'static str = "tempest::task::TaskActor";

pub type TaskResult = Result<Option<Vec<Msg>>, TaskError>;

pub enum TaskErrorKind {
    // General std::io::Error
    Io(std::io::Error),
    /// Task custom error
    Custom(&'static str),
}

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

impl fmt::Debug for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Task error: {{ file: {}, line: {} }}", file!(), line!())
    }
}

pub trait Task {
    // type Msg;
    fn name(&self) -> &'static str;
    fn started(&mut self) {}
    // fn deser(&mut self, msg: Msg) -> Self::Msg;
    fn handle(&mut self, msg: Msg) -> TaskResult {
        Ok(None)
    }
    fn shutdown(&mut self) {}
}

#[derive(Message)]
pub struct TaskMsgWrapper {
    pub service: Addr<TaskService>,
    pub task_msg: TaskMsg,
}

pub struct TaskActor {
    pub name: String,
    pub task: Box<Task + 'static>,
    pub metrics: Metrics,
}

impl Actor for TaskActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        metric::backend::MetricsBackendActor::subscribe(
            "TaskActor",
            ctx.address().clone().recipient(),
        );
    }
}

impl Handler<TaskMsgWrapper> for TaskActor {
    type Result = ();

    fn handle(&mut self, w: TaskMsgWrapper, ctx: &mut Context<Self>) {
        // println!("TaskActor handle TaskMsgWrapper {:?}", &w.task_msg);
        // convert to this to the Task::handle msg
        let edge = &w.task_msg.edge;
        let task_edge = format!("({},{})", &edge.0[..], &edge.1[..]);
        // let timer = self.metrics.timer();
        // let msg = self.task.deser(w.task_msg.msg);
        let msg = w.task_msg.msg;
        // self.metrics
        //     .time_labels(vec!["handle", "deser"], timer, vec![("edge", &task_edge)]);

        // send results to the task service actor
        let timer = self.metrics.timer();
        match self.task.handle(msg) {
            Ok(opts) => {
                // record metrics for how many messages we returned
                match &opts {
                    Some(msgs) => {
                        self.metrics.counter_labels(
                            vec!["msg", "outflow"],
                            *&msgs.len() as isize,
                            vec![("edge", &task_edge)],
                        );
                    }
                    None => {
                        // No metrics for this case
                    }
                }
                let req = TopologyRequest::TaskPut(TaskResponse::Ack(
                    w.task_msg.source_id,
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
                error!(target: TARGET_TASK_ACTOR, "Task.handle error: {:?}", &err);
                self.metrics
                    .incr_labels(vec!["msg", "error"], vec![("edge", &task_edge)]);
                let req = TopologyRequest::TaskPut(TaskResponse::Error(
                    w.task_msg.source_id,
                    w.task_msg.edge,
                    w.task_msg.index,
                ));

                w.service.do_send(req);
                // TODO: handle do_send error here?
            }
        }
        self.metrics
            .time_labels(vec!["handle"], timer, vec![("edge", &task_edge)]);
    }
}

impl Handler<metric::backend::Flush> for TaskActor {
    type Result = ();

    fn handle(&mut self, msg: metric::backend::Flush, ctx: &mut Context<Self>) {
        self.metrics.flush();
    }
}
