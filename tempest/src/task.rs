use crate::source::Msg;

use crate::common::logger::*;
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
    type Msg;

    fn started(&mut self) {}
    fn deser(&mut self, msg: Msg) -> Self::Msg;
    fn handle(&mut self, msg: Self::Msg) -> TaskResult {
        Ok(None)
    }
    fn shutdown(&mut self) {}
}

#[derive(Message)]
pub struct TaskMsgWrapper<T: Task + Default + 'static> {
    pub service: Addr<TaskService<T>>,
    pub task_msg: TaskMsg,
}

#[derive(Default)]
pub struct TaskActor<T> {
    pub task: T,
}

impl<T> Supervised for TaskActor<T> where T: Task + Default + 'static {}

impl<T> SystemService for TaskActor<T> where T: Task + Default + 'static {}

impl<T> Actor for TaskActor<T>
where
    T: Task + Default + 'static,
{
    type Context = Context<Self>;
}

impl<T> Handler<TaskMsgWrapper<T>> for TaskActor<T>
where
    T: Task + Default + 'static,
{
    type Result = ();
    fn handle(&mut self, w: TaskMsgWrapper<T>, ctx: &mut Context<Self>) {
        // println!("TaskActor handle TaskMsgWrapper {:?}", &w.task_msg);
        // convert to this to the Task::handle msg
        let msg = self.task.deser(w.task_msg.msg);
        // send results to the task service actor
        match self.task.handle(msg) {
            Ok(opts) => {
                let req = TopologyRequest::TaskPut(TaskResponse::Ack(
                    w.task_msg.source_id,
                    w.task_msg.edge,
                    w.task_msg.index,
                    opts,
                ));
                w.service.do_send(req);
            }
            Err(err) => {
                error!(target: TARGET_TASK_ACTOR, "Task.handle error: {:?}", &err);
                let req = TopologyRequest::TaskPut(TaskResponse::Error(
                    w.task_msg.source_id,
                    w.task_msg.edge,
                    w.task_msg.index,
                ));
                w.service.do_send(req);
            }
        }
    }
}
