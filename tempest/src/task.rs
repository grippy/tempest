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
    pub name: String,
    pub task: T,
    pub metrics: Metrics,
}

impl<T> Supervised for TaskActor<T> where T: Task + Default + 'static {}

impl<T> SystemService for TaskActor<T> where T: Task + Default + 'static {}

impl<T> Actor for TaskActor<T>
where
    T: Task + Default + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        metric::backend::MetricsBackendActor::subscribe(
            "TaskActor",
            ctx.address().clone().recipient(),
        );
    }
}

impl<T> Handler<TaskMsgWrapper<T>> for TaskActor<T>
where
    T: Task + Default + 'static,
{
    type Result = ();

    fn handle(&mut self, w: TaskMsgWrapper<T>, ctx: &mut Context<Self>) {
        // println!("TaskActor handle TaskMsgWrapper {:?}", &w.task_msg);
        // convert to this to the Task::handle msg
        let edge = &w.task_msg.edge;
        let metric_base_name = format!("task.{}_{}", &edge.0[..], &edge.1[..]);
        let metric_deser_timer = format!("{}.deser.timer", &metric_base_name[..]);
        let metric_handle_timer = format!("{}.handle.timer", &metric_base_name[..]);

        // let timer = self.metrics.timer(&metric_deser_timer[..]);
        // let timer_id = timer.start();
        let msg = self.task.deser(w.task_msg.msg);
        // timer.stop(timer_id);

        // send results to the task service actor
        // let timer = self.metrics.timer(&metric_handle_timer[..]);
        // let timer_id = timer.start();
        match self.task.handle(msg) {
            Ok(opts) => {
                // record metrics for how many messages we returned
                match &opts {
                    Some(msgs) => {
                        let len = msgs.len();
                        let metric_count = format!("{}.response.count", &metric_base_name[..]);
                        let metric_gauge = format!("{}.response.gauge", &metric_base_name[..]);
                        // self.metrics.count(&metric_count[..], len.clone());
                        // self.metrics.gauge(&metric_gauge[..], len as isize);
                    }
                    None => {
                        let metric_none = format!("{}.response.none", &metric_base_name[..]);
                        // self.metrics.marker(&metric_none[..]);
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
                let metric_handle = format!("{}.handle.error", &metric_base_name[..]);
                // self.metrics.marker(&metric_handle[..]);
                let req = TopologyRequest::TaskPut(TaskResponse::Error(
                    w.task_msg.source_id,
                    w.task_msg.edge,
                    w.task_msg.index,
                ));
                w.service.do_send(req);
                // TODO: handle do_send error here?
            }
        }
        // timer.stop(timer_id);
    }
}

impl<T> Handler<metric::backend::Flush> for TaskActor<T>
where
    T: Task + Default + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: metric::backend::Flush, ctx: &mut Context<Self>) {
        self.metrics.flush();
    }
}
