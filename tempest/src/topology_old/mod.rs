use actix::prelude::*;

// use redis_streams;
use serde_derive::{Deserialize, Serialize};
use serde_json;
// use uuid::Uuid;

// use std::collections::HashMap;
// use std::collections::VecDeque;
use std::error;
use std::fmt;
use std::time::Duration;
// use std::time::{SystemTime, UNIX_EPOCH};

pub mod pipeline;

use crate::service::server::TopologyServer;
use crate::source::*;
use pipeline::*;

// fn now_millis() -> usize {
//     match SystemTime::now().duration_since(UNIX_EPOCH) {
//         Ok(n) => n.as_millis() as usize,
//         Err(_) => 0,
//     }
// }

/**
 * This is what a user implements in order to run a topology
 *
 */
pub trait Topology<SB: SourceBuilder> {
    // should return a topology service
    // constructed from the builder
    fn service() {}

    fn builder() -> TopologyBuilder<SB>;
}

// pub trait SourceBuilder {
//     type Source;
//     fn build(self) -> Self::Source;
// }

// /// This is trait is for defining Topology Sources
// pub trait Source {
//     type Connection;

//     fn validate(&mut self) -> SourceResult<()> {
//         Err(SourceError::new(SourceErrorKind::ValidateError(
//             "Validate isn't configured for Source trait".to_string(),
//         )))
//     }

//     fn setup(&mut self) -> SourceResult<()> {
//         Ok(())
//     }

//     fn drain(&mut self) -> SourceResult<()> {
//         Ok(())
//     }

//     fn teardown(&mut self) -> SourceResult<()> {
//         Ok(())
//     }

//     fn connect(&mut self) -> SourceResult<()> {
//         Ok(())
//     }

//     fn ack(&mut self, msg_id: MsgId) -> SourceResult<()> {
//         Ok(())
//     }

//     fn max_backoff(&self) -> SourceResult<usize> {
//         Ok(1000usize)
//     }

//     fn poll_interval(&self) -> SourceResult<SourcePollInterval> {
//         Ok(SourcePollInterval::Millisecond(1))
//     }

//     fn poll(&mut self) -> SourcePollResult {
//         Ok(None)
//     }
// }

#[derive(Clone, Debug, PartialEq)]
pub enum TopologyFailurePolicy {
    BestEffort,
    Retry(usize),
}

impl Default for TopologyFailurePolicy {
    fn default() -> Self {
        TopologyFailurePolicy::BestEffort
    }
}

// Some of these properties are stored as String
// to make it easier to share with Actix actors
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopologyOptions {
    /// Name of the topology
    name: &'static str,

    /// Topology id (host:port)
    id: Option<String>,

    /// The uri to connect with the the redis dn
    tempest_db_uri: Option<String>,

    /// The host:port of the agent topology communicates with
    tempest_agent_id: Option<String>,

    /// How should we handle failures?
    failure_policy: Option<TopologyFailurePolicy>,

    /// Max amount of time, in milliseconds, to wait before
    /// moving a pending a message into a failure state.
    /// What happens after a timeout depends on
    /// the failure policy configuration.
    msg_timeout: Option<usize>,
}

impl TopologyOptions {
    pub fn name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }

    pub fn id<'a>(mut self, id: &'a str) -> Self {
        self.id = Some(id.to_string());
        self
    }

    pub fn tempest_db_uri<'a>(mut self, uri: &'a str) -> Self {
        self.tempest_db_uri = Some(uri.to_string());
        self
    }

    pub fn tempest_agent_id<'a>(mut self, id: &'a str) -> Self {
        self.tempest_agent_id = Some(id.to_string());
        self
    }

    pub fn failure_policy(mut self, policy: TopologyFailurePolicy) -> Self {
        self.failure_policy = Some(policy);
        self
    }

    pub fn msg_timeout(mut self, ms: usize) -> Self {
        self.msg_timeout = Some(ms);
        self
    }
}

#[derive(Default)]
pub struct TopologyBuilder<SB: SourceBuilder> {
    options: TopologyOptions,
    pipeline: Pipeline,
    source_builder: SB,
}

impl<SB> TopologyBuilder<SB>
where
    SB: SourceBuilder + Default,
    <SB as SourceBuilder>::Source: Source + Default,
{
    pub fn options(mut self, opts: TopologyOptions) -> Self {
        self.options = opts;
        self
    }

    pub fn pipeline(mut self, pipe: Pipeline) -> Self {
        self.pipeline = pipe.build();
        self
    }

    pub fn source(mut self, sb: SB) -> Self {
        self.source_builder = sb;
        self
    }

    pub fn build_source_actor(self) -> SourceActor<SB::Source> {
        SourceActor {
            source: self.source_builder.build(),
            // topology_addr: None,
            backoff: 0usize,
            next_poll: 0usize,
        }
    }

    pub fn build_topology_actor(&self) -> TopologyActor {
        TopologyActor {
            options: self.options.clone(),
            // source_recipient: None,
            // pipeline_addr: None,
        }
    }

    pub fn build_pipeline_actor(&self) -> PipelineActor {
        PipelineActor {
            pipeline: self.pipeline.clone(),
            // topology_addr: None,
            inflight: PipelineInflight::new(self.options.msg_timeout.clone()),
            available: PipelineAvailable::new(&self.pipeline.tasks),
            aggregate: PipelineAggregate::new(&self.pipeline.tasks),
        }
    }
}

// /// We need to define how a task should
// /// expect messages to flow into it.
// #[derive(Clone, Debug, PartialEq)]
// pub enum TaskIngress {
//     /// This type makes messages available from upstream nodes
//     /// as soon as their picked up in the PipelineActor
//     Normal,

//     /// This type aggregates all incoming task
//     /// messages from upstream nodes before
//     /// making them available to the task.
//     /// Think Fan-In aggregation.
//     Aggregate,
// }

// impl Default for TaskIngress {
//     fn default() -> Self {
//         TaskIngress::Normal
//     }
// }

// #[derive(Clone, Debug, PartialEq)]
// pub struct TaskOptions {
//     workers: u32,
//     ingress: TaskIngress,
// }

// impl Default for TaskOptions {
//     fn default() -> Self {
//         TaskOptions {
//             workers: 1,
//             ingress: TaskIngress::default(),
//         }
//     }
// }

// impl TaskOptions {
//     pub fn workers(mut self, size: u32) -> Self {
//         self.workers = size;
//         self
//     }
// }

// #[derive(Clone, Debug, Default, PartialEq)]
// pub struct Task {
//     name: &'static str,
//     path: &'static str,
//     options: TaskOptions,
// }

// impl Task {
//     pub fn new(name: &'static str, path: &'static str) -> Self {
//         Task {
//             name: name,
//             path: path,
//             options: TaskOptions::default(),
//         }
//     }

//     pub fn name(mut self, name: &'static str) -> Self {
//         self.name = name;
//         self
//     }

//     pub fn path(mut self, path: &'static str) -> Self {
//         self.path = path;
//         self
//     }

//     pub fn options(mut self, options: TaskOptions) -> Self {
//         self.options = options;
//         self
//     }

//     pub fn workers(mut self, size: u32) -> Self {
//         self.options.workers = size;
//         self
//     }
// }

// // type Edge = (&'static str, &'static str);
// type Edge = (String, String);

// type MatrixRow = (&'static str, &'static str, bool);

// type Matrix = Vec<MatrixRow>;

// #[derive(Clone, Debug, Default)]
// pub struct Pipeline {
//     root: &'static str,
//     tasks: Vec<Task>,
//     adjacent: HashMap<&'static str, Vec<&'static str>>,
//     matrix: Matrix,
// }

// impl Pipeline {
//     pub fn root(mut self, name: &'static str) -> Self {
//         self.root = name;
//         self
//     }

//     fn add(mut self, task: Task) -> Self {
//         // copy name so we can set the root if needed
//         let name = task.name.clone();
//         if !self.tasks.contains(&task) {
//             self.tasks.push(task);
//         }
//         if &self.root == &"" {
//             // root is a special-case for adding an edge...
//             // we need to account for it, otherwise,
//             // we'll skip it as a task.
//             // so what we do is make it an edge("root", &name)
//             let copy = self.edge("root", &name);
//             copy.root(&name)
//         } else {
//             self
//         }
//     }

//     pub fn edge(mut self, left: &'static str, right: &'static str) -> Self {
//         // TODO: what if left or right doesn't exist in self.tasks?
//         // if this row already exists, we don't need to add it again
//         let matrix_row = (left, right, false);
//         if self
//             .matrix
//             .iter()
//             .position(|r| r.0 == left && r.1 == right)
//             .is_none()
//         {
//             self.matrix.push(matrix_row);
//             // add it to the adjacency matrix
//             match self.adjacent.get_mut(left) {
//                 Some(v) => v.push(right),
//                 None => {
//                     self.adjacent.insert(left, vec![right]);
//                 }
//             }
//         }
//         self
//     }

//     fn build(self) -> Self {
//         self
//     }

// }

// pub enum SourcePollInterval {
//     Millisecond(u64),
// }

// impl SourcePollInterval {
//     fn as_duration(&self) -> Duration {
//         match *self {
//             SourcePollInterval::Millisecond(v) => Duration::from_millis(v),
//         }
//     }
// }

// pub type MsgId = Vec<u8>;
// pub type Msg = Vec<u8>;

// #[derive(Default, Message, Debug)]
// pub struct SourceMsg {
//     pub id: MsgId,
//     pub msg: Msg,
//     // timestamp
//     pub ts: usize,
// }

// pub type SourcePollResult = Result<Option<Vec<SourceMsg>>, SourceError>;

// pub type SourceResult<T> = Result<T, SourceError>;

// pub enum SourceErrorKind {
//     // General std::io::Error
//     Io(std::io::Error),
//     /// Error kind when client connection encounters an error
//     Client(String),
//     /// Error kind when source isn't correctly configured
//     ValidateError(String),
//     /// Error kind when we just need one
//     Other(String),
// }

// pub struct SourceError {
//     kind: SourceErrorKind,
// }

// impl SourceError {
//     fn new(kind: SourceErrorKind) -> Self {
//         SourceError { kind: kind }
//     }

//     fn make_io(err: std::io::Error) -> Self {
//         SourceError::new(SourceErrorKind::Io(err))
//     }
// }

// impl fmt::Display for SourceError {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "A Source Error Occurred")
//     }
// }

// impl fmt::Debug for SourceError {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(
//             f,
//             "Source error: {{ file: {}, line: {} }}",
//             file!(),
//             line!()
//         )
//     }
// }

mod source {

    // use std::str::{from_utf8, Utf8Error};

    // use super::{
    //     now_millis, Source, SourceBuilder, SourceError, SourceErrorKind, SourceMsg,
    //     SourcePollInterval, SourcePollResult, SourceResult,
    // };

    // use super::redis_streams::{
    //     client_open, Client, Commands, Connection, ErrorKind, RedisError, RedisResult,
    //     StreamCommands, StreamId, StreamInfoGroup, StreamInfoGroupsReply, StreamReadOptions,
    //     StreamReadReply, ToRedisArgs, Value,
    // };

    // use super::Uuid;

    // pub struct RedisErrorToSourceError;

    // impl RedisErrorToSourceError {
    //     fn convert(err: RedisError) -> SourceError {
    //         let s = err.category().to_string();
    //         SourceError::new(SourceErrorKind::Client(s))
    //     }
    // }

    // #[derive(Default)]
    // pub struct RedisStreamSourceBuilder<'a> {
    //     item: RedisStreamSource<'a>,
    // }

    // impl<'a> RedisStreamSourceBuilder<'a> {
    //     pub fn uri(mut self, uri: &'a str) -> Self {
    //         self.item.uri = Some(uri);
    //         self
    //     }

    //     pub fn key(mut self, key: &'a str) -> Self {
    //         self.item.key = Some(key);
    //         self
    //     }

    //     pub fn group(mut self, name: &'a str) -> Self {
    //         self.item.group = Some(name);
    //         self
    //     }

    //     pub fn with_blocking_read(mut self) -> Self {
    //         self.item.blocking_read = Some(true);
    //         self
    //     }

    //     pub fn group_starting_id(mut self, id: &'a str) -> Self {
    //         self.item.group_starting_id = Some(RedisStreamGroupStartingId::from(id));
    //         self
    //     }

    //     pub fn reclaim_pending_after(mut self, ms: usize) -> Self {
    //         self.item.reclaim_pending_after = Some(ms);
    //         self
    //     }

    //     pub fn read_msg_count(mut self, count: usize) -> Self {
    //         self.item.read_msg_count = Some(count);
    //         self
    //     }

    //     pub fn poll_interval(mut self, ms: u64) -> Self {
    //         self.item.poll_interval = Some(ms);
    //         self
    //     }
    // }

    // impl<'a> SourceBuilder for RedisStreamSourceBuilder<'a> {
    //     type Source = RedisStreamSource<'a>;

    //     fn build(self) -> Self::Source {
    //         self.item
    //     }
    // }

    // #[derive(Clone, Debug)]
    // pub enum RedisStreamGroupStartingId<'a> {
    //     Zero,
    //     Dollar,
    //     Other(&'a str),
    // }

    // impl<'a> RedisStreamGroupStartingId<'a> {
    //     fn from(id: &'a str) -> Self {
    //         match id {
    //             "0" => RedisStreamGroupStartingId::Zero,
    //             "$" => RedisStreamGroupStartingId::Dollar,
    //             _ => RedisStreamGroupStartingId::Other(id),
    //         }
    //     }

    //     fn as_str(&self) -> &str {
    //         return match &self {
    //             RedisStreamGroupStartingId::Zero => "0",
    //             RedisStreamGroupStartingId::Dollar => "$",
    //             RedisStreamGroupStartingId::Other(val) => val,
    //         };
    //     }
    // }

    // impl<'a> Default for RedisStreamGroupStartingId<'a> {
    //     fn default() -> Self {
    //         RedisStreamGroupStartingId::Dollar
    //     }
    // }

    // pub struct RedisStreamSource<'a> {
    //     uri: Option<&'a str>,
    //     key: Option<&'a str>,
    //     group: Option<&'a str>,
    //     consumer: Option<String>,
    //     conn: Option<Connection>,
    //     client: Option<Client>,
    //     group_starting_id: Option<RedisStreamGroupStartingId<'a>>,
    //     reclaim_pending_after: Option<usize>,
    //     blocking_read: Option<bool>,
    //     read_msg_count: Option<usize>,
    //     poll_interval: Option<u64>,
    //     max_backoff: Option<usize>,
    // }

    // impl<'a> Default for RedisStreamSource<'a> {
    //     fn default() -> Self {
    //         RedisStreamSource {
    //             /// The redis uri where the stream lives
    //             uri: None,

    //             /// Thre redis key for the stream
    //             key: None,

    //             /// The group name we should assign to this consumer
    //             group: None,

    //             /// This is the auto-generated hash we use as the group consumer name
    //             consumer: Some(format!("{}", Uuid::new_v4().to_simple())),

    //             /// The redis connection
    //             conn: None,

    //             /// The redis redis client for getting connections
    //             client: None,

    //             /// Defines the message id we should start reading
    //             /// the stream from.
    //             group_starting_id: Some(RedisStreamGroupStartingId::default()),

    //             /// How milliseconds should we consider a pending message before trying to reclaim it?
    //             reclaim_pending_after: None,

    //             /// Configure if we should read consumer group streams in blocking mode.
    //             blocking_read: Some(false),

    //             /// Configure the number of messages we should read per xread redis command
    //             read_msg_count: Some(10usize),

    //             /// Configure the poll interval milliseconds
    //             poll_interval: Some(100u64),

    //             /// Configure the max backoff milliseconds
    //             max_backoff: Some(1000usize),
    //             // TODO: add deadletter queue here
    //             // instantiate as a RedisQueueSource
    //         }
    //     }
    // }

    // impl<'a> RedisStreamSource<'a> {
    //     fn connection(&mut self) -> SourceResult<&mut Connection> {
    //         match &mut self.conn {
    //             Some(conn) => Ok(conn),
    //             None => {
    //                 return Err(SourceError::new(SourceErrorKind::Other(
    //                     "Source connection is None".to_string(),
    //                 )))
    //             }
    //         }
    //     }

    //     fn prime_test_messages(&mut self) -> SourceResult<()> {
    //         let key = self.key.as_ref().unwrap().to_string();
    //         let conn = &mut self.connection()?;
    //         for i in 0..100 {
    //             let _: RedisResult<String> =
    //                 conn.xadd(&key, "*", &[("k", "v"), ("i", &i.to_string())]);
    //         }
    //         Ok(())
    //     }

    //     fn read_unclaimed(&mut self) -> SourceResult<Option<Vec<SourceMsg>>> {
    //         // always read unclaimed messages
    //         let count = self.read_msg_count.as_ref().unwrap();
    //         let key = self.key.as_ref().unwrap().to_string();
    //         let group = self.group.as_ref().unwrap();
    //         let consumer = self.consumer.as_ref().unwrap().to_string();

    //         // TODO: Configure blocking read?
    //         let read_opts = StreamReadOptions::default()
    //             .group(*group, consumer)
    //             .count(*count);

    //         // The special > ID, which means that the
    //         // consumer want to receive only messages
    //         // that were never delivered to any other consumer.
    //         // It just means, give me new messages.
    //         let conn = &mut self.connection()?;
    //         let result: RedisResult<StreamReadReply> =
    //             conn.xread_options(&[key], &[">"], read_opts);

    //         match result {
    //             Ok(reply) => {
    //                 if reply.keys.len() == 0 {
    //                     return Ok(None);
    //                 }

    //                 // convert StreamId to SourceMsg
    //                 let mut msgs = vec![];
    //                 for msg in &reply.keys[0].ids {
    //                     // conver the msg.map => json as byte vec
    //                     let mut json_map = serde_json::map::Map::default();
    //                     for (key, val) in &msg.map {
    //                         match *val {
    //                             Value::Data(ref b) => match from_utf8(b) {
    //                                 Ok(s) => {
    //                                     json_map.insert(
    //                                         key.to_string(),
    //                                         serde_json::Value::String(s.to_string()),
    //                                     );
    //                                 }
    //                                 Err(err) => {}
    //                             },
    //                             _ => {}
    //                         };
    //                     }

    //                     let mut source_msg = SourceMsg::default();
    //                     source_msg.id = msg.id.as_bytes().to_vec();
    //                     source_msg.ts = now_millis();

    //                     // now, convert to byte vec
    //                     match serde_json::to_vec(&serde_json::Value::Object(json_map)) {
    //                         Ok(vec) => source_msg.msg = vec,
    //                         Err(err) => {}
    //                     }

    //                     msgs.push(source_msg);
    //                 }
    //                 Ok(Some(msgs))
    //             }
    //             Err(e) => {
    //                 // TODO: handle this error so caller can
    //                 // figure out what to do next
    //                 println!("Error reading stream {:?}", e);
    //                 Ok(None)
    //             }
    //         }
    //     }

    //     fn group_create(&mut self) -> SourceResult<()> {
    //         // we need to check or the client ends up with a broken pipe
    //         // technically, if we're calling group_create here
    //         // then we should have already created the connection
    //         // we just need to unwrap a few things...
    //         let key = self.key.unwrap();
    //         let group = self.group.unwrap();
    //         let starting_id = self.group_starting_id.clone().unwrap();
    //         let conn = self.connection()?;

    //         let result: RedisResult<bool> = conn.exists(key);
    //         match result {
    //             Ok(true) => {
    //                 // do we already have a group for this stream?
    //                 let info: StreamInfoGroupsReply = conn.xinfo_groups(key).unwrap();
    //                 let group_exists = &info
    //                     .groups
    //                     .into_iter()
    //                     .filter(|g| g.name == group.to_string())
    //                     .collect::<Vec<StreamInfoGroup>>()
    //                     .len()
    //                     > &0;
    //                 if !group_exists {
    //                     let _: RedisResult<String> =
    //                         conn.xgroup_create(key, group, starting_id.as_str());
    //                 }
    //             }
    //             Ok(false) => {
    //                 let _: RedisResult<String> =
    //                     conn.xgroup_create_mkstream(key, group, starting_id.as_str());
    //             }
    //             Err(e) => {
    //                 println!("Error group_create: {:?}", e);
    //             }
    //         }

    //         Ok(())
    //     }
    // }

    // impl<'a> Source for RedisStreamSource<'a> {
    //     type Connection = Connection;

    //     // Any field that's required from setup on
    //     // should be checked here.
    //     fn validate(&mut self) -> SourceResult<()> {
    //         return if self.uri.is_none() {
    //             Err(SourceError::new(SourceErrorKind::ValidateError(
    //                 "Missing redis uri for source.".to_string(),
    //             )))
    //         } else if self.key.is_none() {
    //             Err(SourceError::new(SourceErrorKind::ValidateError(
    //                 "Missing redis key for source.".to_string(),
    //             )))
    //         } else if self.group.is_none() {
    //             Err(SourceError::new(SourceErrorKind::ValidateError(
    //                 "Missing redis group for source.".to_string(),
    //             )))
    //         } else {
    //             Ok(())
    //         };
    //     }

    //     // configure the connection & client
    //     // create the stream and group if needed
    //     fn setup(&mut self) -> SourceResult<()> {
    //         // call validate here
    //         self.validate()?;

    //         let uri = self.uri.unwrap();
    //         println!("create redis client: {:?}", &uri);
    //         match client_open(uri) {
    //             Ok(client) => {
    //                 match client.get_connection() {
    //                     Ok(conn) => {
    //                         self.conn = Some(conn);
    //                     }
    //                     Err(err) => {
    //                         println!("redis client.get_connection error: {:?}", err);
    //                         return Err(RedisErrorToSourceError::convert(err));
    //                     }
    //                 }
    //                 // now store the client
    //                 self.client = Some(client);
    //             }
    //             Err(err) => {
    //                 println!("redis client_open error: {:?}", err);
    //                 return Err(RedisErrorToSourceError::convert(err));
    //             }
    //         }

    //         // we need to create the stream here
    //         println!("create group");
    //         self.group_create()?;

    //         println!("print test messages");
    //         self.prime_test_messages()?;

    //         Ok(())
    //     }

    //     fn max_backoff(&self) -> SourceResult<usize> {
    //         match self.max_backoff {
    //             Some(v) => Ok(v),
    //             None => Source::max_backoff(self),
    //         }
    //     }

    //     fn poll_interval(&self) -> SourceResult<SourcePollInterval> {
    //         match self.poll_interval {
    //             Some(v) => Ok(SourcePollInterval::Millisecond(v)),
    //             None => Source::poll_interval(self),
    //         }
    //     }

    //     fn poll(&mut self) -> SourcePollResult {
    //         self.read_unclaimed()
    //     }
    // }

    // pub enum RedisQueueType {
    //     LPOP,
    //     RPOP,
    // }

    // pub struct RedisQueueSource<'a> {
    //     key: &'a str,
    //     queue_type: RedisQueueType,
    // }

}

pub mod task {

    // use super::Msg;
    // use super::{Deserialize, Serialize};
    // use super::{TaskMsg, TaskResponse};
    // use crate::service::codec::TopologyRequest;
    // use crate::service::task::TaskService;
    // use actix::prelude::*;

    // // TODO: make a custom error for the result
    // pub type TaskResult<T> = Result<T, std::io::Error>;

    // pub trait Task {
    //     type Msg;

    //     fn started(&mut self) {}
    //     fn deser(&mut self, msg: Msg) -> Self::Msg;
    //     fn handle(&mut self, msg: Self::Msg) -> TaskResult<Option<Vec<Msg>>> {
    //         Ok(None)
    //     }
    //     fn shutdown(&mut self) {}
    // }

    // #[derive(Message)]
    // pub struct TaskMsgWrapper<T: Task + Default + 'static> {
    //     pub service: Addr<TaskService<T>>,
    //     pub task_msg: TaskMsg,
    // }

    // #[derive(Default)]
    // pub struct TaskActor<T> {
    //     pub task: T,
    // }

    // impl<T> Supervised for TaskActor<T> where T: Task + Default + 'static {}

    // impl<T> SystemService for TaskActor<T>
    // where
    //     T: Task + Default + 'static,
    // {
    //     fn service_started(&mut self, ctx: &mut Context<Self>) {
    //         println!("Task actor service started");
    //     }
    // }

    // impl<T> Actor for TaskActor<T>
    // where
    //     T: Task + Default + 'static,
    // {
    //     type Context = Context<Self>;
    // }

    // impl<T> Handler<TaskMsgWrapper<T>> for TaskActor<T>
    // where
    //     T: Task + Default + 'static,
    // {
    //     type Result = ();
    //     fn handle(&mut self, w: TaskMsgWrapper<T>, ctx: &mut Context<Self>) {
    //         println!("TaskActor handle TaskMsgWrapper {:?}", &w.task_msg);

    //         // convert to this to the Task::handle msg
    //         let msg = self.task.deser(w.task_msg.msg);
    //         // send results to the task service actor
    //         match self.task.handle(msg) {
    //             Ok(opts) => {
    //                 let ack = TopologyRequest::TaskPut(TaskResponse::Ack(
    //                     w.task_msg.source_id,
    //                     w.task_msg.edge,
    //                     w.task_msg.index,
    //                     opts,
    //                 ));
    //                 w.service.do_send(ack);
    //             }
    //             Err(err) => {
    //                 // TODO: finish this
    //                 // TopologyRequest::TaskPut(TaskResponse::Error())
    //             }
    //         }
    //     }
    // }

}

/// Actors

// A Task can return multiple messages
// in a response which are then passed into
// the next TaskService. For this reason,
// we need to keep track
// of the index of the sub-task so we know
// when to mark the edge as visted.

#[derive(Serialize, Deserialize, Message, Debug)]
pub struct TaskMsg {
    source_id: MsgId,
    edge: Edge,
    index: usize,
    msg: Msg,
}

#[derive(Message, Debug)]
pub enum TaskRequest {
    // session_id, task_name, count
    GetAvailable(usize, String, Option<usize>),
    // session_id, task_name, tasks
    GetAvailableResponse(usize, String, Option<Vec<TaskMsg>>),
}

#[derive(Message, Serialize, Deserialize, Debug)]
pub enum TaskResponse {
    // source_id, edge, index, response
    Ack(MsgId, Edge, usize, Option<Vec<Msg>>),
    // source_id, edge, index
    Error(MsgId, Edge, usize),
}

#[derive(Default)]
pub struct SourceActor<S: Source> {
    source: S,
    // topology_addr: Option<Addr<TopologyActor>>,

    // backoff delay
    backoff: usize,
    // next_poll
    next_poll: usize,
}

impl<S> SourceActor<S>
where
    S: Source + 'static,
{
    fn reset_backoff(&mut self) {
        self.backoff = 0usize;
        self.next_poll = 0usize;
    }

    fn backoff(&mut self) {
        // read max_backoff from source
        let max_backoff = self.source.max_backoff().unwrap();
        if self.backoff < max_backoff {
            self.backoff += 100;
        }
        self.next_poll = now_millis() + self.backoff;
    }

    fn poll(&mut self, _ctx: &mut Context<Self>) {
        let topology = TopologyActor::from_registry();
        if !topology.connected() {
            println!("SourceActor#poll topology isn't connected");
            return;
        }

        let now = now_millis();
        if self.next_poll > now {
            // println!("SourceActor#backoff {}, {}", self.next_poll, now);
            return;
        }

        println!("SourceActor#polling...");

        // 1. send this to the topology_addr
        // Result<Option<Vec>, SourceError>
        match self.source.poll() {
            Ok(option) => match option {
                Some(results) => {
                    // if results are empty
                    // we need to initiate the backoff
                    if results.len() == 0usize {
                        self.backoff();
                        return;
                    }

                    // reset backoff
                    self.reset_backoff();

                    println!("Source polled {:?} msgs", results.len());

                    // TODO: if results length < max_read_msg...
                    // then considier a partial backoff here

                    // send these msg to the topology actor
                    for msg in results {
                        match topology.try_send(msg) {
                            // count errors here...
                            // we need to know when to backoff
                            // if we can reach the addr
                            Err(SendError::Full(msg)) => {
                                // sleep & update backoff here?
                                // we need future `poll` calls
                                // from getting into the method
                                // at the same time we need to slow
                                // down how fast we push messages into
                                // the addr
                            }
                            Err(SendError::Closed(msg)) => {
                                // we need to kill this actor here
                                // TODO: move this to deadletter q?
                            }
                            _ => {}
                        }
                    }
                }
                None => {
                    self.backoff();
                }
            },
            Err(e) => {
                // handle this error
            }
        }
    }
}

impl<S> Actor for SourceActor<S>
where
    S: Source + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        // setup the source here...
        // TODO: verify this isn't an error
        let _ = self.source.setup();
        let poll_interval = match self.source.poll_interval() {
            Ok(int) => int.as_duration(),
            Err(err) => {
                // Not sure how we would end up here...
                println!(
                    "SourceActor#started, failed to initialize poll_interval: {:?}",
                    err
                );
                Duration::from_secs(1)
            }
        };
        ctx.run_interval(poll_interval, Self::poll);
    }
}

impl<S> Supervised for SourceActor<S> where S: Source + 'static {}

impl<S> SystemService for SourceActor<S>
where
    S: Source + 'static + Default,
{
    fn service_started(&mut self, ctx: &mut Context<Self>) {
        println!("SourceActor service started");
    }
}

#[derive(Message, Debug)]
pub struct SourceAckMsg(MsgId);

impl<S> Handler<SourceAckMsg> for SourceActor<S>
where
    S: Source + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: SourceAckMsg, ctx: &mut Context<Self>) {
        println!("ack this message {:?} {:?}", &msg, &ctx);
    }
}

#[derive(Default)]
pub struct TopologyActor {
    options: TopologyOptions,
}

impl Actor for TopologyActor {
    type Context = Context<Self>;
}

impl Supervised for TopologyActor {}

impl SystemService for TopologyActor {
    fn service_started(&mut self, ctx: &mut Context<Self>) {
        println!("Topology service started: {:?}", self.options);
    }
}

// Source -> Pipeline

impl Handler<SourceMsg> for TopologyActor {
    type Result = ();

    fn handle(&mut self, msg: SourceMsg, ctx: &mut Context<Self>) {
        let pipeline = PipelineActor::from_registry();
        if !pipeline.connected() {
            // TODO: wire up a retry here
            println!("PipelineActor isn't connected... dropping msg: {:?}", &msg);
            return;
        }

        match pipeline.try_send(PipelineMsg::TaskRoot(msg)) {
            Err(SendError::Full(msg)) => {
                // TODO: wireup holding pen?
            }
            Err(SendError::Closed(msg)) => {}
            _ => {}
        }
    }
}

// Client -> Server request for available tasks and
// and response back out through the TopologyServer

impl Handler<TaskRequest> for TopologyActor {
    type Result = ();

    fn handle(&mut self, msg: TaskRequest, ctx: &mut Context<Self>) {
        // println!("PipelineActor Handler<TaskRequest> {:?}", &msg);
        match &msg {
            TaskRequest::GetAvailable(_, _, _) => {
                // forward this to pipeline actor
                let pipeline = PipelineActor::from_registry();
                if !pipeline.connected() {
                    return;
                }
                pipeline.do_send(msg);
            }
            TaskRequest::GetAvailableResponse(_, _, _) => {
                let topology_server = TopologyServer::from_registry();
                if !topology_server.connected() {
                    return;
                }
                topology_server.do_send(msg);
            }
        }
    }
}

// Client -> Server response for processed tasks

impl Handler<TaskResponse> for TopologyActor {
    type Result = ();

    // This is for a message moving back up the chain...
    // mostly for stats purposes
    fn handle(&mut self, msg: TaskResponse, ctx: &mut Context<Self>) {
        println!("Handler<TaskResponse> for TopologyActor: {:?}", &msg);
        // forward this to the pipeline actor
        let pipeline = PipelineActor::from_registry();
        if !pipeline.connected() {
            return;
        }

        let pipe_msg = match &msg {
            TaskResponse::Ack(_, _, _, _) => PipelineMsg::TaskAck(msg),
            TaskResponse::Error(_, _, _) => PipelineMsg::TaskAck(msg),
        };
        pipeline.do_send(pipe_msg);
    }
}

impl Handler<PipelineMsg> for TopologyActor {
    type Result = ();

    // This is for a message moving back up the chain...
    // mostly for stats purposes
    fn handle(&mut self, msg: PipelineMsg, ctx: &mut Context<Self>) {
        println!("{:?}", &msg);

        match msg {
            PipelineMsg::SourceMsgAck(msg_id) => {}
            PipelineMsg::SourceMsgTimeout(msg_id) => {}
            PipelineMsg::SourceMsgError(msg_id) => {}
            _ => {
                // not implemented here
            }
        }
    }
}

#[derive(Message, Debug)]
pub enum PipelineMsg {
    TaskRoot(SourceMsg),
    TaskAck(TaskResponse),
    TaskError(TaskResponse),
    SourceMsgAck(MsgId),
    SourceMsgTimeout(MsgId),
    SourceMsgError(MsgId),
}

// #[derive(Debug, Default)]
// pub struct PipelineMsgState {
//     matrix: Matrix,

//     // pending is HashMap<edge, Vec<(index, completed)>>
//     pending: HashMap<Edge, Vec<(usize, bool)>>,
// }

// impl PipelineMsgState {
//     fn new(matrix: Matrix) -> Self {
//         PipelineMsgState {
//             matrix: matrix,
//             pending: HashMap::new(),
//         }
//     }

//     fn edge_start(&mut self, edge: Edge, size: usize) {
//         // initialize a vector to keep track of
//         // of which sub-tasks we've completed
//         let items = (0..size).map(|index| (index, false)).collect();
//         self.pending.insert(edge, items);
//     }

//     fn edge_visit(&mut self, edge: &Edge) {
//         for mut row in &mut self.matrix {
//             if row.0 == edge.0 && row.1 == edge.1 {
//                 row.2 = true;
//                 break;
//             }
//         }
//     }

//     fn edge_visit_deadends(&mut self,  edge: &Edge) {

//         // we need to mark this edge as visited
//         self.edge_visit(&edge);

//         // get all decendants of this edge
//         for decendant in self.decendants(&edge.0) {
//             // now get the ancestors for the right edge
//             // if we have only one ancestor then
//             // recurse down until we reach the end
//             if self.ancestors(&decendant.1.to_string()).len() == 1 {
//                 self.edge_visit_deadends( &(decendant.0.to_string(), decendant.1.to_string()) );
//             }
//         }
//     }

//     fn edge_visit_index(&mut self, edge: &Edge, index: usize) -> Option<bool> {
//         // lookup this edge in the map...
//         // and mark it as completed
//         let pending = match self.pending.get_mut(edge) {
//             Some(p) => p,
//             None => return None,
//         };

//         // iterate the entire vector here...
//         // and count how many completed we have
//         let mut completed = 0;
//         let total = pending.len();

//         for pair in pending {
//             if pair.0 == index {
//                 pair.1 = true;
//             }
//             if pair.1 {
//                 completed += 1;
//             }
//         }

//         // to mark this edge as visited,
//         // completed needs to equal total
//         let next = completed == total;

//         // if all sub-tasks are completed
//         // for this edge then we need to update
//         // and mark this matrix edge as visited
//         if next {
//             self.edge_visit(&edge);
//         }

//         // Response here should be either:
//         // next = false (waiting) or true (visited)...
//         Some(next)
//     }

//     /// Have all the nodes been visited in the matrix?
//     fn finished(&self) -> bool {
//         for row in &self.matrix {
//             if row.2 == false {
//                 return false;
//             }
//         }
//         true
//     }

//     // return the total number of ancestors, and the total number of visted ancestors
//     fn ancestors_visited(&self, name: &String) -> (u16, u16) {
//         let mut total = 0;
//         let mut visited = 0;
//         let name = &name[..];
//         for row in &self.matrix {
//             // right should equal name for this to be an ancestor
//             if row.1 == name {
//                 total += 1;
//                 if row.2 == true {
//                     visited += 1;
//                 }
//             }
//         }
//         (total, visited)
//     }

//     // TODO: figure out how to pre-compute ancestors for
//     // each node without having to iterate the matrix
//     // every time and clone.. and return a Vec<Edge>
//     fn ancestors(&self, name: &String) -> Vec<MatrixRow> {
//         let mut found = Vec::new();
//         let name = &name[..];
//         for row in &self.matrix {
//             if row.1 == name {
//                 found.push(row.clone())
//             }
//         }
//         found
//     }

//     // Since we don't store the adjacent hash we need this for now
//     // TODO: store the adjacent so we don't need to do
//     // all this cloning.. and return a Vec<Edge>
//     fn decendants(&self, name: &String) -> Vec<MatrixRow> {
//         let mut found = Vec::new();
//         let name = &name[..];
//         for row in &self.matrix {
//             if row.0 == name {
//                 found.push(row.clone())
//             }
//         }
//         found
//     }

// }

// /// Enum for communicating the inflight status of pipeline messages.

// pub enum PipelineInflightStatus {
//     // We've reached a completed Edge
//     // Return total, visted for this right node ancestors
//     AckEdge(u16, u16),

//     // We've completed the end of the pipeline
//     AckSource,

//     // We're still waiting to ack inflight edge messages
//     PendingEdge,

//     // This original source msg id ultimately timed out
//     Timeout,

//     // The msg id no longer exists in the map
//     Removed,
// }

// // Pipeline Inflight
// #[derive(Default)]
// pub struct PipelineInflight {
//     msg_timeout: Option<usize>,
//     /// HashMap<source_msg_id, (timestamp, state)>
//     map: HashMap<MsgId, (usize, PipelineMsgState)>,
// }

// impl PipelineInflight {
//     fn new(msg_timeout: Option<usize>) -> Self {
//         PipelineInflight {
//             msg_timeout: msg_timeout,
//             ..PipelineInflight::default()
//         }
//     }

//     // actions we can do here...
//     // insert msgid
//     fn root(&mut self, msg_id: MsgId, timestamp: usize, state: PipelineMsgState) {
//         self.map.insert(msg_id, (timestamp, state));
//     }

//     // This method supports the use-case where an AckEdge triggers
//     // releasing an empty holding pen...
//     // We need to visit all decendant edges with a single ancestor from here
//     // since this method is always called after `ack` we can skip
//     // returning the PipelineInflightStatus here
//     fn ack_deadend(&mut self, msg_id: &MsgId, edge: &Edge) {
//         if let Some((timestamp, msg_state)) = self.map.get_mut(msg_id) {
//             msg_state.edge_visit_deadends(edge);
//         }
//     }

//     fn ack(&mut self, msg_id: &MsgId, edge: &Edge, index: usize) -> PipelineInflightStatus {
//         // get this msg id in the map
//         // and update the index for this edge
//         // in the msg state
//         let status = if let Some((timestamp, msg_state)) = self.map.get_mut(msg_id) {
//             // check for a timeout?
//             if let Some(ms) = &self.msg_timeout {
//                 let timeout = *timestamp + ms;
//                 if timeout < now_millis() {
//                     return PipelineInflightStatus::Timeout;
//                 }
//             }

//             // Now we have three possible states here...
//             // To start, let's mark this index as completed...
//             match msg_state.edge_visit_index(edge, index) {
//                 Some(edge_completed) => {
//                     if edge_completed {
//                         // Now we just need to iterate the msg_state.matrix
//                         // and check the status for all edges
//                         // if all are true then AckSource
//                         if msg_state.finished() {
//                             PipelineInflightStatus::AckSource
//                         } else {
//                             let (total, visited) = msg_state.ancestors_visited(&edge.1);
//                             PipelineInflightStatus::AckEdge(total, visited)
//                         }
//                     } else {
//                         PipelineInflightStatus::PendingEdge
//                     }
//                 }
//                 None => {
//                     // We shouldn't ever hit this...
//                     PipelineInflightStatus::Removed
//                 }
//             }
//         } else {
//             // if we no longer have an entry, this means someone deleted it...
//             // (either timeout or error state was hit)
//             // so just return Removed here
//             PipelineInflightStatus::Removed
//         };
//         status
//     }

//     fn remove(&mut self, msg_id: &MsgId) {
//         self.map.remove(msg_id);
//     }
// }

// #[derive(Default)]
// pub struct PipelineAvailable {
//     // key = Task Name
//     queue: HashMap<String, VecDeque<TaskMsg>>,
// }

// impl PipelineAvailable {
//     // pre-initialize the hashmap key queues
//     fn new(tasks: &Vec<Task>) -> Self {
//         let mut this = Self::default();
//         for task in tasks {
//             let key = task.name.clone();
//             this.queue.insert(key.to_string(), VecDeque::new());
//         }
//         this
//     }

//     /// Pops the n-count of tasks from the front of the queue
//     fn pop(&mut self, name: &String, count: Option<usize>) -> Option<Vec<TaskMsg>> {
//         let count = match count {
//             Some(i) => i,
//             None => 1 as usize,
//         };

//         return self.queue.get_mut(name).map_or(None, |q| {
//             let mut tasks = vec![];
//             while let Some(msg) = q.pop_front() {
//                 &tasks.push(msg);
//                 if tasks.len() >= count {
//                     break;
//                 }
//             }
//             Some(tasks)
//         });
//     }

//     fn push(&mut self, name: &String, msg: TaskMsg) -> Option<usize> {
//         return self.queue.get_mut(name).map_or(None, |q| {
//             q.push_back(msg);
//             Some(q.len())
//         });
//     }
// }

// #[derive(Default)]
// pub struct PipelineAggregate {
//     holding: HashMap<String, HashMap<MsgId, Vec<Msg>>>,
// }

// impl PipelineAggregate {

//     fn new(tasks: &Vec<Task>) -> Self {
//         let mut this = Self::default();
//         for task in tasks {
//             let key = task.name.clone();
//             this.holding.insert(key.to_string(), HashMap::new());
//         }
//         this
//     }

//     // hold this msg for this task_name, source_msg_id, msg
//     pub fn hold(&mut self, name: &String, msg_id: MsgId, mut msgs: Vec<Msg>) -> Option<bool> {
//         return self.holding.get_mut(name).map_or(None, |map| {
//             if !map.contains_key(&msg_id) {
//                 // insert a new msg
//                 map.insert(msg_id, msgs);
//             } else {
//                 // store the msg with this msg_id
//                 map.get_mut(&msg_id).unwrap().append(&mut msgs);
//             }
//             Some(true)
//         });
//     }

//     // returns all messages for this task_name and msg_id
//     pub fn remove(&mut self, name: &String, msg_id: &MsgId) -> Option<Vec<Msg>> {
//         return self
//             .holding
//             .get_mut(name)
//             .map_or(None, |map| map.remove(msg_id));
//     }
// }

#[derive(Default)]
pub struct PipelineActor {
    /// Pipeline task definitions and graph
    pipeline: Pipeline,
    /// Messages in-flight and their state
    /// HashMap<source_msg_id, (timestamp, state)>
    inflight: PipelineInflight,
    /// Queues of available tasks for processing
    /// by connected TaskServices
    available: PipelineAvailable,
    /// Aggregate tasks by msg_id and task_name
    /// for Tasks defined with TaskIngress::Aggregate
    aggregate: PipelineAggregate,
}

impl Actor for PipelineActor {
    type Context = Context<Self>;
}

impl Supervised for PipelineActor {}

impl SystemService for PipelineActor {
    fn service_started(&mut self, ctx: &mut Context<Self>) {
        println!("PipelineActor service started");
    }
}

impl PipelineActor {
    fn task_root(&mut self, src_msg: SourceMsg) {
        let matrix = self.pipeline.matrix.clone();
        let root = matrix[0].0.to_string();
        let task_name = matrix[0].1.to_string();
        let edge = (root, task_name.clone());


        // 1. msg state (we always start with 1 msg)
        let mut msg_state = PipelineMsgState::new(matrix);
        &msg_state.edge_start(edge.clone(), 1);

        // 2. inflight
        self.inflight.root(
            src_msg.id.clone(),
            src_msg.ts.clone(),
            msg_state,
        );

        // 3. available
        // create TaskMsg and it to PipelineAvailable
        // for the starting task name
        let task_msg = TaskMsg {
            source_id: src_msg.id.clone(),
            edge: edge,
            index: 0,
            msg: src_msg.msg.clone(),
        };
        self.available.push(&task_name, task_msg);
    }

    fn task_ack(&mut self, task_resp: TaskResponse) {
        println!("PipelineActor.task_resp: {:?}", task_resp);

        // update pending msg states
        // mark edge visted
        // move to the next task (if necessary, release aggregate holds)
        // determine if we have a timeout error

        match task_resp {
            TaskResponse::Ack(msg_id, edge, index, task_result) => {

                let ack_name = &edge.1[..];

                // grab the list of decendants for this edge
                let mut decendants = &vec![];
                match self.pipeline.adjacent.get(ack_name) {
                    Some(names) => decendants = names,
                    None => {}
                };

                // What's the PipelineInflightStatus for this msg id?
                let status = self.inflight.ack(&msg_id, &edge, index);
                match status {
                    PipelineInflightStatus::AckEdge(ancestors, visited) => {

                        // if this edge has multiple ancestors
                        // see if they've all be visited yet
                        if ancestors == visited {
                            // all ancestor edges are complete...
                            // time to release all aggregate messages siting in the
                            // holding pen to the decendant tasks
                            for name in decendants.iter() {
                                // take the aggregate holding msgs
                                // wrap them in TaskMsg and move them to the holding
                                // queues
                                let next_edge = (ack_name.to_string(), name.to_string());
                                match self.aggregate.remove(&name.to_string(), &msg_id) {
                                    Some(msgs) => {
                                        for (index, msg) in msgs.iter().enumerate() {
                                            let task_msg = TaskMsg {
                                                source_id: msg_id.clone(),
                                                edge: next_edge.clone(),
                                                index: index,
                                                msg: msg.to_vec(),
                                            };
                                            self.available.push(&name.to_string(), task_msg);
                                        }
                                    },
                                    None => {
                                        // what happens if we reach this?
                                        // mark this next_edge as visited
                                        self.inflight.ack_deadend(&msg_id, &next_edge);

                                    }
                                }

                            }
                            // cleanup code goes here?

                        } else {
                            // nothing to do but wait...
                        }


                    }
                    PipelineInflightStatus::AckSource => {



                    }
                    PipelineInflightStatus::PendingEdge => {
                        // pending edge means we're not done receiving msgs yet...

                        // we need to store these msgs on the
                        // decendant tasks so we can move them
                        // them to the queues when all the msgs arrive
                        if let Some(msgs) = task_result {
                            // TODO: self.aggregate.hold_messages
                            for name in decendants.iter() {
                                self.aggregate.hold(&name.to_string(), msg_id.clone(), msgs.clone());
                            }
                        } else {
                            // nothing to do but wait
                        };
                    }
                    PipelineInflightStatus::Removed => {}
                    PipelineInflightStatus::Timeout => {}
                }
            }
            TaskResponse::Error(msg_id, edge, index) => {}
        }
    }

    fn clean(&mut self) {

        // Interval fn for cleaning out "stale" msg
        // stale msgs is something that maybe errored out
        // and we need to clean out the msgs from inflight, available,
        // etc.

    }
}

/// Handle PipelineMsg for PipelineActor

impl Handler<PipelineMsg> for PipelineActor {
    type Result = ();

    fn handle(&mut self, msg: PipelineMsg, ctx: &mut Context<Self>) {
        match msg {
            PipelineMsg::TaskRoot(src_msg) => {
                self.task_root(src_msg);
            }
            PipelineMsg::TaskAck(task_resp) => {
                // this should return a list of PipelineMsg's
                // for sending back up through TopologyActor
                // if we're done processing the original message

                // An Option(None) means we're still not ready
                // for Source Ack yet...
                self.task_ack(task_resp);
            }
            PipelineMsg::TaskError(task_resp) => {
                // update available & aggregate (if necessary)
                // and send PipelineMsg::Ack to topology_addr
            }
            _ => {
                // not implemented here
            }
        }
    }
}

impl Handler<TaskRequest> for PipelineActor {
    type Result = ();

    fn handle(&mut self, msg: TaskRequest, ctx: &mut Context<Self>) {
        // println!("PipelineActor Handler<TaskRequest> {:?}", &msg);
        match msg {
            TaskRequest::GetAvailable(session_id, name, count) => {
                let topology = TopologyActor::from_registry();
                if !topology.connected() {
                    return;
                }
                let tasks = self.available.pop(&name, count);
                topology.do_send(TaskRequest::GetAvailableResponse(session_id, name, tasks));
            }
            _ => {
                println!("Handler<TaskRequest> for PipelineActor only implements TaskRequest::GetAvailable");
            }
        }
    }
}

#[cfg(test)]
mod tests {

    // use actix::prelude::*;

    // use super::task;

    // use super::{
    //     Msg, Pipeline, SourceBuilder, Task, TaskIngress, TaskOptions, Topology, TopologyBuilder,
    //     TopologyOptions,
    // };

    // use super::source::{RedisStreamSource, RedisStreamSourceBuilder};

    // use crate::service::task::TaskService;
    // use crate::service::topology::TopologyService;

    // struct MyTopology {}

    // impl<'a> Topology<RedisStreamSourceBuilder<'a>> for MyTopology {
    //     fn builder() -> TopologyBuilder<RedisStreamSourceBuilder<'a>> {
    //         TopologyBuilder::default()
    //             .options(TopologyOptions::default().name("MyTopology"))
    //             .pipeline(
    //                 Pipeline::default()
    //                     .add(Task::new("t1", "topology::tests::T1").workers(2))
    //                     .add(
    //                         Task::default()
    //                             .name("t2")
    //                             .path("topology::tests::T2")
    //                             .options(TaskOptions::default().workers(3)),
    //                     )
    //                     .add(Task::new("t3", "topology::tests::T3"))
    //                     .add(
    //                         Task::new("t4", "topology::tests::T4")
    //                             .workers(20)
    //                     )
    //                     .edge("t1", "t2")
    //                     .edge("t1", "t3")
    //                     .edge("t2", "t4")
    //                     .edge("t3", "t4"),
    //             )
    //             .source(
    //                 RedisStreamSourceBuilder::default()
    //                     .uri("redis://127.0.0.1/0")
    //                     .key("mystream")
    //                     .group("super"),
    //             )
    //     }
    // }

    // pub struct MyMsg {}

    // #[derive(Default, Debug)]
    // pub struct T1 {}
    // impl task::Task for T1 {
    //     type Msg = MyMsg;

    //     // Convert Vec<u8> to MyMsg
    //     fn deser(&mut self, msg: Msg) -> Self::Msg {
    //         // convert Msg to Self::Msg
    //         MyMsg {}
    //     }

    //     fn handle(&mut self, msg: Self::Msg) -> task::TaskResult<Option<Vec<Msg>>> {
    //         Ok(None)
    //     }
    // }

    // #[derive(Default, Debug)]
    // pub struct T2 {}
    // impl task::Task for T2 {
    //     type Msg = Msg;

    //     fn deser(&mut self, msg: Msg) -> Self::Msg {
    //         msg
    //     }

    //     fn handle(&mut self, msg: Self::Msg) -> task::TaskResult<Option<Vec<Msg>>> {
    //         Ok(None)
    //     }
    // }

    // #[derive(Default, Debug)]
    // pub struct T3 {}
    // impl task::Task for T3 {
    //     type Msg = Msg;

    //     fn deser(&mut self, msg: Msg) -> Self::Msg {
    //         msg
    //     }

    //     fn handle(&mut self, msg: Self::Msg) -> task::TaskResult<Option<Vec<Msg>>> {
    //         Ok(None)
    //     }
    // }

    // #[derive(Default, Debug)]
    // pub struct T4 {}
    // impl task::Task for T4 {
    //     type Msg = Msg;

    //     fn deser(&mut self, msg: Msg) -> Self::Msg {
    //         msg
    //     }

    //     fn handle(&mut self, msg: Self::Msg) -> task::TaskResult<Option<Vec<Msg>>> {
    //         Ok(None)
    //     }
    // }

    // #[test]
    // fn test_topology_builder() {
    //     let builder = MyTopology::builder();
    //     // println!("{:?}", &builder);

    //     let topology = &builder.build_topology_actor();
    //     // println!("{:?}", &topology);

    //     // let source= &builder.build_source_actor();
    //     let pipeline = &builder.build_pipeline_actor();
    //     println!("{:#?}", &pipeline.pipeline);
    // }

    // #[test]
    // fn test_my_topology<'a>() {
    //     TopologyService::run(MyTopology::builder);
    // }

    // #[test]
    // fn test_my_task<'a>() {
    //     TaskService::run(T1::default);
    // }

}
