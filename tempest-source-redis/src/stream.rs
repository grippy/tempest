use serde_derive::Deserialize;
use std::borrow::Cow;
use std::cmp;
use std::collections::{HashMap, VecDeque};
use std::str::from_utf8;

use crate::error::RedisErrorToSourceError;

use tempest::metric::Metrics;
use tempest_source::prelude::*;

use redis_streams::{
    client_open, Client, Commands, Connection, RedisResult, StreamCommands, StreamId,
    StreamInfoGroup, StreamInfoGroupsReply, StreamReadOptions, StreamReadReply, Value,
};

use serde_json;

use uuid::Uuid;

static TARGET_SOURCE: &'static str = "source::RedisStreamSource";
static TARGET_SOURCE_BUILDER: &'static str = "source::RedisStreamSourceBuilder";

/// Enum for holding messages for RedisStreamSource testing
/// See `RedisStreamSourceBuilder#prime` for more details.
#[derive(Clone)]
pub enum RedisStreamPrime {
    Msg(String, Vec<(String, String)>),
}

/// A RedisKey is used for moving stream messages to
/// as part of a pending action.
/// ** This is currently not implemented 100% **
#[derive(PartialEq, Eq, Hash, Clone, Debug, Deserialize)]
#[serde(tag = "type", content = "key")]
pub enum RedisKey {
    List(String),
    Stream(String),
    SortedSet(String),
}

/// This enum provides an action for how to deal with pending messages.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum RedisStreamPendingAction {
    Claim,
    Delete,
    Ack,
    Move(RedisKey),
}

/// Redis streams require additional work
/// for cleaning up pending (unacknowledged) messages.
/// `RedisStreamPendingHandler` configures actions
/// for handling pending messages after some length of time or
/// number of times a message was previously delivered.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "action")]
pub struct RedisStreamPendingHandler {
    /// Pending message min idle time before triggering action
    min_idle_time: usize,
    /// Pending message number of times delivered before triggering action
    times_delivered: usize,
    /// The action for handling a pending message
    action: RedisStreamPendingAction,
}

impl RedisStreamPendingHandler {
    pub fn new(
        min_idle_time: usize,
        times_delivered: usize,
        action: RedisStreamPendingAction,
    ) -> Self {
        Self {
            min_idle_time: min_idle_time,
            times_delivered: times_delivered,
            action: action,
        }
    }
}

/// Source builder for constructing `RedisStreamSource` instances
///
#[derive(Default)]
pub struct RedisStreamSourceBuilder<'a> {
    options: RedisStreamSourceOptions<'a>,
}

impl<'a> RedisStreamSourceBuilder<'a> {
    /// The connection string to configure the redis client with
    /// in the form of `redis://host:port/db`
    ///
    pub fn uri(mut self, uri: &'a str) -> Self {
        self.options.uri = Some(uri.into());
        self
    }

    /// Redis stream key to read from. Will configure key as a stream if it doesn't already exist.
    pub fn key(mut self, key: &'a str) -> Self {
        self.options.key = Some(key.into());
        self
    }

    /// Group name for reading messages from a key
    pub fn group(mut self, name: &'a str) -> Self {
        self.options.group = Some(name.into());
        self
    }

    /// Enable block read option
    pub fn block_read(mut self, ms: usize) -> Self {
        self.options.block_read = Some(ms);
        self
    }

    /// Group starting message id a stream should begin reading from
    pub fn group_starting_id(mut self, id: &'a str) -> Self {
        self.options.group_starting_id = Some(RedisStreamGroupStartingId::from(id));
        self
    }

    /// Configure a pending handler and action
    pub fn pending_handler(mut self, handler: RedisStreamPendingHandler) -> Self {
        self.options.pending_handlers.push(handler);
        self
    }

    /// The total number messages to read each time the source is polled
    pub fn read_msg_count(mut self, count: usize) -> Self {
        self.options.read_msg_count = Some(count);
        self
    }

    /// How often should the Topology poll this source?
    pub fn poll_interval(mut self, ms: u64) -> Self {
        self.options.poll_interval = Some(SourceInterval::Millisecond(ms));
        self
    }

    /// The monitor interval in milliseconds.
    /// RedisStreamSource uses the monitor check
    /// for handling pending redis stream messages.
    pub fn monitor_interval(mut self, ms: u64) -> Self {
        self.options.monitor_interval = Some(SourceInterval::Millisecond(ms));
        self
    }

    /// The ack policy the topology should use for this source
    pub fn ack_policy(mut self, policy: SourceAckPolicy) -> Self {
        self.options.ack_policy = Some(policy);
        self
    }

    /// How often should the topology ack messages with this source?
    pub fn ack_interval(mut self, ms: u64) -> Self {
        self.options.ack_interval = Some(SourceInterval::Millisecond(ms));
        self
    }

    /// Mac backoff value between polling calls.
    pub fn max_backoff(mut self, ms: u64) -> Self {
        self.options.max_backoff = Some(ms);
        self
    }

    /// This is used for messages on a stream for testing purposes.
    /// `prime` takes a closure which returns a vector of tuples.
    // Return type: -> Vec<(timestamp (use "*" for current time), &[(key, value)])>
    pub fn prime(mut self, f: fn() -> Vec<RedisStreamPrime>) -> Self {
        self.options.prime = Some(f);
        self
    }
}

impl<'a> SourceBuilder for RedisStreamSourceBuilder<'a> {
    type Source = RedisStreamSource<'a>;

    /// Override the options from Topology.toml [source.config] value
    /// and use them to configure source options.
    fn parse_config_value(&mut self, cfg: config::Value) {
        // println!("parse_config_value {:?}", &cfg);
        match cfg.into_table() {
            Ok(map) => {
                if let Some(v) = map.get("uri") {
                    let result = v.clone().try_into::<String>();
                    if let Ok(s) = result {
                        self.options.uri = Some(s.into());
                    } else {
                        warn!(
                            target: TARGET_SOURCE_BUILDER,
                            "failed to parse cfg source.uri {:?}", &result
                        );
                    }
                }
                if let Some(v) = map.get("key") {
                    let result = v.clone().try_into::<String>();
                    if let Ok(s) = result {
                        self.options.key = Some(s.into());
                    } else {
                        warn!(
                            target: TARGET_SOURCE_BUILDER,
                            "failed to parse cfg source.key {:?}", &result
                        );
                    }
                }
                if let Some(v) = map.get("group") {
                    let result = v.clone().try_into::<String>();
                    if let Ok(s) = result {
                        self.options.group = Some(s.into());
                    } else {
                        warn!(
                            target: TARGET_SOURCE_BUILDER,
                            "failed to parse cfg source.group {:?}", &result
                        );
                    }
                }
                if let Some(v) = map.get("block_read") {
                    let result = v.clone().try_into::<usize>();
                    if let Ok(b) = result {
                        self.options.block_read = Some(b);
                    } else {
                        warn!(
                            target: TARGET_SOURCE_BUILDER,
                            "failed to parse cfg source.blocking_read {:?}", &result
                        );
                    }
                }
                if let Some(v) = map.get("group_starting_id") {
                    let result = v.clone().try_into::<String>();
                    if let Ok(s) = result {
                        self.options.group_starting_id = Some(RedisStreamGroupStartingId::from(s));
                    } else {
                        warn!(
                            target: TARGET_SOURCE_BUILDER,
                            "failed to parse cfg source.group_starting_id {:?}", &result
                        );
                    }
                }
                if let Some(v) = map.get("ack_policy") {
                    let result = v.clone().try_into::<SourceAckPolicy>();
                    if let Ok(policy) = result {
                        self.options.ack_policy = Some(policy);
                    } else {
                        warn!(
                            target: TARGET_SOURCE_BUILDER,
                            "failed to parse cfg source.ack_policy {:?}", &result
                        );
                    }
                }
                if let Some(v) = map.get("max_backoff") {
                    let result = v.clone().try_into::<u64>();
                    if let Ok(ms) = result {
                        self.options.max_backoff = Some(ms);
                    } else {
                        warn!(
                            target: TARGET_SOURCE_BUILDER,
                            "failed to parse cfg source.max_backoff {:?}", &result
                        );
                    }
                }
                if let Some(v) = map.get("poll_interval") {
                    let result = v.clone().try_into::<u64>();
                    if let Ok(ms) = result {
                        self.options.poll_interval = Some(SourceInterval::Millisecond(ms));
                    } else {
                        warn!(
                            target: TARGET_SOURCE_BUILDER,
                            "failed to parse cfg source.poll_interval {:?}", &result
                        );
                    }
                }
                if let Some(v) = map.get("pending_handlers") {
                    let result = v.clone().try_into::<Vec<RedisStreamPendingHandler>>();
                    if let Ok(handlers) = result {
                        self.options.pending_handlers = handlers;
                    } else {
                        warn!(
                            target: TARGET_SOURCE_BUILDER,
                            "failed to parse cfg source.pending_handlders {:?}", &result
                        );
                    }
                }
                if let Some(v) = map.get("monitor_interval") {
                    let result = v.clone().try_into::<u64>();
                    if let Ok(ms) = result {
                        self.options.monitor_interval = Some(SourceInterval::Millisecond(ms));
                    } else {
                        warn!(
                            target: TARGET_SOURCE_BUILDER,
                            "failed to parse cfg source.monitor_interval {:?}", &result
                        );
                    }
                }
                if let Some(v) = map.get("read_msg_count") {
                    let result = v.clone().try_into::<usize>();
                    if let Ok(count) = result {
                        self.options.read_msg_count = Some(count);
                    } else {
                        warn!(
                            target: TARGET_SOURCE_BUILDER,
                            "failed to parse cfg source.read_msg_count {:?}", &result
                        );
                    }
                }
            }
            Err(err) => error!(target: TARGET_SOURCE_BUILDER, "Error {:?}", err),
        }
    }

    /// `build` returns a `RedisStreamSource` instance
    ///
    fn build(&self) -> Self::Source {
        debug!(
            target: TARGET_SOURCE_BUILDER,
            "RedisStreamSource build w/ opts: {:?}", &self.options
        );
        let mut source = Self::Source::default();
        source.options = self.options.clone();
        source
    }
}

/// Enum for defining where group consumers should start
/// reading messages from.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum RedisStreamGroupStartingId<'a> {
    /// Start reading from the first msg id
    Zero,
    /// Start reading from the last msg id
    Dollar,
    /// Start reading from some other point.
    /// Default redis stream msg id is a {timestamp}-{counter}.
    Other(Cow<'a, str>),
}

impl<'a> RedisStreamGroupStartingId<'a> {
    fn from<S>(id: S) -> Self
    where
        S: Into<Cow<'a, str>>,
    {
        let v = id.into();
        match v {
            Cow::Borrowed("0") => RedisStreamGroupStartingId::Zero,
            Cow::Borrowed("$") => RedisStreamGroupStartingId::Dollar,
            _ => RedisStreamGroupStartingId::Other(v),
        }
    }

    fn as_str(&self) -> &str {
        return match &self {
            RedisStreamGroupStartingId::Zero => "0",
            RedisStreamGroupStartingId::Dollar => "$",
            RedisStreamGroupStartingId::Other(val) => val,
        };
    }
}

impl<'a> Default for RedisStreamGroupStartingId<'a> {
    fn default() -> Self {
        RedisStreamGroupStartingId::Dollar
    }
}

/// Builder options for configuring `RedisStreamSource`
/// instances.
#[derive(Clone, Debug)]
pub struct RedisStreamSourceOptions<'a> {
    /// The redis connection string as a uri: `redis://host:port/db`.
    uri: Option<Cow<'a, str>>,
    /// The redis stream key (will create if it doesn't exist).
    key: Option<Cow<'a, str>>,
    /// The redis group name to use for consumer reads.
    group: Option<Cow<'a, str>>,
    /// The redis group consumer name to use for reading messages.
    /// This is an auto-generated uuid.
    consumer: Option<String>,
    /// The stream starting message id to begin reading from
    group_starting_id: Option<RedisStreamGroupStartingId<'a>>,
    /// Defines how we should handle acknowledging stream messages (individual or batch?)
    ack_policy: Option<SourceAckPolicy>,
    /// How often we should acknowledge accumulated messages.
    ack_interval: Option<SourceInterval>,
    /// A vector of actions for dealing with pending redis stream messages
    pending_handlers: Vec<RedisStreamPendingHandler>,
    /// Enable block stream reads
    block_read: Option<usize>,
    /// The total number of messages to read each time the source is polled
    read_msg_count: Option<usize>,
    /// Time in milliseconds to wait before polling for new source messages
    poll_interval: Option<SourceInterval>,
    /// Time in milliseconds to wait before running the monitor
    monitor_interval: Option<SourceInterval>,
    /// The maximum value we should backoff before polling for new source messages.
    max_backoff: Option<u64>,
    /// A closure for generating test messages to prime the stream with.
    /// Used for testing purposes.
    prime: Option<fn() -> Vec<RedisStreamPrime>>,
}

impl<'a> Default for RedisStreamSourceOptions<'a> {
    fn default() -> Self {
        RedisStreamSourceOptions {
            /// The redis uri where the stream lives
            uri: None,

            /// The redis key for the stream
            key: None,

            /// The group name we should assign to this consumer
            group: None,

            /// This is the auto-generated hash we use as the group consumer name
            consumer: Some(format!("{}", Uuid::new_v4().to_simple())),

            /// Defines the message id we should start reading
            /// the stream from.
            group_starting_id: Some(RedisStreamGroupStartingId::default()),

            /// Vector of handlers for processing pending messages
            pending_handlers: Vec::new(),

            /// Configure if we should read consumer group streams in blocking mode.
            /// Value should be Some(milliseconds)
            block_read: None,

            /// Configure the number of messages we should read per xread redis command
            read_msg_count: Some(10usize),

            /// Configure the poll interval (default is 1 ms)
            poll_interval: Some(SourceInterval::default()),

            // Configure the ack policy
            ack_policy: Some(SourceAckPolicy::Batch(10)),

            /// Configure the ack interval (default is 1000 ms)
            ack_interval: Some(SourceInterval::Millisecond(1000)),

            /// Configure how often the monitor fn should run
            /// Default is zero (which means never)
            monitor_interval: Some(SourceInterval::Millisecond(0)),

            /// Configure the max backoff milliseconds
            max_backoff: Some(1000u64),

            /// Closure for generating test messages to add to stream
            prime: None,
        }
    }
}

/// The main data structure for interacting with Redis Streams.
/// This controls reading and acking messages, actions for dealing
/// with pending messages, and group creation.
///
/// Learn more about [Redis Streams](https://redis.io/topics/streams-intro).
///
/// Please note: Redis Streams doesn't provide the most efficient interface
/// for interacting with pending messages and taking actions on them.
///
/// Generating a list of pending messages could be costly.
/// The cost consideration ultimately depends on topology throughput and configuration.
/// You may consider running the monitor every minute (as opposed to second or sub-second).
pub struct RedisStreamSource<'a> {
    /// Redis stream options
    options: RedisStreamSourceOptions<'a>,
    /// Redis connection
    conn: Option<Connection>,
    /// Redis client
    client: Option<Client>,
    /// A queue of reclaimed messages that have been delivered at least once
    /// for this client. This is used to store reclaimed messages for this consumer
    /// if pending action handlers are enabled.
    reclaimed: VecDeque<SourceMsg>,
    /// Metrics
    metrics: Metrics,
}

impl<'a> Default for RedisStreamSource<'a> {
    fn default() -> Self {
        RedisStreamSource {
            options: RedisStreamSourceOptions::default(),
            conn: None,
            client: None,
            reclaimed: VecDeque::new(),
            metrics: Metrics::default().named(vec!["source"]),
        }
    }
}

impl<'a> RedisStreamSource<'a> {
    /// Returns a redis connection
    fn connection(&mut self) -> SourceResult<&mut Connection> {
        match &mut self.conn {
            Some(conn) => Ok(conn),
            None => {
                return Err(SourceError::new(SourceErrorKind::Other(
                    "Source connection is None".to_string(),
                )))
            }
        }
    }

    /// Special method for priming a redis stream with test messages.
    fn prime_stream(&mut self) -> SourceResult<()> {
        if let None = self.options.prime {
            return Ok(());
        }
        debug!(target: TARGET_SOURCE, "Prime test messages");
        let msgs = &self.options.prime.unwrap()();
        let key = self.options.key.as_ref().unwrap().to_string();
        let group = self.options.group.as_ref().unwrap().as_ref().to_owned();
        let conn = &mut self.connection()?;
        // remove the existing data (if any exists)
        let _: RedisResult<bool> = conn.del(&key);
        let _: RedisResult<String> = conn.xgroup_create_mkstream(&key, group, "0");
        for msg in msgs {
            match msg {
                RedisStreamPrime::Msg(ts, values) => {
                    let _: RedisResult<String> = conn.xadd(&key, ts, values);
                }
            }
        }
        Ok(())
    }

    /// Report the length of reclaimed messages
    pub fn reclaimed_size(&mut self) -> usize {
        self.reclaimed.len()
    }

    /// Parse a vec of StreamId to SourceMsg
    fn parse_stream_ids(stream_ids: &Vec<StreamId>, msgs: &mut Vec<SourceMsg>) {
        for msg in stream_ids {
            // convert the msg.map => json as byte vec
            let mut json_map = serde_json::map::Map::default();
            for (key, val) in &msg.map {
                match *val {
                    Value::Data(ref b) => match from_utf8(b) {
                        Ok(s) => {
                            json_map
                                .insert(key.to_string(), serde_json::Value::String(s.to_string()));
                        }
                        Err(_err) => {}
                    },
                    _ => {}
                };
            }

            let mut source_msg = SourceMsg::default();
            source_msg.id = msg.id.as_bytes().to_vec();
            source_msg.ts = now_millis();

            // now, convert to byte vec
            match serde_json::to_vec(&serde_json::Value::Object(json_map)) {
                Ok(vec) => source_msg.msg = vec,
                Err(err) => {
                    error!("error converting object to byte vec {:?}", &err);
                }
            }

            msgs.push(source_msg);
        }
    }

    /// Force ack a list of pending msg ids picked up by read_pending
    fn ack_pending(
        &mut self,
        key: &String,
        group: &String,
        pending_ids: Vec<&String>,
    ) -> SourceResult<()> {
        let conn = &mut self.connection()?;
        let result: RedisResult<i32> = conn.xack(key, group, &pending_ids);
        match result {
            Ok(count) => {
                trace!(target: TARGET_SOURCE, "read_pending acked {} msgs", count);
                self.metrics
                    .counter(vec!["read_pending", "force_ack", "success"], count as isize);
            }
            Err(err) => {
                error!(target: TARGET_SOURCE, "read_pending ack error {:?}", err);
                self.metrics.counter(
                    vec!["read_pending", "force_ack", "error"],
                    pending_ids.len() as isize,
                );
            }
        }
        Ok(())
    }

    /// Force delete a list of pending msg ids picked up by read_pending
    fn delete_pending(&mut self, key: &String, pending_ids: Vec<&String>) -> SourceResult<()> {
        let conn = &mut self.connection()?;
        let result: RedisResult<i32> = conn.xdel(key, &pending_ids);
        match result {
            Ok(count) => {
                trace!(target: TARGET_SOURCE, "read_pending deleted {} msgs", count);
                self.metrics
                    .counter(vec!["pending", "force_delete", "success"], count as isize);
            }
            Err(err) => {
                error!(target: TARGET_SOURCE, "read_pending delete error {:?}", err);
                self.metrics.counter(
                    vec!["pending", "force_delete", "error"],
                    pending_ids.len() as isize,
                );
            }
        }

        Ok(())
    }

    /// Reclaim msg ids picked up by read_pending and move them
    /// to the internal reclaimed queue
    fn claim_pending(
        &mut self,
        key: &String,
        group: &String,
        min_idle_time: usize,
        pending_ids: Vec<&String>,
    ) -> SourceResult<()> {
        let consumer = self.options.consumer.as_ref().unwrap().to_string();
        let conn = &mut self.connection()?;
        let result = conn.xclaim(key, group, consumer, min_idle_time, &pending_ids);
        match result {
            Ok(reply) => {
                trace!(target: TARGET_SOURCE, "stream claim reply: {:?}", &reply);
                let mut msgs = vec![];
                Self::parse_stream_ids(&reply.ids, &mut msgs);
                let count = &msgs.len();
                for msg in msgs {
                    self.reclaimed.push_back(msg);
                }
                trace!(
                    target: TARGET_SOURCE,
                    "claim_pending reclaimed {} msgs",
                    &count
                );
                self.metrics
                    .counter(vec!["pending", "claim_pending", "success"], *count as isize);
            }
            Err(err) => {
                error!(target: TARGET_SOURCE, "read_pending claim error {:?}", err);
                self.metrics.counter(
                    vec!["pending", "claim", "error"],
                    pending_ids.len() as isize,
                );
            }
        }

        Ok(())
    }

    /// Move a list of pending_ids to another redis key (list or stream)
    /// **Currently unimplemented**
    fn move_pending(
        &mut self,
        _key: &String,
        _group: &String,
        _min_idle_time: usize,
        _move_to: HashMap<&RedisKey, Vec<&String>>,
    ) -> SourceResult<()> {
        // let consumer = self.options.consumer.as_ref().unwrap().to_string();
        // let conn = &mut self.connection()?;
        // for (redis_key, pending_ids) in move_to.iter() {
        //     let result = conn.xclaim(key, group, &consumer, min_idle_time, pending_ids);
        //     match result {
        //         Ok(reply) => {
        //             trace!(
        //                 target: TARGET_SOURCE,
        //                 "move to stream claim reply: {:?}",
        //                 &reply
        //             );
        //             match redis_key {
        //                 RedisKey::List(rkey) => {
        //                     // how should we store this message?
        //                     // byte vec, string, or json?
        //                     let mut msgs = vec![];
        //                     Self::parse_stream_ids(&reply.ids, &mut msgs);
        //                     let count = &msgs.len();
        //                     for msg in msgs {}
        //                 }
        //                 RedisKey::SortedSet(rkey) => {}
        //                 RedisKey::Stream(rkey) => {}
        //             }
        //         }
        //         Err(err) => {}
        //     }
        // }
        warn!("RedisStreamPendingAction::Move not implemented yet! Sorry :(");
        Ok(())
    }

    /// Read pending message and group them by handler type so we can take
    /// action on them.
    fn read_pending(&mut self) -> SourceResult<()> {
        if self.options.pending_handlers.len() == 0 {
            return Ok(());
        }

        let key = self.options.key.as_ref().unwrap().to_string();
        let group = self.options.group.as_ref().unwrap().as_ref().to_owned();
        let pending_handlers = self.options.pending_handlers.clone();

        // This is going to iterate over all pending messages
        // for all clients... we need to find the messages that match
        // our handlers... unfortunately no efficient way to do this

        let conn = &mut self.connection()?;
        let reply = match conn.xpending_count(&key, &group, "-", "+", 100) {
            Ok(reply) => reply,
            Err(err) => {
                error!("Error calling xpending_count {:?}", err);
                return Err(SourceError::new(SourceErrorKind::Other(err.to_string())));
            }
        };

        let mut ack_ids = vec![];
        let mut delete_ids = vec![];

        let mut move_to: HashMap<&RedisKey, Vec<&String>> = HashMap::new();
        let mut move_to_min_idle_time = 0;
        let mut claim_ids = vec![];
        let mut claim_min_idle_time = 0;

        // reply is a StreamPendingCountReply
        for pending in &reply.ids {
            for handler in &pending_handlers {
                let mut action = false;
                if pending.last_delivered_ms > handler.min_idle_time
                    && pending.times_delivered >= handler.times_delivered
                {
                    action = true
                }
                if !action {
                    continue;
                }

                // Now process this message id
                match &handler.action {
                    RedisStreamPendingAction::Ack => {
                        ack_ids.push(&pending.id);
                        trace!(
                            target: TARGET_SOURCE,
                            "Force ack id {} idle: {}, delivered: {}",
                            &pending.id,
                            &pending.last_delivered_ms,
                            &pending.times_delivered
                        );
                        break;
                    }
                    RedisStreamPendingAction::Delete => {
                        delete_ids.push(&pending.id);
                        trace!(
                            target: TARGET_SOURCE,
                            "Delete id {} idle: {}, delivered: {}",
                            &pending.id,
                            &pending.last_delivered_ms,
                            &pending.times_delivered
                        );

                        break;
                    }
                    RedisStreamPendingAction::Move(redis_key) => {
                        // move should claim the entire message
                        // and move it to this key
                        if move_to_min_idle_time == 0 {
                            move_to_min_idle_time = handler.min_idle_time;
                        }
                        move_to_min_idle_time =
                            cmp::min(move_to_min_idle_time, handler.min_idle_time);

                        if !move_to.contains_key(&redis_key) {
                            move_to.insert(redis_key, vec![]);
                        }

                        match move_to.get_mut(&redis_key) {
                            Some(pending_ids) => pending_ids.push(&pending.id),
                            None => {}
                        }

                        trace!(
                            target: TARGET_SOURCE,
                            "Move id {} idle: {}, delivered: {}",
                            &pending.id,
                            &pending.last_delivered_ms,
                            &pending.times_delivered
                        );

                        break;
                    }
                    // this should come last...
                    RedisStreamPendingAction::Claim => {
                        // save the min_idle_time
                        if claim_min_idle_time == 0 {
                            claim_min_idle_time = handler.min_idle_time;
                        }
                        claim_min_idle_time = cmp::min(claim_min_idle_time, handler.min_idle_time);
                        claim_ids.push(&pending.id);
                        trace!(
                            target: TARGET_SOURCE,
                            "Claim id {} idle: {}, delivered: {}",
                            &pending.id,
                            &pending.last_delivered_ms,
                            &pending.times_delivered
                        );
                        break;
                    }
                }
            }
        }

        if ack_ids.len() > 0 {
            let _ = self.ack_pending(&key, &group, ack_ids);
        }

        if delete_ids.len() > 0 {
            let _ = self.delete_pending(&key, delete_ids);
        }

        if claim_ids.len() > 0 {
            let _ = self.claim_pending(&key, &group, claim_min_idle_time, claim_ids);
        }

        if move_to.len() > 0 {
            let _ = self.move_pending(&key, &group, move_to_min_idle_time, move_to);
        }

        Ok(())
    }

    /// Read messages that have never been claimed before.
    fn read_unclaimed(&mut self) -> SourcePollResult {
        // TOOD: read_opts should be cached
        // always read unclaimed messages
        let count = self.options.read_msg_count.as_ref().unwrap();
        let key = self.options.key.as_ref().unwrap().to_string();
        let group = self.options.group.as_ref().unwrap().as_ref().to_owned();
        let consumer = self.options.consumer.as_ref().unwrap().to_string();

        // Configure read options
        let mut read_opts = StreamReadOptions::default()
            .group(group, consumer)
            .count(*count);

        // configure [BLOCK ms]?
        if let Some(ms) = self.options.block_read {
            if ms > 0 {
                read_opts = read_opts.block(ms);
            }
        }

        // configure [NOACK]?
        let mut ack = true;
        if let Some(ack_policy) = &self.options.ack_policy {
            match ack_policy {
                SourceAckPolicy::None => ack = false,
                _ => {}
            }
        } else {
            // no reachable with the current default
            ack = false;
        }
        if !ack {
            read_opts = read_opts.noack();
        }

        // The special char '>' ID  means that the
        // consumer wants to receive only messages
        // that were never delivered to a consumer or reclaimed.
        let conn = &mut self.connection()?;
        let result: RedisResult<StreamReadReply> = conn.xread_options(&[key], &[">"], read_opts);

        match result {
            Ok(reply) => {
                if reply.keys.len() == 0 {
                    return Ok(None);
                }

                // convert StreamId to SourceMsg
                let mut msgs = vec![];
                Self::parse_stream_ids(&reply.keys[0].ids, &mut msgs);
                Ok(Some(msgs))
            }
            Err(e) => {
                // TODO: handle this error so caller can
                // figure out what to do next
                error!(target: TARGET_SOURCE, "Error reading stream {:?}", e);
                Ok(None)
            }
        }
    }

    /// Create a group if it hasn't already been created for the stream key
    fn group_create(&mut self) -> SourceResult<()> {
        // we need to check or the client ends up with a broken pipe
        // technically, if we're calling group_create here
        // then we should have already created the connection
        // we just need to unwrap a few things...
        let key = self.options.key.as_ref().unwrap().as_ref().to_owned();
        let group = self.options.group.as_ref().unwrap().as_ref().to_owned();
        let starting_id = self.options.group_starting_id.clone().unwrap();
        let conn = self.connection()?;

        let result: RedisResult<bool> = conn.exists(&key);
        match result {
            Ok(true) => {
                // do we already have a group for this stream?
                let info: StreamInfoGroupsReply = conn.xinfo_groups(&key).unwrap();
                let group_exists = &info
                    .groups
                    .into_iter()
                    .filter(|g| g.name == group.to_string())
                    .collect::<Vec<StreamInfoGroup>>()
                    .len()
                    > &0;
                if !group_exists {
                    let _: RedisResult<String> =
                        conn.xgroup_create(key, group, starting_id.as_str());
                }
            }
            Ok(false) => {
                let _: RedisResult<String> =
                    conn.xgroup_create_mkstream(key, group, starting_id.as_str());
            }
            Err(e) => {
                // TODO: this should raise the error
                error!(target: TARGET_SOURCE, "Error group_create: {:?}", e);
            }
        }

        Ok(())
    }

    /// Take a Vec<MsgId> as input and ack them.
    /// This method returns SourceResult<(msgs count, acked count)>
    fn stream_ack(&mut self, msgs: Vec<MsgId>) -> SourceResult<(i32, i32)> {
        let mut ack_ids = vec![];
        let input_msgs = msgs.len() as i32;
        for msg_id in msgs {
            match from_utf8(&msg_id) {
                Ok(id) => ack_ids.push(id.to_owned()),
                Err(err) => {
                    // log this
                    error!("Failed to convert msg id to str before ack: {:?}", err);
                }
            }
        }
        let key = self.options.key.as_ref().unwrap().as_ref().to_owned();
        let group = self.options.group.as_ref().unwrap().as_ref().to_owned();
        let conn = self.connection()?;
        // TODO: chunk the ack_ids into `Batch(size)`?
        let result: RedisResult<i32> = conn.xack(key, group, &ack_ids);
        trace!(target: TARGET_SOURCE, "stream ack: {:?}", &result);

        match result {
            Ok(acked) => Ok((input_msgs, acked)),
            Err(err) => {
                error!("Failed to ack source: {:?}", err);
                Err(SourceError::new(SourceErrorKind::Client(
                    "Ack error".to_string(),
                )))
            }
        }
    }
}

impl<'a> Source for RedisStreamSource<'a> {
    fn name(&self) -> &'static str {
        "RedisStreamSource"
    }

    /// Validation method that's called at the start of setup.
    /// Any field that's required by this source is checked here.
    /// Required fields include: `uri`, `key`, and `group`.
    fn validate(&mut self) -> SourceResult<()> {
        return if self.options.uri.is_none() {
            Err(SourceError::new(SourceErrorKind::ValidateError(
                "Missing redis uri for source.".to_string(),
            )))
        } else if self.options.key.is_none() {
            Err(SourceError::new(SourceErrorKind::ValidateError(
                "Missing redis key for source.".to_string(),
            )))
        } else if self.options.group.is_none() {
            Err(SourceError::new(SourceErrorKind::ValidateError(
                "Missing redis group for source.".to_string(),
            )))
        } else {
            Ok(())
        };
    }

    /// Setup this as a source. This is where we validate options,
    /// configure the connection & client, prime stream messages,
    /// and create the stream and group (if needed).
    fn setup(&mut self) -> SourceResult<()> {
        // call validate here
        self.validate()?;

        let uri = self.options.uri.as_ref().unwrap().as_ref().to_owned();
        debug!(target: TARGET_SOURCE, "Create redis client: {:?}", &uri);
        match client_open(&uri[..]) {
            Ok(client) => {
                match client.get_connection() {
                    Ok(conn) => {
                        self.conn = Some(conn);
                    }
                    Err(err) => {
                        error!(
                            target: TARGET_SOURCE,
                            "redis client.get_connection error: {:?}", err
                        );
                        return Err(RedisErrorToSourceError::convert(err));
                    }
                }
                // now store the client
                self.client = Some(client);
            }
            Err(err) => {
                error!(target: TARGET_SOURCE, "redis client_open error: {:?}", err);
                return Err(RedisErrorToSourceError::convert(err));
            }
        }

        // prime the stream w/ messages?
        self.prime_stream()?;

        // we need to create the stream here
        self.group_create()?;

        Ok(())
    }

    /// Ack a single stream message
    fn ack(&mut self, msg_id: MsgId) -> SourceResult<(i32, i32)> {
        self.stream_ack(vec![msg_id])
    }
    /// Batch ack stream messages
    fn batch_ack(&mut self, msgs: Vec<MsgId>) -> SourceResult<(i32, i32)> {
        self.stream_ack(msgs)
    }

    /// Return the max backoff time
    fn max_backoff(&self) -> SourceResult<&u64> {
        match &self.options.max_backoff {
            Some(v) => Ok(v),
            None => Source::max_backoff(self),
        }
    }

    /// Return the currently configured `SourceAckPolicy`
    /// or default it `SourceAckPolicy::None`
    fn ack_policy(&self) -> SourceResult<&SourceAckPolicy> {
        match &self.options.ack_policy {
            Some(v) => Ok(v),
            None => Ok(&SourceAckPolicy::None),
        }
    }

    /// Return the configured ack interval or default to
    /// `Source::ack_interval(self)`
    fn ack_interval(&self) -> SourceResult<&SourceInterval> {
        match self.options.ack_interval {
            Some(ref v) => Ok(v),
            None => Source::ack_interval(self),
        }
    }

    /// Return the configured monitor interval for handling pending actions
    /// or default to `Source::monitor_interval(self)`
    fn monitor_interval(&self) -> SourceResult<&SourceInterval> {
        match self.options.monitor_interval {
            Some(ref v) => Ok(v),
            None => Source::monitor_interval(self),
        }
    }

    /// Return the configured poll interval or default
    /// to Source::poll_interval(self)
    fn poll_interval(&self) -> SourceResult<&SourceInterval> {
        match self.options.poll_interval {
            Some(ref v) => Ok(v),
            None => Source::poll_interval(self),
        }
    }

    /// Monitor callback function. This currently initiates handling pending messages.
    fn monitor(&mut self) -> SourceResult<()> {
        self.read_pending()
    }

    /// Poll callback for returning messages.
    /// Messages are either generated from `read_unclaimed`
    /// or drained from the internal `reclaimed` queue.
    fn poll(&mut self) -> SourcePollResult {
        // This fork leans towards always reading
        // unclaimed messages...
        // If reclaimed messages exist then
        // drain each poll interval until empty
        if self.reclaimed_size() == 0 {
            self.read_unclaimed()
        } else {
            // slice up to the read msg count
            let count = self.options.read_msg_count.as_ref().unwrap();
            let ub = cmp::min(self.reclaimed.len(), *count);
            Ok(Some(self.reclaimed.drain(..ub).collect()))
        }
    }

    /// Health check: test if the redis connection works
    fn healthy(&mut self) -> bool {
        // try using the redis connection
        // ping, echo, info aren't implemented by redis-rs
        // this key should exist by the time heatlhy is called
        // the first time...
        let key = self.options.key.as_ref().unwrap().as_ref().to_owned();
        match self.connection() {
            Err(_) => false,
            Ok(conn) => {
                let result: RedisResult<bool> = conn.exists(&key);
                match result {
                    Ok(exists) => exists,
                    _ => false,
                }
            }
        }
    }
}
