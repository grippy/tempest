use std::str::{from_utf8, Utf8Error};

use crate::error::RedisErrorToSourceError;

use tempest::common::now_millis;
use tempest::source::{
    MsgId, Source, SourceBuilder, SourceError, SourceErrorKind, SourceMsg, SourceMsgAckPolicy,
    SourcePollInterval, SourcePollResult, SourceResult,
};

use redis_streams::{
    client_open, Client, Commands, Connection, ErrorKind, RedisError, RedisResult, StreamCommands,
    StreamId, StreamInfoGroup, StreamInfoGroupsReply, StreamReadOptions, StreamReadReply,
    ToRedisArgs, Value,
};

use serde_json;

use uuid::Uuid;

#[derive(Default)]
pub struct RedisStreamSourceBuilder<'a> {
    options: RedisStreamSourceOptions<'a>,
}

impl<'a> RedisStreamSourceBuilder<'a> {
    pub fn uri(mut self, uri: &'a str) -> Self {
        self.options.uri = Some(uri);
        self
    }

    pub fn key(mut self, key: &'a str) -> Self {
        self.options.key = Some(key);
        self
    }

    pub fn group(mut self, name: &'a str) -> Self {
        self.options.group = Some(name);
        self
    }

    pub fn with_blocking_read(mut self) -> Self {
        self.options.blocking_read = Some(true);
        self
    }

    pub fn group_starting_id(mut self, id: &'a str) -> Self {
        self.options.group_starting_id = Some(RedisStreamGroupStartingId::from(id));
        self
    }

    pub fn reclaim_pending_after(mut self, ms: usize) -> Self {
        self.options.reclaim_pending_after = Some(ms);
        self
    }

    pub fn read_msg_count(mut self, count: usize) -> Self {
        self.options.read_msg_count = Some(count);
        self
    }

    pub fn poll_interval(mut self, ms: u64) -> Self {
        self.options.poll_interval = Some(ms);
        self
    }

    pub fn ack_policy(mut self, policy: SourceMsgAckPolicy) -> Self {
        self.options.ack_policy = Some(policy);
        self
    }

    pub fn max_backoff(mut self, ms: u64) -> Self {
        self.options.max_backoff = Some(ms);
        self
    }
}

impl<'a> SourceBuilder for RedisStreamSourceBuilder<'a> {
    type Source = RedisStreamSource<'a>;

    fn build(&self) -> Self::Source {
        let mut source = Self::Source::default();
        source.options = self.options.clone();
        source
    }
}

#[derive(Clone, Debug)]
pub enum RedisStreamGroupStartingId<'a> {
    Zero,
    Dollar,
    Other(&'a str),
}

impl<'a> RedisStreamGroupStartingId<'a> {
    fn from(id: &'a str) -> Self {
        match id {
            "0" => RedisStreamGroupStartingId::Zero,
            "$" => RedisStreamGroupStartingId::Dollar,
            _ => RedisStreamGroupStartingId::Other(id),
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

#[derive(Clone, Debug)]
pub struct RedisStreamSourceOptions<'a> {
    uri: Option<&'a str>,
    key: Option<&'a str>,
    group: Option<&'a str>,
    consumer: Option<String>,
    group_starting_id: Option<RedisStreamGroupStartingId<'a>>,
    ack_policy: Option<SourceMsgAckPolicy>,
    reclaim_pending_after: Option<usize>,
    blocking_read: Option<bool>,
    read_msg_count: Option<usize>,
    poll_interval: Option<u64>,
    max_backoff: Option<u64>,
}

impl<'a> Default for RedisStreamSourceOptions<'a> {
    fn default() -> Self {
        RedisStreamSourceOptions {
            /// The redis uri where the stream lives
            uri: None,

            /// Thre redis key for the stream
            key: None,

            /// The group name we should assign to this consumer
            group: None,

            /// This is the auto-generated hash we use as the group consumer name
            consumer: Some(format!("{}", Uuid::new_v4().to_simple())),

            /// Defines the message id we should start reading
            /// the stream from.
            group_starting_id: Some(RedisStreamGroupStartingId::default()),

            /// How milliseconds should we consider a pending message before trying to reclaim it?
            reclaim_pending_after: None,

            /// Configure if we should read consumer group streams in blocking mode.
            blocking_read: Some(false),

            /// Configure the number of messages we should read per xread redis command
            read_msg_count: Some(10usize),

            /// Configure the poll interval milliseconds
            poll_interval: Some(100u64),

            // Configure the ack policy
            ack_policy: Some(SourceMsgAckPolicy::Batch(10)),

            /// Configure the max backoff milliseconds
            max_backoff: Some(1000u64),
            // TODO: add deadletter queue here
            // instantiate as a RedisQueueSource
        }
    }
}

pub struct RedisStreamSource<'a> {
    options: RedisStreamSourceOptions<'a>,
    conn: Option<Connection>,
    client: Option<Client>,
}

impl<'a> Default for RedisStreamSource<'a> {
    fn default() -> Self {
        RedisStreamSource {
            options: RedisStreamSourceOptions::default(),
            conn: None,
            client: None,
        }
    }
}

impl<'a> RedisStreamSource<'a> {
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

    fn prime_test_messages(&mut self) -> SourceResult<()> {
        let key = self.options.key.as_ref().unwrap().to_string();
        let conn = &mut self.connection()?;
        for i in 0..100 {
            let _: RedisResult<String> = conn.xadd(&key, "*", &[("k", "v"), ("i", &i.to_string())]);
        }
        Ok(())
    }

    fn read_unclaimed(&mut self) -> SourcePollResult {
        // always read unclaimed messages
        let count = self.options.read_msg_count.as_ref().unwrap();
        let key = self.options.key.as_ref().unwrap().to_string();
        let group = self.options.group.as_ref().unwrap();
        let consumer = self.options.consumer.as_ref().unwrap().to_string();

        // TODO: Configure blocking read?
        let read_opts = StreamReadOptions::default()
            .group(*group, consumer)
            .count(*count);

        // The special char '>' ID  means that the
        // consumer wants to receive only messages
        // that were never delivered to any other consumer.
        // In other words, return new messages...
        let conn = &mut self.connection()?;
        let result: RedisResult<StreamReadReply> = conn.xread_options(&[key], &[">"], read_opts);

        match result {
            Ok(reply) => {
                if reply.keys.len() == 0 {
                    return Ok(None);
                }

                // convert StreamId to SourceMsg
                let mut msgs = vec![];
                for msg in &reply.keys[0].ids {
                    // conver the msg.map => json as byte vec
                    let mut json_map = serde_json::map::Map::default();
                    for (key, val) in &msg.map {
                        match *val {
                            Value::Data(ref b) => match from_utf8(b) {
                                Ok(s) => {
                                    json_map.insert(
                                        key.to_string(),
                                        serde_json::Value::String(s.to_string()),
                                    );
                                }
                                Err(err) => {}
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
                        Err(err) => {}
                    }

                    msgs.push(source_msg);
                }
                Ok(Some(msgs))
            }
            Err(e) => {
                // TODO: handle this error so caller can
                // figure out what to do next
                println!("Error reading stream {:?}", e);
                Ok(None)
            }
        }
    }

    fn group_create(&mut self) -> SourceResult<()> {
        // we need to check or the client ends up with a broken pipe
        // technically, if we're calling group_create here
        // then we should have already created the connection
        // we just need to unwrap a few things...
        let key = self.options.key.unwrap();
        let group = self.options.group.unwrap();
        let starting_id = self.options.group_starting_id.clone().unwrap();
        let conn = self.connection()?;

        let result: RedisResult<bool> = conn.exists(key);
        match result {
            Ok(true) => {
                // do we already have a group for this stream?
                let info: StreamInfoGroupsReply = conn.xinfo_groups(key).unwrap();
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
                println!("Error group_create: {:?}", e);
            }
        }

        Ok(())
    }

    fn stream_ack(&mut self, msgs: Vec<MsgId>) -> SourceResult<()> {
        let mut ack_ids = vec![];
        for msg_id in msgs {
            match from_utf8(&msg_id) {
                Ok(id) => ack_ids.push(id.to_owned()),
                Err(err) => {
                    // TODO
                    // maybe it's possible a msg id was messed up along the way?

                }
            }
        }
        let key = self.options.key.unwrap();
        let group = self.options.group.unwrap();
        let conn = self.connection()?;
        // TODO: chunk the ack_ids into Batch size or leave as is
        let result: RedisResult<i32> = conn.xack(key, group, &ack_ids);
        println!("source redis stream ack: {:?}", &result);
        Ok(())
    }
}

impl<'a> Source for RedisStreamSource<'a> {
    // Any field that's required from setup on
    // should be checked here.
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

    // configure the connection & client
    // create the stream and group if needed
    fn setup(&mut self) -> SourceResult<()> {
        // call validate here
        self.validate()?;

        let uri = self.options.uri.unwrap();
        println!("create redis client: {:?}", &uri);
        match client_open(uri) {
            Ok(client) => {
                match client.get_connection() {
                    Ok(conn) => {
                        self.conn = Some(conn);
                    }
                    Err(err) => {
                        println!("redis client.get_connection error: {:?}", err);
                        return Err(RedisErrorToSourceError::convert(err));
                    }
                }
                // now store the client
                self.client = Some(client);
            }
            Err(err) => {
                println!("redis client_open error: {:?}", err);
                return Err(RedisErrorToSourceError::convert(err));
            }
        }

        // we need to create the stream here
        println!("create group");
        self.group_create()?;

        println!("print test messages");
        self.prime_test_messages()?;

        Ok(())
    }

    fn ack(&mut self, msg_id: MsgId) -> SourceResult<()> {
        self.stream_ack(vec![msg_id])
    }

    fn batch_ack(&mut self, msgs: Vec<MsgId>) -> SourceResult<()> {
        self.stream_ack(msgs)
    }

    fn max_backoff(&self) -> SourceResult<&u64> {
        match &self.options.max_backoff {
            Some(v) => Ok(v),
            None => Source::max_backoff(self),
        }
    }

    fn ack_policy(&self) -> SourceResult<&SourceMsgAckPolicy> {
        match &self.options.ack_policy {
            Some(v) => Ok(v),
            None => Source::ack_policy(self),
        }
    }

    fn poll_interval(&self) -> SourceResult<SourcePollInterval> {
        match self.options.poll_interval {
            Some(v) => Ok(SourcePollInterval::Millisecond(v)),
            None => Source::poll_interval(self),
        }
    }

    fn poll(&mut self) -> SourcePollResult {
        self.read_unclaimed()
    }

    fn healthy(&mut self) -> SourceResult<()> {
        Ok(())
    }
}
