#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use std::str::{from_utf8, Utf8Error};
use std::collections::VecDeque;
use std::cmp::min;

use tempest::common::now_millis;
use tempest::source::*;
use uuid::Uuid;

#[derive(Default)]
pub struct MockSourceBuilder {
    options: MockSourceOptions,
}

impl MockSourceBuilder {
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
    pub fn prime(mut self, f: fn(mock: &mut  MockSource)) -> Self {
        self.options.prime = Some(f);
        self
    }

}

impl SourceBuilder for MockSourceBuilder {
    type Source = MockSource;

    fn build(&self) -> Self::Source {
        let mut source = Self::Source::default();
        source.options = self.options.clone();
        source
    }
}

#[derive(Clone)]
pub struct MockSourceOptions {
    ack_policy: Option<SourceMsgAckPolicy>,
    read_msg_count: Option<usize>,
    poll_interval: Option<u64>,
    max_backoff: Option<u64>,
    prime: Option<fn(mock: &mut  MockSource)>,
}

//Uuid::new_v4().to_simple()
impl Default for MockSourceOptions {
    fn default() -> Self {
        MockSourceOptions {
            read_msg_count: Some(10usize),
            poll_interval: Some(100u64),
            ack_policy: Some(SourceMsgAckPolicy::Batch(10)),
            max_backoff: Some(1000u64),
            prime: None,
        }
    }
}

pub struct MockSource {
    options: MockSourceOptions,
    pub queue: VecDeque<SourceMsg>,
    acked: usize,
}

impl Default for MockSource {
    fn default() -> Self {
        MockSource {
            options: MockSourceOptions::default(),
            queue: VecDeque::new(),
            acked: 0,
        }
    }
}

impl MockSource {

    fn read(&mut self) -> SourcePollResult {

        let count = self.options.read_msg_count.as_ref().unwrap();
        // println!("drain count: {}", &count);
        let len = self.queue.len();
        if len > 0 {
            let msgs = self.queue.drain(..min(len, *count)).collect::<Vec<SourceMsg>>();
            if msgs.len() > 0 {
                Ok(Some(msgs))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }

    }
}

impl Source for MockSource {

    fn setup(&mut self) -> SourceResult<()> {
        match self.options.prime {
            Some(f) => f(self),
            None => {}
        }
        Ok(())
    }

    fn ack(&mut self, msg_id: MsgId) -> SourceResult<()> {
        self.acked += 1;
        Ok(())
    }

    fn batch_ack(&mut self, msgs: Vec<MsgId>) -> SourceResult<()> {
        self.acked += msgs.len();
        println!("acked: {}", &self.acked);
        Ok(())
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
        self.read()
    }

    fn healthy(&mut self) -> SourceResult<()> {
        Ok(())
    }
}