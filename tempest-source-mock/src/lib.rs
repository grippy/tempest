use std::cmp::min;
use std::collections::VecDeque;

// use tempest::common::logger::*;
use tempest_source::prelude::*;

static TARGET_SOURCE: &'static str = "source::MockSource";

pub mod prelude {
    pub use super::{MockSource, MockSourceBuilder, MockSourceOptions};
}

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
        self.options.poll_interval = Some(SourceInterval::Millisecond(ms));
        self
    }

    pub fn ack_policy(mut self, policy: SourceAckPolicy) -> Self {
        self.options.ack_policy = Some(policy);
        self
    }

    pub fn max_backoff(mut self, ms: u64) -> Self {
        self.options.max_backoff = Some(ms);
        self
    }

    pub fn prime(mut self, f: fn(mock: &mut MockSource)) -> Self {
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
    ack_policy: Option<SourceAckPolicy>,
    ack_interval: Option<SourceInterval>,
    read_msg_count: Option<usize>,
    poll_interval: Option<SourceInterval>,
    max_backoff: Option<u64>,
    prime: Option<fn(mock: &mut MockSource)>,
}

impl Default for MockSourceOptions {
    fn default() -> Self {
        MockSourceOptions {
            read_msg_count: Some(10usize),
            poll_interval: Some(SourceInterval::Millisecond(1u64)),
            ack_policy: Some(SourceAckPolicy::Batch(10)),
            ack_interval: Some(SourceInterval::Millisecond(100u64)),
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
            let msgs = self
                .queue
                .drain(..min(len, *count))
                .collect::<Vec<SourceMsg>>();
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
    fn name(&self) -> &'static str {
        "MockSource"
    }

    fn setup(&mut self) -> SourceResult<()> {
        match self.options.prime {
            Some(f) => f(self),
            None => {}
        }
        Ok(())
    }

    fn ack(&mut self, _msg_id: MsgId) -> SourceResult<(i32, i32)> {
        self.acked += 1;
        Ok((1, 1))
    }

    fn batch_ack(&mut self, msgs: Vec<MsgId>) -> SourceResult<(i32, i32)> {
        self.acked += msgs.len();
        trace!(target: TARGET_SOURCE, "acked total: {}", &self.acked);
        Ok((msgs.len() as i32, msgs.len() as i32))
    }

    fn max_backoff(&self) -> SourceResult<&u64> {
        match &self.options.max_backoff {
            Some(v) => Ok(v),
            None => Source::max_backoff(self),
        }
    }

    fn ack_policy(&self) -> SourceResult<&SourceAckPolicy> {
        match &self.options.ack_policy {
            Some(v) => Ok(v),
            None => Source::ack_policy(self),
        }
    }

    fn ack_interval(&self) -> SourceResult<&SourceInterval> {
        match self.options.ack_interval {
            Some(ref v) => Ok(v),
            None => Source::ack_interval(self),
        }
    }

    fn poll_interval(&self) -> SourceResult<&SourceInterval> {
        match self.options.poll_interval {
            Some(ref v) => Ok(v),
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
