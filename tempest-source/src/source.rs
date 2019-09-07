use config;
use log::{debug, error, info, log, trace, warn};
use serde_derive::Deserialize;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static TARGET_SOURCE_BUILDER: &'static str = "tempest::source::SourceBuilder";

/// `SourceBuilder` trait for defining how to configure
/// structs that implements the `Source` trait.
pub trait SourceBuilder {
    type Source;
    fn build(&self) -> Self::Source;

    /// Given a Topology.toml for a [source.config] `config::Value` override the options
    /// for the source.
    fn parse_config_value(&mut self, _cfg: config::Value) {
        debug!(
            target: TARGET_SOURCE_BUILDER,
            "SourceBuilder.parse_config_value not implemented"
        );
    }
}

/// `Source` trait is used define actions
pub trait Source {
    /// return the name of this source
    fn name(&self) -> &'static str;

    /// Validation logic for determining if a source is properly configured
    /// at runtime.
    fn validate(&mut self) -> SourceResult<()> {
        Err(SourceError::new(SourceErrorKind::ValidateError(
            "Validate isn't configured for Source trait".to_string(),
        )))
    }

    /// Source setup logic should go here
    fn setup(&mut self) -> SourceResult<()> {
        Ok(())
    }

    /// Source shutdown logic for closing connections, performing cleanup logic, etc.
    fn shutdown(&mut self) -> SourceResult<()> {
        Ok(())
    }

    /// Source connection logic for creating client connections
    fn connect(&mut self) -> SourceResult<()> {
        Ok(())
    }

    /// Source health check. Should be used to determine if clients
    /// are still able to reach upstream brokers
    fn healthy(&mut self) -> bool {
        true
    }

    /// Poll for new message from the source
    fn poll(&mut self) -> SourcePollResult {
        Ok(None)
    }

    /// Monitor is a special method which is the callback for the monitor interval
    /// It's intended to ack as a special hook for keeping track of your source structure
    /// or performing other actions.
    fn monitor(&mut self) -> SourceResult<()> {
        Ok(())
    }

    /// Ack a single method with the upstream source
    fn ack(&mut self, _msg_id: MsgId) -> SourceResult<(i32, i32)> {
        Ok((1, 0))
    }

    /// Batch ack a vector of messages with an upstream source
    fn batch_ack(&mut self, msgs: Vec<MsgId>) -> SourceResult<(i32, i32)> {
        Ok((msgs.len() as i32, 0))
    }

    /// The configured maximum amount of time in milliseconds a source should
    /// backoff. This value is read by the TopologyActor when scheduling
    /// the next `source.poll` call
    fn max_backoff(&self) -> SourceResult<&u64> {
        Ok(&1000u64)
    }

    /// Poll interval controls how often a Topology should ask for new messages
    fn poll_interval(&self) -> SourceResult<&SourceInterval> {
        Ok(&SourceInterval::Millisecond(1))
    }

    /// Monitor interval controls how often the source `monitor` is called
    fn monitor_interval(&self) -> SourceResult<&SourceInterval> {
        Ok(&SourceInterval::Millisecond(0))
    }

    /// Ack policy configuration
    fn ack_policy(&self) -> SourceResult<&SourceAckPolicy> {
        Ok(&SourceAckPolicy::Individual)
    }

    /// Configures how often the source should check for new messages to ack
    fn ack_interval(&self) -> SourceResult<&SourceInterval> {
        Ok(&SourceInterval::Millisecond(1000))
    }

    /// Sources can implement Metrics. This method is called by TopologyActor
    /// to internal source flush metrics to configured backend targets.
    fn flush_metrics(&mut self) {}
}

/// Ack policy configuration options
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum SourceAckPolicy {
    /// Accumulate messages and ack them in batches
    Batch(u64),
    /// Ack a single message at a time
    Individual,
    /// Disable message acking.
    None,
}

impl Default for SourceAckPolicy {
    fn default() -> Self {
        SourceAckPolicy::Individual
    }
}

/// Source interval enum time value
#[derive(Clone, Debug, PartialEq)]
pub enum SourceInterval {
    Millisecond(u64),
}

impl Default for SourceInterval {
    fn default() -> Self {
        SourceInterval::Millisecond(1u64)
    }
}

impl SourceInterval {
    pub fn as_duration(&self) -> Duration {
        match *self {
            SourceInterval::Millisecond(v) => Duration::from_millis(v),
        }
    }
}

/// All messages must have a unique message id
/// This value is used to ack messages
pub type MsgId = Vec<u8>;
/// Source type for representing messages
pub type Msg = Vec<u8>;

/// Data structure for representing a message as it moves through
/// a topology
#[derive(Default, Debug, Clone)]
pub struct SourceMsg {
    /// MsgId as Vec<u8> used for keeping track of a source msg
    pub id: MsgId,
    /// Msg as Vec<u8>
    pub msg: Msg,
    /// Source msg read timestamp. Message timeouts are computed from
    /// this field value
    pub ts: usize,
    /// How many times has this message been delivered?
    /// Used for tracking internal retries if topology is
    /// configured with `TopologyFailurePolicy::Retry(count)`
    pub delivered: usize,
}

/// Return type for polled sources
pub type SourcePollResult = Result<Option<Vec<SourceMsg>>, SourceError>;

/// Generic return type
pub type SourceResult<T> = Result<T, SourceError>;

/// Wrapper for handling source error states
pub enum SourceErrorKind {
    // General std::io::Error
    Io(std::io::Error),
    /// Error kind when client connection encounters an error
    Client(String),
    /// Error kind when source isn't correctly configured
    ValidateError(String),
    /// Error kind when we just need one
    Other(String),
}

#[allow(dead_code)]
pub struct SourceError {
    kind: SourceErrorKind,
}

impl SourceError {
    pub fn new(kind: SourceErrorKind) -> Self {
        SourceError { kind: kind }
    }

    pub fn from_io_err(err: std::io::Error) -> Self {
        SourceError::new(SourceErrorKind::Io(err))
    }
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "A Source Error Occurred")
    }
}

impl fmt::Debug for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Source error: {{ file: {}, line: {} }}",
            file!(),
            line!()
        )
    }
}

/// Helper function for returning the current system time as epoch milliseconds
pub fn now_millis() -> usize {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_millis() as usize,
        Err(_) => 0,
    }
}
