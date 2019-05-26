use actix::prelude::Message;
use std::fmt;
use std::time::Duration;

pub trait SourceBuilder {
    type Source;
    fn build(&self) -> Self::Source;
}

/// This is trait is for defining Topology Sources
pub trait Source {
    // type Connection;

    fn validate(&mut self) -> SourceResult<()> {
        Err(SourceError::new(SourceErrorKind::ValidateError(
            "Validate isn't configured for Source trait".to_string(),
        )))
    }

    fn setup(&mut self) -> SourceResult<()> {
        Ok(())
    }

    fn drain(&mut self) -> SourceResult<()> {
        Ok(())
    }

    fn teardown(&mut self) -> SourceResult<()> {
        Ok(())
    }

    fn connect(&mut self) -> SourceResult<()> {
        Ok(())
    }

    fn healthy(&mut self) -> SourceResult<()>;

    fn ack(&mut self, msg_id: MsgId) -> SourceResult<()> {
        Ok(())
    }

    fn batch_ack(&mut self, msgs: Vec<MsgId>) -> SourceResult<()> {
        for msg_id in msgs {
            let _ = self.ack(msg_id);
        }
        Ok(())
    }

    fn max_backoff(&self) -> SourceResult<&u64> {
        Ok(&1000u64)
    }

    fn poll_interval(&self) -> SourceResult<SourcePollInterval> {
        Ok(SourcePollInterval::Millisecond(1))
    }

    fn ack_policy(&self) -> SourceResult<&SourceMsgAckPolicy> {
        Ok(&SourceMsgAckPolicy::Individual)
    }

    fn poll(&mut self) -> SourcePollResult {
        Ok(None)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SourceMsgAckPolicy {
    Batch(usize),
    Individual,
}

impl Default for SourceMsgAckPolicy {
    fn default() -> Self {
        SourceMsgAckPolicy::Individual
    }
}

pub enum SourcePollInterval {
    Millisecond(u64),
}

impl SourcePollInterval {
    pub fn as_duration(&self) -> Duration {
        match *self {
            SourcePollInterval::Millisecond(v) => Duration::from_millis(v),
        }
    }
}

pub type MsgId = Vec<u8>;
pub type Msg = Vec<u8>;

#[derive(Default, Message, Debug, Clone)]
pub struct SourceMsg {
    /// MsgId as Vec<u8> used for keeping track of a source msg
    pub id: MsgId,
    /// Msg as Vec<u8>
    pub msg: Msg,
    /// Source msg read timestamp
    pub ts: usize,
}

pub type SourcePollResult = Result<Option<Vec<SourceMsg>>, SourceError>;

pub type SourceResult<T> = Result<T, SourceError>;

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
