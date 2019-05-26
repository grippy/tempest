use redis_streams::RedisError;
use tempest::source::{SourceError, SourceErrorKind};

pub struct RedisErrorToSourceError;

impl RedisErrorToSourceError {
    pub fn convert(err: RedisError) -> SourceError {
        let s = err.category().to_string();
        SourceError::new(SourceErrorKind::Client(s))
    }
}
