use redis_streams::RedisError;
use tempest_source::prelude::{SourceError, SourceErrorKind};

pub struct RedisErrorToSourceError;

impl RedisErrorToSourceError {
    pub fn convert(err: RedisError) -> SourceError {
        let s = err.category().to_string();
        SourceError::new(SourceErrorKind::Client(s))
    }
}
