// #![allow(dead_code)]
// #![allow(unused_variables)]
// #![allow(unused_imports)]

mod error;
mod stream;

pub mod prelude {
    pub use crate::stream::{
        RedisStreamGroupStartingId, RedisStreamPendingAction, RedisStreamPendingHandler,
        RedisStreamPrime, RedisStreamSource, RedisStreamSourceBuilder,
    };
}
