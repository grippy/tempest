#[allow(unused_imports)]
mod source;

pub mod prelude {

    pub use crate::source::{
        now_millis, Msg, MsgId, Source, SourceAckPolicy, SourceBuilder, SourceError,
        SourceErrorKind, SourceInterval, SourceMsg, SourcePollResult, SourceResult,
    };
    pub use config;

    pub use log::{debug, error, info, log, trace, warn};
}
