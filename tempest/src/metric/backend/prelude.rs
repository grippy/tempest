pub use std::collections::HashMap;
pub use std::fs::{self, File};
pub use std::io::prelude::*;
pub use std::io::stdout;
pub use std::time::Duration;

pub use actix::prelude::*;

pub use crate::metric::{Bucket, Labels, MetricTarget, Metrics, ROOT};
pub use crate::metric::backend::{merge_labels, Backend, Msg};

pub use crate::common::logger::*;
pub use crate::topology::SourceActor;

pub type Format = &'static str;

pub static COLON: Format = ":";
pub static COMMA: Format = ",";
pub static EQUAL: Format = "=";
pub static PERIOD: Format = ".";
pub static POUND: Format = "#";
pub static SEMICOLON: Format = ";";
pub static UNDERSCORE: Format = "_";

pub static LPAREN: Format = "{";
pub static RPAREN: Format = "}";
pub static SPACE: Format = " ";
pub static QUOTE: Format = "\"";
pub static LBR: Format = "\n";
pub static TAB: Format = "\t";