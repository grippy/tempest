pub use std::collections::HashMap;
pub use std::fs::{self, File};
pub use std::io::prelude::*;
pub use std::io::stdout;
pub use std::time::Duration;

pub use actix::prelude::*;

pub(crate) use crate::metric::backend::{
    merge_labels, Backend, BackendError, BackendErrorKind, BackendResult, Msg,
};
pub(crate) use crate::metric::{FormatedMetric, MetricFormat, MetricTarget};

pub use crate::common::logger::*;

pub(crate) type Format = &'static str;

pub(crate) static EMPTY: Format = "";
pub(crate) static COLON: Format = ":";
pub(crate) static COMMA: Format = ",";
pub(crate) static EQUAL: Format = "=";
pub(crate) static PIPE: Format = "|";
pub(crate) static PERIOD: Format = ".";
pub(crate) static POUND: Format = "#";
#[allow(dead_code)]
pub(crate) static SEMICOLON: Format = ";";
pub(crate) static UNDERSCORE: Format = "_";

pub(crate) static LPAREN: Format = "{";
pub(crate) static RPAREN: Format = "}";
pub(crate) static SPACE: Format = " ";
pub(crate) static QUOTE: Format = "\"";
pub(crate) static LBR: Format = "\n";
#[allow(dead_code)]
pub(crate) static TAB: Format = "\t";
