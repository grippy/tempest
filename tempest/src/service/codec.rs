#![allow(dead_code)]
use actix::Message;
use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use serde_derive::{Deserialize, Serialize};
use serde_json as json;
use std::io;
use tokio_io::codec::{Decoder, Encoder};

use crate::topology::{TaskMsg, TaskResponse};

/// Client request
#[derive(Serialize, Deserialize, Debug, Message)]
#[serde(tag = "cmd", content = "data")]
pub enum TopologyRequest {
    /// TaskGet(task_name, count)
    TaskGet(String, u16),
    /// TaskPut response
    TaskPut(TaskResponse),
    /// Ping
    Ping,
}

/// Server response
#[derive(Serialize, Deserialize, Debug, Message)]
#[serde(tag = "cmd", content = "data")]
pub enum TopologyResponse {
    Ping,
    TaskGet(Option<Vec<TaskMsg>>),
}

/// Codec for Client -> Server transport
pub struct TopologyCodec;

impl Decoder for TopologyCodec {
    type Item = TopologyRequest;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let size = {
            if src.len() < 2 {
                return Ok(None);
            }
            BigEndian::read_u16(src.as_ref()) as usize
        };

        if src.len() >= size + 2 {
            src.split_to(2);
            let buf = src.split_to(size);
            Ok(Some(json::from_slice::<TopologyRequest>(&buf)?))
        } else {
            Ok(None)
        }
    }
}

impl Encoder for TopologyCodec {
    type Item = TopologyResponse;
    type Error = io::Error;

    fn encode(&mut self, msg: TopologyResponse, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let msg = json::to_string(&msg).unwrap();
        let msg_ref: &[u8] = msg.as_ref();

        dst.reserve(msg_ref.len() + 2);
        dst.put_u16_be(msg_ref.len() as u16);
        dst.put(msg_ref);

        Ok(())
    }
}

/// Codec for Server -> Client transport
pub struct TopologyClientCodec;

impl Decoder for TopologyClientCodec {
    type Item = TopologyResponse;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let size = {
            if src.len() < 2 {
                return Ok(None);
            }
            BigEndian::read_u16(src.as_ref()) as usize
        };

        if src.len() >= size + 2 {
            src.split_to(2);
            let buf = src.split_to(size);
            Ok(Some(json::from_slice::<TopologyResponse>(&buf)?))
        } else {
            Ok(None)
        }
    }
}

impl Encoder for TopologyClientCodec {
    type Item = TopologyRequest;
    type Error = io::Error;

    fn encode(&mut self, msg: TopologyRequest, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let msg = json::to_string(&msg).unwrap();
        let msg_ref: &[u8] = msg.as_ref();

        dst.reserve(msg_ref.len() + 2);
        dst.put_u16_be(msg_ref.len() as u16);
        dst.put(msg_ref);

        Ok(())
    }
}
