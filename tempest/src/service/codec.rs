#![allow(dead_code)]
use actix::Message;
use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use serde_derive::{Deserialize, Serialize};
use serde_json as json;
use std::io;
use tokio_io::codec::{Decoder, Encoder};

use crate::metric::AggregateMetrics;
use crate::topology::{TaskMsg, TaskResponse};

/// Message type for sending requests
/// to a `TopologyService`
#[derive(Serialize, Deserialize, Debug, Message)]
#[serde(tag = "cmd", content = "data")]
pub(crate) enum TopologyRequest {
    /// TaskGet(task_name, count)
    TaskGet(String, u16),
    /// TaskPut response
    TaskPut(TaskResponse),
    /// Ping
    Ping,
}

/// Message type for sending responses
/// from a `TopologyService`
#[derive(Serialize, Deserialize, Debug, Message)]
#[serde(tag = "cmd", content = "data")]
pub(crate) enum TopologyResponse {
    Ping,
    TaskGet(Option<Vec<TaskMsg>>),
}

/// Wire codec for decoding client
/// `TopologyRequest` and encoding server
/// `TopologyResponse`.
pub(crate) struct TopologyCodec;

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

/// Wire codec for decoding server
/// `TopologyResponse` and encoding client
/// `TopologyRequest`.
pub(crate) struct TopologyClientCodec;

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

/// AgentCodec

/// Client request
#[derive(Serialize, Deserialize, Message, Debug)]
#[serde(tag = "cmd", content = "data")]
pub(crate) enum AgentRequest {
    AggregateMetricsPut(AggregateMetrics),
    Ping,
}

/// Server response
#[derive(Serialize, Deserialize, Message, Debug)]
#[serde(tag = "cmd", content = "data")]
pub(crate) enum AgentResponse {
    Ping,
}

/// Codec for Client -> Server transport
pub(crate) struct AgentCodec;

impl Decoder for AgentCodec {
    type Item = AgentRequest;
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
            Ok(Some(json::from_slice::<Self::Item>(&buf)?))
        } else {
            Ok(None)
        }
    }
}

impl Encoder for AgentCodec {
    type Item = AgentResponse;
    type Error = io::Error;

    fn encode(&mut self, msg: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let msg = json::to_string(&msg).unwrap();
        let msg_ref: &[u8] = msg.as_ref();

        dst.reserve(msg_ref.len() + 2);
        dst.put_u16_be(msg_ref.len() as u16);
        dst.put(msg_ref);

        Ok(())
    }
}

/// Codec for Server -> Client transport
pub(crate) struct AgentClientCodec;

impl Decoder for AgentClientCodec {
    type Item = AgentResponse;
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
            Ok(Some(json::from_slice::<Self::Item>(&buf)?))
        } else {
            Ok(None)
        }
    }
}

impl Encoder for AgentClientCodec {
    type Item = AgentRequest;
    type Error = io::Error;

    fn encode(&mut self, msg: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let msg = json::to_string(&msg).unwrap();
        let msg_ref: &[u8] = msg.as_ref();

        dst.reserve(msg_ref.len() + 2);
        dst.put_u16_be(msg_ref.len() as u16);
        dst.put(msg_ref);

        Ok(())
    }
}
