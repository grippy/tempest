//! `ChatServer` is an actor. It maintains list of connection client session.
//! And manages available rooms. Peers send messages to other peers in same
//! room through `ChatServer`.

use actix::prelude::*;
use actix::Response;
use rand::{self, Rng};
use std::collections::{HashMap, HashSet};

use crate::service::codec::TopologyResponse;
use crate::service::session;
use crate::topology::{TaskMsg, TaskRequest};

/// Message for chat server communications

/// New chat session is created
pub struct Connect {
    pub addr: Addr<session::TopologySession>,
}

/// Response type for Connect message
///
/// Chat server returns unique session id
impl actix::Message for Connect {
    type Result = usize;
}

/// Session is disconnected
#[derive(Message)]
pub struct Disconnect {
    pub id: usize,
}

/// `ChatServer` manages chat rooms and responsible for coordinating chat
/// session. implementation is super primitive
pub struct TopologyServer {
    sessions: HashMap<usize, Addr<session::TopologySession>>,
}

impl Default for TopologyServer {
    fn default() -> TopologyServer {
        TopologyServer {
            sessions: HashMap::new(),
        }
    }
}

impl Supervised for TopologyServer {}

impl SystemService for TopologyServer {}

/// Make actor from `ChatServer`
impl Actor for TopologyServer {
    /// We are going to use simple Context, we just need ability to communicate
    /// with other actors.
    type Context = Context<Self>;
}

/// Handler for Connect message.
///
/// Register new session and assign unique id to this session
impl Handler<Connect> for TopologyServer {
    type Result = usize;

    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) -> Self::Result {
        println!("TaskService joined");
        // register session with random id
        let id = rand::thread_rng().gen::<usize>();
        self.sessions.insert(id, msg.addr);
        // send id back
        id
    }
}

/// Handler for Disconnect message.
impl Handler<Disconnect> for TopologyServer {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        println!("Someone disconnected");
        // remove address
        self.sessions.remove(&msg.id);
    }
}

impl Handler<TaskRequest> for TopologyServer {
    type Result = ();

    fn handle(&mut self, msg: TaskRequest, ctx: &mut Context<Self>) {
        // println!("Handler<TaskRequest> for TopologyServer: {:?}", &msg);
        match msg {
            TaskRequest::GetAvailableResponse(session_id, name, tasks) => {
                // TODO: this client might not exist anymore...
                // we need a way to put that back?
                // println!("returning tasks for session id: {}", &session_id);
                match self.sessions.get_mut(&session_id) {
                    Some(session) => {
                        // println!("TaskRequest::ReturnAvailable tasks: {:?}", &tasks);
                        session.do_send(TopologyResponse::TaskGet(tasks));
                    }
                    None => {
                        println!(
                            "Can't find session matching this id: total sessions: {}",
                            self.sessions.len()
                        );
                    }
                }
            }
            _ => {
                println!("here borked");
            }
        }
    }
}
