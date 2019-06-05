use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use crate::common::now_millis;
use crate::source::{Msg, MsgId};
use crate::topology::TaskMsg;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Task {
    name: &'static str,
    path: &'static str,
}

impl Task {
    pub fn new(name: &'static str, path: &'static str) -> Self {
        Task {
            name: name,
            path: path,
        }
    }

    pub fn name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }

    pub fn path(mut self, path: &'static str) -> Self {
        self.path = path;
        self
    }
}

// type Edge = (&'static str, &'static str);
pub type Edge = (String, String);

pub type MatrixRow = (&'static str, &'static str, bool);

pub type Matrix = Vec<MatrixRow>;

#[derive(Clone, Debug, Default)]
pub struct Pipeline {
    pub root: &'static str,

    // store all tasks by task name
    pub tasks: HashMap<&'static str, Task>,

    // store all task names upstream from key
    pub ancestors: HashMap<&'static str, Vec<&'static str>>,

    // store all task names downstream from key
    pub decendants: HashMap<&'static str, Vec<&'static str>>,

    pub matrix: Matrix,
}

impl Pipeline {
    pub fn root(mut self, name: &'static str) -> Self {
        self.root = name;
        self
    }

    pub fn add(mut self, task: Task) -> Self {
        // copy name so we can set the root if needed
        let name = task.name.clone();
        if !self.tasks.contains_key(&task.name) {
            self.tasks.insert(task.name.clone(), task);
        }
        if &self.root == &"" {
            // root is a special-case for adding an edge...
            // we need to account for it, otherwise,
            // we'll skip it as a task.
            // so what we do is make it an edge("root", &name)
            let copy = self.edge("root", &name);
            copy.root(&name)
        } else {
            self
        }
    }

    pub fn edge(mut self, left: &'static str, right: &'static str) -> Self {
        // TODO: what if left or right doesn't exist in self.tasks?
        // if this row already exists, we don't need to add it again
        let matrix_row = (left, right, false);
        if self
            .matrix
            .iter()
            .position(|r| r.0 == left && r.1 == right)
            .is_none()
        {
            self.matrix.push(matrix_row);

            // add it to the decendants
            match self.decendants.get_mut(left) {
                Some(v) => v.push(right.clone()),
                None => {
                    self.decendants.insert(left.clone(), vec![right.clone()]);
                }
            }

            // add it to the ancestors
            match self.ancestors.get_mut(right) {
                Some(v) => v.push(left),
                None => {
                    self.ancestors.insert(right, vec![left]);
                }
            }
        }
        self
    }

    pub fn build(self) -> Self {
        self
    }

    // Generate graphviz compatible string
    // for http://www.webgraphviz.com/
    pub fn to_graphviz(&self) -> String {
        let mut edges = vec![];
        for edge in &self.matrix {
            edges.push(format!("\t\"{}\" -> \"{}\";", &edge.0, &edge.1));
        }
        format!(
            "digraph G {{
    rankdir=LR;
    {}
    }}",
            edges.join("\n")
        )
    }
}

type MsgStatePendingRow = (usize, bool);

#[derive(Debug, Default)]
pub struct PipelineMsgState {
    matrix: Matrix,

    task_visited: HashSet<String>,

    // pending is HashMap<edge, Vec<(index, completed)>>
    pending: HashMap<Edge, Vec<MsgStatePendingRow>>,
}

impl PipelineMsgState {
    pub fn new(matrix: Matrix) -> Self {
        PipelineMsgState {
            matrix: matrix,
            task_visited: HashSet::new(),
            pending: HashMap::new(),
        }
    }

    pub fn get(&self, edge: &Edge) -> Option<&Vec<MsgStatePendingRow>> {
        self.pending.get(&edge)
    }

    pub fn task_visit(&mut self, name: String) {
        self.task_visited.insert(name);
    }

    pub fn edge_start(&mut self, edge: Edge, size: usize) {
        // initialize a vector to keep track of
        // of which sub-tasks we've completed
        let items = (0..size).map(|index| (index, false)).collect();
        self.pending.insert(edge, items);
    }

    // When we mark an edge as visited,
    // we need to see if all the ancestor edges are
    // also visited so we can mark this task
    // as visited
    pub fn edge_visit(&mut self, edge: &Edge) {
        let mut ancestors = 0;
        let mut visited = 0;

        for mut row in &mut self.matrix {
            if row.0 == edge.0 && row.1 == edge.1 {
                // println!("Mark edge as visited: {:?}", &edge);
                row.2 = true;
            }

            if row.1 == edge.1 {
                ancestors += 1;
                if row.2 {
                    visited += 1;
                }
            }
        }

        if ancestors == visited {
            // println!("Mark task as visited: {:?}", &edge.1);
            self.task_visit(edge.1.clone());
        }
    }

    // A `deadend` occurs after we ack all incoming task edges
    // and we have no messages to move into the
    // decendant edges. In this case, we need to find
    // all decendants, from here to the end of the matrix, with single ancestors and mark
    // each task as visited. If more than one ancestor, we skip it.
    pub fn task_deadends(&mut self, edge: &Edge, pipeline: &Pipeline) {
        // println!("task_deadends: {:?}", &edge,);
        self.edge_visit(edge);
        match pipeline.decendants.get(&edge.1[..]) {
            Some(decendants) => {
                for decendant in decendants {
                    let e = (edge.1.clone(), decendant.clone().to_string());
                    self.task_deadends(&e, &pipeline)
                }
            }
            None => {}
        };
    }

    pub fn edge_visit_index(&mut self, edge: &Edge, index: usize) -> Option<bool> {
        // lookup this edge in the map...
        // and mark it as completed
        let pending = match self.pending.get_mut(edge) {
            Some(p) => p,
            None => return None,
        };

        // iterate the entire vector here...
        // and count how many completed we have
        let mut completed = 0;
        let total = pending.len();

        for pair in pending {
            if pair.0 == index {
                pair.1 = true;
            }
            if pair.1 {
                completed += 1;
            }
        }

        // to mark this edge as visited,
        // completed needs to equal total
        let next = completed == total;

        // if all sub-tasks are completed
        // for this edge then we need to update
        // and mark this matrix edge as visited
        if next {
            self.edge_visit(&edge);
        }

        // Response here should be either:
        // next = false (waiting) or true (visited)...
        Some(next)
    }

    /// Have all the nodes been visited in the matrix?
    pub fn finished(&self) -> bool {
        for row in &self.matrix {
            if row.2 == false {
                // println!("unfinished: {:?}", &row);
                return false;
            }
        }
        true
    }
}

/// Enum for communicating the inflight status of pipeline messages.
#[derive(Debug)]
pub enum PipelineInflightStatus {
    // We've reached a completed Edge
    // Returns a bool if we visited the Task
    AckEdge(bool),

    // We've completed the end of the pipeline
    AckSource,

    // We're still waiting to ack inflight edge messages
    PendingEdge,

    // This original source msg id ultimately timed out
    Timeout,

    // The msg id no longer exists in the map
    Removed,
}

type MsgInflightState = (usize, PipelineMsgState);

// Pipeline Inflight
#[derive(Debug, Default)]
pub struct PipelineInflight {
    msg_timeout: Option<usize>,
    /// HashMap<source_msg_id, (timestamp, state)>
    map: HashMap<MsgId, MsgInflightState>,
}

impl PipelineInflight {
    pub fn new(msg_timeout: Option<usize>) -> Self {
        PipelineInflight {
            msg_timeout: msg_timeout,
            ..PipelineInflight::default()
        }
    }

    pub fn get(&self, msg_id: &MsgId) -> Option<&MsgInflightState> {
        self.map.get(msg_id)
    }

    pub fn get_mut(&mut self, msg_id: &MsgId) -> Option<&mut MsgInflightState> {
        self.map.get_mut(msg_id)
    }

    // actions we can do here...
    // insert msgid
    pub fn root(&mut self, msg_id: MsgId, timestamp: usize, state: PipelineMsgState) {
        self.map.insert(msg_id, (timestamp, state));
    }

    // This method supports the use-case where an AckEdge triggers
    // releasing an empty holding pen...
    // We need to visit all decendant edges with a single ancestor from here
    // since this method is always called after `ack` we can skip
    // returning the PipelineInflightStatus here
    pub fn ack_deadend(&mut self, msg_id: &MsgId, edge: &Edge, pipeline: &Pipeline) {
        if let Some((timestamp, msg_state)) = self.map.get_mut(msg_id) {
            msg_state.task_deadends(edge, pipeline);
        }
    }

    pub fn ack(&mut self, msg_id: &MsgId, edge: &Edge, index: usize) -> PipelineInflightStatus {
        // get this msg id in the map
        // and update the index for this edge
        // in the msg state
        let status = if let Some((timestamp, msg_state)) = self.map.get_mut(msg_id) {
            // check for a timeout?
            if let Some(ms) = &self.msg_timeout {
                let timeout = *timestamp + ms;
                if timeout < now_millis() {
                    return PipelineInflightStatus::Timeout;
                }
            }

            // Now we have three possible states here...
            // To start, let's mark this index as completed...
            match msg_state.edge_visit_index(edge, index) {
                Some(edge_completed) => {
                    if edge_completed {
                        // Now we just need to iterate the msg_state.matrix
                        // and check the status for all edges
                        // if all are true then AckSource
                        if msg_state.finished() {
                            PipelineInflightStatus::AckSource
                        } else {
                            PipelineInflightStatus::AckEdge(
                                msg_state.task_visited.contains(&edge.1),
                            )
                        }
                    } else {
                        PipelineInflightStatus::PendingEdge
                    }
                }
                None => {
                    // We shouldn't ever hit this...
                    PipelineInflightStatus::Removed
                }
            }
        } else {
            // if we no longer have an entry, this means someone deleted it...
            // (either timeout or error state was hit)
            // so just return Removed here
            PipelineInflightStatus::Removed
        };
        status
    }

    pub fn remove(&mut self, msg_id: &MsgId) {
        self.map.remove(msg_id);
    }

    pub fn finished(&self, msg_id: &MsgId) -> bool {
        if let Some((ts, msg_state)) = self.get(&msg_id) {
            msg_state.finished()
        } else {
            // if the msg doesn't exist, consider it finished
            true
        }
    }
}

#[derive(Debug, Default)]
pub struct PipelineAvailable {
    // key = Task Name
    queue: HashMap<String, VecDeque<TaskMsg>>,
}

impl PipelineAvailable {
    // pre-initialize the hashmap key queues
    pub fn new(tasks: &HashMap<&'static str, Task>) -> Self {
        let mut this = Self::default();
        for key in tasks.keys() {
            this.queue.insert(key.clone().to_string(), VecDeque::new());
        }
        this
    }

    /// Pops the n-count of tasks from the front of the queue
    pub fn len(&mut self, name: &String) -> Option<usize> {
        return self.queue.get_mut(name).map_or(None, |q| Some(q.len()));
    }

    /// Pops the n-count of tasks from the front of the queue
    pub fn pop(&mut self, name: &String, count: Option<usize>) -> Option<Vec<TaskMsg>> {
        let count = match count {
            Some(i) => i,
            None => 1 as usize,
        };

        return self.queue.get_mut(name).map_or(None, |q| {
            let mut tasks = vec![];
            while let Some(msg) = q.pop_front() {
                &tasks.push(msg);
                if tasks.len() >= count {
                    break;
                }
            }
            Some(tasks)
        });
    }

    pub fn push(&mut self, name: &String, msg: TaskMsg) -> Option<usize> {
        return self.queue.get_mut(name).map_or(None, |q| {
            q.push_back(msg);
            Some(q.len())
        });
    }
}

#[derive(Default, Debug)]
pub struct PipelineAggregate {
    holding: HashMap<String, HashMap<MsgId, Vec<Msg>>>,
}

impl PipelineAggregate {
    pub fn new(tasks: &HashMap<&'static str, Task>) -> Self {
        let mut this = Self::default();
        for key in tasks.keys() {
            this.holding.insert(key.clone().to_string(), HashMap::new());
        }
        this
    }

    // hold this msg for this task_name, source_msg_id, msg
    pub fn hold(&mut self, name: &String, msg_id: MsgId, mut msgs: Vec<Msg>) -> Option<bool> {
        return self.holding.get_mut(name).map_or(None, |map| {
            if !map.contains_key(&msg_id) {
                // insert a new msg
                map.insert(msg_id, msgs);
            } else {
                // store the msg with this msg_id
                map.get_mut(&msg_id).unwrap().append(&mut msgs);
            }
            Some(true)
        });
    }

    // returns all messages for this task_name and msg_id
    pub fn remove(&mut self, name: &String, msg_id: &MsgId) -> Option<Vec<Msg>> {
        return self
            .holding
            .get_mut(name)
            .map_or(None, |map| map.remove(msg_id));
    }
}
