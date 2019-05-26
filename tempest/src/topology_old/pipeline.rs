use std::collections::HashMap;
use std::collections::VecDeque;

/// We need to define how a task should
/// expect messages to flow into it.
#[derive(Clone, Debug, PartialEq)]
pub enum TaskIngress {
    /// This type makes messages available from upstream nodes
    /// as soon as their picked up in the PipelineActor
    Normal,

    /// This type aggregates all incoming task
    /// messages from upstream nodes before
    /// making them available to the task.
    /// Think Fan-In aggregation.
    Aggregate,
}

impl Default for TaskIngress {
    fn default() -> Self {
        TaskIngress::Normal
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TaskOptions {
    workers: u32,
    ingress: TaskIngress,
}

impl Default for TaskOptions {
    fn default() -> Self {
        TaskOptions {
            workers: 1,
            ingress: TaskIngress::default(),
        }
    }
}

impl TaskOptions {
    pub fn workers(mut self, size: u32) -> Self {
        self.workers = size;
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Task {
    name: &'static str,
    path: &'static str,
    options: TaskOptions,
}

impl Task {
    pub fn new(name: &'static str, path: &'static str) -> Self {
        Task {
            name: name,
            path: path,
            options: TaskOptions::default(),
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

    pub fn options(mut self, options: TaskOptions) -> Self {
        self.options = options;
        self
    }

    pub fn workers(mut self, size: u32) -> Self {
        self.options.workers = size;
        self
    }
}

// type Edge = (&'static str, &'static str);
type Edge = (String, String);

type MatrixRow = (&'static str, &'static str, bool);

type Matrix = Vec<MatrixRow>;

#[derive(Clone, Debug, Default)]
pub struct Pipeline {
    root: &'static str,
    tasks: Vec<Task>,
    adjacent: HashMap<&'static str, Vec<&'static str>>,
    matrix: Matrix,
}

impl Pipeline {
    pub fn root(mut self, name: &'static str) -> Self {
        self.root = name;
        self
    }

    fn add(mut self, task: Task) -> Self {
        // copy name so we can set the root if needed
        let name = task.name.clone();
        if !self.tasks.contains(&task) {
            self.tasks.push(task);
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
            // add it to the adjacency matrix
            match self.adjacent.get_mut(left) {
                Some(v) => v.push(right),
                None => {
                    self.adjacent.insert(left, vec![right]);
                }
            }
        }
        self
    }

    fn build(self) -> Self {
        self
    }

}

#[derive(Debug, Default)]
pub struct PipelineMsgState {
    matrix: Matrix,

    // pending is HashMap<edge, Vec<(index, completed)>>
    pending: HashMap<Edge, Vec<(usize, bool)>>,
}

impl PipelineMsgState {
    fn new(matrix: Matrix) -> Self {
        PipelineMsgState {
            matrix: matrix,
            pending: HashMap::new(),
        }
    }

    fn edge_start(&mut self, edge: Edge, size: usize) {
        // initialize a vector to keep track of
        // of which sub-tasks we've completed
        let items = (0..size).map(|index| (index, false)).collect();
        self.pending.insert(edge, items);
    }

    fn edge_visit(&mut self, edge: &Edge) {
        for mut row in &mut self.matrix {
            if row.0 == edge.0 && row.1 == edge.1 {
                row.2 = true;
                break;
            }
        }
    }

    fn edge_visit_deadends(&mut self,  edge: &Edge) {

        // // we need to mark this edge as visited
        // self.edge_visit(&edge);

        // // get all decendants of this edge
        // for decendant in self.decendants(&edge.0) {
        //     // now get the ancestors for the right edge
        //     // if we have only one ancestor then
        //     // recurse down until we reach the end
        //     if self.ancestors(&decendant.1.to_string()).len() == 1 {
        //         self.edge_visit_deadends( &(decendant.0.to_string(), decendant.1.to_string()) );
        //     }
        // }
    }

    fn edge_visit_index(&mut self, edge: &Edge, index: usize) -> Option<bool> {
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
    fn finished(&self) -> bool {
        for row in &self.matrix {
            if row.2 == false {
                return false;
            }
        }
        true
    }

    // return the total number of ancestors, and the total number of visted ancestors
    fn ancestors_visited(&self, name: &String) -> (u16, u16) {
        let mut total = 0;
        let mut visited = 0;
        let name = &name[..];
        for row in &self.matrix {
            // right should equal name for this to be an ancestor
            if row.1 == name {
                total += 1;
                if row.2 == true {
                    visited += 1;
                }
            }
        }
        (total, visited)
    }

    // TODO: figure out how to pre-compute ancestors for
    // each node without having to iterate the matrix
    // every time and clone.. and return a Vec<Edge>
    fn ancestors(&self, name: &String) -> Vec<MatrixRow> {
        let mut found = Vec::new();
        let name = &name[..];
        for row in &self.matrix {
            if row.1 == name {
                found.push(row.clone())
            }
        }
        found
    }

    // Since we don't store the adjacent hash we need this for now
    // TODO: store the adjacent so we don't need to do
    // all this cloning.. and return a Vec<Edge>
    fn decendants(&self, name: &String) -> Vec<MatrixRow> {
        let mut found = Vec::new();
        let name = &name[..];
        for row in &self.matrix {
            if row.0 == name {
                found.push(row.clone())
            }
        }
        found
    }

}

/// Enum for communicating the inflight status of pipeline messages.

pub enum PipelineInflightStatus {
    // We've reached a completed Edge
    // Return total, visted for this right node ancestors
    AckEdge(u16, u16),

    // We've completed the end of the pipeline
    AckSource,

    // We're still waiting to ack inflight edge messages
    PendingEdge,

    // This original source msg id ultimately timed out
    Timeout,

    // The msg id no longer exists in the map
    Removed,
}

// Pipeline Inflight
#[derive(Default)]
pub struct PipelineInflight {
    msg_timeout: Option<usize>,
    /// HashMap<source_msg_id, (timestamp, state)>
    map: HashMap<MsgId, (usize, PipelineMsgState)>,
}

impl PipelineInflight {
    fn new(msg_timeout: Option<usize>) -> Self {
        PipelineInflight {
            msg_timeout: msg_timeout,
            ..PipelineInflight::default()
        }
    }

    // actions we can do here...
    // insert msgid
    fn root(&mut self, msg_id: MsgId, timestamp: usize, state: PipelineMsgState) {
        self.map.insert(msg_id, (timestamp, state));
    }

    // This method supports the use-case where an AckEdge triggers
    // releasing an empty holding pen...
    // We need to visit all decendant edges with a single ancestor from here
    // since this method is always called after `ack` we can skip
    // returning the PipelineInflightStatus here
    fn ack_deadend(&mut self, msg_id: &MsgId, edge: &Edge) {
        if let Some((timestamp, msg_state)) = self.map.get_mut(msg_id) {
            msg_state.edge_visit_deadends(edge);
        }
    }

    fn ack(&mut self, msg_id: &MsgId, edge: &Edge, index: usize) -> PipelineInflightStatus {
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
                            let (total, visited) = msg_state.ancestors_visited(&edge.1);
                            PipelineInflightStatus::AckEdge(total, visited)
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

    fn remove(&mut self, msg_id: &MsgId) {
        self.map.remove(msg_id);
    }
}

#[derive(Default)]
pub struct PipelineAvailable {
    // key = Task Name
    queue: HashMap<String, VecDeque<TaskMsg>>,
}

impl PipelineAvailable {
    // pre-initialize the hashmap key queues
    fn new(tasks: &Vec<Task>) -> Self {
        let mut this = Self::default();
        for task in tasks {
            let key = task.name.clone();
            this.queue.insert(key.to_string(), VecDeque::new());
        }
        this
    }

    /// Pops the n-count of tasks from the front of the queue
    fn pop(&mut self, name: &String, count: Option<usize>) -> Option<Vec<TaskMsg>> {
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

    fn push(&mut self, name: &String, msg: TaskMsg) -> Option<usize> {
        return self.queue.get_mut(name).map_or(None, |q| {
            q.push_back(msg);
            Some(q.len())
        });
    }
}

#[derive(Default)]
pub struct PipelineAggregate {
    holding: HashMap<String, HashMap<MsgId, Vec<Msg>>>,
}

impl PipelineAggregate {

    fn new(tasks: &Vec<Task>) -> Self {
        let mut this = Self::default();
        for task in tasks {
            let key = task.name.clone();
            this.holding.insert(key.to_string(), HashMap::new());
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
