use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use crate::common::logger::*;
use crate::common::now_millis;
use crate::task::Task;
use crate::topology::TaskMsg;

use tempest_source::prelude::{Msg, MsgId};

static ROOT_NAME: &'static str = "root";

/// An edge defines the relationship between two tasks.
/// For now, there is no need to store a weight here since it doesn't
/// mean anything.
pub(crate) type Edge = (String, String);

/// A matrix row defines an edge between two tasks
/// and tracks it was visited for a particular message id.
pub(crate) type MatrixRow = (&'static str, &'static str, bool);

/// A matrix is a vector of visited task edges.
pub(crate) type Matrix = Vec<MatrixRow>;

/// A pipeline is a collection of tasks and defines
/// how they relate to each other in the form a directed acyclic graph.
///
#[derive(Default)]
pub struct Pipeline {
    /// The root task name of our pipeline
    /// Auto-assigned when calling `.add` with the first `Task`
    pub(crate) root: &'static str,

    /// store all tasks by task name
    pub(crate) tasks: HashMap<&'static str, Box<dyn Task>>,

    /// store all task names upstream from Task name
    pub(crate) ancestors: HashMap<&'static str, Vec<&'static str>>,

    /// store all task names downstream from key
    pub(crate) descendants: HashMap<&'static str, Vec<&'static str>>,

    /// Master DAG matrix of all tasks. This is cloned per message id
    /// so we can keep track of message states within the pipeline
    pub(crate) matrix: Matrix,
}

impl Pipeline {
    pub(crate) fn runtime(&self) -> Self {
        Self {
            root: self.root.clone(),
            tasks: HashMap::new(),
            ancestors: self.ancestors.clone(),
            descendants: self.descendants.clone(),
            matrix: self.matrix.clone(),
        }
    }

    pub(crate) fn names(&self) -> Vec<String> {
        self.tasks.keys().map(|k| k.to_string()).collect()
    }

    pub(crate) fn remove(&mut self, name: &str) -> Option<Box<dyn Task>> {
        self.tasks.remove(name)
    }

    pub(crate) fn get(&self, name: &str) -> Option<&dyn Task> {
        self.tasks
            .get(name)
            .and_then(|boxed| Some(&**boxed as &(dyn Task)))
    }

    #[allow(dead_code)]
    pub(crate) fn get_mut(&mut self, name: &str) -> Option<&mut dyn Task> {
        self.tasks
            .get_mut(name)
            .and_then(|boxed| Some(&mut **boxed as &mut (dyn Task)))
    }

    /// Return the tasks size
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Set the root Task name of our pipeline.
    pub fn root(mut self, name: &'static str) -> Self {
        self.root = name;
        self
    }

    /// Adds a Task to the pipeline. By default, the first task added is
    /// labeled as the root task. Naming a task with "root" isn't allowed.
    pub fn task<T: Task + 'static>(mut self, task: T) -> Self {
        // copy name so we can set the root if needed
        let name = task.name().clone();

        if name == ROOT_NAME {
            panic!("Task.name \"root\" isn't allowed. Use a different name.");
        }

        if self.get(&name).is_none() {
            self.tasks.insert(task.name(), Box::new(task));
        }

        if &self.root == &"" {
            // root is a special-case for adding an edge...
            // we need to account for it, otherwise,
            // we'll skip it as a task.
            // so what we do is make it an edge("root", &name)
            let copy = self.edge(ROOT_NAME, &name);
            copy.root(&name)
        } else {
            self
        }
    }

    /// Use depth-first search to check for cycles
    fn dfs_check(
        &self,
        task: &'static str,
        mut visited: &mut HashSet<&'static str>,
        mut stack: &mut HashSet<&'static str>,
    ) -> bool {
        visited.insert(task.clone());
        stack.insert(task.clone());
        match self.descendants.get(task) {
            Some(descendants) => {
                for descendant in descendants {
                    if !visited.contains(descendant) {
                        if self.dfs_check(descendant, &mut visited, &mut stack) {
                            return true;
                        }
                    } else if stack.contains(descendant) {
                        return true;
                    }
                }
            }
            None => {}
        }
        // pop task from the stack
        stack.remove(task);
        false
    }

    /// Pipeline shouldn't have any cyclical tasks
    fn cyclical(&self) -> bool {
        let mut visited = HashSet::new();
        let mut stack = HashSet::new();
        for task in self.tasks.keys() {
            if !visited.contains(task) {
                if self.dfs_check(task, &mut visited, &mut stack) {
                    error!("Pipeline task is cyclical: {}", &task);
                    return true;
                }
            }
        }
        false
    }

    /// Defines an ancestor (left) Task name and descendent (right) Task name edge
    ///
    pub fn edge(mut self, left: &'static str, right: &'static str) -> Self {
        // Skip adding if left and right are the same
        //
        if &left == &right {
            panic!(
                "Pipeline has the same value for left & right edges: {:?}",
                &left
            );
        }

        // what if left or right aren't defined in self.tasks?
        if left != ROOT_NAME && !self.tasks.contains_key(&left) {
            panic!("Pipeline.tasks missing a task with the name {}", &left);
        }
        if !self.tasks.contains_key(&right) {
            panic!("Pipeline.tasks missing a task with the name {}", &right);
        }

        // if this row already exists, we don't need to add it again
        let matrix_row = (left, right, false);
        if self
            .matrix
            .iter()
            .position(|r| r.0 == left && r.1 == right)
            .is_none()
        {
            self.matrix.push(matrix_row);

            // add it to the descendants
            match self.descendants.get_mut(left) {
                Some(v) => v.push(right.clone()),
                None => {
                    self.descendants.insert(left.clone(), vec![right.clone()]);
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

        // validate no cycles exist...
        if self.cyclical() {
            panic!("Pipeline contains a cycle: {:?}", &self.matrix);
        }

        self
    }

    /// A convenience method for return self
    pub fn build(self) -> Self {
        // We need to make sure we have at least one task.
        if self.tasks.len() == 0 {
            panic!("Pipeline didn't define any tasks.")
        }
        self
    }

    /// Useful for debugging pipeline Task relationships
    /// Generate graphviz compatible string
    /// for http://www.webgraphviz.com/
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

/// When a message is read from the Source, it's feed into the first task.
/// The output of this first task can generate N messages. This type is
/// used to keep track of all N messages and if they've been acked.
/// Once all messages are acked for original input message, we can then proceed to
/// mark the edge as visited.
type MsgStatePendingRow = (usize, bool);

/// This stores a `MsgId` and along with its state.
#[derive(Debug, Default)]
pub struct PipelineMsgState {
    /// The cloned copy of our `Pipeline.matrix`
    matrix: Matrix,

    /// The list of which tasks we've already visited
    task_visited: HashSet<String>,

    /// Stores a vector of which edges still have unacked (pending)
    /// messages
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

    /// Returns a list of pending messages for a given edge
    pub fn get(&self, edge: &Edge) -> Option<&Vec<MsgStatePendingRow>> {
        self.pending.get(&edge)
    }
    /// Marks the task name as visited
    pub fn task_visit(&mut self, name: String) {
        self.task_visited.insert(name);
    }
    /// Marks the edge as being started for processing
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

    /// Marks an edge as visted and marks the task as visited
    /// if all task ancestors have also been visted
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
            // trace!(target: TARGET_PIPELINE, "Mark task as visited: {:?}", &edge.1);
            self.task_visit(edge.1.clone());
        }
    }

    // A `dead-end` occurs after we ack all incoming task edges
    // and we have no messages to move into the
    // descendant edges. In this case, we need to find
    // all descendants, from here to the end of the matrix, with single ancestors and mark
    // each task as visited. If more than one ancestor, we skip it.

    /// A task dead-end occurs when the output of an ancestor edge returns no
    /// messages to move into descendant tasks. This does the appropriate
    /// cleanup to mark all downstream descendants as visited for a given edge.
    pub fn task_dead_ends(&mut self, edge: &Edge, pipeline: &Pipeline) {
        // println!("task_dead_ends: {:?}", &edge,);
        self.edge_visit(edge);
        match pipeline.descendants.get(&edge.1[..]) {
            Some(descendants) => {
                for descendant in descendants {
                    let e = (edge.1.clone(), descendant.clone().to_string());
                    self.task_dead_ends(&e, &pipeline)
                }
            }
            None => {}
        };
    }

    /// This is called when we ack a message by its index for an edge
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

    /// Have all the nodes been visited in the matrix
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
    /// We've reached a completed Edge
    /// Returns a bool if we visited the Task
    AckEdge(bool),

    /// We've completed the end of the pipeline
    AckSource,

    /// We're still waiting to ack inflight edge messages
    PendingEdge,

    /// This original source msg id ultimately timed out
    Timeout,

    /// The msg id no longer exists in the map
    Removed,
}

/// An inflight message is currently being processed by a TaskService
/// Keep track of these so we can properly clean them up if they timeout, etc.
type MsgInflightState = (usize, PipelineMsgState);

/// All messages inflight (i.e. being worked on by tasks).
#[derive(Debug, Default)]
pub struct PipelineInflight {
    /// Stores the max timeout as ms allowed for an inflight Pipeline message
    /// TODO: wire this up
    msg_timeout: Option<usize>,
    /// HashMap<source_msg_id, (timestamp, state)>
    map: HashMap<MsgId, MsgInflightState>,
}

impl PipelineInflight {
    /// Initialize a this struct using the max msg timeout value
    pub fn new(msg_timeout: Option<usize>) -> Self {
        PipelineInflight {
            msg_timeout: msg_timeout,
            ..PipelineInflight::default()
        }
    }

    pub fn size(&mut self) -> usize {
        self.map.len()
    }

    /// Returns the `MsgInflightStatus` for a given message id
    pub fn get(&self, msg_id: &MsgId) -> Option<&MsgInflightState> {
        self.map.get(msg_id)
    }

    pub fn get_mut(&mut self, msg_id: &MsgId) -> Option<&mut MsgInflightState> {
        self.map.get_mut(msg_id)
    }

    /// This method is called when first we first see a message
    /// It keeps track of the timestamp (the message was read by the source
    /// so we can use it to determine timeouts later) and the initialized `PipelineMsgState`
    pub fn root(&mut self, msg_id: MsgId, timestamp: usize, state: PipelineMsgState) {
        self.map.insert(msg_id, (timestamp, state));
    }

    /// This method supports the use-case where an AckEdge triggers
    /// releasing an empty holding pen...
    /// We need to visit all descendant edges with a single ancestor from here
    /// since this method is always called after `ack` we can skip
    /// returning the PipelineInflightStatus here
    pub fn ack_dead_end(&mut self, msg_id: &MsgId, edge: &Edge, pipeline: &Pipeline) {
        if let Some((_timestamp, msg_state)) = self.map.get_mut(msg_id) {
            msg_state.task_dead_ends(edge, pipeline);
        }
    }

    /// This method is called after a message by index, for a given edge, is acked
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

    /// Remove a message by id
    pub fn clean_msg_id(&mut self, msg_id: &MsgId) {
        self.map.remove(msg_id);
    }

    /// Mark this message state as finished
    pub fn finished(&self, msg_id: &MsgId) -> bool {
        if let Some((_ts, msg_state)) = self.get(&msg_id) {
            msg_state.finished()
        } else {
            // if the msg doesn't exist, consider it finished
            true
        }
    }
}

/// This stores a queue of all available messages for a given Task by name.
#[derive(Debug, Default)]
pub struct PipelineAvailable {
    /// The queue of available messages by task name
    pub(crate) queue: HashMap<String, VecDeque<TaskMsg>>,
}

impl PipelineAvailable {
    // pre-initialize the hashmap key queues
    pub fn new(task_names: Vec<String>) -> Self {
        let mut this = Self::default();
        for name in task_names {
            this.queue.insert(name, VecDeque::new());
        }
        this
    }

    /// Returns the current queue length
    pub fn len(&mut self, name: &String) -> usize {
        return self.queue.get(name).map_or(0usize, |q| q.len());
    }

    /// Pops n-count of tasks from the front of the queue
    pub(crate) fn pop(&mut self, name: &String, count: Option<usize>) -> Option<Vec<TaskMsg>> {
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

    /// Pushes a task to the end of the queue
    pub(crate) fn push(&mut self, name: &String, msg: TaskMsg) -> Option<usize> {
        return self.queue.get_mut(name).map_or(None, |q| {
            q.push_back(msg);
            Some(q.len())
        });
    }

    pub fn stats(&mut self) -> HashMap<String, isize> {
        let mut stats = HashMap::new();
        let _ = self.queue.iter().map(|(k, q)| {
            stats.insert(k.to_string(), q.len() as isize);
        });
        stats
    }
}

// We store all these so they can be released
// at one time into the next set of downstream descendant tasks

/// This is a holding pen that aggregates edge messages
/// returned from a `TaskService`.
#[derive(Default, Debug)]
pub struct PipelineAggregate {
    /// A map of aggregated messages by task name
    holding: HashMap<String, HashMap<MsgId, Vec<Msg>>>,
}

impl PipelineAggregate {
    pub fn new(task_names: Vec<String>) -> Self {
        let mut this = Self::default();
        for name in task_names {
            this.holding.insert(name, HashMap::new());
        }
        this
    }

    /// Hold this msg for this task_name, msg_id, msg
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

    /// Returns all messages for this task_name and msg_id
    /// for processing
    pub fn remove(&mut self, name: &String, msg_id: &MsgId) -> Option<Vec<Msg>> {
        return self
            .holding
            .get_mut(name)
            .map_or(None, |map| map.remove(msg_id));
    }

    pub fn clean_msg_id(&mut self, msg_id: &MsgId) {
        for (_k, map) in self.holding.iter_mut() {
            map.remove(msg_id);
        }
    }

    pub fn stats(&mut self) -> HashMap<String, isize> {
        let mut stats = HashMap::new();
        let _ = self.holding.iter().map(|(k, hold)| {
            stats.insert(k.to_string(), hold.len() as isize);
        });
        stats
    }
}

#[cfg(test)]
mod tests {

    use actix::*;
    use tempest_source::prelude::SourceMsg;

    use crate::common::now_millis;
    use crate::metric::Metrics;
    use crate::pipeline::*;
    use crate::task;
    use crate::topology::{PipelineActor, TaskResponse};

    static T1: &'static str = "T1";
    static T2: &'static str = "T2";
    static T3: &'static str = "T3";
    static T4: &'static str = "T4";
    static T5: &'static str = "T5";
    static T6: &'static str = "T6";
    static T7: &'static str = "T7";

    fn get_source_msg(id: u8) -> SourceMsg {
        SourceMsg {
            id: vec![0, 0, id.clone()],
            msg: vec![id.clone()],
            ts: now_millis(),
            delivered: 0,
        }
    }

    fn get_pipeline() -> Pipeline {
        Pipeline::default()
            .task(T1::default())
            .task(T2::default())
            .task(T3::default())
            .task(T4::default())
            .task(T5::default())
            .task(T6::default())
            .task(T7::default())
            .edge(T1, T2)
            .edge(T1, T3)
            .edge(T2, T4)
            .edge(T3, T4)
            .edge(T4, T5)
            .edge(T4, T6)
            .edge(T5, T6)
            .edge(T5, T7)
            .build()
    }

    #[derive(Default, Debug)]
    pub struct T1 {}
    impl task::Task for T1 {
        fn name(&self) -> &'static str {
            T1
        }
    }

    #[derive(Default, Debug)]
    pub struct T2 {}
    impl task::Task for T2 {
        fn name(&self) -> &'static str {
            T2
        }
    }

    #[derive(Default, Debug)]
    pub struct T3 {}
    impl task::Task for T3 {
        fn name(&self) -> &'static str {
            T3
        }
    }

    #[derive(Default, Debug)]
    pub struct T4 {}
    impl task::Task for T4 {
        fn name(&self) -> &'static str {
            T4
        }
    }

    #[derive(Default, Debug)]
    pub struct T5 {}
    impl task::Task for T5 {
        fn name(&self) -> &'static str {
            T5
        }
    }

    #[derive(Default, Debug)]
    pub struct T6 {}
    impl task::Task for T6 {
        fn name(&self) -> &'static str {
            T6
        }
    }

    #[derive(Default, Debug)]
    pub struct T7 {}
    impl task::Task for T7 {
        fn name(&self) -> &'static str {
            T7
        }
    }

    #[derive(Default, Debug)]
    pub struct TaskRoot {}
    impl task::Task for TaskRoot {
        fn name(&self) -> &'static str {
            "root"
        }
    }

    #[test]
    #[should_panic]
    fn test_pipeline_cycle() {
        let _ = Pipeline::default()
            .task(T1::default())
            .task(T2::default())
            .task(T3::default())
            .edge(T1, T2)
            .edge(T2, T3)
            .edge(T3, T1)
            .build();
    }

    #[test]
    #[should_panic]
    fn test_empty_pipeline() {
        let _ = Pipeline::default().build();
    }

    #[test]
    #[should_panic]
    fn test_pipeline_task_root() {
        let _ = Pipeline::default().task(TaskRoot::default()).build();
    }

    #[test]
    #[should_panic]
    fn test_pipeline_same_edge() {
        let _ = Pipeline::default()
            .task(T1::default())
            .task(T2::default())
            .edge(T1, T1)
            .build();
    }

    #[test]
    #[should_panic]
    fn test_pipeline_undefined_task_edge() {
        let _ = Pipeline::default()
            .task(T1::default())
            .task(T2::default())
            .edge("Y1", "Y2")
            .build();
    }

    #[test]
    fn test_pipeline() {
        // simulate messages being sent through the PipelineActor
        // define the pipeline

        let _ = System::new("Task");

        let pipeline = get_pipeline();

        // println!("{:#?}", &pipeline);
        let mut pipeline_act = PipelineActor {
            pipeline: pipeline.runtime(),
            inflight: PipelineInflight::new(None),
            available: PipelineAvailable::new(pipeline.names()),
            aggregate: PipelineAggregate::new(pipeline.names()),
            metrics: Metrics::default().named(vec!["pipeline"]),
        };

        // define source msg
        let src_msg = get_source_msg(0);
        let src_msg_copy = src_msg.clone();

        // start task root
        pipeline_act.task_root(src_msg_copy);

        let result = pipeline_act.inflight.get(&vec![0, 0, 0]);
        assert_eq!(result.is_some(), true);

        if let Some((_ts, msg_state)) = result {
            println!("{:?}", msg_state);

            let pending = msg_state.get(&("root".to_owned(), T1.to_owned()));
            assert_eq!(pending.is_some(), true);
            assert_eq!(pending.unwrap().len(), 1);
        };

        // we should have one message available on t1
        assert_eq!(pipeline_act.available.len(&T1.to_owned()), 1usize);

        // now that the root task is pending
        // we need to simulate TaskResponse

        let task_resp = TaskResponse::Ack(
            src_msg.id.clone(),
            ("root".to_string(), T1.to_string()),
            0,
            Some(vec![vec![1], vec![2]]),
        );

        pipeline_act.task_ack(task_resp);

        // t1, t2
        for index in 0..2 {
            let task_resp = TaskResponse::Ack(
                src_msg.id.clone(),
                (T1.to_string(), T2.to_string()),
                index,
                Some(vec![vec![1], vec![2]]),
            );
            pipeline_act.task_ack(task_resp);
        }

        // t1, t3
        for index in 0..2 {
            let task_resp = TaskResponse::Ack(
                src_msg.id.clone(),
                (T1.to_string(), T3.to_string()),
                index,
                Some(vec![vec![1], vec![2], vec![3]]),
            );
            pipeline_act.task_ack(task_resp);
        }

        // t2, t4
        for index in 0..4 {
            let task_resp = TaskResponse::Ack(
                src_msg.id.clone(),
                (T2.to_string(), T4.to_string()),
                index,
                None,
            );
            pipeline_act.task_ack(task_resp);
        }

        // t2, t4
        for index in 0..4 {
            let task_resp = TaskResponse::Ack(
                src_msg.id.clone(),
                (T2.to_string(), T4.to_string()),
                index,
                None,
            );
            pipeline_act.task_ack(task_resp);
        }

        // t3, t4
        for index in 0..6 {
            let task_resp = TaskResponse::Ack(
                src_msg.id.clone(),
                (T3.to_string(), T4.to_string()),
                index,
                None,
            );
            pipeline_act.task_ack(task_resp);
        }

        // println!("{:?}", pipeline_act.inflight);
        // println!("{:?}", pipeline_act.aggregate);
        // println!("{:?}", pipeline_act.available);
        // println!("{:#?}", &pipeline_act.inflight.get(&src_msg.id));
        // println!("{}", &pipeline_act.pipeline.to_graphviz());
    }
}
