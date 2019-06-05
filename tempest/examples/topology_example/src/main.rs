#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use std::io;
use std::thread;
use std::time::Duration;

use tempest::prelude::*;
use tempest::StructOpt;
use tempest_source_redis::prelude::*;

struct MyTopology {}

impl<'a> Topology<RedisStreamSourceBuilder<'a>> for MyTopology {
    fn builder() -> TopologyBuilder<RedisStreamSourceBuilder<'a>> {
        TopologyBuilder::default()
            .name("MyTopology")
            .pipeline(
                Pipeline::default()
                    .add(Task::new("t1", "T1"))
                    .add(Task::default().name("t2").path("T2"))
                    .add(Task::new("t3", "T3"))
                    .add(Task::new("t4", "T4"))
                    .edge("t1", "t2")
                    .edge("t1", "t3")
                    .edge("t2", "t4")
                    .edge("t3", "t4"),
            )
            .source(
                RedisStreamSourceBuilder::default()
                    .uri("redis://127.0.0.1/0")
                    .key("mystream")
                    .group("super"),
            )
    }
}

pub struct MyMsg {}

#[derive(Default, Debug)]
pub struct T1 {}
impl task::Task for T1 {
    type Msg = MyMsg;

    // Convert Vec<u8> to MyMsg
    fn deser(&mut self, msg: Msg) -> Self::Msg {
        // convert Msg to Self::Msg
        MyMsg {}
    }

    fn handle(&mut self, msg: Self::Msg) -> task::TaskResult {
        Ok(Some(vec![vec![1]]))
    }
}

#[derive(Default, Debug)]
pub struct T2 {}
impl task::Task for T2 {
    type Msg = Msg;

    fn deser(&mut self, msg: Msg) -> Self::Msg {
        msg
    }

    fn handle(&mut self, msg: Self::Msg) -> task::TaskResult {
        Ok(Some(vec![vec![1], vec![2]]))
    }
}

#[derive(Default, Debug)]
pub struct T3 {}
impl task::Task for T3 {
    type Msg = Msg;

    fn deser(&mut self, msg: Msg) -> Self::Msg {
        msg
    }

    fn handle(&mut self, msg: Self::Msg) -> task::TaskResult {
        Ok(Some(vec![vec![1], vec![2], vec![3]]))
    }
}

#[derive(Default, Debug)]
pub struct T4 {}
impl task::Task for T4 {
    type Msg = Msg;

    fn deser(&mut self, msg: Msg) -> Self::Msg {
        msg
    }

    fn handle(&mut self, msg: Self::Msg) -> task::TaskResult {
        Ok(Some(vec![vec![1], vec![2], vec![3], vec![4]]))
    }
}

fn run_my_topology() {
    // give the topology server time to start
    let opts = PackageOpt::from_args();
    let topology_opt = opts.clone();
    let topology_service = thread::spawn(move || {
        TopologyService::run(topology_opt, MyTopology::builder);
    });
    let _ = thread::sleep(Duration::from_millis(500));
    let topology = MyTopology::builder();
    let mut workers = vec![topology_service];

    for (name, task) in topology.pipeline.tasks {
        let task_opts = opts.clone();

        let handle = thread::spawn(move || {
            let n = name.clone().to_string().to_owned();

            if name == "t1" {
                TaskService::run(n, task_opts.clone(), T1::default);
            } else if name == "t2" {
                TaskService::run(n, task_opts.clone(), T2::default);
            } else if name == "t3" {
                TaskService::run(n, task_opts.clone(), T3::default);
            } else if name == "t4" {
                TaskService::run(n, task_opts.clone(), T4::default);
            }
        });
        workers.push(handle);
    }

    for handle in workers {
        let _ = handle.join();
    }
}

struct MyTopology2 {}

impl<'a> Topology<RedisStreamSourceBuilder<'a>> for MyTopology2 {
    fn builder() -> TopologyBuilder<RedisStreamSourceBuilder<'a>> {
        TopologyBuilder::default()
            .name("MyTopology2")
            .pipeline(Pipeline::default().add(Task::new("t5", "T5")))
            .source(
                RedisStreamSourceBuilder::default()
                    .uri("redis://127.0.0.1/0")
                    .key("s2")
                    .group("g2"),
            )
    }
}

#[derive(Default, Debug)]
pub struct T5 {}
impl task::Task for T5 {
    type Msg = Msg;

    fn deser(&mut self, msg: Msg) -> Self::Msg {
        msg
    }

    fn handle(&mut self, msg: Self::Msg) -> task::TaskResult {
        // Ok(None)
        let err = task::TaskError::from_error_kind(io::ErrorKind::InvalidInput);
        Err(err)
    }
}

fn run_my_topology2() {
    // give the topology server time to start
    let opts = PackageOpt::from_args();

    let topology_opt = opts.clone();
    let topology_service = thread::spawn(move || {
        TopologyService::run(topology_opt, MyTopology2::builder);
    });
    let _ = thread::sleep(Duration::from_millis(500));
    let topology = MyTopology2::builder();
    let mut workers = vec![topology_service];

    for (name, task) in topology.pipeline.tasks {
        let task_opts = opts.clone();
        let handle = thread::spawn(move || {
            let n = name.clone().to_string().to_owned();
            if name == "t5" {
                TaskService::run(n, task_opts, T5::default);
            }
        });
        workers.push(handle);
    }

    for handle in workers {
        let _ = handle.join();
    }
}

fn main() {
    run_my_topology();
    // run_my_topology2();
    println!("Shutting down workers...");
    println!("Done...");
}
