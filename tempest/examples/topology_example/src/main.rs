#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use std::io;
use std::thread;
use std::time::Duration;

use tempest::prelude::*;
use tempest_source_redis::prelude::*;

use pretty_env_logger;

static TOPOLOGY_NAME: &'static str = "MyTopology";
static T1: &'static str = "T1";
static T2: &'static str = "T2";
static T3: &'static str = "T3";
static T4: &'static str = "T4";

// Define a source builder type
type Source = RedisStreamSourceBuilder<'static>;

struct MyTopology {}

impl Topology<Source> for MyTopology {
    fn builder() -> TopologyBuilder<Source> {
        TopologyBuilder::default()
            .name(TOPOLOGY_NAME)
            .pipeline(
                Pipeline::default()
                    .task(T1::default())
                    .task(T2::default())
                    .task(T3::default())
                    .task(T4::default())
                    .edge(T1, T2)
                    .edge(T1, T3)
                    .edge(T2, T4)
                    .edge(T3, T4),
            )
            .source(
                Source::default()
                    .uri("redis://127.0.0.1/0")
                    .key("mystream")
                    .group("super"),
            )
    }
}

#[derive(Default, Debug)]
pub struct T1 {}
impl task::Task for T1 {
    fn name(&self) -> &'static str {
        T1
    }

    fn handle(&mut self, msg: Msg) -> task::TaskResult {
        Ok(Some(vec![vec![1]]))
    }
}

#[derive(Default, Debug)]
pub struct T2 {}
impl task::Task for T2 {
    fn name(&self) -> &'static str {
        T2
    }

    fn handle(&mut self, msg: Msg) -> task::TaskResult {
        Ok(Some(vec![vec![1], vec![2]]))
    }
}

#[derive(Default, Debug)]
pub struct T3 {}
impl task::Task for T3 {
    fn name(&self) -> &'static str {
        T3
    }

    fn handle(&mut self, msg: Msg) -> task::TaskResult {
        Ok(Some(vec![vec![1], vec![2], vec![3]]))
    }
}

#[derive(Default, Debug)]
pub struct T4 {}
impl task::Task for T4 {
    fn name(&self) -> &'static str {
        T4
    }

    fn handle(&mut self, msg: Msg) -> task::TaskResult {
        Ok(Some(vec![vec![1], vec![2], vec![3], vec![4]]))
    }
}

// struct MyTopology2 {}

// impl<'a> Topology<RedisStreamSourceBuilder<'a>> for MyTopology2 {
//     fn builder() -> TopologyBuilder<RedisStreamSourceBuilder<'a>> {
//         TopologyBuilder::default()
//             .name("MyTopology2")
//             .pipeline(Pipeline::default().add(T5::default()))
//             .source(
//                 RedisStreamSourceBuilder::default()
//                     .uri("redis://127.0.0.1/0")
//                     .key("s2")
//                     .group("g2"),
//             )
//     }
// }

// #[derive(Default, Debug)]
// pub struct T5 {}
// impl task::Task for T5 {

//     fn name(&self) -> &'static str {
//         "T5"
//     }

//     fn handle(&mut self, msg: Msg) -> task::TaskResult {
//         // Ok(None)
//         let err = task::TaskError::from_error_kind(io::ErrorKind::InvalidInput);
//         Err(err)
//     }
// }

fn main() {
    pretty_env_logger::init();
    run(MyTopology::builder);
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempest::rt::test;

    fn setup() {
        pretty_env_logger::init();
    }

    #[test]
    fn test_my_topology_1() {
        setup();

        let metrics = test::TestRun::new(MyTopology::builder)
            .duration_secs(10)
            .graceful_shutdown_secs(5)
            .run();

        assert_eq!(metrics.get("MyTopology.source.msg.read"), Some(&1000isize));
        assert_eq!(
            metrics.get("MyTopology.source.msg.acked.success"),
            Some(&1000isize)
        );
        assert_eq!(
            metrics.get("MyTopology.T1.task.msg.outflow"),
            Some(&1000isize)
        );
        assert_eq!(
            metrics.get("MyTopology.T2.task.msg.outflow"),
            Some(&2000isize)
        );
        assert_eq!(
            metrics.get("MyTopology.T3.task.msg.outflow"),
            Some(&3000isize)
        );
        assert_eq!(
            metrics.get("MyTopology.T4.task.msg.outflow"),
            Some(&20000isize)
        );
    }
}
