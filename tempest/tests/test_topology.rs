// use actix::prelude::*;

// use super::task;

// use super::{
//     Msg, Pipeline, SourceBuilder, Task, TaskIngress, TaskOptions, Topology, TopologyBuilder,
//     TopologyOptions,
// };

// use super::source::{RedisStreamSource, RedisStreamSourceBuilder};

// use crate::service::task::TaskService;
// use crate::service::topology::TopologyService;

// struct MyTopology {}

// impl<'a> Topology<RedisStreamSourceBuilder<'a>> for MyTopology {
//     fn builder() -> TopologyBuilder<RedisStreamSourceBuilder<'a>> {
//         TopologyBuilder::default()
//             .options(TopologyOptions::default().name("MyTopology"))
//             .pipeline(
//                 Pipeline::default()
//                     .add(Task::new("t1", "topology::tests::T1").workers(2))
//                     .add(
//                         Task::default()
//                             .name("t2")
//                             .path("topology::tests::T2")
//                             .options(TaskOptions::default().workers(3)),
//                     )
//                     .add(Task::new("t3", "topology::tests::T3"))
//                     .add(
//                         Task::new("t4", "topology::tests::T4")
//                             .workers(20)
//                     )
//                     .edge("t1", "t2")
//                     .edge("t1", "t3")
//                     .edge("t2", "t4")
//                     .edge("t3", "t4"),
//             )
//             .source(
//                 RedisStreamSourceBuilder::default()
//                     .uri("redis://127.0.0.1/0")
//                     .key("mystream")
//                     .group("super"),
//             )
//     }
// }

// pub struct MyMsg {}

// #[derive(Default, Debug)]
// pub struct T1 {}
// impl task::Task for T1 {
//     type Msg = MyMsg;

//     // Convert Vec<u8> to MyMsg
//     fn deser(&mut self, msg: Msg) -> Self::Msg {
//         // convert Msg to Self::Msg
//         MyMsg {}
//     }

//     fn handle(&mut self, msg: Self::Msg) -> task::TaskResult<Option<Vec<Msg>>> {
//         Ok(None)
//     }
// }

// #[derive(Default, Debug)]
// pub struct T2 {}
// impl task::Task for T2 {
//     type Msg = Msg;

//     fn deser(&mut self, msg: Msg) -> Self::Msg {
//         msg
//     }

//     fn handle(&mut self, msg: Self::Msg) -> task::TaskResult<Option<Vec<Msg>>> {
//         Ok(None)
//     }
// }

// #[derive(Default, Debug)]
// pub struct T3 {}
// impl task::Task for T3 {
//     type Msg = Msg;

//     fn deser(&mut self, msg: Msg) -> Self::Msg {
//         msg
//     }

//     fn handle(&mut self, msg: Self::Msg) -> task::TaskResult<Option<Vec<Msg>>> {
//         Ok(None)
//     }
// }

// #[derive(Default, Debug)]
// pub struct T4 {}
// impl task::Task for T4 {
//     type Msg = Msg;

//     fn deser(&mut self, msg: Msg) -> Self::Msg {
//         msg
//     }

//     fn handle(&mut self, msg: Self::Msg) -> task::TaskResult<Option<Vec<Msg>>> {
//         Ok(None)
//     }
// }

// #[test]
// fn test_topology_builder() {
//     let builder = MyTopology::builder();
//     // println!("{:?}", &builder);

//     let topology = &builder.build_topology_actor();
//     // println!("{:?}", &topology);

//     // let source= &builder.build_source_actor();
//     let pipeline = &builder.build_pipeline_actor();
//     println!("{:#?}", &pipeline.pipeline);
// }

// #[test]
// fn test_my_topology<'a>() {
//     TopologyService::run(MyTopology::builder);
// }

// #[test]
// fn test_my_task<'a>() {
//     TaskService::run(T1::default);
// }
