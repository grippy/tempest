// use std::sync::{Once, ONCE_INIT};

// use tempest::common::now_millis;
// use tempest::prelude::*;
// use tempest_source_mock::prelude::*;

// static INIT: Once = ONCE_INIT;

// pub fn setup() {
//     INIT.call_once(|| {
//         pretty_env_logger::init();
//     });
// }

// fn mock_seed(mock: &mut MockSource) {
//     for i in 0u32..1000u32 {
//         let msg = SourceMsg {
//             id: i.to_string().as_bytes().to_vec(),
//             msg: format!("{}", i).as_bytes().to_vec(),
//             ts: now_millis(),
//             delivered: 0,
//         };
//         mock.queue.push_back(msg);
//     }
// }

// pub mod simple {

//     use super::*;
//     static TOPOLOGY_NAME: &'static str = "SimpleTopology";
//     static T1: &'static str = "T1";

//     type Source = MockSourceBuilder;

//     pub struct SimpleTopology;

//     impl Topology<Source> for SimpleTopology {
//         fn builder() -> TopologyBuilder<Source> {
//             TopologyBuilder::default()
//                 .name(TOPOLOGY_NAME)
//                 .pipeline(Pipeline::default().task(T1::default()))
//                 .source(Source::default().read_msg_count(10))
//         }

//         fn test_builder() -> TopologyBuilder<Source> {
//             TopologyBuilder::default()
//                 .name(TOPOLOGY_NAME)
//                 .pipeline(Pipeline::default().task(T1::default()))
//                 .source(Source::default().read_msg_count(100).prime(mock_seed))
//         }
//     }

//     #[derive(Default, Debug)]
//     pub struct T1 {
//         counter: usize,
//     }
//     impl task::Task for T1 {
//         fn name(&self) -> &'static str {
//             T1
//         }

//         fn handle(&mut self, _msg: Msg) -> task::TaskResult {
//             Ok(Some(vec![vec![1]]))
//         }
//     }

// }

// pub mod error_prone {

//     use super::*;
//     static TOPOLOGY_NAME: &'static str = "ErrorTopology";
//     static T1: &'static str = "T1";

//     type Source = MockSourceBuilder;

//     pub struct ErrorTopology;

//     impl Topology<Source> for ErrorTopology {
//         fn builder() -> TopologyBuilder<Source> {
//             TopologyBuilder::default()
//                 .name(TOPOLOGY_NAME)
//                 .pipeline(Pipeline::default().task(T1::default()))
//                 .source(Source::default().read_msg_count(10))
//         }

//         fn test_builder() -> TopologyBuilder<Source> {
//             TopologyBuilder::default()
//                 .name(TOPOLOGY_NAME)
//                 .pipeline(Pipeline::default().task(T1::default()))
//                 .source(Source::default().read_msg_count(100).prime(mock_seed))
//         }
//     }

//     #[derive(Default, Debug)]
//     pub struct T1 {
//         counter: usize,
//     }
//     impl task::Task for T1 {
//         fn name(&self) -> &'static str {
//             T1
//         }

//         fn handle(&mut self, _msg: Msg) -> task::TaskResult {
//             let results = if self.counter % 2 == 0 {
//                 Ok(Some(vec![vec![1]]))
//             } else {
//                 Err(task::TaskError::custom("Bad day for this message"))
//             };
//             self.counter += 1;
//             results
//         }
//     }

// }

// pub mod failure_policy_retry {

//     use super::*;
//     static TOPOLOGY_NAME: &'static str = "RetryTopology";
//     static T1: &'static str = "T1";

//     type Source = MockSourceBuilder;

//     pub struct RetryTopology;

//     impl Topology<Source> for RetryTopology {
//         fn builder() -> TopologyBuilder<Source> {
//             RetryTopology::test_builder()
//         }

//         fn test_builder() -> TopologyBuilder<Source> {
//             TopologyBuilder::default()
//                 .name(TOPOLOGY_NAME)
//                 .pipeline(Pipeline::default().task(T1::default()))
//                 .source(Source::default().read_msg_count(100).prime(mock_seed))
//                 .failure_policy(TopologyFailurePolicy::Retry(1))
//         }
//     }

//     #[derive(Default, Debug)]
//     pub struct T1 {
//         counter: usize,
//     }
//     impl task::Task for T1 {
//         fn name(&self) -> &'static str {
//             T1
//         }

//         fn handle(&mut self, _msg: Msg) -> task::TaskResult {
//             let results = if self.counter % 2 == 0 {
//                 Ok(Some(vec![vec![1]]))
//             } else {
//                 Err(task::TaskError::custom("Bad day for this message"))
//             };
//             self.counter += 1;
//             results
//         }
//     }

// }

// pub mod failure_policy_none {

//     use super::*;
//     static TOPOLOGY_NAME: &'static str = "FailurePolicyNoneTopology";
//     static T1: &'static str = "T1";

//     type Source = MockSourceBuilder;

//     pub struct FailurePolicyNoneTopology;

//     impl Topology<Source> for FailurePolicyNoneTopology {
//         fn builder() -> TopologyBuilder<Source> {
//             FailurePolicyNoneTopology::test_builder()
//         }

//         fn test_builder() -> TopologyBuilder<Source> {
//             TopologyBuilder::default()
//                 .name(TOPOLOGY_NAME)
//                 .pipeline(Pipeline::default().task(T1::default()))
//                 .source(Source::default().read_msg_count(100).prime(mock_seed))
//                 .failure_policy(TopologyFailurePolicy::None)
//         }
//     }

//     #[derive(Default, Debug)]
//     pub struct T1 {
//         counter: usize,
//     }
//     impl task::Task for T1 {
//         fn name(&self) -> &'static str {
//             T1
//         }

//         fn handle(&mut self, _msg: Msg) -> task::TaskResult {
//             let results = if self.counter % 100 > 0 {
//                 Ok(None)
//             } else {
//                 Err(task::TaskError::custom("Bad day for this message"))
//             };
//             self.counter += 1;
//             results
//         }
//     }

// }
