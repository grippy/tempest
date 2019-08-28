// use tempest::prelude::*;
// use tempest::rt::test;

// mod topology;

// #[test]
// fn simple() {
//     topology::setup();

//     let builder = topology::simple::SimpleTopology::test_builder;
//     let metrics = test::TestRun::new(builder)
//         .duration_secs(5)
//         .graceful_shutdown_secs(5)
//         .agent_port(6336)
//         .run();
//     println!("{:?}", &metrics);
//     assert_eq!(
//         metrics.get("SimpleTopology.source.msg.read"),
//         Some(&1000isize)
//     );
//     assert_eq!(
//         metrics.get("SimpleTopology.source.msg.acked.success"),
//         Some(&1000isize)
//     );
//     assert_eq!(
//         metrics.get("SimpleTopology.topology.msg.moved"),
//         Some(&1000isize)
//     );
//     assert_eq!(
//         metrics.get("SimpleTopology.topology.source.msg.ack"),
//         Some(&1000isize)
//     );
//     assert_eq!(
//         metrics.get("SimpleTopology.pipeline.task.available"),
//         Some(&1000isize)
//     );
//     assert_eq!(
//         metrics.get("SimpleTopology.T1.task.msg.outflow"),
//         Some(&1000isize)
//     );
// }

// #[test]
// fn error_prone() {
//     topology::setup();

//     let builder = topology::error_prone::ErrorTopology::test_builder;
//     let metrics = test::TestRun::new(builder)
//         .duration_secs(5)
//         .graceful_shutdown_secs(5)
//         .run();
//     println!("{:?}", &metrics);
//     assert_eq!(
//         metrics.get("ErrorTopology.source.msg.read"),
//         Some(&1000isize)
//     );
//     assert_eq!(
//         metrics.get("ErrorTopology.source.msg.acked.success"),
//         Some(&500isize)
//     );
//     assert_eq!(
//         metrics.get("ErrorTopology.topology.source.msg.ack.dropped"),
//         Some(&500isize)
//     );
//     assert_eq!(
//         metrics.get("ErrorTopology.topology.msg.moved"),
//         Some(&1000isize)
//     );
//     assert_eq!(
//         metrics.get("ErrorTopology.topology.source.msg.ack"),
//         Some(&500isize)
//     );
//     assert_eq!(
//         metrics.get("ErrorTopology.topology.source.msg.error"),
//         Some(&500isize)
//     );
//     assert_eq!(
//         metrics.get("ErrorTopology.pipeline.task.available"),
//         Some(&1000isize)
//     );
//     assert_eq!(
//         metrics.get("ErrorTopology.T1.task.msg.outflow"),
//         Some(&500isize)
//     );

//     assert_eq!(
//         metrics.get("ErrorTopology.T1.task.msg.error"),
//         Some(&500isize)
//     );
// }

// #[test]
// fn failure_policy_retry() {
//     topology::setup();

//     let builder = topology::failure_policy_retry::RetryTopology::test_builder;
//     // retry interval is every 60s
//     let metrics = test::TestRun::new(builder)
//         .duration_secs(90)
//         .graceful_shutdown_secs(5)
//         .run();
//     println!("{:?}", &metrics);
//     assert_eq!(
//         metrics.get("RetryTopology.source.msg.read"),
//         Some(&1000isize)
//     );
//     assert_eq!(
//         metrics.get("RetryTopology.source.msg.acked.success"),
//         Some(&750isize)
//     );
//     assert_eq!(
//         metrics.get("RetryTopology.topology.msg.moved"),
//         Some(&1500isize)
//     );
//     assert_eq!(
//         metrics.get("RetryTopology.topology.source.msg.ack"),
//         Some(&750isize)
//     );
//     assert_eq!(
//         metrics.get("RetryTopology.topology.source.msg.error"),
//         Some(&750isize)
//     );
//     assert_eq!(
//         metrics.get("RetryTopology.pipeline.task.available"),
//         Some(&1500isize)
//     );
//     assert_eq!(
//         metrics.get("RetryTopology.T1.task.msg.outflow"),
//         Some(&750isize)
//     );
//     assert_eq!(
//         metrics.get("RetryTopology.T1.task.msg.error"),
//         Some(&750isize)
//     );
//     assert_eq!(
//         metrics.get("RetryTopology.topology.source.msg.retry.dropped"),
//         Some(&250isize)
//     );
//     assert_eq!(
//         metrics.get("RetryTopology.topology.source.msg.retry.queued"),
//         Some(&500isize)
//     );
// }

// #[test]
// fn failure_policy_none() {
//     topology::setup();

//     // Test TopologyFailurePolicy::None
//     // with 1% task result errors

//     let builder = topology::failure_policy_none::FailurePolicyNoneTopology::test_builder;
//     let metrics = test::TestRun::new(builder)
//         .duration_secs(5)
//         .graceful_shutdown_secs(5)
//         .run();
//     // println!("{:?}", &metrics);
//     assert_eq!(
//         metrics.get("FailurePolicyNoneTopology.source.msg.read"),
//         Some(&1000isize)
//     );
//     assert_eq!(
//         metrics.get("FailurePolicyNoneTopology.source.msg.acked.success"),
//         Some(&990isize)
//     );
//     assert_eq!(
//         metrics.get("FailurePolicyNoneTopology.topology.source.msg.ack.dropped"),
//         Some(&10isize)
//     );
//     assert_eq!(
//         metrics.get("FailurePolicyNoneTopology.topology.msg.moved"),
//         Some(&1000isize)
//     );
//     assert_eq!(
//         metrics.get("FailurePolicyNoneTopology.topology.source.msg.ack"),
//         Some(&990isize)
//     );
//     assert_eq!(
//         metrics.get("FailurePolicyNoneTopology.topology.source.msg.error"),
//         Some(&10isize)
//     );
//     assert_eq!(
//         metrics.get("FailurePolicyNoneTopology.pipeline.task.available"),
//         Some(&1000isize)
//     );
//     assert_eq!(
//         metrics.get("FailurePolicyNoneTopology.T1.task.msg.outflow"),
//         None
//     );
//     assert_eq!(
//         metrics.get("FailurePolicyNoneTopology.T1.task.msg.error"),
//         Some(&10isize)
//     );
// }
