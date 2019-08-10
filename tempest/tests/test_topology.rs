use tempest::prelude::*;
use tempest::rt::test;

mod topology;

#[test]
fn simple() {
    topology::setup();

    let builder = topology::simple::SimpleTopology::test_builder;
    let metrics = test::TestRun::new(builder)
        .duration_secs(5)
        .graceful_shutdown_secs(5)
        .agent_port(6336)
        .run();

    assert_eq!(
        metrics.get("SimpleTopology.source.msg.read"),
        Some(&1000isize)
    );
    assert_eq!(
        metrics.get("SimpleTopology.source.msg.acked.success"),
        Some(&1000isize)
    );
    assert_eq!(
        metrics.get("SimpleTopology.topology.msg.moved"),
        Some(&1000isize)
    );
    assert_eq!(
        metrics.get("SimpleTopology.topology.source.msg.ack"),
        Some(&1000isize)
    );
    assert_eq!(
        metrics.get("SimpleTopology.pipeline.task.available"),
        Some(&1000isize)
    );
    assert_eq!(
        metrics.get("SimpleTopology.T1.task.msg.outflow"),
        Some(&1000isize)
    );
}
