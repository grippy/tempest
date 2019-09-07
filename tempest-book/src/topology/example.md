# Topology Example

Ok, now that we've covered all the building blocks, it's finally time to show a Topology example.

```rust
use tempest::prelude::*;
use tempest_source_mock::prelude::*;

// First, we define a SourceBuilder type alias
// MockSourceBuilder is used for testing-purposes only
type Source = MockSourceBuilder;

// Next, define a pipeline Task
static T1: &'static str = "T1";

#[derive(Default)]
pub struct T1 {}

impl task::Task for T1 {
    fn name(&self) -> &'static str {
        T1
    }

    fn handle(&mut self, _msg: Msg) -> task::TaskResult {
        // Msg handling code goes here...
        // Ack msg with an empty response
        Ok(None)
    }
}

// Next, define the topology

struct MyTopology {}

impl Topology<Source> for MyTopology {

    // This is what our "production" topology might look like
    fn builder() -> TopologyBuilder<Source> {
        TopologyBuilder::default()
            .name("MyTopology")
            .pipeline(
                // Pipeline constructor
                Pipeline::default()
                    .task(T1::default())
            )
            .source(
                // Source constructor
                Source::default()
                    .read_msg_count(10)
            )
    }

    // This is a special test_builder configured for testing
    fn test_builder() -> TopologyBuilder<Source> {
        TopologyBuilder::default()
            .name("MyTopology")
            .pipeline(
                // Pipeline constructor
                Pipeline::default()
                    .task(T1::default())
            )
            .source(
                // Source constructor
                Source::default()
                    .read_msg_count(10)
                    .prime(|mock| {
                        // hydrate the mock queue with 1000 test messages
                        for i in 0u32..1000u32 {
                            let msg = SourceMsg {
                                id: i.to_string().as_bytes().to_vec(),
                                msg: format!("{}", i).as_bytes().to_vec(),
                                ts: now_millis(),
                                delivered: 0,
                            };
                            mock.queue.push_back(msg);
                        }
                    })
            )
    }
}

fn main() {

    // Take the test_builder example
    // and fire off a TestRun...

    let metrics = rt::test::TestRun::new(MyTopology::test_builder)
        .duration_secs(5)
        .graceful_shutdown_secs(5)
        .run();

    // Write assertions using the test run metrics

    // Did we read messages from the source?
    assert_eq!(
        metrics.get("MyTopology.source.msg.read"),
        Some(&1000isize)
    );

    // How many messages were handled?
    assert_eq!(
        metrics.get("MyTopology.T1.task.msg.handle"),
        Some(&1000isize)
    );

    // How many messages were acked at the source?
    assert_eq!(
        metrics.get("MyTopology.source.msg.acked.success"),
        Some(&1000isize)
    );

}

```