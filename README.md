# Tempest

Tempest is a message processing framework written in Rust and inspired by Apache Storm (hence the name).

## Motivation

- Take high-level Apache Storm concepts and port them to Rust.
- Learn more about processing Redis Streams.

## Design

Take a look at the [design doc](DESIGN.md) for the basic view of how Tempest is structured.

## Topology Example

A simple topology with a single task:

```rust
use tempest::prelude::*;
use tempest_source_redis::prelude::*;

// Source builder
type Source = RedisStreamSourceBuilder<'static>;

// Topology definition
struct MyTopology {}

impl Topology<Source> for MyTopology {
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
                    .uri("redis://127.0.0.1/0")
                    .key("some-stream")
                    .group("some-group"),
            )
    }
}

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

fn main() {
    // Run the topology package
    // The run command will parse all cli args
    rt::run(MyTopology::builder);
}
```

Take a look at the book (coming soon) for more examples.

# Pipelines

Defining pipelines, like the one above, is fairly straight-forward. Here's an example of a pipeline that has multiple edges:

Assume these tasks and constants are previously defined:

```rust
Pipeline::default()
    .task(T1::default())
    .task(T2::default())
    .task(T3::default())
    .task(T4::default())
    .task(T5::default())
    .edge(T1, T2)
    .edge(T1, T3)
    .edge(T2, T4)
    .edge(T3, T4)
    .edge(T4, T5)
```

Pipeline edges are how you link tasks.

# Sources

Right now, only two tempest sources exist:

- Redis Stream
- Mock

(Update w/ link after publishing crates).

# Source Trait

The source trait can be used to define your own structs for polling & acking message from a source.



# Disclaimers

- This is alpha and hasn't seen a production installation (yet).

- Many more things need to be worked on to make this a more complete framework.
    - Code is littered w/ TODO comments
    - Missing features:
        - `Web UI` for launching topologies, seeing stats, etc.
        - `Agent` for managing server installations and topology deployments.
        - `Cli` for interacting with topologies, creating new projects, etc.
