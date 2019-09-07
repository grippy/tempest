# Pipeline

This data structure forms a directed acyclic graph of registered `Task` instances.

- [`Pipeline` docs](https://docs.rs/tempest/TEMPEST_VERSION/tempest/pipeline/struct.Pipeline.html)

## Example

Let's take a look at a basic Pipeline example with a single task.

```rust
use tempest::prelude::*;

#[derive(Default)]
struct T1 {}

impl task::Task for T1 {

    fn name(&self) -> &'static str {
        "T1"
    }

    fn handle(&mut self, _msg: Msg) -> task::TaskResult {
        // handle _msg here!
        // ...
        // Return an empty TaskResult
        Ok(None)
    }
}

fn main() {
    // The first task is turned into the "root" task
    let pipeline = Pipeline::default()
        .task(T1::default());
    assert_eq!(pipeline.len(), 1);
}

```

Next, let's take a look at a more complex pipeline example and talk about how messages move through a pipeline.