# Task Example

```rust
use tempest::prelude::*;

#[derive(Default)]
struct SomeTask {}

impl task::Task for SomeTask {

    fn name(&self) -> &'static str {
        "SomeTask"
    }

    fn handle(&mut self, _msg: Msg) -> task::TaskResult {
        Ok(None)
    }
}

fn main() {
    use tempest::task::Task; // required for this example
    let task = SomeTask::default();
    assert_eq!(task.name(), "SomeTask");
}

```