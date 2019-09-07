# Tempest Source for Mock Queues

This source implements a basic queue for mocking message polling. Most `tempest` topology tests use this `Source`

## Install

`Cargo.toml`

```
[dependencies]
tempest-source-mock = "0.1.0"
```

## Usage

```rust
use tempest_source_mock::prelude::*;

// create a type alias to the MockSourceBuilder
type Source = MockSourceBuilder;

// configure a topology with this source
struct MyTopology {}
impl Topology<Source> for MyTopology {
    // implementation code here
}
```