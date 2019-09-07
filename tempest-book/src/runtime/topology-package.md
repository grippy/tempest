# Topology Package

A topology package is defined as such:

- All code required to run a `Topology` and all its `Tasks` are compiled into a single binary package.

- A topology package is configurable at runtime via command line arguments or by providing `Topology.toml` to the run command.

# Runtime packaging

Once you've constructed a Topology, you just need to package it as a binary.

`Cargo.toml`

```toml
[package]
name = "my_topology"

[[bin]]
name = "my_topology"
path = "src/main.rs"

[dependencies]
pretty_env_logger = "0.3.0"
tempest = "0.1.0"
```

`main.rs`

```rust,ignore

use pretty_env_logger;
use tempese::prelude::*;

// Abbreviated topology builder

struct MyTopology {}

impl Topology<Source> for MyTopology {
    fn builder() -> TopologyBuilder<Source> {
        TopologyBuilder::default()
    }
}

fn main() {
    // Initialize logger
    pretty_env_logger::init();

    // Initiate the package run command
    rt::run(MyTopology::builder);
}

```