# Tasks

Tasks are the main building blocks for all use-cases. They define implementation details for how to handle a message. Since tasks are structs, they can store state (counters, db connections, http clients, etc).

When a topology consumes messages from a source, it becomes a central broker for all its pipeline tasks. Tasks are just workers that ask for new messages.

Messages are brokered as byte vectors (`Vec<u8>`) so they may require additional work (de-serialization/transformation) to make them easier to work with.

# Configuration

- A task is configurable at runtime with cli args defined by `PackageOpt`
- A task is configurable with `Topology.toml` configuration.

[See Topology.toml for more details](./runtime/topology-toml)