# Tempest

Tempest is a message handling framework written in Rust and inspired by Apache Storm (hence the name).

Similar to Storm, this framework constructs `Topologies` for processing messages polled from a `Source` (i.e. Spouts).

Topologies are constructed as `Pipelines` (directed acyclic graphs) of `Tasks` (i.e. Bolts).

# Definitions

## Topology

In order to define a topology, you need to define three basic components:

- Name: All topologies have a name.
- Pipeline: All topologies must define a graph of tasks for handling messages.
- Source: All topologies must know where to read messages from.

A topology should be configurable with a few rules:

- A topology should have a failure policy for defining how message failure should be processed (best effort, retry, or none).

- A topology is packaged as a single binary, which runs the Topology and all it's Task instances as standalone processes.

## Pipeline

A pipeline is a data structure which links tasks together in the form of a DAG. Some basic rules are as follows:

- A pipeline can have a single task (i.e. no edges) which is the root task.
- A pipeline can define task edges if more than one task exists.
- A pipeline acknowledges a source message as a success if it generated no task errors.
- A pipeline may define a message timeout for automatically moving messages into an error state if they haven't been acknowledged by a certain time after being delivered.

## Task

A task is a data structure which implements the `Task` trait and defines the following rules:

- A task must implement a handle function that takes a byte vector as input and returns a `TaskResult`
- A `TaskResult` (`Result<Option<Vec<Msg>>, TaskError>`) is sent back to the `TopologyService` and acknowledges handling completion.
- A `TaskResult` may contain the original message or new messages for handling by descendent pipeline tasks.
- A `TaskResult` may contain a None response which means we ack the message but nothing else should happen downstream for this original message.
- A `TaskResult` may return an `TaskError`.

## Source

A source is a data structure which implements the `Source` trait and defines the following rules:

- A source must implement a `poll` method which returns a vector of byte vector messages.
- A source may implement an `ack` or `batch_ack` method for properly acknowledging a message was delivered.

# Services

Under the hood, Tempest is series of `Actix` services broken up into smaller components.

- `TopologyService`: Runs an individual topology as a standalone process. Responsible for pulling messages from a source and tracking state as they move through a pipeline.

- `TaskService`: Runs a single topology task as a standalone process. Responsible for executing task handling code pulled from `TopologyService`.

Each service has different levels of configuration:

- Process command line arguments: features a small sub-set of over-rideable runtime settings.
- `TopologyBuilder` options: compiled configuration options
- `Topology.toml` configuration: Runtime configuration for topology, tasks, source, and metrics.

# Runtime

Tempest has a few different runtime configurations.

- `Standalone`: Runs a topology and all it's tasks as a single, multi-threaded process. Not really meant for production as the tasks communicate via TCP (ideally they would use mpsc channels, instead).

- `Topology`: Runs a single topology process which brokers source messages for tasks requesting work.

- `Task`: Runs a single topology task process which handles task messages.

- `Test`: Similar to `Standalone` but configures an additional `Agent` thread for collecting metric aggregations to facilitate unit testing.

# Metrics

A topology can define metric targets for sending application metrics to some backend:

- Statsd
- Prometheus
- File
- Log
- Console (stdout)

# Cli

Tempest should eventually ship with a cli for managing various actions.

Some ideas:

- Skeleton generation, topology, and tasks, etc.
- Starting/stopping a topology.
- Deploying topology and tasks.

See this for installing a the project from a git repo
https://github.com/ashleygwilliams/cargo-generate
