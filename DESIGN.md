# Tempest

Tempest is a message handling framework written in Rust and inspired by Apache Storm (hence the name).

Similar to Storm, this framework constructs `Topologies` for processing messages polled from a `Source` (i.e. Spouts).

Topologies are constructed as `Pipelines` (directed acyclic graphs) of `Tasks` (i.e. Bolts).

# Concepts

Terminology and concepts central to this project.

## Topology

In order to define a topology, you need to define three basic components:

- `Name`: All topologies have a name.
- `Pipeline`: All topologies must define a graph of tasks for handling messages.
- `Source`: All topologies must know where to read messages from.

A topology should be configurable with a few rules:

- A topology should have a failure policy for defining how message failures should be processed (best effort, retry, or none).

- A topology is packaged as a single binary, which runs the Topology and all it's Task instances as standalone processes.

## Pipeline

A pipeline is a data structure which links tasks together in the form of a DAG. Some basic rules are as follows:

- A pipeline can have a single task (i.e. no edges).
- A pipeline can define task edges if more than one task exists.
- A pipeline acknowledges a source message as a success if it generated no task errors.
- A pipeline may define a message timeout for automatically moving messages into an error state if they haven't been acknowledged by a certain time after being delivered.

## Task

A task is a data structure, responsible for handling messages, which implements the `Task` trait and defines the following rules:

- A task must implement a handle function that takes a byte vector as input and returns a `TaskResult`
- A `TaskResult` (`Result<Option<Vec<Msg>>, TaskError>`) is sent back to the `TopologyService` as an acknowledgement of completion.
- A `TaskResult` may contain the original message or generate new messages for handling by downstream pipeline tasks.
- A `TaskResult` may contain a None response which means we ack the message and nothing else should happen downstream for this original message.
- A `TaskResult` may return an `TaskError`.

## Source

A source is a data structure, responsible for reading messages from a broker, which implements the `Source` trait and meets the following rules:

- A source must implement a `poll` method which returns a vector of byte vector messages.
- A source may implement config parsing for converting `source.config` sections from `Topology.toml`.
- A source may implement an `ack` or `batch_ack` method for properly acknowledging a message was delivered.
- A source may implement other methods.

# Services

Under the hood, Tempest is collection of `Actix` services broken up into smaller components.

- `TopologyService`: Runs an individual topology as a standalone process. Responsible for pulling messages from a source and tracking state as they move through a pipeline. A topology is a TCP server which binds to a port.

- `TaskService`: Runs a single topology task as a standalone process. Responsible for polling its `TopologyService` and handling messages.

- `AgentService`: Runs as a standalone process on a single network node within a cluster and is responsible for launching and killing topologies/tasks and metric aggregation. (`Status: implementation TBD`)

- `DirectorService`: Web UI view of all running topologies within a cluster of network nodes. Aggregates `AgentService` stats and features controls for uploading and deploying new Topology packages. (`Status: implementation TBD`)

# Topology Package

A package is a compiled binary which bundles all the code required to run a topology and its tasks.

# Service Configuration

Each service has different levels of configuration:

- Package command line arguments: features a small sub-set of over-rideable runtime settings.
- `TopologyBuilder` options: compiled configuration options
- `Topology.toml` configuration: Runtime configuration for topology, tasks, source, and metrics.
- `Cluster.toml` configuration: Runtime configuration for running a cluster (director and agent services). (`Status: implementation TBD`)

# Topology Runtimes

Tempest has a few different runtime configurations for running topologies.

- `Standalone`: Runs a topology and all its tasks as a single, multi-threaded process. Not really meant for production as the tasks communicate via TCP (ideally they would use mpsc channels, instead).

- `Topology`: Runs a single topology process which brokers source messages for tasks requesting work.

- `Task`: Runs a single topology task process which handles task messages.

- `Test`: Similar to `Standalone` but configures an additional `Agent` thread for collecting metric aggregations to facilitate unit testing.

# Metrics

A topology can define metric targets for sending service metrics various backends:

- Statsd
- Prometheus
- File
- Log
- Console (stdout)

# Cli

Tempest cli is used for managing various actions. (`Status: implementation TBD`)

- Cluster configuration
- Scaffolding: topology, and tasks, source, etc.
- Starting/stopping a topology.
- Deploying topology and tasks.
