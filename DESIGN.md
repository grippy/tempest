# Tempest

Tempest is a distributed pipeline framework that draws inspiration
from Storm.

## Project

A tempest project is similar to a Cargo workspace.

### Configuration

At the root of your project is file called `Project.toml`.
This file details where we can find all our member configuration files.
The project file is used as the basis of the `build.rs` process.

Example `Project.toml`:
```
[project]
members = [
    "project/project_a.toml"
]

```
Member configuration files group topologies together.

Example member configuration:
```
# TODO: consider naming this `cluster`
[project]
name = "xyz"
description = "super awesome streams go here"

[[topology]]
mod = "topology::TopologyX"

[[topology]]
mod = "topology::TopologyY"

```

### Structure

A project structure might look something like this:

```
/my_tempest
    /projects
        project_1.toml
        project_2.toml

    /builds
        /**
            TBD if we can do this...
            this folder is auto-generated
            and it contains wrappers for generating
            project topology and task binaries
        */

    /src
        /topology
            mod.rs
            topology_x.rs
            topology_y.rs
        /tests
            test_tasks.rs
    lib.rs
    build.rs
    Project.toml
    Cargo.toml
```

Aside from the `Project.toml` the end-user is free to layout the project however they like.

### Builds

The `build.rs` file maps the `Project.toml` members and creates topology and task binaries.
Project member binaries are run on agent nodes. They provide a cli wrapper for initializing Topology and Task Services.

Inside the target directory there should be binaries for running each
topology and task service by name.

```
/target
    /build
        my-project-task-name-1-{build_version}
        my-project-task-name-2-{build_version}

```

## Director

A director is the main service responsible for communicating with agent services.

### Service

A director is a web service at the core which features the following actions:

- Topology Stats
- Topology Actions (shutdown/start/reload)
- Topology Logs?

### Redis

Redis is used to coordinate all meta details within a tempest installation.

- Active services and ip:host
- Service stats

## Agent

An agent runs a service which handles node topology coordination.

### Service

- Runs topology services
- Runs topology task services

## Topology

This service runs as a standalone process
which binds to a port as a tcp server

### Service

The TCP server handles the following actions:

- Shutdown
- Stats
- Task Heartbeats
- Task Queue

The structure looks like this:

- Configuration
    - Tasks
    - Graph
    - Source
- SourceActor
- PipelineActor
- AckActor

## Task

This service runs a task actor as a standalone process
as a tcp server

### Service

This service has the following structure
- Configuration
    - Topology ip:port

- TaskActor
    - Should implement Serde
    - Results are either Self::Message or Vec<Self::Message>

## Pipeline

Can we define a Pipeline format which turns actors into a DAG?
This might be difficult to do in code.

We could do this with string configs similar to v1_v2 prototype
and wire this up in the build.rs process.

TaskServices with with Vec<M> responses so map the output into the
next tasks input.

### DAG Config?

Can we define dags in configuration format?

Mermaid has some nice features we could also use to auto-generate
flowcharts for each topology.

```
[[topology.x]]
    [[task]]
    name = "A"
    type = "tasks::T1"
    [[task]]
    name = "B"
    type = "tasks::T2"
    [[task]]
    name = "C"
    type = "tasks::T3"
    [[task]]
    name = "D"
    type = "tasks::T4"

graph = """
    A --> B;
    A --> C;
    B --> D;
    C --> D;
"""
```

## Serde

Sending messages between Topology and Task Services
should be done with a simple structure:

```
{
    topology_id,
    msg_id,
    edge,
    msg << user defined msg here
}
```

The messages should be compact and backwards compatible for changes.

The `msg` property should have it's own pass at serialization so it can
be loaded as the Task Service message type.

## Runtime Configuration

A project should have a runtime configuration file.
This needs to be broken down by topology and should provide config sections for
for each type of topology actor.

```
[project]

tempest_home = "/.tempest

[[project.a]]

    [[topology.x]]
        [message]
        timeout = 5000
        retry = 3

        [source]
        type = "sources::RedisStream"
        uri = "redis://127.0.0.1/0
        key = "stream.key.1"
        group = "filter_abc"

```

Take a look at this library for defining env, cli args, or files overrides
https://github.com/mehcode/config-rs

StructOpt
https://docs.rs/structopt/0.2.15/structopt/


## Node Directory Structure

Each tempest node (director, agent) should have the same directory layout using the TEMPEST_HOME variable as the root.

The directory layout is something like this:

```
~/.tempest
    /bin
    /config
```

# Cli

Tempest should ship with a cli for managing various actions around a running tempest installation.

Some ideas:

- Skeleton generation for base project, topology, and tasks, etc.
- Starting/stopping a topology.
- Deploying topology and tasks.

See this for installing a the project from a git repo
https://github.com/ashleygwilliams/cargo-generate


# Stats

We have a bunch of stats we need to collect along the way...

We need to track the following...

Source:
- pending
- errors
- acked

Topology: ???

Pipeline
- inflight by task name
- available by task name
- aggregate by task name

This library is used to wrap methods for tracking stats (inflight, throughput, hit counts, error counts, etc)
https://crates.io/crates/metered

Stats collector (statsd, prometheus)
https://github.com/fralalonde/dipstick


# General Process Manager

In addition to running topology, tasks, and agents, could we come up with a framework
for running independent agents?

A independent agent is just a process that loops. What it does internally isn't part of the Tempest framework.
This would give someone the flexibility to create workers/process monitors, etc.

https://github.com/ccqpein/supervisor-rs
https://github.com/fafhrd91/fectl
https://github.com/pmarino90/igniter
https://github.com/mopemope/firestarter
https://github.com/andrewchambers/orderly
https://github.com/wecohere/loom
