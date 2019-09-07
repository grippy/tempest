# TODO

- Book
    - [ ] Host this somewhere

- [ ] Remove unused crates.
- [ ] Move TODO list to Issues

- Task
    - [ ] async handler/filter
    - [ ] Task shutdown
    - [ ] Task message de-serialization

- RedisStreamSource
    - [ ] Move to key

- [ ] Handle Actix Errors
    - [ ] TopologyService
    - [ ] TopologySession
    - [ ] TopologyActor
    - [ ] SourceActor
    - [ ] PipelineActor
    - [ ] TaskService
    - [ ] TaskActor

- [ ] Health checks and backoffs aren't well defined
    - [ ] TopologyActor Mailbox capacity is unlimited (should it be?)
        - [ ] Topology needs a max_pending count for tracking inflight counts
            - [ ] Topology can just adjust the source.poll if this value is reached

    - [ ] Define what it means to be unhealthy and how to deal with it.
        - `source.healthy()` should return a boolean
        - N number of unheatlhy responses should shutdown the Topology

- [ ] Pipeline inflight sweeper
    We might need a periodic sweeper to evict stale messages
    For example: if a task requests a message and it dies before returning it,
    that message will be stuck inflight and will eventually lead to memory leaks


# Future

- [ ] Metrics Backend: Graphite (skip for now)
- [ ] Deadletter Option: This is just a different "SourceBuilder" that does a push instead of a poll?
- [ ] Cleanup all TODO's
- [ ] Cleanup all shit code
- [ ] Add more examples
- [ ] Add RedisListSource
- [ ] Aggregate metrics tmp file configuration
- [ ] Kafka Source
- [ ] AMQP Source
- [ ] Task workers
    - Define what this means and how we start them
- [ ] Task connection retries/back off

## Tests

- [ ] Unit tests
    - [ ] More code coverage

# Tempest Cli

- [ ] List available source crates
- [ ] Create project/topology

# Finished

- [x] PipelineActor TaskRequest should route through TopologyActor and then into PipelineActor.
      This change will start to centralize a place where we can record stats
        -> TopologySession -> TopologyActor -> PipelineActor

- [x] Refactor mega topology into sensible modules
- [x] Example topology so we can run multiple tasks as thread pools for testing
- [x] Source acking
        - [x] Batch msgs
        - [x] Individual msgs

- [x] SourceRedisStream acking:
        - [x] Batch msgs
        - [x] Individual msgs

- [x] Need to define a TaskError as being what we return from TaskResult

- [x] Rework source polling to use run_once w/ backoff vs using run_interval w/ backoff + time checks

- [x] Tempest Project: https://github.com/grippy/tempest_project

  - [x] skeleton
  - [x] build.rs
  - [x] Topology.toml

- [x] Tempest Project: https://github.com/grippy/tempest_project

  - [x] skeleton
  - [x] build.rs
  - [x] Topology.toml

- [x] Create git repo: https://github.com/grippy/tempest

- [x] Standalone server which spawns threads for each TaskService and TopologyService

- [x] Source
    - [x] Ack interval

- [x] Package Cli
    - [x] Task
    - [x] Topology
    - [x] Standalone
    - [x] Config from Topology.toml
        - [x] merges config options for topology, source, and task name

- [x] Logging facilities

- [x] Metrics
        Add metrics for all io and data moving through the system.
        Ideally, we'd have some type of uber actor responsible
        for aggregating stats sent to it from each backend target

        - [x] MetricsBackendActor (uber actor)
        - [x] Topology.toml configuration
        - [x] TopologyOption configuration
        - [x] Backend
                - [x] Flush/probe interval
                - [x] Timer & Histogram
                - [x] Statsd
                - [x] File
- [x] Pipeline
    - [x] Pipeline edge shouldn't allow the same task name as left and right
    - [x] prevent task cycles
    - [x] check for root task name
    - [x] check for task name exists for edges

- [x] Replace metric::ROOT lazy_static! w/ thread_local!
- [x] TopologyFailurePolicy
    - [x] None,
    - [x] BestEffort
    - [x] Retry(count)
        - [x] TopologyRetry mechanism (crude)

- [x] Topology Timeouts & Errors (for each TopologyFailurePolicy)
  - [x] PipelineMsg::Timeout
  - [x] PipelineMsg::Error

- [x] Runtime: Test
        - [x] MetricAggregatorActor
        - [x] TestRun builder
        - [x] Write/read AggregatedMetrics to tmp/
        - [x] Implement shutdown
                - [x] SIGTERM: Graceful + delay
                - [x] SIGINT: Ctrl+C

- Tasks
    - [x] Where do outflow messages end up if there are no more tasks?
        - We drop them
    - [x] Task filter
    - [x] Task metrics
        - [x] Add flush_metrics so tasks can use Metrics

- [x] Circle CI configuration

- [x] Docker container for running tests?
    - [x] Rust
    - [x] Redis

- [x] Multi-threaded cargo tests
    - [x] Add suffix to /tmp/aggregate-metrics to avoid it being overwritten

- RedisStreamSource
    - [x] Pending handlers:
        - [x] Ack
        - [x] Claim
        - [x] Delete
    - [x] Configure prime test messages

- Tests:
    Topology Errors
    - [x] BestEffort
    - [x] Retry
    - [x] None

- [x] Remove these statements
    - [x] #![allow(dead_code)]
    - [x] #![allow(unused_imports)]
    - [x] #![allow(unused_variables)]

- [x] Only make things pub where absolutely necessary
    - [x] determine what should be pub, pub(crate), private

- [x] Publish `redis-streams-rs` on crates.io
- [x] Publish `tempest` on crates.io
- [x] Publish `tempest-source-mock` on crates.io
- [x] Publish `tempest-source-redis` on crates.io
- [x] Source trait should be moved to it's own package. Currently, source implementations must have tempest as a dependency (which pulls in a lot of dependencies).

- [x] Documentation
    - [X] All code
    - [X] Book rough draft
    - [x] Build script and replace TEMPEST_VERSION & TEMPEST_SOURCE_VERSION

# Not possible
- Move metrics to two packages:
    Can't do this since both packages reference each other
        metric/mod.rs pulls the backend actor in the flush command
    - [ ] tempest-metrics: metrics/mod.rs
    - [ ] tempest-metrics-backend: All actix + backend code
