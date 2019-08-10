# TODO

- [ ] Task metrics
    - [] Add flush_metrics so tasks can use Metrics

- [ ] Task message deserialization

- [ ] Tests:
        Topology Errors
        - [ ] BestEffort
        - [ ] None
        - [ ] Retry

- [ ] RedisStreamSource
    - [ ] Pending handlers:
        - [x] Ack
        - [x] Claim
        - [x] Delete
        - [ ] Move to key
    - [ ] Configure prime test messages

- [ ] Handle Actix Errors
    - [ ] TopologyService
    - [ ] TopologySession
    - [ ] TopologyActor
    - [ ] SourceActor
    - [ ] PipelineActor
    - [ ] TaskService
    - [ ] TaskActor

- [ ] Source poll needs back off
        - [ ] TopologyActor Mailbox capacity is unlimited (should it be?)

- [ ] Task connection retries/back off
- [ ] TopologyBuilder metric configuration
- [ ] Aggregate metrics tmp file configuration
    - [ ] Add this to TestRun?

# Future

- [ ] Metrics Backend: Graphite (skip for now)
- [ ] Deadletter Option: This is just a different "SourceBuilder" that does a push instead of a poll?
- [ ] Cleanup all TODO's
- [ ] Cleanup all shit code
- [ ] Add documentation and more examples
- [ ] Add RedisListSource
- [ ] Add RedisSortedSetSource

## Tests

- [ ] Docker container for running tests?
- [ ] Unit tests
    - [ ] Needs more code coverage
    - [ ] Unit tests for PipelineActor.task_ack
    - [ ] Unit tests for PipelineInflight
    - [ ] Unit tests for PipelineAggregate
    - [ ] Unit tests for Pipeline DAG completion

# Tempest Project

- [ ] Make an empty topology

# Tempest Cli

- [ ] TBD

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