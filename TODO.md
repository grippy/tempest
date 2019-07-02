# TODO

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
    - [x] Max pending
        - [ ] Backoff source poll (requires stats before we can do this)
    - [x] Ack interval

- [x] Package Cli
    - [x] Task
    - [x] Topology
    - [x] Standalone
    - [x] Config from Topology.toml
        - [x] merges config options for topology, source, and task name

- [x] Logging facilities

- [ ] Topology Timeouts & Errors:
  - [ ] PipelineMsg::Timeout
  - [ ] PipelineMsg::Error

        Should the topology define handlers for each type?
        What does a FailurePolicy::Retry mean and how is it processed?

        For a redis stream the failure policy is inacted after a timeout
        not during an error. So, this source should just skip acking
        for both types.

        For a redis queue the policy is only wired up after moving a msg into the deadletter queue
        but we would need to have some way to keep track of how many times a message
        has been retried. Where is this state stored or encapsulated?

- [ ] Metrics
        Add metrics for all io and data moving through the system.
        Ideally, we'd have some type of uber actor responsible
        for aggregating stats sent to it from each of the following

        - [x] Flush/probe interval
        - [x] Timer & Histogram
        - [x] Statsd
        - File

  - [x] MetricsBackendActor (uber actor)
  - [x] TopologyService
    - [x] TopologySession
    - [x] TopologyActor
    - [x] SourceActor
    - [x] PipelineActor
  - [x] TaskService
    - [x] TaskActor

- [ ] RedisStreamSource
      - [ ] Pending actor.
        This would be nice if the end-user could control how they want this to execute.
        At the minimum, the source needs a flag to initiate it.

  - [ ] The FailurePolicy would need to be Retry and we need to define a retry after millis value
  - [ ] What does claim do here? It should reclaim messages from the existing consumer and make them globally available again
  - [ ] Prime test messages

- [ ] Deadletter Option: This is just a different "SourceBuilder" that does a push instead of a poll?

- [ ] Nice-to-have: a retry for Task connections so we can test topology rebuilds

- [ ] Cleanup all shit code

- [ ] Add documentation and more examples

- [ ] Add RedisListSource
- [ ] Add RedisSortedSetSource

## Tests
- [ ] Unit tests for PipelineActor.task_ack
- [ ] Unit tests for PipelineInflight
- [ ] Unit tests for PipelineAggregate
- [ ] Unit tests for Pipeline DAG completion


# Tempest Project