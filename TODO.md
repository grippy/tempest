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

- [ ] Create a private git repo

- [ ] Determine how to handle:
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


- [ ] TopologyService Cli

- [ ] TaskService Cli

- [ ] Logging

- [ ] Stats
        Add stats for all io and data moving through the system.
        Ideally, we'd have some type of uber actor responsible
        for aggregating stats sent to it from each of the following

        - [ ] TopologyService
                - [ ] TopologySession
                - [ ] TopologyActor
                - [ ] SourceActor
                - [ ] PipelineActor
        - [ ] TaskService
                - [ ] TaskActor

- [ ] RedisStreamSource pending worker. This would be nice if the end-user could control how they want this to execute.
      At the minimum, the source needs a flag to initiate it.
        - [ ] The FailurePolicy would need to be Retry and we need to define a retry after millis value
        - [ ] What does claim do here? It should reclaim messages from the existing consumer
                and make them globally available again

- [ ] Deadletter Option: This is just a different "SourceBuilder" that does a push instead of a poll?

- [ ] Nice-to-have a retry for Task connections so we can test topology rebuilds

- [ ] Tempest Project
        - [ ] skeleton
        - [ ] build.rs
        - [ ] project.rs

## Tests
- [ ] Unit tests for PipelineActor.task_ack
- [ ] Unit tests for PipelineInflight
- [ ] Unit tests for PipelineAggregate
- [ ] Unit tests for Pipeline DAG completion
