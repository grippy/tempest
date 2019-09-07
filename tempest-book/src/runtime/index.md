# Runtime

Tempest has a few different runtime options available. Here's a summary of each type below.

## Production

In production, Tempest is designed to run topologies as multiple processes.

- `Topology`: Polls source messages and brokers them to `Task` processes.

- `Task`: Each topology task runs as an individual processes. You can horizontally scale task workers by launching more processes.

## Development

-  `Standalone`: This runtime will run an entire topology and all its tasks as a single, multi-threaded process. It should be used for topology development-only.

## Tests

-  `Test`: This is similar to Standalone, only you configure a `TestRun` and pass it options for how long you would like to run a topology for. After some duration, a `TestRun` will return a `TestMetrics` struct instance for wiring up assertion tests. This is really helpful if your Tasks implement their own task metrics.