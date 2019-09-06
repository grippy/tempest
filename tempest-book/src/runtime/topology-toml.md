# Topology.toml

If passed at runtime, Topology packages will parse `Topology.toml` configuration files and apply overrides.

Here's a breakdown of how this file is structured:

```toml
# Topology.toml example

# The name of our topology (this will override the name declared in the package)
name = "NameGoesHere"

# host address to listen on
host = "0.0.0.0"

# port address to listen on
port = "5555"

# if shutting down topology, how long should we wait before a hard kill?
graceful_shutdown = 10000

# Enable metric targets (optional)
[metric]

# how often should we flush metrics to configured targets?
flush_interval = 5000

# a list of enabled metric targets
target = [
  { type = "console", prefix="stdout" },
  { type = "log", level="warn", prefix = "mylog" },
  { type = "prometheus", uri = "http://localhost:9091/metrics/job/hello_world2" prefix="prom" },
  { type = "statsd", host = "0.0.0.0:9125", prefix="statsd" },
  { type = "file", path = "/tmp/tempest/metrics", prefix="file", clobber=true },
]

# This section is where you configure source configuration
# (if applicable). Consult source documentation
# for example.
[source.config]

# Configure task overrides here
# Note: the task name must match the compiled task names
# defined in src or Tempest won't know how to configure which tasks
# to apply these override too.
[[task]]

# task name to apply overrides
name = "T1"

# the number of task messages to read each
# time a topology is polled.
poll_count = 100

# how often should a task check for new messages?
poll_interval = 1

# max time to delay poll interval
# if backoff in place
max_backoff = 30000

# nothing to override for this task
[[task]]
name = "T2"
```
