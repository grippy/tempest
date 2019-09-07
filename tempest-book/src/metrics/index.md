# Metrics

Tempest has a library for naming & labeling metric `counters`, `gauges`, and `timers`.

Aggregated metrics are flushed on some interval to backend targets.

The current list of targets include:

- `Console`: Flush aggregated metris to stdout.
- `Log`: Flush aggregated metrics to a log file.
- `File`: Flush aggregated metrics to a file.
- `Statsd`: Flush aggregated metrics to a process compatible with the statsd api.
- `Prometheus`: Flush aggregated metrics to a process compatible with the prometheus api. This will auto-generate histograms from `timers`.
- `Graphite`: _Not currently implemented_.

## Task & Source Integration

`Task` and `Source` traits both provide hooks for flushing metrics. To wire up metrics, all you need to do is implement the following code.

```rust

use tempest::prelude::*;

#[derive(Default)]
struct T1 {
  // Custom metrics
  metrics: metric::Metrics
}

impl task::Task for T1 {

    fn name(&self) -> &'static str {
        "T1"
    }

    // Implement this function to wire up
    // flushing task metrics to the backend
    fn flush_metrics(&mut self) {
      self.metrics.flush();
    }

    fn handle(&mut self, msg: Msg) -> task::TaskResult {

      // Based on some message properties
      // Incr a counter with the names ["perfect", "msg"]
      if msg.len() == 42 {
        self.metrics.incr(vec!["perfect", "msg"]);
      }

      Ok(None)
    }
}

fn main() {}

```

## Runtime Configuration

Here's an example of configuring a `Topology Package` with runtime metric targets.

Add the following to `Topology.toml` configuration:

```toml
[metric]
flush_interval = 1000
target = [
  { type = "console", prefix="stdout" },
  { type = "file", path = "/tmp/tempest/metrics", prefix="file", clobber=true },
  { type = "log", level="warn", prefix = "mylog" },
  { type = "statsd", host = "0.0.0.0:9125", prefix="statsd" },
  { type = "prometheus", uri = "http://localhost:9091/metrics/job/my-job", prefix="prom" },
]
```
