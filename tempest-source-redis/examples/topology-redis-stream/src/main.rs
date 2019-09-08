use tempest::prelude::*;
use tempest_source_redis::prelude::*;

use pretty_env_logger;

static TOPOLOGY_NAME: &'static str = "TopologyExample";
static T1: &'static str = "T1";
static T2: &'static str = "T2";
static T3: &'static str = "T3";
static T4: &'static str = "T4";

// Define a source builder type
type Source = RedisStreamSourceBuilder<'static>;

struct TopologyExample {}

impl Topology<Source> for TopologyExample {
    fn builder() -> TopologyBuilder<Source> {
        // Re-use test_builder for this example...
        TopologyExample::test_builder()
            .metric_target(MetricTarget::console(Some("console-prefix".into())))
    }

    fn test_builder() -> TopologyBuilder<Source> {
        TopologyBuilder::default()
            .name(TOPOLOGY_NAME)
            .pipeline(
                Pipeline::default()
                    .task(T1::default())
                    .task(T2::default())
                    .task(T3::default())
                    .task(T4::default())
                    .edge(T1, T2)
                    .edge(T1, T3)
                    .edge(T2, T4)
                    .edge(T3, T4),
            )
            .source(
                Source::default()
                    .uri("redis://127.0.0.1/0")
                    .key("mystream")
                    .group("super")
                    .prime(move || -> Vec<RedisStreamPrime> {
                        let mut msgs: Vec<RedisStreamPrime> = Vec::new();
                        for i in 0..1000 {
                            msgs.push(RedisStreamPrime::Msg(
                                "*".into(),
                                vec![("k".into(), "v".into()), ("i".into(), i.to_string())],
                            ));
                        }
                        msgs
                    }),
            )
    }
}

pub struct T1 {
    metrics: metric::Metrics,
    seen: usize,
}

impl Default for T1 {
    fn default() -> Self {
        T1 {
            metrics: metric::Metrics::default(),
            seen: 0,
        }
    }
}

impl task::Task for T1 {
    fn name(&self) -> &'static str {
        T1
    }

    fn handle(&mut self, _msg: Msg) -> task::TaskResult {
        // `handle` metrics are automatically recorded
        // here's an example of wiring up a
        // custom metric counter for this task
        if self.seen % 2 == 0 {
            self.metrics.incr(vec!["even"]);
        } else {
            self.metrics.incr(vec!["odd"]);
        }
        self.seen += 1;
        Ok(Some(vec![vec![1]]))
    }

    fn flush_metrics(&mut self) {
        self.metrics.flush();
    }
}

#[derive(Default)]
pub struct T2 {}
impl task::Task for T2 {
    fn name(&self) -> &'static str {
        T2
    }

    fn handle(&mut self, _msg: Msg) -> task::TaskResult {
        Ok(Some(vec![vec![1], vec![2]]))
    }
}

#[derive(Default)]
pub struct T3 {}
impl task::Task for T3 {
    fn name(&self) -> &'static str {
        T3
    }

    fn handle(&mut self, _msg: Msg) -> task::TaskResult {
        Ok(Some(vec![vec![1], vec![2], vec![3]]))
    }
}

#[derive(Default)]
pub struct T4 {}
impl task::Task for T4 {
    fn name(&self) -> &'static str {
        T4
    }

    fn handle(&mut self, _msg: Msg) -> task::TaskResult {
        Ok(Some(vec![vec![1], vec![2], vec![3], vec![4]]))
    }
}

fn main() {
    pretty_env_logger::init();
    rt::run(TopologyExample::builder);
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempest::rt::test;

    fn setup() {
        pretty_env_logger::init();
    }

    #[test]
    fn test_my_topology_1() {
        setup();

        let metrics = test::TestRun::new(TopologyExample::test_builder)
            .duration_secs(10)
            .graceful_shutdown_secs(5)
            .run();

        assert_eq!(
            metrics.get("TopologyExample.source.msg.read"),
            Some(&1000isize)
        );
        assert_eq!(
            metrics.get("TopologyExample.source.msg.acked.success"),
            Some(&1000isize)
        );
        assert_eq!(
            metrics.get("TopologyExample.T1.task.msg.outflow"),
            Some(&1000isize)
        );

        // Task metrics
        assert_eq!(metrics.get("TopologyExample.T1.even"), Some(&500isize));
        assert_eq!(metrics.get("TopologyExample.T1.odd"), Some(&500isize));

        assert_eq!(
            metrics.get("TopologyExample.T2.task.msg.outflow"),
            Some(&2000isize)
        );
        assert_eq!(
            metrics.get("TopologyExample.T3.task.msg.outflow"),
            Some(&3000isize)
        );
        assert_eq!(
            metrics.get("TopologyExample.T4.task.msg.outflow"),
            Some(&20000isize)
        );
    }
}
