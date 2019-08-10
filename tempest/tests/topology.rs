use tempest::prelude::*;
use tempest_source_mock::prelude::*;

pub fn setup() {
    pretty_env_logger::init();
}

pub mod simple {

    use super::*;
    static TOPOLOGY_NAME: &'static str = "SimpleTopology";
    static T1: &'static str = "T1";

    type Source = MockSourceBuilder;

    pub struct SimpleTopology;

    impl Topology<Source> for SimpleTopology {
        fn builder() -> TopologyBuilder<Source> {
            TopologyBuilder::default()
                .name(TOPOLOGY_NAME)
                .pipeline(Pipeline::default().task(T1::default()))
                .source(Source::default().read_msg_count(10))
        }

        fn test_builder() -> TopologyBuilder<Source> {
            TopologyBuilder::default()
                .name(TOPOLOGY_NAME)
                .pipeline(Pipeline::default().task(T1::default()))
                .source(Source::default().read_msg_count(100).prime(|mock| {
                    for i in 0u32..1000u32 {
                        let msg = SourceMsg {
                            id: i.to_string().as_bytes().to_vec(),
                            msg: format!("{}", i).as_bytes().to_vec(),
                            ts: now_millis(),
                            delivered: 0,
                        };
                        mock.queue.push_back(msg);
                    }
                }))
        }
    }

    #[derive(Default, Debug)]
    pub struct T1 {
        counter: usize,
    }
    impl task::Task for T1 {
        fn name(&self) -> &'static str {
            T1
        }

        fn handle(&mut self, _msg: Msg) -> task::TaskResult {
            // println!("T1 counter {}", self.counter);
            // self.counter += 1;
            Ok(Some(vec![vec![1]]))
            // Ok(None)
        }
    }

}
